use std::{collections::VecDeque, time::Duration};

use anyhow::{bail, ensure, Context};
use tokio::time::sleep;
use tracing::{debug, warn};

use crate::{resp::RespData, KeyValueStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushPopDirection {
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: RespData,
        expires: Option<Duration>, // Optional expiration duration
        args: Vec<String>,         // Additional arguments if needed
    },
    Get(String),
    ListPush {
        key: String,
        values: Vec<RespData>,
        direction: PushPopDirection,
    },
    ListRange {
        key: String,
        start: i64,
        end: i64,
    },
    ListLen(String),
    ListPop {
        key: String,
        count: u32,
        direction: PushPopDirection,
    },
}

impl TryFrom<RespData> for Command {
    type Error = anyhow::Error;

    #[allow(clippy::too_many_lines)]
    fn try_from(value: RespData) -> Result<Self, Self::Error> {
        let elements = match value {
            RespData::Array(Some(elements)) if !elements.is_empty() => elements,
            _ => bail!(
                "Expected a non-empty array for command parsing, got {:?}",
                value
            ),
        };
        let command = match &elements[0] {
            RespData::BulkString(Some(cmd)) => {
                String::from_utf8_lossy(cmd).to_uppercase().to_string()
            }
            _ => bail!("Expected a bulk string command, got {:?}", elements[0]),
        };
        match command.as_str() {
            "PING" => Ok(Command::Ping),
            "ECHO" => {
                if let Some(RespData::BulkString(Some(arg))) = elements.get(1) {
                    let arg = String::from_utf8_lossy(arg).to_string();
                    Ok(Command::Echo(arg))
                } else {
                    bail!("ECHO command requires a bulk string argument");
                }
            }
            "SET" => {
                if let (Some(RespData::BulkString(Some(key))), Some(value)) =
                    (elements.get(1), elements.get(2))
                {
                    let args: Vec<String> = elements
                        .iter()
                        .skip(3)
                        .filter_map(|arg| {
                            if let RespData::BulkString(Some(arg)) = arg {
                                Some(String::from_utf8_lossy(arg).to_string())
                            } else {
                                None
                            }
                        })
                        .collect();
                    let px = args.iter().position(|s| s.to_uppercase() == "PX");
                    let expires = if let Some(px_index) = px {
                        if px_index + 1 < args.len() {
                            let millis: u64 = args[px_index + 1]
                                .parse()
                                .context("Failed to parse expiration duration")?;
                            Some(Duration::from_millis(millis))
                        } else {
                            warn!("PX argument requires a value");
                            None
                        }
                    } else {
                        None
                    };
                    Ok(Command::Set {
                        key: String::from_utf8_lossy(key).to_string(),
                        value: value.clone(),
                        expires,
                        args,
                    })
                } else {
                    bail!("SET command requires bulk string arguments for key and value");
                }
            }
            "GET" => {
                if let Some(RespData::BulkString(Some(key))) = elements.get(1) {
                    Ok(Command::Get(String::from_utf8_lossy(key).to_string()))
                } else {
                    bail!("GET command requires a key argument");
                }
            }
            "RPUSH" | "LPUSH" => {
                if let Some(RespData::BulkString(Some(key))) = elements.get(1) {
                    let direction = if command == "RPUSH" {
                        PushPopDirection::Right
                    } else {
                        PushPopDirection::Left
                    };
                    Ok(Command::ListPush {
                        key: String::from_utf8_lossy(key).to_string(),
                        values: elements.iter().skip(2).cloned().collect(),
                        direction,
                    })
                } else {
                    bail!("RPUSH command requires a key argument and a value");
                }
            }
            "LRANGE" => {
                if let Some(RespData::BulkString(Some(key))) = elements.get(1) {
                    let indexes: Vec<i64> = elements
                        .iter()
                        .skip(2)
                        .filter_map(|arg| match arg {
                            RespData::Integer(index) => Some(*index),
                            RespData::SimpleString(index_str) => index_str.parse::<i64>().ok(),
                            RespData::BulkString(Some(index_str)) => {
                                String::from_utf8_lossy(index_str).parse::<i64>().ok()
                            }
                            _ => None,
                        })
                        .take(2)
                        .collect();
                    ensure!(
                        indexes.len() == 2,
                        "LRANGE command requires two integer indexes"
                    );
                    Ok(Command::ListRange {
                        key: String::from_utf8_lossy(key).to_string(),
                        start: indexes[0],
                        end: indexes[1],
                    })
                } else {
                    bail!("LRANGE command requires a key argument and two integer indexes");
                }
            }
            "LLEN" => {
                if let Some(RespData::BulkString(Some(key))) = elements.get(1) {
                    Ok(Command::ListLen(String::from_utf8_lossy(key).to_string()))
                } else {
                    bail!("LLEN command requires a key argument");
                }
            }
            "LPOP" | "RPOP" => {
                if let Some(RespData::BulkString(Some(key))) = elements.get(1) {
                    let direction = if command == "RPOP" {
                        PushPopDirection::Right
                    } else {
                        PushPopDirection::Left
                    };
                    let count = match elements.get(2) {
                        Some(RespData::Integer(count)) => u32::try_from(*count)?,
                        Some(RespData::SimpleString(count_str)) => count_str.parse::<u32>()?,
                        Some(RespData::BulkString(Some(count_str))) => {
                            String::from_utf8_lossy(count_str).parse::<u32>()?
                        }
                        _ => 1, // Default to popping one element if no count is specified
                    };
                    Ok(Command::ListPop {
                        key: String::from_utf8_lossy(key).to_string(),
                        count,
                        direction,
                    })
                } else {
                    bail!("LPOP/RPOP command requires a key argument");
                }
            }
            _ => bail!("Unsupported command"),
        }
    }
}

impl TryFrom<&[u8]> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let resp_data = RespData::try_from(value)?;
        Command::try_from(resp_data)
    }
}

impl TryFrom<&mut &[u8]> for Command {
    type Error = anyhow::Error;

    fn try_from(value: &mut &[u8]) -> Result<Self, Self::Error> {
        let resp_data = RespData::try_from(value)?;
        Command::try_from(resp_data)
    }
}

async fn expire_key(kv: KeyValueStore, key: String, duration: Duration) {
    sleep(duration).await;
    kv.lock().await.remove(&key);
}

impl Command {
    #[allow(clippy::too_many_lines)]
    pub async fn handle(self, kv: KeyValueStore) -> anyhow::Result<RespData> {
        let response = match self {
            Command::Ping => RespData::simple_string("PONG"),
            Command::Echo(arg) => RespData::bulk_string(&arg),
            Command::Set {
                key,
                value,
                expires,
                args: _args,
            } => {
                debug!("Setting `{key}` to `{value}`");
                kv.lock().await.insert(key.clone(), value);
                if let Some(expires) = expires {
                    tokio::spawn(expire_key(kv.clone(), key, expires));
                }
                RespData::simple_string("OK")
            }
            Command::Get(key) => {
                debug!("Getting value for key: {}", key);
                let store = kv.lock().await;
                if let Some(value) = store.get(&key) {
                    value.clone()
                } else {
                    RespData::null_bulk_string()
                }
            }
            Command::ListPush {
                key,
                values,
                direction,
            } => {
                let mut store = kv.lock().await;
                let array = store
                    .entry(key)
                    .or_insert_with(|| RespData::Array(Some(VecDeque::new())));
                let len = match (array, direction) {
                    (RespData::Array(Some(elements)), PushPopDirection::Right) => {
                        elements.extend(values);
                        elements.len()
                    }
                    (RespData::Array(Some(elements)), PushPopDirection::Left) => {
                        for value in values {
                            elements.push_front(value);
                        }
                        elements.len()
                    }
                    _ => unreachable!("known to be an array"),
                };
                RespData::Integer(i64::try_from(len)?)
            }
            Command::ListRange { key, start, end } => {
                debug!("Getting range for key: {}", key);
                let store = kv.lock().await;
                let response_array = if let Some(RespData::Array(Some(elements))) = store.get(&key)
                {
                    let len = i64::try_from(elements.len())?;
                    let start = if start < 0 {
                        (len + start).max(0)
                    } else if start >= len {
                        len
                    } else {
                        start
                    };
                    let end = if end < 0 {
                        (len + end).max(0)
                    } else if end >= len {
                        len - 1
                    } else {
                        end
                    };
                    elements
                        .iter()
                        .skip(usize::try_from(start)?)
                        .take(usize::try_from(end - start + 1)?)
                        .cloned()
                        .collect()
                } else {
                    VecDeque::new()
                };
                RespData::array(response_array)
            }
            Command::ListLen(key) => {
                let store = kv.lock().await;
                if let Some(RespData::Array(Some(elements))) = store.get(&key) {
                    RespData::Integer(i64::try_from(elements.len())?)
                } else {
                    RespData::Integer(0)
                }
            }
            Command::ListPop {
                key,
                count,
                direction,
            } => {
                let mut store = kv.lock().await;
                let len = if let Some(RespData::Array(Some(elements))) = store.get(&key) {
                    elements.len()
                } else {
                    0
                };
                if len == 0 {
                    // If the list is already empty, remove the key and return an empty array
                    store.remove(&key);
                    return Ok(RespData::array(VecDeque::new()));
                }
                if count == 0 {
                    // If count is 0, return an empty array
                    return Ok(RespData::array(VecDeque::new()));
                }
                if usize::try_from(count).unwrap_or(usize::MAX) > len {
                    // If count is greater or equal than the list length
                    // remove the key and return the entire list
                    let array = store
                        .remove(&key)
                        .unwrap_or(RespData::Array(Some(VecDeque::new())));
                    return Ok(array);
                }
                if count == 1 {
                    // If count is 1, pop a single element
                    // However, 1 is a special case as we return the popped value directly
                    // instead of an array
                    if let Some(RespData::Array(Some(elements))) = store.get_mut(&key) {
                        let popped_value = match direction {
                            PushPopDirection::Right => elements.pop_back(),
                            PushPopDirection::Left => elements.pop_front(),
                        };
                        return Ok(popped_value.expect("Empty list was handled above"));
                    } else {
                        return Ok(RespData::array(VecDeque::new()));
                    }
                }
                if let Some(RespData::Array(Some(elements))) = store.get_mut(&key) {
                    let mut popped_values = VecDeque::new();
                    for _ in 0..count {
                        if let Some(value) = match direction {
                            PushPopDirection::Right => elements.pop_back(),
                            PushPopDirection::Left => elements.pop_front(),
                        } {
                            popped_values.push_back(value);
                        } else {
                            break; // No more elements to pop
                        }
                    }
                    RespData::array(popped_values)
                } else {
                    RespData::array(VecDeque::new())
                }
            }
        };
        Ok(response)
    }
}
