use std::time::Duration;

use anyhow::{bail, Context};
use tracing::warn;

use crate::resp::RespData;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushDirection {
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
    ListPush(String, RespData, PushDirection),
}

impl TryFrom<RespData> for Command {
    type Error = anyhow::Error;

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
                if let (Some(RespData::BulkString(Some(key))), Some(value)) =
                    (elements.get(1), elements.get(2))
                {
                    let direction = if command == "RPUSH" {
                        PushDirection::Right
                    } else {
                        PushDirection::Left
                    };
                    Ok(Command::ListPush(
                        String::from_utf8_lossy(key).to_string(),
                        value.clone(),
                        direction,
                    ))
                } else {
                    bail!("RPUSH command requires a key argument and a value");
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
