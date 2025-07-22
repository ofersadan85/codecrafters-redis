use std::time::Duration;

use anyhow::{bail, Context};
use tracing::warn;

use crate::resp::RespData;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
        expires: Option<Duration>, // Optional expiration duration
        args: Vec<String>,         // Additional arguments if needed
    },
    Get(String),
}

impl TryFrom<RespData> for Command {
    type Error = anyhow::Error;

    fn try_from(value: RespData) -> Result<Self, Self::Error> {
        match value {
            RespData::Array(Some(elements)) if !elements.is_empty() => match &elements[0] {
                RespData::BulkString(Some(cmd)) if cmd == b"PING" => Ok(Command::Ping),
                RespData::BulkString(Some(cmd)) if cmd == b"ECHO" => {
                    if let Some(RespData::BulkString(Some(arg))) = elements.get(1) {
                        Ok(Command::Echo(String::from_utf8_lossy(arg).to_string()))
                    } else {
                        bail!("ECHO command requires a bulk string argument");
                    }
                }
                RespData::BulkString(Some(cmd)) if cmd == b"SET" => {
                    if elements.len() < 3 {
                        bail!("SET command requires a key and value");
                    }
                    if let (
                        Some(RespData::BulkString(Some(key))),
                        Some(RespData::BulkString(Some(value))),
                    ) = (elements.get(1), elements.get(2))
                    {
                        let args: Vec<String> = elements[3..]
                            .iter()
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
                            value: String::from_utf8_lossy(value).to_string(),
                            expires,
                            args,
                        })
                    } else {
                        bail!("SET command requires bulk string arguments for key and value");
                    }
                }
                RespData::BulkString(Some(cmd)) if cmd == b"GET" => {
                    if let Some(RespData::BulkString(Some(key))) = elements.get(1) {
                        Ok(Command::Get(String::from_utf8_lossy(key).to_string()))
                    } else {
                        bail!("GET command requires a key argument");
                    }
                }
                _ => bail!("Unsupported command"),
            },
            _ => bail!("Invalid command format"),
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
