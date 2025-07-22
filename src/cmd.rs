use anyhow::bail;

use crate::resp::RespData;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(String),
    Set(String, String),
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
                        Ok(Command::Set(
                            String::from_utf8_lossy(key).to_string(),
                            String::from_utf8_lossy(value).to_string(),
                        ))
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
