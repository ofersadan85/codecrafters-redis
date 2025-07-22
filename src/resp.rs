use anyhow::{anyhow, bail, ensure};
use std::{collections::VecDeque, fmt::Display, str::FromStr};

const CRLF: &[u8] = b"\r\n";

#[derive(Debug, Clone)]
pub enum RespData {
    /// +OK\r\n
    SimpleString(String),
    /// -Error message\r\n
    SimpleError { kind: String, message: String },
    /// :[<+|->]<value>\r\n
    Integer(i64),
    /// $<length>\r\n<data>\r\n
    /// An empty string is represented as `BulkString(Vec::new())`
    /// An empty string is serialized as $0\r\n\r\n (length 0)
    /// While a null string is represented as `BulkString(None)`.
    /// A null string is serialized as $-1\r\n
    BulkString(Option<Vec<u8>>),
    /// *<number-of-elements>\r\n<element-1>...<element-n>
    /// An empty array is represented as `Array(Vec::new())`
    /// An empty array is serialized as *0\r\n
    /// A null array is represented as `Array(None)`.
    /// A null array is serialized as *-1\r\n
    Array(Option<VecDeque<RespData>>),
    /// _\r\n
    Null,
    /// #<t|f>\r\n
    /// #t\r\n is true & #f\r\n is false
    Boolean(bool),
    ///// ,[<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]\r\n
    //// Float(f64),
    ///// ([+|-]<number>\r\n
    //// BigNumber(i128), // TODO: This might be bigger than i128
    ///// !<length>\r\n<error>\r\n
    //// BulkError { kind: String, message: String },
    ///// =<length>\r\n<encoding>:<data>\r\n
    ///// Exactly three (3) bytes represent the data's encoding
    //// VerbatimString { encoding: String, data: Vec<u8> },
    ///// %<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
    //// Map(HashMap<RespData, RespData>),
    ///// |<number-of-entries>\r\n<key-1><value-1>...<key-n><value-n>
    //// Attributes(HashMap<RespData, RespData>),
    ///// ~<number-of-elements>\r\n<element-1>...<element-n>
    //// Set(HashSet<RespData>),
    ///// ><number-of-elements>\r\n<element-1>...<element-n>
    //// Push(Vec<RespData>),
}

fn from_lead_until_crlf(lead: char, value: &[u8]) -> anyhow::Result<&[u8]> {
    ensure!(
        value.first().is_some_and(|&b| b == lead as u8),
        "Expected item to start with {lead}"
    );
    let mut buf = &value[1..];
    for (i, w) in value[1..].windows(2).enumerate() {
        if w == CRLF {
            break;
        }
        buf = &value[1..i + 2];
    }
    if buf.len() + 1 /* for w[1] */ == value[1..].len() {
        bail!("Must end with CRLF");
    }
    Ok(buf)
}

impl RespData {
    pub fn simple_string(s: impl AsRef<str>) -> Self {
        RespData::SimpleString(s.as_ref().to_string())
    }

    pub fn bulk_string(s: impl AsRef<str>) -> Self {
        RespData::BulkString(Some(s.as_ref().as_bytes().to_vec()))
    }

    pub fn null_bulk_string() -> Self {
        RespData::BulkString(None)
    }

    pub fn array(elements: VecDeque<RespData>) -> Self {
        RespData::Array(Some(elements))
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        match self {
            RespData::SimpleString(s) => format!("+{s}\r\n").into_bytes(),
            RespData::SimpleError { kind, message } => {
                format!("-{kind} {message}\r\n").into_bytes()
            }
            RespData::Integer(num) => format!(":{num}\r\n").into_bytes(),
            RespData::BulkString(Some(s)) => {
                format!("${}\r\n{}\r\n", s.len(), String::from_utf8_lossy(s)).into_bytes()
            }
            RespData::BulkString(None) => b"$-1\r\n".to_vec(),
            RespData::Array(Some(elements)) => {
                let mut result = format!("*{}\r\n", elements.len()).into_bytes();
                for element in elements {
                    result.extend_from_slice(&element.as_bytes());
                }
                result
            }
            RespData::Array(None) => b"*-1\r\n".to_vec(),
            RespData::Null => b"_\r\n".to_vec(),
            RespData::Boolean(true) => b"#t\r\n".to_vec(),
            RespData::Boolean(false) => b"#f\r\n".to_vec(),
        }
    }

    fn parse_simple_string(value: &mut &[u8]) -> anyhow::Result<Self> {
        let buf = from_lead_until_crlf('+', value)?;
        let buf_len = buf.len();
        let s = String::from_utf8(buf.to_vec())?;
        *value = &value[1 /* Leading char */ + buf_len + CRLF.len()..];
        Ok(RespData::SimpleString(s))
    }

    fn parse_simple_error(value: &mut &[u8]) -> anyhow::Result<Self> {
        let buf = from_lead_until_crlf('-', value)?;
        let buf_len = buf.len();
        let s = String::from_utf8(buf.to_vec())?;
        *value = &value[1 /* Leading char */ + buf_len + CRLF.len()..];

        // Clippy gives a false positive here, with `map_unwrap_or` we would have to clone the string
        #[allow(clippy::map_unwrap_or)]
        let (kind, message) = s
            .split_once(' ')
            .map(|(k, m)| (k.to_string(), m.to_string()))
            .unwrap_or_else(|| (s, String::new()));
        Ok(RespData::SimpleError { kind, message })
    }

    fn parse_integer(value: &mut &[u8]) -> anyhow::Result<Self> {
        let buf = from_lead_until_crlf(':', value)?;
        let buf_len = buf.len();
        let num = String::from_utf8(buf.to_vec())?.parse()?;
        *value = &value[1 /* Leading char */ + buf_len + CRLF.len()..];
        Ok(RespData::Integer(num))
    }

    fn parse_bulk_string(value: &mut &[u8]) -> anyhow::Result<Self> {
        let buf = from_lead_until_crlf('$', value)?;
        let len = String::from_utf8(buf.to_vec())?
            .parse::<usize>()
            .map_err(|e| anyhow!("Invalid length: {e}"))?;
        ensure!(
            value.len() > len + CRLF.len(), // +1 for the leading '$'
            "Bulk string data length mismatch"
        );
        let start_of_s = 1 /* Leading char */ + buf.len() + CRLF.len();
        let s = value
            .get(start_of_s..start_of_s + len)
            .ok_or_else(|| anyhow!("Bulk string data is too short"))?;
        ensure!(
            value
                .get(start_of_s + len..)
                .is_some_and(|f| f.starts_with(CRLF)),
            "Bulk string must end with CRLF"
        );
        *value = &value[start_of_s + len + CRLF.len()..];
        Ok(RespData::BulkString(Some(s.to_vec())))
    }

    fn parse_array(value: &mut &[u8]) -> anyhow::Result<Self> {
        let buf = from_lead_until_crlf('*', value)?;
        let len_s = String::from_utf8(buf.to_vec())?;
        match len_s.as_str() {
            "0" => {
                *value = &value[1 /* Leading char */ + buf.len() + CRLF.len()..];
                Ok(RespData::Array(Some(VecDeque::new())))
            }
            "-1" => {
                *value = &value[1 /* Leading char */ + buf.len() + CRLF.len()..];
                Ok(RespData::Array(None))
            }
            len_s => {
                let len = len_s
                    .parse::<usize>()
                    .map_err(|e| anyhow!("Invalid length: {e}"))?;
                *value = &value[1 /* Leading char */ + buf.len() + CRLF.len()..];
                let mut elements = VecDeque::with_capacity(len);
                for _ in 0..len {
                    let element = Self::from_bytes(value)?;
                    elements.push_back(element);
                }
                // Note: Array doesn't end with CRLF, so we don't check for it here.
                Ok(RespData::Array(Some(elements)))
            }
        }
    }

    fn parse_null(value: &mut &[u8]) -> anyhow::Result<Self> {
        ensure!(
            value.get(..1) == Some(b"_") && value.get(1..3) == Some(CRLF),
            "Expected null as _\r\n"
        );
        *value = &value[1 + CRLF.len()..];
        Ok(RespData::Null)
    }

    fn parse_boolean(value: &mut &[u8]) -> anyhow::Result<Self> {
        let buf = from_lead_until_crlf('#', value)?;
        let bool_value = match buf {
            b"t" => true,
            b"f" => false,
            _ => bail!("Invalid boolean value: {}", String::from_utf8_lossy(buf)),
        };
        *value = &value[1 /* Leading char */ + buf.len() + CRLF.len()..];
        Ok(RespData::Boolean(bool_value))
    }

    fn from_bytes(value: &mut &[u8]) -> anyhow::Result<Self> {
        if value.len() < 3 {
            return Err(anyhow!("Invalid RESP data"));
        }
        let first_byte = value.first().expect("non empty value");
        match first_byte {
            b'+' => Self::parse_simple_string(value),
            b'-' => Self::parse_simple_error(value),
            b':' => Self::parse_integer(value),
            b'$' => Self::parse_bulk_string(value),
            b'*' => Self::parse_array(value),
            b'_' => Self::parse_null(value),
            b'#' => Self::parse_boolean(value),
            b',' => todo!("Parse float"),
            b'(' => todo!("Parse big number"),
            b'!' => todo!("Parse bulk error"),
            b'=' => todo!("Parse verbatim string"),
            b'%' => todo!("Parse map"),
            b'|' => todo!("Parse attributes"),
            b'~' => todo!("Parse set"),
            b'>' => todo!("Parse push"),
            _ => Err(anyhow!("Unknown RESP type")),
        }
    }
}

impl TryFrom<&[u8]> for RespData {
    type Error = anyhow::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let mut buf = value;
        Self::from_bytes(&mut buf)
    }
}

impl TryFrom<&mut &[u8]> for RespData {
    type Error = anyhow::Error;

    fn try_from(value: &mut &[u8]) -> Result<Self, Self::Error> {
        Self::from_bytes(value)
    }
}

impl FromStr for RespData {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = s.as_bytes();
        Self::try_from(bytes)
    }
}

impl Display for RespData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.as_bytes();
        f.write_str(&String::from_utf8_lossy(&bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let mut data = b"+OK\r\n".as_ref();
        let resp = RespData::parse_simple_string(&mut data).unwrap();
        let RespData::SimpleString(s) = resp else {
            panic!("Expected SimpleString, got {resp:?}");
        };
        assert_eq!(s, "OK");
        assert!(data.is_empty());
    }

    #[test]
    fn test_parse_simple_error() {
        let mut data = b"-Error message\r\n".as_ref();
        let resp = RespData::parse_simple_error(&mut data).unwrap();
        let RespData::SimpleError { kind, message } = resp else {
            panic!("Expected SimpleError, got {resp:?}");
        };
        assert_eq!(kind, "Error");
        assert_eq!(message, "message");
        assert!(data.is_empty());
    }

    #[test]
    fn test_parse_integer() {
        let mut data = b":42\r\n".as_ref();
        let resp = RespData::parse_integer(&mut data).unwrap();
        let RespData::Integer(num) = resp else {
            panic!("Expected Integer, got {resp:?}");
        };
        assert_eq!(num, 42);
        assert!(data.is_empty());
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut data = b"$5\r\nHello\r\n".as_ref();
        let resp = RespData::parse_bulk_string(&mut data).unwrap();
        let RespData::BulkString(Some(s)) = resp else {
            panic!("Expected BulkString, got {resp:?}");
        };
        assert_eq!(s, "Hello".as_bytes());
        assert!(data.is_empty());
    }

    #[test]
    fn test_parse_array() {
        let mut data = b"*3\r\n_\r\n_\r\n_\r\n".as_ref();
        let resp = RespData::parse_array(&mut data).unwrap();
        let RespData::Array(Some(elements)) = resp else {
            panic!("Expected Array, got {resp:?}");
        };
        assert_eq!(elements.len(), 3);
        assert!(elements.iter().all(|e| matches!(e, RespData::Null)));
    }

    #[test]
    fn test_parse_null() {
        let mut data = b"_\r\n".as_ref();
        let resp = RespData::parse_null(&mut data).unwrap();
        assert!(matches!(resp, RespData::Null));
        assert!(data.is_empty());
    }

    #[test]
    fn test_parse_boolean() {
        let mut data = b"#t\r\n".as_ref();
        let resp = RespData::parse_boolean(&mut data).unwrap();
        assert!(matches!(resp, RespData::Boolean(true)));
        assert!(data.is_empty());

        let mut data = b"#f\r\n".as_ref();
        let resp = RespData::parse_boolean(&mut data).unwrap();
        assert!(matches!(resp, RespData::Boolean(false)));
        assert!(data.is_empty());
    }
}
