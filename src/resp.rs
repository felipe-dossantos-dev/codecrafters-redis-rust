const SYMBOL_SIMPLE_STRING: char = '+';
const SYMBOL_ERROR: char = '-';
const SYMBOL_INTEGER: char = ':';
const SYMBOL_BULK_STRING: char = '$';
const SYMBOL_ARRAY: char = '*';
const SYMBOL_END_COMMAND: &'static str = "\r\n";

#[derive(Debug, PartialEq)]
pub enum RespDataType {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespDataType>),
    NullArray,
    Null,
}

impl RespDataType {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RespDataType::SimpleString(s) => {
                format!("{}{}{}", SYMBOL_SIMPLE_STRING, s, SYMBOL_END_COMMAND).into_bytes()
            }
            RespDataType::Error(s) => {
                format!("{}{}{}", SYMBOL_ERROR, s, SYMBOL_END_COMMAND).into_bytes()
            }
            RespDataType::Integer(s) => {
                format!("{}{}{}", SYMBOL_INTEGER, s, SYMBOL_END_COMMAND).into_bytes()
            }
            RespDataType::BulkString(s) => {
                let mut result =
                    format!("{}{}{}", SYMBOL_BULK_STRING, s.len(), SYMBOL_END_COMMAND).into_bytes();
                result.extend_from_slice(s);
                result.extend_from_slice(SYMBOL_END_COMMAND.as_bytes());
                result
            }
            RespDataType::Array(arr) => {
                let mut result =
                    format!("{}{}{}", SYMBOL_ARRAY, arr.len(), SYMBOL_END_COMMAND).into_bytes();
                for elem in arr {
                    result.extend(elem.serialize());
                }
                result
            }
            RespDataType::NullArray => {
                format!("{}-1{}", SYMBOL_ARRAY, SYMBOL_END_COMMAND).into_bytes()
            }
            _ => format!("{}-1{}", SYMBOL_BULK_STRING, SYMBOL_END_COMMAND).into_bytes(),
        }
    }

    pub fn parse(values: Vec<u8>) -> Vec<RespDataType> {
        let mut values_iter = values.into_iter();
        let mut results: Vec<RespDataType> = Vec::new();

        while !values_iter.as_slice().is_empty() {
            _parse(&mut values_iter, &mut results);
        }
        return results;
    }

    pub fn bulk_string(value: &str) -> RespDataType {
        return RespDataType::BulkString(String::from(value).into_bytes());
    }

    pub fn simple_string(value: &str) -> RespDataType {
        return RespDataType::SimpleString(String::from(value));
    }

    pub fn ok() -> RespDataType {
        return RespDataType::simple_string("OK");
    }

    pub fn pong() -> RespDataType {
        return RespDataType::simple_string("PONG");
    }

    pub fn to_string(&self) -> Option<String> {
        match self {
            RespDataType::SimpleString(val) => Some(val.to_string()),
            RespDataType::Error(val) => Some(val.to_string()),
            RespDataType::Integer(val) => Some(format!("{}", val)),
            RespDataType::BulkString(val) => String::from_utf8(val.to_vec()).ok(),
            _ => None,
        }
    }

    pub fn to_int(&self) -> Option<i64> {
        match self {
            RespDataType::SimpleString(val) => val.parse().ok(),
            RespDataType::Error(val) => val.parse().ok(),
            RespDataType::Integer(val) => Some(*val),
            RespDataType::BulkString(val) => String::from_utf8(val.to_vec()).unwrap().parse().ok(),
            _ => None,
        }
    }

    pub fn to_float(&self) -> Option<f64> {
        match self {
            RespDataType::SimpleString(val) => val.parse().ok(),
            RespDataType::Error(val) => val.parse().ok(),
            RespDataType::Integer(val) => Some(*val as f64),
            RespDataType::BulkString(val) => String::from_utf8(val.to_vec()).unwrap().parse().ok(),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn new_array(values: Vec<&str>) -> RespDataType {
        let bulk_string = values.iter().map(|x| Self::bulk_string(x)).collect();
        RespDataType::Array(bulk_string)
    }
}

fn read_next_word(iter: &mut std::vec::IntoIter<u8>) -> Vec<u8> {
    let mut word = Vec::new();
    while let Some(byte) = iter.next() {
        if byte == b'\r' {
            iter.next();
            break;
        }
        word.push(byte);
    }
    word
}

fn _parse(values_iter: &mut std::vec::IntoIter<u8>, results: &mut Vec<RespDataType>) {
    let first_byte = values_iter.next().unwrap();
    match first_byte as char {
        SYMBOL_SIMPLE_STRING => {
            let content_bytes = read_next_word(values_iter);
            let content = String::from_utf8(content_bytes).unwrap();
            results.push(RespDataType::SimpleString(content));
        }
        SYMBOL_ERROR => {
            let content_bytes = read_next_word(values_iter);
            let content = String::from_utf8(content_bytes).unwrap();
            results.push(RespDataType::Error(content));
        }
        SYMBOL_INTEGER => {
            let content_bytes = read_next_word(values_iter);
            let content_str = String::from_utf8(content_bytes).unwrap();
            let number: i64 = content_str.parse().unwrap();
            results.push(RespDataType::Integer(number));
        }
        SYMBOL_BULK_STRING => {
            let length_bytes = read_next_word(values_iter);
            let length_str = String::from_utf8(length_bytes).unwrap();
            let length: i64 = length_str.parse().unwrap();
            if length == -1 {
                results.push(RespDataType::Null);
            } else {
                let content_bytes = read_next_word(values_iter);
                results.push(RespDataType::BulkString(content_bytes));
            }
        }
        SYMBOL_ARRAY => {
            let length_bytes = read_next_word(values_iter);
            let length_str = String::from_utf8(length_bytes).unwrap();
            let length: i64 = length_str.parse().unwrap();

            if length == -1 {
                results.push(RespDataType::Null);
            } else {
                let mut current_array_elements: Vec<RespDataType> = Vec::new();
                for _ in 0..length {
                    _parse(values_iter, &mut current_array_elements);
                }
                results.push(RespDataType::Array(current_array_elements));
            }
        }
        _ => (),
    }
}

// A anotação `#[cfg(test)]` diz ao compilador para só incluir
// este código quando executamos `cargo test`.
#[cfg(test)]
mod tests {
    // `use super::*;` importa tudo do módulo pai (neste caso, Person).
    use super::*;

    #[test]
    fn test_serialize_simple_string() {
        let result = RespDataType::SimpleString(String::from("hey")).serialize();
        let expected = String::from("+hey\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_serialize_error() {
        let result = RespDataType::Error(String::from("hey")).serialize();
        let expected = String::from("-hey\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_serialize_integer() {
        let result = RespDataType::Integer(128).serialize();
        let expected = String::from(":128\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_serialize_bulk_string() {
        let result = RespDataType::BulkString(b"foobar".to_vec()).serialize();
        let expected = b"$6\r\nfoobar\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_empty_bulk_string() {
        let result = RespDataType::BulkString(b"".to_vec()).serialize();
        let expected = b"$0\r\n\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_null() {
        let result = RespDataType::Null.serialize();
        let expected = b"$-1\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_empty_array() {
        let result = RespDataType::Array(vec![
            RespDataType::Integer(128),
            RespDataType::BulkString(b"foobar".to_vec()),
        ])
        .serialize();
        let expected = b"*2\r\n:128\r\n$6\r\nfoobar\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_null() {
        let result = RespDataType::parse(b"$-1\r\n".to_vec());
        assert_eq!(result, vec![RespDataType::Null]);
    }

    #[test]
    fn test_parse_simple_string() {
        let result = RespDataType::parse(b"+hello\r\n".to_vec());
        assert_eq!(
            result,
            vec![RespDataType::SimpleString(String::from("hello"))]
        )
    }

    #[test]
    fn test_parse_error() {
        let result = RespDataType::parse(b"-hello\r\n".to_vec());
        assert_eq!(result, vec![RespDataType::Error(String::from("hello"))])
    }

    #[test]
    fn test_parse_integer() {
        let result = RespDataType::parse(b":123\r\n".to_vec());
        assert_eq!(result, vec![RespDataType::Integer(123)])
    }

    #[test]
    fn test_parse_bulk_string() {
        let result = RespDataType::parse(b"$6\r\nfoobar\r\n".to_vec());
        let expected = vec![RespDataType::BulkString(b"foobar".to_vec())];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let result = RespDataType::parse(b"$0\r\n\r\n".to_vec());
        let expected = vec![RespDataType::BulkString(b"".to_vec())];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let result = RespDataType::parse(b"$-1\r\n".to_vec());
        assert_eq!(result, vec![RespDataType::Null]);
    }

    #[test]
    fn test_parse_null_array() {
        let result = RespDataType::parse(b"*-1\r\n".to_vec());
        assert_eq!(result, vec![RespDataType::Null]);
    }

    #[test]
    fn test_parse_array() {
        let result = RespDataType::parse(b"*1\r\n$6\r\nfoobar\r\n".to_vec());
        assert_eq!(
            result,
            vec![RespDataType::Array(vec![RespDataType::BulkString(
                b"foobar".to_vec()
            )])]
        );
    }

    #[test]
    fn test_serialize_null_array() {
        let result = RespDataType::NullArray.serialize();
        let expected = b"*-1\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_multiple_commands() {
        let result = RespDataType::parse(b"+OK\r\n-Error message\r\n:1000\r\n".to_vec());
        assert_eq!(
            result,
            vec![
                RespDataType::SimpleString("OK".to_string()),
                RespDataType::Error("Error message".to_string()),
                RespDataType::Integer(1000)
            ]
        );
    }

    #[test]
    fn test_parse_nested_array() {
        let result = RespDataType::parse(b"*2\r\n*1\r\n:1\r\n*2\r\n+hello\r\n-world\r\n".to_vec());
        assert_eq!(
            result,
            vec![RespDataType::Array(vec![
                RespDataType::Array(vec![RespDataType::Integer(1)]),
                RespDataType::Array(vec![
                    RespDataType::SimpleString("hello".to_string()),
                    RespDataType::Error("world".to_string())
                ])
            ])]
        );
    }

    #[test]
    fn test_bulk_string_helper() {
        let result = RespDataType::bulk_string("hello");
        assert_eq!(result, RespDataType::BulkString(b"hello".to_vec()));
    }

    #[test]
    fn test_simple_string_helper() {
        let result = RespDataType::simple_string("hello");
        assert_eq!(result, RespDataType::SimpleString("hello".to_string()));
    }

    #[test]
    fn test_ok_helper() {
        let result = RespDataType::ok();
        assert_eq!(result, RespDataType::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_pong_helper() {
        let result = RespDataType::pong();
        assert_eq!(result, RespDataType::SimpleString("PONG".to_string()));
    }

    #[test]
    fn test_to_string() {
        assert_eq!(
            RespDataType::SimpleString("hello".to_string()).to_string(),
            Some("hello".to_string())
        );
        assert_eq!(
            RespDataType::Error("error".to_string()).to_string(),
            Some("error".to_string())
        );
        assert_eq!(
            RespDataType::Integer(123).to_string(),
            Some("123".to_string())
        );
        assert_eq!(
            RespDataType::BulkString(b"bulk".to_vec()).to_string(),
            Some("bulk".to_string())
        );
        assert_eq!(RespDataType::Array(vec![]).to_string(), None);
        assert_eq!(RespDataType::Null.to_string(), None);
        assert_eq!(RespDataType::NullArray.to_string(), None);
    }

    #[test]
    fn test_to_int() {
        assert_eq!(
            RespDataType::SimpleString("123".to_string()).to_int(),
            Some(123)
        );
        assert_eq!(RespDataType::SimpleString("abc".to_string()).to_int(), None);
        assert_eq!(RespDataType::Error("456".to_string()).to_int(), Some(456));
        assert_eq!(RespDataType::Integer(789).to_int(), Some(789));
        assert_eq!(
            RespDataType::BulkString(b"101".to_vec()).to_int(),
            Some(101)
        );
        assert_eq!(RespDataType::BulkString(b"xyz".to_vec()).to_int(), None);
        assert_eq!(RespDataType::Array(vec![]).to_int(), None);
        assert_eq!(RespDataType::Null.to_int(), None);
        assert_eq!(RespDataType::NullArray.to_int(), None);
    }

    #[test]
    fn test_to_float() {
        assert_eq!(
            RespDataType::SimpleString("123.45".to_string()).to_float(),
            Some(123.45)
        );
        assert_eq!(
            RespDataType::SimpleString("abc".to_string()).to_float(),
            None
        );
        assert_eq!(
            RespDataType::Error("456.78".to_string()).to_float(),
            Some(456.78)
        );
        assert_eq!(RespDataType::Integer(789).to_float(), Some(789.0));
        assert_eq!(
            RespDataType::BulkString(b"101.12".to_vec()).to_float(),
            Some(101.12)
        );
        assert_eq!(RespDataType::BulkString(b"xyz".to_vec()).to_float(), None);
        assert_eq!(RespDataType::Array(vec![]).to_float(), None);
        assert_eq!(RespDataType::Null.to_float(), None);
        assert_eq!(RespDataType::NullArray.to_float(), None);
    }
}
