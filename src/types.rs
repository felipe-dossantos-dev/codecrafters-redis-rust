const SYMBOL_SIMPLE_STRING: char = '+';
const SYMBOL_ERROR: char = '-';
const SYMBOL_INTEGER: char = ':';
const SYMBOL_BULK_STRING: char = '$';
const SYMBOL_ARRAY: char = '*';
const SYMBOL_END_COMMAND: &'static str = "\r\n";

#[derive(Debug, PartialEq)]
pub enum RedisType {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RedisType>),
    NullArray,
    Null,
}

impl RedisType {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RedisType::SimpleString(s) => {
                format!("{}{}{}", SYMBOL_SIMPLE_STRING, s, SYMBOL_END_COMMAND).into_bytes()
            }
            RedisType::Error(s) => {
                format!("{}{}{}", SYMBOL_ERROR, s, SYMBOL_END_COMMAND).into_bytes()
            }
            RedisType::Integer(s) => {
                format!("{}{}{}", SYMBOL_INTEGER, s, SYMBOL_END_COMMAND).into_bytes()
            }
            RedisType::BulkString(s) => {
                let mut result =
                    format!("{}{}{}", SYMBOL_BULK_STRING, s.len(), SYMBOL_END_COMMAND).into_bytes();
                result.extend_from_slice(s);
                result.extend_from_slice(SYMBOL_END_COMMAND.as_bytes());
                result
            }
            RedisType::Array(arr) => {
                let mut result =
                    format!("{}{}{}", SYMBOL_ARRAY, arr.len(), SYMBOL_END_COMMAND).into_bytes();
                for elem in arr {
                    result.extend(elem.serialize());
                }
                result
            }
            RedisType::NullArray => {
                format!("{}-1{}", SYMBOL_ARRAY, SYMBOL_END_COMMAND).into_bytes()
            }
            _ => format!("{}-1{}", SYMBOL_BULK_STRING, SYMBOL_END_COMMAND).into_bytes(),
        }
    }

    pub fn parse(values: Vec<u8>) -> Vec<RedisType> {
        let mut values_iter = values.into_iter();
        let mut results: Vec<RedisType> = Vec::new();

        while !values_iter.as_slice().is_empty() {
            _parse(&mut values_iter, &mut results);
        }
        return results;
    }

    pub fn bulk_string(value: &str) -> RedisType {
        return RedisType::BulkString(String::from(value).into_bytes());
    }

    pub fn simple_string(value: &str) -> RedisType {
        return RedisType::SimpleString(String::from(value));
    }

    pub fn ok() -> RedisType {
        return RedisType::simple_string("OK");
    }

    pub fn pong() -> RedisType {
        return RedisType::simple_string("PONG");
    }

    pub fn to_string(&self) -> Option<String> {
        match self {
            RedisType::SimpleString(val) => Some(val.to_string()),
            RedisType::Error(val) => Some(val.to_string()),
            RedisType::Integer(val) => Some(format!("{}", val)),
            RedisType::BulkString(val) => String::from_utf8(val.to_vec()).ok(),
            _ => None,
        }
    }

    pub fn to_int(&self) -> Option<i64> {
        match self {
            RedisType::SimpleString(val) => val.parse().ok(),
            RedisType::Error(val) => val.parse().ok(),
            RedisType::Integer(val) => Some(*val),
            RedisType::BulkString(val) => String::from_utf8(val.to_vec()).unwrap().parse().ok(),
            _ => None,
        }
    }

    pub fn to_float(&self) -> Option<f64> {
        match self {
            RedisType::SimpleString(val) => val.parse().ok(),
            RedisType::Error(val) => val.parse().ok(),
            RedisType::Integer(val) => Some(*val as f64),
            RedisType::BulkString(val) => String::from_utf8(val.to_vec()).unwrap().parse().ok(),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn new_array(values: Vec<&str>) -> RedisType {
        let bulk_string = values.iter().map(|x| Self::bulk_string(x)).collect();
        RedisType::Array(bulk_string)
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

fn _parse(values_iter: &mut std::vec::IntoIter<u8>, results: &mut Vec<RedisType>) {
    let first_byte = values_iter.next().unwrap();
    match first_byte as char {
        SYMBOL_SIMPLE_STRING => {
            let content_bytes = read_next_word(values_iter);
            let content = String::from_utf8(content_bytes).unwrap();
            results.push(RedisType::SimpleString(content));
        }
        SYMBOL_ERROR => {
            let content_bytes = read_next_word(values_iter);
            let content = String::from_utf8(content_bytes).unwrap();
            results.push(RedisType::Error(content));
        }
        SYMBOL_INTEGER => {
            let content_bytes = read_next_word(values_iter);
            let content_str = String::from_utf8(content_bytes).unwrap();
            let number: i64 = content_str.parse().unwrap();
            results.push(RedisType::Integer(number));
        }
        SYMBOL_BULK_STRING => {
            let length_bytes = read_next_word(values_iter);
            let length_str = String::from_utf8(length_bytes).unwrap();
            let length: i64 = length_str.parse().unwrap();
            if length == -1 {
                results.push(RedisType::Null);
            } else {
                let content_bytes = read_next_word(values_iter);
                results.push(RedisType::BulkString(content_bytes));
            }
        }
        SYMBOL_ARRAY => {
            let length_bytes = read_next_word(values_iter);
            let length_str = String::from_utf8(length_bytes).unwrap();
            let length: i64 = length_str.parse().unwrap();

            if length == -1 {
                results.push(RedisType::Null);
            } else {
                let mut current_array_elements: Vec<RedisType> = Vec::new();
                for _ in 0..length {
                    _parse(values_iter, &mut current_array_elements);
                }
                results.push(RedisType::Array(current_array_elements));
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
        let result = RedisType::SimpleString(String::from("hey")).serialize();
        let expected = String::from("+hey\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_serialize_error() {
        let result = RedisType::Error(String::from("hey")).serialize();
        let expected = String::from("-hey\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_serialize_integer() {
        let result = RedisType::Integer(128).serialize();
        let expected = String::from(":128\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_serialize_bulk_string() {
        let result = RedisType::BulkString(b"foobar".to_vec()).serialize();
        let expected = b"$6\r\nfoobar\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_empty_bulk_string() {
        let result = RedisType::BulkString(b"".to_vec()).serialize();
        let expected = b"$0\r\n\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_null() {
        let result = RedisType::Null.serialize();
        let expected = b"$-1\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_empty_array() {
        let result = RedisType::Array(vec![
            RedisType::Integer(128),
            RedisType::BulkString(b"foobar".to_vec()),
        ])
        .serialize();
        let expected = b"*2\r\n:128\r\n$6\r\nfoobar\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_null() {
        let result = RedisType::parse(b"$-1\r\n".to_vec());
        assert_eq!(result, vec![RedisType::Null]);
    }

    #[test]
    fn test_parse_simple_string() {
        let result = RedisType::parse(b"+hello\r\n".to_vec());
        assert_eq!(result, vec![RedisType::SimpleString(String::from("hello"))])
    }

    #[test]
    fn test_parse_error() {
        let result = RedisType::parse(b"-hello\r\n".to_vec());
        assert_eq!(result, vec![RedisType::Error(String::from("hello"))])
    }

    #[test]
    fn test_parse_integer() {
        let result = RedisType::parse(b":123\r\n".to_vec());
        assert_eq!(result, vec![RedisType::Integer(123)])
    }

    #[test]
    fn test_parse_bulk_string() {
        let result = RedisType::parse(b"$6\r\nfoobar\r\n".to_vec());
        let expected = vec![RedisType::BulkString(b"foobar".to_vec())];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let result = RedisType::parse(b"$0\r\n\r\n".to_vec());
        let expected = vec![RedisType::BulkString(b"".to_vec())];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let result = RedisType::parse(b"$-1\r\n".to_vec());
        assert_eq!(result, vec![RedisType::Null]);
    }

    #[test]
    fn test_parse_null_array() {
        let result = RedisType::parse(b"*-1\r\n".to_vec());
        assert_eq!(result, vec![RedisType::Null]);
    }

    #[test]
    fn test_parse_array() {
        let result = RedisType::parse(b"*1\r\n$6\r\nfoobar\r\n".to_vec());
        assert_eq!(
            result,
            vec![RedisType::Array(vec![RedisType::BulkString(
                b"foobar".to_vec()
            )])]
        );
    }

    #[test]
    fn test_serialize_null_array() {
        let result = RedisType::NullArray.serialize();
        let expected = b"*-1\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_multiple_commands() {
        let result = RedisType::parse(b"+OK\r\n-Error message\r\n:1000\r\n".to_vec());
        assert_eq!(
            result,
            vec![
                RedisType::SimpleString("OK".to_string()),
                RedisType::Error("Error message".to_string()),
                RedisType::Integer(1000)
            ]
        );
    }

    #[test]
    fn test_parse_nested_array() {
        let result = RedisType::parse(b"*2\r\n*1\r\n:1\r\n*2\r\n+hello\r\n-world\r\n".to_vec());
        assert_eq!(
            result,
            vec![RedisType::Array(vec![
                RedisType::Array(vec![RedisType::Integer(1)]),
                RedisType::Array(vec![
                    RedisType::SimpleString("hello".to_string()),
                    RedisType::Error("world".to_string())
                ])
            ])]
        );
    }

    #[test]
    fn test_bulk_string_helper() {
        let result = RedisType::bulk_string("hello");
        assert_eq!(result, RedisType::BulkString(b"hello".to_vec()));
    }

    #[test]
    fn test_simple_string_helper() {
        let result = RedisType::simple_string("hello");
        assert_eq!(result, RedisType::SimpleString("hello".to_string()));
    }

    #[test]
    fn test_ok_helper() {
        let result = RedisType::ok();
        assert_eq!(result, RedisType::SimpleString("OK".to_string()));
    }

    #[test]
    fn test_pong_helper() {
        let result = RedisType::pong();
        assert_eq!(result, RedisType::SimpleString("PONG".to_string()));
    }

    #[test]
    fn test_to_string() {
        assert_eq!(
            RedisType::SimpleString("hello".to_string()).to_string(),
            Some("hello".to_string())
        );
        assert_eq!(
            RedisType::Error("error".to_string()).to_string(),
            Some("error".to_string())
        );
        assert_eq!(RedisType::Integer(123).to_string(), Some("123".to_string()));
        assert_eq!(
            RedisType::BulkString(b"bulk".to_vec()).to_string(),
            Some("bulk".to_string())
        );
        assert_eq!(RedisType::Array(vec![]).to_string(), None);
        assert_eq!(RedisType::Null.to_string(), None);
        assert_eq!(RedisType::NullArray.to_string(), None);
    }

    #[test]
    fn test_to_int() {
        assert_eq!(
            RedisType::SimpleString("123".to_string()).to_int(),
            Some(123)
        );
        assert_eq!(RedisType::SimpleString("abc".to_string()).to_int(), None);
        assert_eq!(RedisType::Error("456".to_string()).to_int(), Some(456));
        assert_eq!(RedisType::Integer(789).to_int(), Some(789));
        assert_eq!(RedisType::BulkString(b"101".to_vec()).to_int(), Some(101));
        assert_eq!(RedisType::BulkString(b"xyz".to_vec()).to_int(), None);
        assert_eq!(RedisType::Array(vec![]).to_int(), None);
        assert_eq!(RedisType::Null.to_int(), None);
        assert_eq!(RedisType::NullArray.to_int(), None);
    }

    #[test]
    fn test_to_float() {
        assert_eq!(
            RedisType::SimpleString("123.45".to_string()).to_float(),
            Some(123.45)
        );
        assert_eq!(RedisType::SimpleString("abc".to_string()).to_float(), None);
        assert_eq!(
            RedisType::Error("456.78".to_string()).to_float(),
            Some(456.78)
        );
        assert_eq!(RedisType::Integer(789).to_float(), Some(789.0));
        assert_eq!(
            RedisType::BulkString(b"101.12".to_vec()).to_float(),
            Some(101.12)
        );
        assert_eq!(RedisType::BulkString(b"xyz".to_vec()).to_float(), None);
        assert_eq!(RedisType::Array(vec![]).to_float(), None);
        assert_eq!(RedisType::Null.to_float(), None);
        assert_eq!(RedisType::NullArray.to_float(), None);
    }
}
