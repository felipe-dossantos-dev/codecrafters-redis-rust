const SYMBOL_SIMPLE_STRING: char = '+';
const SYMBOL_ERROR: char = '-';
const SYMBOL_INTEGER: char = ':';
const SYMBOL_BULK_STRING: char = '$';
const SYMBOL_ARRAY: char = '*';
const SYMBOL_NULL: char = '_';
const SYMBOL_END_COMMAND: &'static str = "\r\n";

/// Representa um valor no Redis Serialization Protocol (RESP).
/// `#[derive(Debug, PartialEq)]` nos permite imprimir para depuração e comparar valores nos testes.
#[derive(Debug, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Vec<u8>),
    Array(Vec<RespValue>),
    Null,
}

impl RespValue {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => {
                format!("{}{}{}", SYMBOL_SIMPLE_STRING, s, SYMBOL_END_COMMAND).into_bytes()
            }
            RespValue::Error(s) => {
                format!("{}{}{}", SYMBOL_ERROR, s, SYMBOL_END_COMMAND).into_bytes()
            }
            RespValue::Integer(s) => {
                format!("{}{}{}", SYMBOL_INTEGER, s, SYMBOL_END_COMMAND).into_bytes()
            }
            RespValue::BulkString(s) => {
                let mut result =
                    format!("{}{}{}", SYMBOL_BULK_STRING, s.len(), SYMBOL_END_COMMAND).into_bytes();
                result.extend_from_slice(s);
                result.extend_from_slice(SYMBOL_END_COMMAND.as_bytes());
                result
            }
            RespValue::Array(arr) => {
                let mut result =
                    format!("{}{}{}", SYMBOL_ARRAY, arr.len(), SYMBOL_END_COMMAND).into_bytes();
                for elem in arr {
                    result.extend(elem.serialize());
                }
                result
            }
            _ => format!("{}{}", SYMBOL_NULL, SYMBOL_END_COMMAND).into_bytes(),
        }
    }

    pub fn parse(values: Vec<u8>) -> Vec<RespValue> {
        let mut values_iter = values.into_iter();
        let mut results: Vec<RespValue> = Vec::new();

        while !values_iter.as_slice().is_empty() {
            _parse(&mut values_iter, &mut results);
        }
        return results;
    }

    pub fn bulk_string(value: &str) -> RespValue {
        return RespValue::BulkString(String::from(value).into_bytes());
    }

    pub fn simple_string(value: &str) -> RespValue {
        return RespValue::SimpleString(String::from(value));
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

fn _parse(values_iter: &mut std::vec::IntoIter<u8>, results: &mut Vec<RespValue>) {
    let first_byte = values_iter.next().unwrap();
    match first_byte as char {
        SYMBOL_SIMPLE_STRING => {
            let content_bytes = read_next_word(values_iter);
            let content = String::from_utf8(content_bytes).unwrap();
            results.push(RespValue::SimpleString(content));
        }
        SYMBOL_ERROR => {
            let content_bytes = read_next_word(values_iter);
            let content = String::from_utf8(content_bytes).unwrap();
            results.push(RespValue::Error(content));
        }
        SYMBOL_INTEGER => {
            let content_bytes = read_next_word(values_iter);
            let content_str = String::from_utf8(content_bytes).unwrap();
            let number: i64 = content_str.parse().unwrap();
            results.push(RespValue::Integer(number));
        }
        SYMBOL_NULL => {
            read_next_word(values_iter);
            results.push(RespValue::Null);
        }
        SYMBOL_BULK_STRING => {
            let length_bytes = read_next_word(values_iter);
            let length_str = String::from_utf8(length_bytes).unwrap();
            let length: i64 = length_str.parse().unwrap();
            if length == -1 {
                results.push(RespValue::Null);
            } else {
                let content_bytes = read_next_word(values_iter);
                results.push(RespValue::BulkString(content_bytes));
            }
        }
        SYMBOL_ARRAY => {
            let length_bytes = read_next_word(values_iter);
            let length_str = String::from_utf8(length_bytes).unwrap();
            let length: i64 = length_str.parse().unwrap();

            if length == -1 {
                results.push(RespValue::Null);
            } else {
                let mut current_array_elements: Vec<RespValue> = Vec::new();
                for _ in 0..length {
                    _parse(values_iter, &mut current_array_elements);
                }
                results.push(RespValue::Array(current_array_elements));
            }
        }
        _ => (),
    }
}

#[derive(Debug, PartialEq)]
pub enum RedisCommand {
    Get(String),
    Set(String, String),
    Ping,
    Echo(String),
}

impl RedisCommand {
    pub fn build(values: Vec<RespValue>) -> Vec<RedisCommand> {
        let mut commands: Vec<RedisCommand> = Vec::new();
        let mut values_iter = values.iter();
        for value in values_iter {
            match value {
                // TODO - implementar pensando no pipeline
                // TODO - implementar ECHO
                // TODO - implementar GET
                // TODO - implementar SET
                RespValue::SimpleString(s) => {
                    check_ping_command(&mut commands, s);
                },
                RespValue::BulkString(items) => {
                    let value_str = String::from_utf8(items.clone()).unwrap();
                    check_ping_command(&mut commands, &value_str);
                },
                RespValue::Integer(_) => todo!(),
                RespValue::Array(resp_values) => todo!(),
                _ => (),
            }
        }
        return commands;
    }
}

fn check_ping_command(commands: &mut Vec<RedisCommand>, value: &String) {
    if value.eq_ignore_ascii_case("ping") {
        commands.push(RedisCommand::Ping);
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
        let result = RespValue::SimpleString(String::from("hey")).serialize();
        let expected = String::from("+hey\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_serialize_error() {
        let result = RespValue::Error(String::from("hey")).serialize();
        let expected = String::from("-hey\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_serialize_integer() {
        let result = RespValue::Integer(128).serialize();
        let expected = String::from(":128\r\n").into_bytes();
        assert_eq!(result, expected)
    }

    #[test]
    fn test_serialize_bulk_string() {
        let result = RespValue::BulkString(b"foobar".to_vec()).serialize();
        let expected = b"$6\r\nfoobar\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_empty_bulk_string() {
        let result = RespValue::BulkString(b"".to_vec()).serialize();
        let expected = b"$0\r\n\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_null() {
        let result = RespValue::Null.serialize();
        let expected = b"_\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_serialize_empty_array() {
        let result = RespValue::Array(vec![
            RespValue::Integer(128),
            RespValue::BulkString(b"foobar".to_vec()),
        ])
        .serialize();
        let expected = b"*2\r\n:128\r\n$6\r\nfoobar\r\n".to_vec();
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_null() {
        let result = RespValue::parse(b"_\r\n".to_vec());
        assert_eq!(result, vec![RespValue::Null])
    }

    #[test]
    fn test_parse_simple_string() {
        let result = RespValue::parse(b"+hello\r\n".to_vec());
        assert_eq!(result, vec![RespValue::SimpleString(String::from("hello"))])
    }

    #[test]
    fn test_parse_error() {
        let result = RespValue::parse(b"-hello\r\n".to_vec());
        assert_eq!(result, vec![RespValue::Error(String::from("hello"))])
    }

    #[test]
    fn test_parse_integer() {
        let result = RespValue::parse(b":123\r\n".to_vec());
        assert_eq!(result, vec![RespValue::Integer(123)])
    }

    #[test]
    fn test_parse_bulk_string() {
        let result = RespValue::parse(b"$6\r\nfoobar\r\n".to_vec());
        let expected = vec![RespValue::BulkString(b"foobar".to_vec())];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let result = RespValue::parse(b"$0\r\n\r\n".to_vec());
        let expected = vec![RespValue::BulkString(b"".to_vec())];
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let result = RespValue::parse(b"$-1\r\n".to_vec());
        assert_eq!(result, vec![RespValue::Null]);
    }

    #[test]
    fn test_parse_null_array() {
        let result = RespValue::parse(b"*-1\r\n".to_vec());
        assert_eq!(result, vec![RespValue::Null]);
    }

    #[test]
    fn test_parse_array() {
        let result = RespValue::parse(b"*1\r\n$6\r\nfoobar\r\n".to_vec());
        assert_eq!(
            result,
            vec![RespValue::Array(vec![RespValue::BulkString(
                b"foobar".to_vec()
            )])]
        );
    }

    #[test]
    fn test_commands_build_ping() {
        let result = RedisCommand::build(vec![RespValue::bulk_string("ping"), RespValue::simple_string("ping")]);
        assert_eq!(result, vec![RedisCommand::Ping, RedisCommand::Ping]);
    }
}
