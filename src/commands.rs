use std::{
    time::{SystemTime, UNIX_EPOCH},
    vec::IntoIter,
};

use crate::types::RedisType;

#[derive(Debug, PartialEq)]
pub struct RedisKeyValue {
    value: String,
    expired_at_millis: Option<u128>,
}

impl RedisKeyValue {
    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn is_expired(&self) -> bool {
        if let Some(val) = self.expired_at_millis {
            return val <= now_millis();
        }
        false
    }

    pub fn parse(mut args: IntoIter<RedisType>) -> Option<Self> {
        let mut expired_at_millis: Option<u128> = None;

        if let Some(value) = args.next() {
            while let Some(prop_name) = args.next() {
                match prop_name.to_string().to_ascii_uppercase().as_str() {
                    "PX" => {
                        if let Some(ttl_arg) = args.next() {
                            let ttl_value: u128 = ttl_arg
                                .to_string()
                                .parse()
                                .expect("Cannot parse the integer value");
                            expired_at_millis = Some(now_millis() + ttl_value);
                        }
                    }
                    _ => (),
                }
            }
            return Some(Self {
                value: value.to_string(),
                expired_at_millis: expired_at_millis,
            });
        }
        return None;
    }
}

#[derive(Debug, PartialEq)]
pub enum RedisCommand {
    GET(RedisType),
    SET(RedisType, RedisKeyValue),
    PING,
    ECHO(RedisType),
    RPUSH(RedisType, Vec<String>),
    LRANGE(RedisType, RedisType, RedisType)
}

impl RedisCommand {
    pub fn build(values: Vec<RedisType>) -> Vec<RedisCommand> {
        let mut commands: Vec<RedisCommand> = Vec::new();

        for value in values {
            match value {
                RedisType::Array(arr) => {
                    // https://redis.io/docs/latest/develop/reference/protocol-spec/#sending-commands-to-a-redis-server
                    // o client até pode mandar outros tipos, mas o que o protocolo entende como entrada de comandos
                    // é somente um array de bulk strings
                    if arr.is_empty() {
                        continue;
                    }
                    let mut args = arr.into_iter();
                    if let Some(RedisType::BulkString(command_bytes)) = args.next() {
                        if let Ok(command_name) = String::from_utf8(command_bytes) {
                            match command_name.to_ascii_uppercase().as_str() {
                                "PING" => {
                                    commands.push(RedisCommand::PING);
                                }
                                "ECHO" => {
                                    if let Some(s) = args.next() {
                                        commands.push(RedisCommand::ECHO(s));
                                    }
                                }
                                "GET" => {
                                    if let Some(s) = args.next() {
                                        commands.push(RedisCommand::GET(s));
                                    }
                                }
                                "SET" => {
                                    if let Some(key) = args.next() {
                                        let key_value = RedisKeyValue::parse(args)
                                            .expect("Erro ao fazer parse do valor e das configs");
                                        commands.push(RedisCommand::SET(key, key_value));
                                    }
                                }
                                "RPUSH" => {
                                    let mut list: Vec<String> = Vec::new();
                                    if let Some(key) = args.next() {
                                        while let Some(value) = args.next() {
                                            list.push(value.to_string());
                                        }
                                        commands.push(RedisCommand::RPUSH(key, list))
                                    }
                                },
                                "LRANGE" => {
                                    if let (Some(key), Some(start), Some(end)) = (args.next(), args.next(), args.next()) {
                                        commands.push(RedisCommand::LRANGE(key, start, end));
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                RedisType::BulkString(bytes) if bytes.eq_ignore_ascii_case(b"PING") => {
                    commands.push(RedisCommand::PING);
                }
                RedisType::SimpleString(s) if s.eq_ignore_ascii_case("PING") => {
                    commands.push(RedisCommand::PING);
                }
                _ => (),
            }
        }
        commands
    }

    pub fn parse(values: Vec<u8>) -> Vec<RedisCommand> {
        let received_values = RedisType::parse(values);
        println!("Received values: {:?}", received_values);

        let received_commands = Self::build(received_values);
        println!("Received commands: {:?}", received_commands);

        return received_commands;
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Problem with time!")
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_commands_build_ping() {
        let result = RedisCommand::build(vec![
            RedisType::bulk_string("ping"),
            RedisType::simple_string("ping"),
        ]);
        assert_eq!(result, vec![RedisCommand::PING, RedisCommand::PING]);
    }

    #[test]
    fn test_commands_build_echo() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["echo", "teste teste"])]);
        assert_eq!(
            result,
            vec![RedisCommand::ECHO(RedisType::bulk_string("teste teste"))]
        );
    }

    #[test]
    fn test_commands_build_get() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["get", "test"])]);
        assert_eq!(
            result,
            vec![RedisCommand::GET(RedisType::bulk_string("test"))]
        );
    }

    #[test]
    fn test_commands_build_set() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["set", "test", "test"])]);
        assert_eq!(
            result,
            vec![RedisCommand::SET(
                RedisType::bulk_string("test"),
                RedisKeyValue {
                    value: "test".to_string(),
                    expired_at_millis: None
                }
            )]
        );
    }

    #[test]
    fn test_commands_build_set_with_px() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "set", "key", "value", "PX", "100",
        ])]);
        assert_eq!(result.len(), 1);
        let command = &result[0];
        if let RedisCommand::SET(key, key_value) = command {
            assert_eq!(*key, RedisType::bulk_string("key"));
            assert_eq!(key_value.value(), "value");
            assert!(key_value.expired_at_millis.is_some());
        } else {
            panic!("Expected RedisCommand::Set, but got {:?}", command);
        }
    }

    #[test]
    fn test_commands_build_set_with_px_case_insensitive() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "set", "key", "value", "pX", "100",
        ])]);
        assert_eq!(result.len(), 1);
        let command = &result[0];
        if let RedisCommand::SET(key, key_value) = command {
            assert_eq!(*key, RedisType::bulk_string("key"));
            assert_eq!(key_value.value(), "value");
            assert!(
                key_value.expired_at_millis.is_some(),
                "PX option should be case-insensitive"
            );
        } else {
            panic!("Expected RedisCommand::Set, but got {:?}", command);
        }
    }

    #[test]
    fn test_commands_build_rpush() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "rpush", "mylist", "one", "two",
        ])]);
        assert_eq!(
            result,
            vec![RedisCommand::RPUSH(
                RedisType::bulk_string("mylist"),
                vec!["one".to_string(), "two".to_string()]
            )]
        );
    }
}
