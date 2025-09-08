use std::{
    time::{SystemTime, UNIX_EPOCH},
    vec::IntoIter,
};

use crate::types::RedisType;

#[derive(Debug, PartialEq)]
pub struct RedisKeyValue {
    pub value: String,
    expired_at_millis: Option<u128>,
}

impl RedisKeyValue {
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
    GET(String),
    SET(String, RedisKeyValue),
    PING,
    ECHO(String),
    RPUSH(String, Vec<String>),
    LPUSH(String, Vec<String>),
    LRANGE(String, i64, i64),
    LLEN(String),
    LPOP(String, i64),
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
                                        match s {
                                            RedisType::BulkString(bs) => {
                                                commands.push(RedisCommand::ECHO(
                                                    String::from_utf8(bs).unwrap(),
                                                ));
                                            }
                                            RedisType::SimpleString(ss) => {
                                                commands.push(RedisCommand::ECHO(ss));
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                "GET" => {
                                    if let Some(s) = args.next() {
                                        match s {
                                            RedisType::BulkString(bs) => {
                                                commands.push(RedisCommand::GET(
                                                    String::from_utf8(bs).unwrap(),
                                                ));
                                            }
                                            RedisType::SimpleString(ss) => {
                                                commands.push(RedisCommand::GET(ss));
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                "SET" => {
                                    if let Some(key) = args.next() {
                                        match key {
                                            RedisType::BulkString(bs) => {
                                                let key_value = RedisKeyValue::parse(args).expect(
                                                    "Erro ao fazer parse do valor e das configs",
                                                );
                                                commands.push(RedisCommand::SET(
                                                    String::from_utf8(bs).unwrap(),
                                                    key_value,
                                                ));
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                "RPUSH" => {
                                    let mut list: Vec<String> = Vec::new();
                                    if let Some(key) = args.next() {
                                        match key {
                                            RedisType::BulkString(bs) => {
                                                while let Some(value) = args.next() {
                                                    list.push(value.to_string());
                                                }
                                                commands.push(RedisCommand::RPUSH(
                                                    String::from_utf8(bs).unwrap(),
                                                    list,
                                                ))
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                "LPUSH" => {
                                    let mut list: Vec<String> = Vec::new();
                                    if let Some(key) = args.next() {
                                        match key {
                                            RedisType::BulkString(bs) => {
                                                while let Some(value) = args.next() {
                                                    list.push(value.to_string());
                                                }
                                                commands.push(RedisCommand::LPUSH(
                                                    String::from_utf8(bs).unwrap(),
                                                    list,
                                                ))
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                "LRANGE" => {
                                    if let (Some(key), Some(start), Some(end)) =
                                        (args.next(), args.next(), args.next())
                                    {
                                        let start_value: i64 =
                                            start.to_string().parse().expect("Expected a integer");
                                        let end_value: i64 =
                                            end.to_string().parse().expect("Expected a integer");
                                        commands.push(RedisCommand::LRANGE(
                                            key.to_string(),
                                            start_value,
                                            end_value,
                                        ));
                                    }
                                }
                                "LLEN" => {
                                    if let Some(s) = args.next() {
                                        match s {
                                            RedisType::BulkString(bs) => {
                                                commands.push(RedisCommand::LLEN(
                                                    String::from_utf8(bs).unwrap(),
                                                ));
                                            }
                                            RedisType::SimpleString(ss) => {
                                                commands.push(RedisCommand::LLEN(ss));
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                "LPOP" => {
                                    if let Some(key) = args.next() {
                                        let mut count = 1;
                                        if let Some(count_arg) = args.next() {
                                            match count_arg.to_int() {
                                                Some(v) => count = v,
                                                None => count = 1
                                            }
                                        }
                                        match key {
                                            RedisType::BulkString(bs) => {
                                                commands.push(RedisCommand::LPOP(
                                                    String::from_utf8(bs).unwrap(),
                                                    count,
                                                ));
                                            }
                                            RedisType::SimpleString(ss) => {
                                                commands.push(RedisCommand::LPOP(ss, count));
                                            }
                                            _ => {}
                                        }
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
        assert_eq!(result, vec![RedisCommand::ECHO("teste teste".to_string())]);
    }

    #[test]
    fn test_commands_build_get() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["get", "test"])]);
        assert_eq!(result, vec![RedisCommand::GET("test".to_string())]);
    }

    #[test]
    fn test_commands_build_set() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["set", "test", "test"])]);
        assert_eq!(
            result,
            vec![RedisCommand::SET(
                "test".to_string(),
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
            assert_eq!(*key, "key".to_string());
            assert_eq!(key_value.value, "value");
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
            assert_eq!(*key, "key".to_string());
            assert_eq!(key_value.value, "value");
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
                "mylist".to_string(),
                vec!["one".to_string(), "two".to_string()]
            )]
        );
    }

    #[test]
    fn test_commands_build_lpush() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "Lpush", "mylist", "one", "two",
        ])]);
        assert_eq!(
            result,
            vec![RedisCommand::LPUSH(
                "mylist".to_string(),
                vec!["one".to_string(), "two".to_string()]
            )]
        );
    }

    #[test]
    fn test_commands_build_llen() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["llen", "mylist"])]);
        assert_eq!(result, vec![RedisCommand::LLEN("mylist".to_string())]);
    }

    #[test]
    fn test_commands_build_lpop() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["LPOP", "mylist"])]);
        assert_eq!(result, vec![RedisCommand::LPOP("mylist".to_string(), 1)]);

        let result = RedisCommand::build(vec![RedisType::new_array(vec!["LPOP", "mylist", "2"])]);
        assert_eq!(result, vec![RedisCommand::LPOP("mylist".to_string(), 2)]);
    }
}
