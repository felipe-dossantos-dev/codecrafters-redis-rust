pub mod blpop;
pub mod command_utils;
pub mod echo;
pub mod get;
pub mod key_value;
pub mod llen;
pub mod lpop;
pub mod lpush;
pub mod lrange;
pub mod ping;
pub mod rpush;
pub mod set;
pub mod sorted_sets;
pub mod zadd;
pub mod zrank;

use std::{
    time::{SystemTime, UNIX_EPOCH},
    vec::IntoIter,
};

use crate::{
    commands::{
        blpop::BLPopCommand, echo::EchoCommand, get::GetCommand, key_value::RedisKeyValue,
        llen::LLenCommand, lpop::LPopCommand, lpush::LPushCommand, lrange::LRangeCommand,
        ping::PingCommand, rpush::RPushCommand, set::SetCommand, zadd::ZAddCommand,
        zrank::ZRankCommand,
    },
    types::RedisType,
    utils,
};

#[derive(Debug, PartialEq, Clone)]
pub enum RedisCommand {
    GET(GetCommand),
    SET(SetCommand),
    PING(PingCommand),
    ECHO(EchoCommand),
    RPUSH(RPushCommand),
    LPUSH(LPushCommand),
    LRANGE(LRangeCommand),
    LLEN(LLenCommand),
    LPOP(LPopCommand),
    BLPOP(BLPopCommand),
    ZADD(ZAddCommand),
    ZRANK(ZRankCommand),
}

impl RedisCommand {
    pub fn build(values: Vec<RedisType>) -> Result<Vec<RedisCommand>, String> {
        let mut commands: Vec<RedisCommand> = Vec::new();
        for value in values {
            match value {
                RedisType::Array(arr) => {
                    if arr.is_empty() {
                        continue;
                    }

                    let mut args = arr.into_iter();

                    let command_name_result = args
                        .next()
                        .and_then(|f| f.to_string())
                        .ok_or_else(|| "cant get the command name".to_string())?;
                    let command_name_uppercase = command_name_result.to_ascii_uppercase();
                    let command_name = command_name_uppercase.as_str();

                    match command_name {
                        "PING" => {
                            commands.push(RedisCommand::PING(PingCommand::parse(&mut args)?));
                        }
                        "ECHO" => {
                            let echo_command = EchoCommand::parse(&mut args)?;
                            commands.push(RedisCommand::ECHO(echo_command));
                        }
                        "GET" => {
                            commands.push(RedisCommand::GET(GetCommand::parse(&mut args)?));
                        }
                        "SET" => {
                            commands.push(RedisCommand::SET(SetCommand::parse(&mut args)?));
                        }
                        "RPUSH" => {
                            commands.push(RedisCommand::RPUSH(RPushCommand::parse(&mut args)?));
                        }
                        "LPUSH" => {
                            commands.push(RedisCommand::LPUSH(LPushCommand::parse(&mut args)?));
                        }
                        "LRANGE" => {
                            commands.push(RedisCommand::LRANGE(LRangeCommand::parse(&mut args)?));
                        }
                        "LLEN" => {
                            commands.push(RedisCommand::LLEN(LLenCommand::parse(&mut args)?));
                        }
                        "LPOP" => {
                            commands.push(RedisCommand::LPOP(LPopCommand::parse(&mut args)?));
                        }
                        "BLPOP" => {
                            commands.push(RedisCommand::BLPOP(BLPopCommand::parse(&mut args)?));
                        }
                        "ZADD" => {
                            commands.push(RedisCommand::ZADD(ZAddCommand::parse(&mut args)?));
                        }
                        "ZRANK" => {
                            commands.push(RedisCommand::ZRANK(ZRankCommand::parse(&mut args)?));
                        }
                        _ => {
                            return Err("command not found".to_string());
                        }
                    }
                }
                RedisType::BulkString(bytes) if bytes.eq_ignore_ascii_case(b"PING") => {
                    commands.push(RedisCommand::PING(PingCommand));
                }
                RedisType::SimpleString(s) if s.eq_ignore_ascii_case("PING") => {
                    commands.push(RedisCommand::PING(PingCommand));
                }
                _ => {
                    return Err("value type not correct".to_string());
                }
            }
        }
        Ok(commands)
    }

    pub fn parse(values: Vec<u8>) -> Result<Vec<RedisCommand>, String> {
        let received_values = RedisType::parse(values);
        println!("Received values: {:?}", received_values);

        let received_commands = Self::build(received_values)?;
        println!("Received commands: {:?}", received_commands);

        return Ok(received_commands);
    }
}

#[cfg(test)]
mod tests {
    use crate::commands::sorted_sets::{SortedAddOptions, SortedValue};

    use super::*;

    #[test]
    fn test_commands_build_ping() {
        let result = RedisCommand::build(vec![
            RedisType::bulk_string("ping"),
            RedisType::simple_string("ping"),
        ]);
        assert_eq!(
            result,
            Ok(vec![
                RedisCommand::PING(PingCommand),
                RedisCommand::PING(PingCommand)
            ])
        );
    }

    #[test]
    fn test_commands_build_echo() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["echo", "teste teste"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::ECHO(EchoCommand {
                message: "teste teste".to_string()
            })])
        );
    }

    #[test]
    fn test_commands_build_get() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["get", "test"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::GET(GetCommand {
                key: "test".to_string()
            })])
        )
    }

    #[test]
    fn test_commands_build_set() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["set", "test", "test"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::SET(SetCommand {
                key: "test".to_string(),
                value: RedisKeyValue {
                    value: "test".to_string(),
                    expired_at_millis: None
                }
            })])
        );
    }

    #[test]
    fn test_commands_build_set_with_px() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "set", "key", "value", "PX", "100",
        ])])
        .unwrap();
        assert_eq!(result.len(), 1);
        let command = &result[0];
        if let RedisCommand::SET(set_command) = command {
            assert_eq!(set_command.key, "key".to_string());
            assert_eq!(set_command.value.value, "value");
            assert!(set_command.value.expired_at_millis.is_some());
        } else {
            panic!("Expected Ok(RedisCommand::Set), but got {:?}", command);
        }
    }

    #[test]
    fn test_commands_build_set_with_px_case_insensitive() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "set", "key", "value", "pX", "100",
        ])])
        .unwrap();
        assert_eq!(result.len(), 1);
        if let RedisCommand::SET(set_command) = &result[0] {
            assert_eq!(set_command.key, "key".to_string());
            assert_eq!(set_command.value.value, "value");
            assert!(
                set_command.value.expired_at_millis.is_some(),
                "PX option should be case-insensitive"
            );
        }
    }

    #[test]
    fn test_commands_build_rpush() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "rpush", "mylist", "one", "two",
        ])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::RPUSH(RPushCommand {
                key: "mylist".to_string(),
                values: vec!["one".to_string(), "two".to_string()]
            })])
        );
    }

    #[test]
    fn test_commands_build_lpush() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "Lpush", "mylist", "one", "two",
        ])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::LPUSH(LPushCommand {
                key: "mylist".to_string(),
                values: vec!["one".to_string(), "two".to_string()]
            })])
        );
    }

    #[test]
    fn test_commands_build_llen() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["llen", "mylist"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::LLEN(LLenCommand {
                key: "mylist".to_string()
            })])
        );
    }

    #[test]
    fn test_commands_build_lpop() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["LPOP", "mylist"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::LPOP(LPopCommand {
                key: "mylist".to_string(),
                count: 1
            })])
        );

        let result = RedisCommand::build(vec![RedisType::new_array(vec!["LPOP", "mylist", "2"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::LPOP(LPopCommand {
                key: "mylist".to_string(),
                count: 2
            })])
        );
    }

    #[test]
    fn test_commands_build_blpop() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["bLPOP", "mylist"])]);
        assert_eq!(result, Err("timeout not found".to_string()));

        let result =
            RedisCommand::build(vec![RedisType::new_array(vec!["bLPOP", "mylist", "err"])]);
        assert_eq!(result, Err("timeout is not a float".to_string()));

        let result = RedisCommand::build(vec![RedisType::new_array(vec!["bLPOP", "mylist", "2"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::BLPOP(BLPopCommand {
                key: "mylist".to_string(),
                timeout: 2.0
            })])
        );
    }

    #[test]
    fn test_sorted_add_options_parse() {
        let mut args = vec![
            RedisType::bulk_string("XX"),
            RedisType::bulk_string("GT"),
            RedisType::bulk_string("CH"),
        ]
        .into_iter();
        let options = SortedAddOptions::parse(&mut args);
        assert!(options.xx);
        assert!(options.gt);
        assert!(options.ch);
        assert!(!options.nx);
        assert!(!options.lt);
        assert!(!options.incr);
    }

    #[test]
    fn test_sorted_add_options_parse_case_insensitive() {
        let mut args = vec![
            RedisType::bulk_string("nx"),
            RedisType::bulk_string("lt"),
            RedisType::bulk_string("incr"),
        ]
        .into_iter();
        let options = SortedAddOptions::parse(&mut args);
        assert!(options.nx);
        assert!(options.lt);
        assert!(options.incr);
        assert!(!options.xx);
        assert!(!options.gt);
        assert!(!options.ch);
    }

    #[test]
    fn test_sorted_value_parse() {
        let mut args = vec![
            RedisType::bulk_string("10.5"),
            RedisType::bulk_string("member1"),
            RedisType::bulk_string("-1"),
            RedisType::bulk_string("member2"),
        ]
        .into_iter();
        let values = SortedValue::parse(&mut args).unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].score, 10.5);
        assert_eq!(values[0].member, "member1");
        assert_eq!(values[1].score, -1.0);
        assert_eq!(values[1].member, "member2");
    }

    #[test]
    fn test_redis_key_value_is_expired() {
        let not_expired = RedisKeyValue {
            value: "value".to_string(),
            expired_at_millis: Some(utils::now_millis() + 10000),
        };
        assert!(!not_expired.is_expired());

        let expired = RedisKeyValue {
            value: "value".to_string(),
            expired_at_millis: Some(utils::now_millis() - 1),
        };
        assert!(expired.is_expired());

        let no_expiry = RedisKeyValue {
            value: "value".to_string(),
            expired_at_millis: None,
        };
        assert!(!no_expiry.is_expired());
    }

    #[test]
    fn test_redis_key_value_parse_errors() {
        // No value for key
        let mut args = vec![].into_iter();
        assert!(RedisKeyValue::parse(&mut args).is_err());

        // PX with no value
        let mut args = vec![
            RedisType::bulk_string("value"),
            RedisType::bulk_string("PX"),
        ]
        .into_iter();
        assert_eq!(
            RedisKeyValue::parse(&mut args).unwrap_err(),
            "PX option requires a value"
        );

        // PX with non-numeric value
        let mut args = vec![
            RedisType::bulk_string("value"),
            RedisType::bulk_string("PX"),
            RedisType::bulk_string("abc"),
        ]
        .into_iter();
        assert!(RedisKeyValue::parse(&mut args)
            .unwrap_err()
            .contains("Cannot parse PX value"));
    }

    #[test]
    fn test_sorted_value_parse_edge_cases() {
        // Empty args
        let mut args = vec![].into_iter();
        assert_eq!(SortedValue::parse(&mut args), None);

        // Odd number of args
        let mut args = vec![RedisType::bulk_string("10.5")].into_iter();
        assert_eq!(SortedValue::parse(&mut args), None);

        // Invalid score
        let mut args = vec![
            RedisType::bulk_string("not-a-float"),
            RedisType::bulk_string("member1"),
        ]
        .into_iter();
        assert_eq!(SortedValue::parse(&mut args), None);
    }

    #[test]
    fn test_commands_build_zadd() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "zadd", "myzset", "1", "one", "2", "two",
        ])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZADD(ZAddCommand {
                key: "myzset".to_string(),
                options: SortedAddOptions {
                    xx: false,
                    nx: false,
                    lt: false,
                    gt: false,
                    ch: false,
                    incr: false
                },
                values: vec![
                    SortedValue {
                        score: 1.0,
                        member: "one".to_string()
                    },
                    SortedValue {
                        score: 2.0,
                        member: "two".to_string()
                    }
                ]
            })])
        );
    }

    #[test]
    fn test_commands_build_zadd_with_options() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "zadd", "myzset", "NX", "CH", "1", "one",
        ])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZADD(ZAddCommand {
                key: "myzset".to_string(),
                options: SortedAddOptions {
                    xx: false,
                    nx: true,
                    lt: false,
                    gt: false,
                    ch: true,
                    incr: false
                },
                values: vec![SortedValue {
                    score: 1.0,
                    member: "one".to_string()
                },]
            })])
        );
    }

    #[test]
    fn test_commands_build_errors() {
        // RPUSH with no values
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["rpush", "mylist"])]);
        assert_eq!(result, Err("RPUSH requires at least one value".to_string()));

        // LRANGE with missing args
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["lrange", "mylist", "0"])]);
        assert_eq!(
            result,
            Err("Expected values for LRANGE start and end".to_string())
        );

        // LRANGE with non-integer args
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "lrange", "mylist", "a", "b",
        ])]);
        assert_eq!(
            result,
            Err("Expected integer values for LRANGE start and end".to_string())
        );
    }

    #[test]
    fn test_commands_build_zrank() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["zrank"])]);
        assert_eq!(result, Err("ZADD command requires a key".to_string()));

        let result = RedisCommand::build(vec![RedisType::new_array(vec!["zRank", "sorted_set"])]);
        assert_eq!(result, Err("ZADD command requires a member".to_string()));

        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "Zrank",
            "sorted_set",
            "mykey",
        ])]);

        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZRANK(ZRankCommand {
                key: "sorted_set".to_string(),
                member: "mykey".to_string()
            })])
        );
    }
}
