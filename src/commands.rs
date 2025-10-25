pub mod blpop;
pub mod echo;
pub mod get;
pub mod key_type;
pub mod llen;
pub mod lpop;
pub mod lpush;
pub mod lrange;
pub mod ping;
pub mod rpush;
pub mod set;
pub mod traits;
pub mod zadd;
pub mod zcard;
pub mod zrange;
pub mod zrank;
pub mod zrem;
pub mod zscore;

use std::sync::Arc;
use std::{
    time::{SystemTime, UNIX_EPOCH},
    vec::IntoIter,
};

use crate::command_parser;
use crate::{
    commands::{
        blpop::BLPopCommand,
        echo::EchoCommand,
        get::GetCommand,
        key_type::KeyTypeCommand,
        llen::LLenCommand,
        lpop::LPopCommand,
        lpush::LPushCommand,
        lrange::LRangeCommand,
        ping::PingCommand,
        rpush::RPushCommand,
        set::SetCommand,
        traits::{ParseableCommand, RunnableCommand},
        zadd::ZAddCommand,
        zcard::ZCardCommand,
        zrange::ZRangeCommand,
        zrank::ZRankCommand,
        zrem::ZRemCommand,
        zscore::ZScoreCommand,
    },
    resp::RespDataType,
    store::RedisStore,
    utils,
};
use tokio::sync::Notify;

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
    ZRANGE(ZRangeCommand),
    ZCARD(ZCardCommand),
    ZSCORE(ZScoreCommand),
    ZREM(ZRemCommand),
    TYPE(KeyTypeCommand),
}

// TODO - tentar implementar algo como uma linguagem para fazer o parse, algo declarativo
// e que cuide se está valido a quantidade de argumentos e os valores dos argumentos
impl RedisCommand {
    pub fn build(values: Vec<RespDataType>) -> Result<Vec<RedisCommand>, String> {
        let mut commands: Vec<RedisCommand> = Vec::new();
        for value in values {
            match value {
                RespDataType::Array(arr) => {
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

                    command_parser! {
                        command_name,
                        args,
                        commands,
                        "PING" => (PING, PingCommand),
                        "ECHO" => (ECHO, EchoCommand),
                        "GET" => (GET, GetCommand),
                        "SET" => (SET, SetCommand),
                        "RPUSH" => (RPUSH, RPushCommand),
                        "LPUSH" => (LPUSH, LPushCommand),
                        "LRANGE" => (LRANGE, LRangeCommand),
                        "LLEN" => (LLEN, LLenCommand),
                        "LPOP" => (LPOP, LPopCommand),
                        "BLPOP" => (BLPOP, BLPopCommand),
                        "ZADD" => (ZADD, ZAddCommand),
                        "ZRANK" => (ZRANK, ZRankCommand),
                        "ZRANGE" => (ZRANGE, ZRangeCommand),
                        "ZCARD" => (ZCARD, ZCardCommand),
                        "ZSCORE" => (ZSCORE, ZScoreCommand),
                        "ZREM" => (ZREM, ZRemCommand),
                        "TYPE" => (TYPE, KeyTypeCommand),
                    }
                }
                RespDataType::BulkString(bytes) if bytes.eq_ignore_ascii_case(b"PING") => {
                    commands.push(RedisCommand::PING(PingCommand));
                }
                RespDataType::SimpleString(s) if s.eq_ignore_ascii_case("PING") => {
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
        let received_values = RespDataType::parse(values);
        println!("Received values: {:?}", received_values);

        let received_commands = Self::build(received_values)?;
        println!("Received commands: {:?}", received_commands);

        return Ok(received_commands);
    }
}

impl RunnableCommand for RedisCommand {
    async fn execute(
        &self,
        client_id: &str,
        store: &Arc<RedisStore>,
        client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        match self {
            RedisCommand::PING(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::ECHO(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::GET(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::SET(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::LPUSH(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::RPUSH(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::LRANGE(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::LLEN(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::LPOP(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::BLPOP(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::ZADD(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::ZCARD(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::ZRANK(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::ZRANGE(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::ZSCORE(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::ZREM(cmd) => cmd.execute(client_id, store, client_notifier).await,
            RedisCommand::TYPE(cmd) => cmd.execute(client_id, store, client_notifier).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        commands::zadd::ZAddOptions,
        values::{key_value::KeyValue, sorted_set::SortedValue},
    };

    use super::*;

    #[test]
    fn test_commands_build_ping() {
        let result = RedisCommand::build(vec![
            RespDataType::bulk_string("ping"),
            RespDataType::simple_string("ping"),
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
        let result =
            RedisCommand::build(vec![RespDataType::new_array(vec!["echo", "teste teste"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::ECHO(EchoCommand {
                message: "teste teste".to_string()
            })])
        );
    }

    #[test]
    fn test_commands_build_get() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["get", "test"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::GET(GetCommand {
                key: "test".to_string()
            })])
        )
    }

    #[test]
    fn test_commands_build_set() {
        let result =
            RedisCommand::build(vec![RespDataType::new_array(vec!["set", "test", "test"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::SET(SetCommand {
                key: "test".to_string(),
                value: KeyValue {
                    value: "test".to_string(),
                    expired_at_millis: None
                }
            })])
        );
    }

    #[test]
    fn test_commands_build_set_with_px() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
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
        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
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
        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
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
        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
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
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["llen", "mylist"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::LLEN(LLenCommand {
                key: "mylist".to_string()
            })])
        );
    }

    #[test]
    fn test_commands_build_lpop() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["LPOP", "mylist"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::LPOP(LPopCommand {
                key: "mylist".to_string(),
                count: 1
            })])
        );

        let result =
            RedisCommand::build(vec![RespDataType::new_array(vec!["LPOP", "mylist", "2"])]);
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
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["bLPOP", "mylist"])]);
        assert_eq!(result, Err("timeout not found".to_string()));

        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "bLPOP", "mylist", "err",
        ])]);
        assert_eq!(result, Err("timeout is not a float".to_string()));

        let result =
            RedisCommand::build(vec![RespDataType::new_array(vec!["bLPOP", "mylist", "2"])]);
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
            RespDataType::bulk_string("XX"),
            RespDataType::bulk_string("GT"),
            RespDataType::bulk_string("CH"),
        ]
        .into_iter();
        let options = ZAddOptions::parse(&mut args);
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
            RespDataType::bulk_string("nx"),
            RespDataType::bulk_string("lt"),
            RespDataType::bulk_string("incr"),
        ]
        .into_iter();
        let options = ZAddOptions::parse(&mut args);
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
            RespDataType::bulk_string("10.5"),
            RespDataType::bulk_string("member1"),
            RespDataType::bulk_string("-1"),
            RespDataType::bulk_string("member2"),
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
        let not_expired = KeyValue {
            value: "value".to_string(),
            expired_at_millis: Some(utils::now_millis() + 10000),
        };
        assert!(!not_expired.is_expired());

        let expired = KeyValue {
            value: "value".to_string(),
            expired_at_millis: Some(utils::now_millis() - 1),
        };
        assert!(expired.is_expired());

        let no_expiry = KeyValue {
            value: "value".to_string(),
            expired_at_millis: None,
        };
        assert!(!no_expiry.is_expired());
    }

    #[test]
    fn test_redis_key_value_parse_errors() {
        // No value for key
        let mut args = vec![].into_iter();
        assert!(KeyValue::parse(&mut args).is_err());

        // PX with no value
        let mut args = vec![
            RespDataType::bulk_string("value"),
            RespDataType::bulk_string("PX"),
        ]
        .into_iter();
        assert_eq!(
            KeyValue::parse(&mut args).unwrap_err(),
            "PX option requires a value"
        );

        // PX with non-numeric value
        let mut args = vec![
            RespDataType::bulk_string("value"),
            RespDataType::bulk_string("PX"),
            RespDataType::bulk_string("abc"),
        ]
        .into_iter();
        assert!(KeyValue::parse(&mut args)
            .unwrap_err()
            .contains("Cannot parse PX value"));
    }

    #[test]
    fn test_sorted_value_parse_edge_cases() {
        // Empty args
        let mut args = vec![].into_iter();
        assert_eq!(SortedValue::parse(&mut args), None);

        // Odd number of args
        let mut args = vec![RespDataType::bulk_string("10.5")].into_iter();
        assert_eq!(SortedValue::parse(&mut args), None);

        // Invalid score
        let mut args = vec![
            RespDataType::bulk_string("not-a-float"),
            RespDataType::bulk_string("member1"),
        ]
        .into_iter();
        assert_eq!(SortedValue::parse(&mut args), None);
    }

    #[test]
    fn test_commands_build_zadd() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "zadd", "myzset", "1", "one", "2", "two",
        ])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZADD(ZAddCommand {
                key: "myzset".to_string(),
                options: ZAddOptions {
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
        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "zadd", "myzset", "NX", "CH", "1", "one",
        ])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZADD(ZAddCommand {
                key: "myzset".to_string(),
                options: ZAddOptions {
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
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["rpush", "mylist"])]);
        assert_eq!(result, Err("RPUSH requires at least one value".to_string()));

        // LRANGE with missing args
        let result =
            RedisCommand::build(vec![RespDataType::new_array(vec!["lrange", "mylist", "0"])]);
        assert_eq!(
            result,
            Err("Expected values for LRANGE start and end".to_string())
        );

        // LRANGE with non-integer args
        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "lrange", "mylist", "a", "b",
        ])]);
        assert_eq!(
            result,
            Err("Expected integer values for LRANGE start and end".to_string())
        );
    }

    #[test]
    fn test_commands_build_zrank() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["zrank"])]);
        assert_eq!(result, Err("ZADD command requires a key".to_string()));

        let result =
            RedisCommand::build(vec![RespDataType::new_array(vec!["zRank", "sorted_set"])]);
        assert_eq!(result, Err("ZADD command requires a member".to_string()));

        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
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

    #[test]
    fn test_commands_build_zrange() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["zrange"])]);
        assert_eq!(result, Err("ZRANGE command requires a key".to_string()));

        let result =
            RedisCommand::build(vec![RespDataType::new_array(vec!["zRange", "sorted_set"])]);
        assert_eq!(
            result,
            Err("Expected values for ZRANGE start and end".to_string())
        );

        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "zRange",
            "sorted_set",
            "A",
        ])]);
        assert_eq!(
            result,
            Err("Expected values for ZRANGE start and end".to_string())
        );

        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "zRange",
            "sorted_set",
            "A",
            "A",
        ])]);
        assert_eq!(
            result,
            Err("Expected integer values for ZRANGE start and end".to_string())
        );

        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "zRange",
            "sorted_set",
            "1",
            "A",
        ])]);
        assert_eq!(
            result,
            Err("Expected integer values for ZRANGE start and end".to_string())
        );

        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "zRange",
            "sorted_set",
            "0",
            "1",
        ])]);

        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZRANGE(ZRangeCommand {
                key: "sorted_set".to_string(),
                start: 0,
                end: 1
            })])
        );
    }

    #[test]
    fn test_commands_build_zcard() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["ZCARD"])]);
        assert_eq!(result, Err("ZCARD command requires a key".to_string()));

        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["ZCARD", "zset_key"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZCARD(ZCardCommand {
                key: "zset_key".to_string(),
            })])
        );
    }

    #[test]
    fn test_commands_build_zscore() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["zscore"])]);
        assert_eq!(result, Err("ZSCORE command requires a key".to_string()));

        let result =
            RedisCommand::build(vec![RespDataType::new_array(vec!["zscore", "sorted_set"])]);
        assert_eq!(result, Err("ZSCORE command requires a member".to_string()));

        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "zscore",
            "sorted_set",
            "mykey",
        ])]);

        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZSCORE(ZScoreCommand {
                key: "sorted_set".to_string(),
                member: "mykey".to_string()
            })])
        );
    }

    #[test]
    fn test_commands_build_zrem() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["zrem"])]);
        assert_eq!(result, Err("ZREM command requires a key".to_string()));

        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["zrem", "sorted_set"])]);
        assert_eq!(result, Err("ZREM command requires a member".to_string()));

        let result = RedisCommand::build(vec![RespDataType::new_array(vec![
            "zrem",
            "sorted_set",
            "mykey",
        ])]);

        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZREM(ZRemCommand {
                key: "sorted_set".to_string(),
                member: "mykey".to_string()
            })])
        );
    }

    #[test]
    fn test_commands_build_type() {
        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["type"])]);
        assert_eq!(result, Err("TYPE command requires a key".to_string()));

        let result = RedisCommand::build(vec![RespDataType::new_array(vec!["type", "sorted_set"])]);

        assert_eq!(
            result,
            Ok(vec![RedisCommand::TYPE(KeyTypeCommand {
                key: "sorted_set".to_string(),
            })])
        );
    }
}
