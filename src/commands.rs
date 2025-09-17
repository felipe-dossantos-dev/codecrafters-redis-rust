use std::{
    time::{SystemTime, UNIX_EPOCH},
    vec::IntoIter,
};

use crate::{types::RedisType, utils};

#[derive(Debug, PartialEq)]
pub struct RedisKeyValue {
    pub value: String,
    pub expired_at_millis: Option<u128>,
}

impl RedisKeyValue {
    pub fn is_expired(&self) -> bool {
        if let Some(val) = self.expired_at_millis {
            return val <= utils::now_millis();
        }
        false
    }

    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let value = args
            .next()
            .ok_or_else(|| "Expected a value for RedisKeyValue".to_string())?;

        let mut expired_at_millis: Option<u128> = None;

        while let Some(prop_name) = args.next().and_then(|p| p.to_string()) {
            match prop_name.to_ascii_uppercase().as_str() {
                "PX" => {
                    let ttl_arg = args
                        .next()
                        .and_then(|p| p.to_string())
                        .ok_or("PX option requires a value")?;
                    let ttl_value: u128 = ttl_arg
                        .parse()
                        .map_err(|e| format!("Cannot parse PX value: {}", e))?;
                    expired_at_millis = Some(utils::now_millis() + ttl_value);
                }
                _ => (),
            }
        }
        let value_str = value
            .to_string()
            .ok_or("Expected a string value for RedisKeyValue")?;
        Ok(Self {
            value: value_str,
            expired_at_millis,
        })
    }
}

impl Clone for RedisKeyValue {
    fn clone(&self) -> Self {
        RedisKeyValue {
            value: self.value.clone(),
            expired_at_millis: self.expired_at_millis.clone(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct SortedAddOptions {
    pub xx: bool,   // Only update elements that already exist. Don't add new elements.
    pub nx: bool,   // Only add new elements. Don't update already existing elements.
    pub lt: bool, // Only update existing elements if the new score is less than the current score. This flag doesn't prevent adding new elements.
    pub gt: bool, // Only update existing elements if the new score is greater than the current score. This flag doesn't prevent adding new elements.
    pub ch: bool, // Modify the return value from the number of new elements added, to the total number of elements changed.
    pub incr: bool, // When this option is specified ZADD acts like ZINCRBY. Only one score-element pair can be specified in this mode.
}

impl SortedAddOptions {
    pub fn new() -> SortedAddOptions {
        return SortedAddOptions {
            xx: false,
            nx: false,
            lt: false,
            gt: false,
            ch: false,
            incr: false,
        };
    }

    pub fn parse(args: &mut IntoIter<RedisType>) -> Self {
        let mut options = SortedAddOptions::new();

        while let Some(prop_name) = args.as_slice().get(0) {
            let option_str = prop_name
                .to_string()
                .expect("cannot convert value to string")
                .to_ascii_uppercase();
            match option_str.as_str() {
                "XX" => {
                    options.xx = true;
                }
                "NX" => {
                    options.nx = true;
                }
                "LT" => {
                    options.lt = true;
                }
                "GT" => {
                    options.gt = true;
                }
                "CH" => {
                    options.ch = true;
                }
                "INCR" => {
                    options.incr = true;
                }
                _ => {
                    break;
                }
            }
            args.next();
        }
        options
    }
}

impl Clone for SortedAddOptions {
    fn clone(&self) -> Self {
        SortedAddOptions {
            xx: self.xx.clone(),
            nx: self.nx.clone(),
            lt: self.lt.clone(),
            gt: self.gt.clone(),
            ch: self.ch.clone(),
            incr: self.incr.clone(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct SortedValue {
    pub member: String,
    pub score: f64,
}

impl Clone for SortedValue {
    fn clone(&self) -> Self {
        SortedValue {
            member: self.member.clone(),
            score: self.score.clone(),
        }
    }
}

impl SortedValue {
    fn parse(args: &mut IntoIter<RedisType>) -> Option<Vec<SortedValue>> {
        let mut sorted_values: Vec<SortedValue> = Vec::new();
        while let (Some(score_arg), Some(member_arg)) = (args.next(), args.next()) {
            if let (Some(score), Some(member)) = (score_arg.to_float(), member_arg.to_string()) {
                sorted_values.push(SortedValue { member, score });
            } else {
                return None;
            }
        }
        if sorted_values.is_empty() {
            None
        } else {
            Some(sorted_values)
        }
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
    BLPOP(String, f64),
    ZADD(String, SortedAddOptions, Vec<SortedValue>),
}

impl Clone for RedisCommand {
    fn clone(&self) -> Self {
        match self {
            RedisCommand::GET(key) => RedisCommand::GET(key.clone()),
            RedisCommand::SET(key, value) => RedisCommand::SET(key.clone(), value.clone()),
            RedisCommand::PING => RedisCommand::PING,
            RedisCommand::ECHO(msg) => RedisCommand::ECHO(msg.clone()),
            RedisCommand::RPUSH(key, values) => RedisCommand::RPUSH(key.clone(), values.clone()),
            RedisCommand::LPUSH(key, values) => RedisCommand::LPUSH(key.clone(), values.clone()),
            RedisCommand::LRANGE(key, start, end) => {
                RedisCommand::LRANGE(key.clone(), *start, *end)
            }
            RedisCommand::LLEN(key) => RedisCommand::LLEN(key.clone()),
            RedisCommand::LPOP(key, count) => RedisCommand::LPOP(key.clone(), count.clone()),
            RedisCommand::BLPOP(key, timeout) => RedisCommand::BLPOP(key.clone(), timeout.clone()),
            RedisCommand::ZADD(key, options, values) => {
                RedisCommand::ZADD(key.clone(), options.clone(), values.clone())
            }
        }
    }
}

impl RedisCommand {
    pub fn build(values: Vec<RedisType>) -> Result<Vec<RedisCommand>, String> {
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

                    let command_name_result = Self::get_string(&mut args)
                        .or_else(|_| Err("cant get the command name".to_string()))?;
                    let command_name_uppercase = command_name_result.to_ascii_uppercase();
                    let command_name = command_name_uppercase.as_str();

                    match command_name {
                        "PING" => {
                            commands.push(RedisCommand::PING);
                        }
                        "ECHO" => {
                            let value = Self::get_string(&mut args)?;
                            commands.push(RedisCommand::ECHO(value));
                        }
                        "GET" => {
                            let key = Self::get_key(&mut args)?;
                            commands.push(RedisCommand::GET(key));
                        }
                        "SET" => {
                            let key = Self::get_key(&mut args)?;
                            let kv = RedisKeyValue::parse(&mut args)?;
                            commands.push(RedisCommand::SET(key, kv));
                        }
                        "RPUSH" => {
                            let key = Self::get_key(&mut args)?;
                            let values: Vec<String> =
                                args.by_ref().filter_map(|t| t.to_string()).collect();
                            if !values.is_empty() {
                                commands.push(RedisCommand::RPUSH(key, values));
                            } else {
                                return Err("no values to push".to_string());
                            }
                        }
                        "LPUSH" => {
                            let key = Self::get_key(&mut args)?;
                            let values: Vec<String> =
                                args.by_ref().filter_map(|t| t.to_string()).collect();
                            if !values.is_empty() {
                                commands.push(RedisCommand::LPUSH(key, values));
                            } else {
                                return Err("no values to push".to_string());
                            }
                        }
                        "LRANGE" => {
                            let key = Self::get_key(&mut args)?;
                            let start_arg = args.next().ok_or_else(|| {
                                "Expected values for LRANGE start and end".to_string()
                            })?;
                            let end_arg = args.next().ok_or_else(|| {
                                "Expected values for LRANGE start and end".to_string()
                            })?;

                            let start_int = start_arg.to_int().ok_or_else(|| {
                                "Expected integer values for LRANGE start and end".to_string()
                            })?;
                            let end_int = end_arg.to_int().ok_or_else(|| {
                                "Expected integer values for LRANGE start and end".to_string()
                            })?;
                            commands.push(RedisCommand::LRANGE(key, start_int, end_int));
                        }
                        "LLEN" => {
                            let key = Self::get_key(&mut args)?;
                            commands.push(RedisCommand::LLEN(key));
                        }
                        "LPOP" => {
                            let key = Self::get_key(&mut args)?;
                            let count_arg = args.next().unwrap_or(RedisType::Integer(1));
                            let count = count_arg
                                .to_int()
                                .ok_or_else(|| "cannot convert count to int".to_string())?;

                            commands.push(RedisCommand::LPOP(key, count));
                        }
                        "BLPOP" => {
                            let key = Self::get_key(&mut args)?;
                            let timeout_arg = args.next().unwrap_or(RedisType::bulk_string("0"));
                            let timeout = timeout_arg
                                .to_float()
                                .ok_or_else(|| "cannot convert timeout to float".to_string())?;
                            commands.push(RedisCommand::BLPOP(key, timeout));
                        }
                        "ZADD" => {
                            let key = Self::get_key(&mut args)?;
                            let options = SortedAddOptions::parse(&mut args);
                            let values = SortedValue::parse(&mut args)
                                .ok_or_else(|| "cant parse values".to_string())?;

                            commands.push(RedisCommand::ZADD(key, options, values));
                        }
                        _ => {
                            return Err("command not found".to_string());
                        }
                    }
                }
                RedisType::BulkString(bytes) if bytes.eq_ignore_ascii_case(b"PING") => {
                    commands.push(RedisCommand::PING);
                }
                RedisType::SimpleString(s) if s.eq_ignore_ascii_case("PING") => {
                    commands.push(RedisCommand::PING);
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

    fn get_string(args: &mut IntoIter<RedisType>) -> Result<String, String> {
        if let Some(arg) = args.next().and_then(|f| f.to_string()) {
            return Ok(arg);
        }
        Err("cant convert to string".to_string())
    }

    fn get_key(args: &mut IntoIter<RedisType>) -> Result<String, String> {
        if let Some(arg) = args.next().and_then(|f| f.to_string()) {
            return Ok(arg);
        }
        Err("cant found key".to_string())
    }
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
        assert_eq!(result, Ok(vec![RedisCommand::PING, RedisCommand::PING]));
    }

    #[test]
    fn test_commands_build_echo() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["echo", "teste teste"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::ECHO("teste teste".to_string())])
        );
    }

    #[test]
    fn test_commands_build_get() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["get", "test"])]);
        assert_eq!(result, Ok(vec![RedisCommand::GET("test".to_string())]))
    }

    #[test]
    fn test_commands_build_set() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["set", "test", "test"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::SET(
                "test".to_string(),
                RedisKeyValue {
                    value: "test".to_string(),
                    expired_at_millis: None
                }
            )])
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
        if let RedisCommand::SET(key, key_value) = command {
            assert_eq!(*key, "key".to_string());
            assert_eq!(key_value.value, "value");
            assert!(key_value.expired_at_millis.is_some());
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
        if let RedisCommand::SET(key, key_value) = &result[0] {
            assert_eq!(*key, "key".to_string());
            assert_eq!(key_value.value, "value");
            assert!(
                key_value.expired_at_millis.is_some(),
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
            Ok(vec![RedisCommand::RPUSH(
                "mylist".to_string(),
                vec!["one".to_string(), "two".to_string()]
            )])
        );
    }

    #[test]
    fn test_commands_build_lpush() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "Lpush", "mylist", "one", "two",
        ])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::LPUSH(
                "mylist".to_string(),
                vec!["one".to_string(), "two".to_string()]
            )])
        );
    }

    #[test]
    fn test_commands_build_llen() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["llen", "mylist"])]);
        assert_eq!(result, Ok(vec![RedisCommand::LLEN("mylist".to_string())]));
    }

    #[test]
    fn test_commands_build_lpop() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["LPOP", "mylist"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::LPOP("mylist".to_string(), 1)])
        );

        let result = RedisCommand::build(vec![RedisType::new_array(vec!["LPOP", "mylist", "2"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::LPOP("mylist".to_string(), 2)])
        );
    }

    #[test]
    fn test_commands_build_blpop() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["bLPOP", "mylist"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::BLPOP("mylist".to_string(), 0.0)])
        );

        let result = RedisCommand::build(vec![RedisType::new_array(vec!["bLPOP", "mylist", "2"])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::BLPOP("mylist".to_string(), 2.0)])
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
            Ok(vec![RedisCommand::ZADD(
                "myzset".to_string(),
                SortedAddOptions {
                    xx: false,
                    nx: false,
                    lt: false,
                    gt: false,
                    ch: false,
                    incr: false
                },
                vec![
                    SortedValue {
                        score: 1.0,
                        member: "one".to_string()
                    },
                    SortedValue {
                        score: 2.0,
                        member: "two".to_string()
                    }
                ]
            )])
        );
    }

    #[test]
    fn test_commands_build_zadd_with_options() {
        let result = RedisCommand::build(vec![RedisType::new_array(vec![
            "zadd", "myzset", "NX", "CH", "1", "one",
        ])]);
        assert_eq!(
            result,
            Ok(vec![RedisCommand::ZADD(
                "myzset".to_string(),
                SortedAddOptions {
                    xx: false,
                    nx: true,
                    lt: false,
                    gt: false,
                    ch: true,
                    incr: false
                },
                vec![SortedValue {
                    score: 1.0,
                    member: "one".to_string()
                },]
            )])
        );
    }

    #[test]
    fn test_commands_build_errors() {
        // RPUSH with no values
        let result = RedisCommand::build(vec![RedisType::new_array(vec!["rpush", "mylist"])]);
        assert_eq!(result, Err("no values to push".to_string()));

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
}
