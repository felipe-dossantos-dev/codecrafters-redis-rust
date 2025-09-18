use crate::types::RedisType;
use std::vec::IntoIter;
use super::command_utils;

#[derive(Debug, PartialEq, Clone)]
pub struct LRangeCommand {
    pub key: String,
    pub start: i64,
    pub end: i64,
}

impl LRangeCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "LRANGE command requires a key")?;

        let start_arg = args
            .next()
            .ok_or_else(|| "Expected values for LRANGE start and end".to_string())?;
        let end_arg = args
            .next()
            .ok_or_else(|| "Expected values for LRANGE start and end".to_string())?;

        let start = start_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for LRANGE start and end".to_string())?;
        let end = end_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for LRANGE start and end".to_string())?;

        Ok(LRangeCommand { key, start, end })
    }
}
