use crate::types::RedisType;
use std::vec::IntoIter;
use super::command_utils;

#[derive(Debug, PartialEq, Clone)]
pub struct LPopCommand {
    pub key: String,
    pub count: i64,
}

impl LPopCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "LPOP command requires a key")?;

        let count_arg = args.next().unwrap_or(RedisType::Integer(1));
        let count = count_arg
            .to_int()
            .ok_or_else(|| "cannot convert count to int".to_string())?;

        Ok(LPopCommand { key, count })
    }
}
