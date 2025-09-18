use crate::types::RedisType;
use std::vec::IntoIter;
use super::command_utils;

#[derive(Debug, PartialEq, Clone)]
pub struct LLenCommand {
    pub key: String,
}

impl LLenCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "LLEN command requires a key")?;
        Ok(LLenCommand { key })
    }
}
