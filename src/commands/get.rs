use super::command_utils;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct GetCommand {
    pub key: String,
}

impl GetCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "GET command requires a key")?;
        Ok(GetCommand { key })
    }
}
