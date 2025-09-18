use crate::{
    commands::key_value::RedisKeyValue,
    types::RedisType,
};
use std::vec::IntoIter;
use super::command_utils;

#[derive(Debug, PartialEq, Clone)]
pub struct SetCommand {
    pub key: String,
    pub value: RedisKeyValue,
}

impl SetCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "SET command requires a key")?;
        let value = RedisKeyValue::parse(args)?;
        Ok(SetCommand { key, value })
    }
}
