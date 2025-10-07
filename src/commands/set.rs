
use super::traits::ParseableCommand;
use crate::{commands::key_value::RedisKeyValue, resp::RespDataType};
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct SetCommand {
    pub key: String,
    pub value: RedisKeyValue,
}

impl ParseableCommand for SetCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "SET command requires a key")?;
        let value = RedisKeyValue::parse(args)?;
        Ok(SetCommand { key, value })
    }
}
