use super::traits::ParseableCommand;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct ZRemCommand {
    pub key: String,
    pub member: String,
}

impl ParseableCommand for ZRemCommand {
    fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZREM command requires a key")?;
        let member = Self::get_arg_as_string(args, "ZREM command requires a member")?;

        Ok(ZRemCommand { key, member })
    }
}
