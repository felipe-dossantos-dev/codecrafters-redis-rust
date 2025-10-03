use super::traits::ParseableCommand;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct ZRankCommand {
    pub key: String,
    pub member: String,
}

impl ParseableCommand for ZRankCommand {
    fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZADD command requires a key")?;
        let member = Self::get_arg_as_string(args, "ZADD command requires a member")?;

        Ok(ZRankCommand { key, member })
    }
}
