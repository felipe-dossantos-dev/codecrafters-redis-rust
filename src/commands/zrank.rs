use super::traits::ParseableCommand;
use crate::resp::RespDataType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct ZRankCommand {
    pub key: String,
    pub member: String,
}

impl ParseableCommand for ZRankCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZADD command requires a key")?;
        let member = Self::get_arg_as_string(args, "ZADD command requires a member")?;

        Ok(ZRankCommand { key, member })
    }
}
