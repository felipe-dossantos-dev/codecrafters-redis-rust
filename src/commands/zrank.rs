use super::command_utils;
use crate::commands::sorted_sets::{SortedAddOptions, SortedValue};
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct ZRankCommand {
    pub key: String,
    pub member: String,
}

impl ZRankCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "ZADD command requires a key")?;
        let member = command_utils::get_arg_as_string(args, "ZADD command requires a member")?;

        Ok(ZRankCommand { key, member })
    }
}
