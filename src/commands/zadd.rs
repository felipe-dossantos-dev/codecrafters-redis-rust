use crate::commands::sorted_sets::{SortedAddOptions, SortedValue};
use crate::types::RedisType;
use std::vec::IntoIter;
use super::command_utils;

#[derive(Debug, PartialEq, Clone)]
pub struct ZAddCommand {
    pub key: String,
    pub options: SortedAddOptions,
    pub values: Vec<SortedValue>,
}

impl ZAddCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "ZADD command requires a key")?;

        let options = SortedAddOptions::parse(args);
        let values = SortedValue::parse(args)
            .ok_or_else(|| "cant parse values".to_string())?;

        Ok(ZAddCommand {
            key,
            options,
            values,
        })
    }
}
