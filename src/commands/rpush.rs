use crate::types::RedisType;
use std::vec::IntoIter;
use super::command_utils;

#[derive(Debug, PartialEq, Clone)]
pub struct RPushCommand {
    pub key: String,
    pub values: Vec<String>,
}

impl RPushCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "RPUSH command requires a key")?;
        let values: Vec<String> = args.filter_map(|t| t.to_string()).collect();
        if values.is_empty() {
            return Err("RPUSH requires at least one value".to_string());
        }

        Ok(RPushCommand { key, values })
    }
}
