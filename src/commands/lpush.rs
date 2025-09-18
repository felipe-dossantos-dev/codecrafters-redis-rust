use crate::types::RedisType;
use std::vec::IntoIter;
use super::command_utils;

#[derive(Debug, PartialEq, Clone)]
pub struct LPushCommand {
    pub key: String,
    pub values: Vec<String>,
}

impl LPushCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "LPUSH command requires a key")?;
        let values: Vec<String> = args.filter_map(|t| t.to_string()).collect();
        if values.is_empty() {
            return Err("LPUSH requires at least one value".to_string());
        }

        Ok(LPushCommand { key, values })
    }
}
