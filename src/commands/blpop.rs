use super::command_utils;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct BLPopCommand {
    pub key: String,
    pub timeout: f64,
}

impl BLPopCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "BLPOP command requires a key")?;
        let timeout = args
            .next()
            .ok_or_else(|| "timeout not found")?
            .to_float()
            .ok_or_else(|| "timeout is not a float".to_string())?;

        Ok(BLPopCommand { key, timeout })
    }
}
