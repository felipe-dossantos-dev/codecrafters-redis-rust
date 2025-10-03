
use super::traits::ParseableCommand;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct LLenCommand {
    pub key: String,
}

impl ParseableCommand for LLenCommand {
    fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "LLEN command requires a key")?;
        Ok(LLenCommand { key })
    }
}