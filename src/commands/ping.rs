use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct PingCommand;

impl PingCommand {
    pub fn parse(_args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        Ok(PingCommand)
    }
}
