use super::traits::ParseableCommand;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct PingCommand;

impl ParseableCommand for PingCommand {
    fn parse(_args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        Ok(PingCommand)
    }
}