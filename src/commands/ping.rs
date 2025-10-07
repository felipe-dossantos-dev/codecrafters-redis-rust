use super::traits::ParseableCommand;
use crate::resp::RespDataType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct PingCommand;

impl ParseableCommand for PingCommand {
    fn parse(_args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        Ok(PingCommand)
    }
}