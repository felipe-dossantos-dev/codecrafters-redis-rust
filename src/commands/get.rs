use super::traits::ParseableCommand;
use crate::resp::RespDataType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct GetCommand {
    pub key: String,
}

impl ParseableCommand for GetCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "GET command requires a key")?;
        Ok(GetCommand { key })
    }
}
