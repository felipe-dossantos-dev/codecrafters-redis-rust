use super::traits::ParseableCommand;
use crate::resp::RespDataType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct KeyTypeCommand {
    pub key: String,
}

impl ParseableCommand for KeyTypeCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "TYPE command requires a key")?;

        Ok(KeyTypeCommand { key })
    }
}
