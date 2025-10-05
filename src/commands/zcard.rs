use super::traits::ParseableCommand;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct ZCardCommand {
    pub key: String,
}

impl ParseableCommand for ZCardCommand {
    fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZCARD command requires a key")?;

        Ok(ZCardCommand { key })
    }
}
