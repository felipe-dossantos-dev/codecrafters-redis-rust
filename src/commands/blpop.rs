use super::traits::ParseableCommand;
use crate::resp::RespDataType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct BLPopCommand {
    pub key: String,
    pub timeout: f64,
}

impl ParseableCommand for BLPopCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "BLPOP command requires a key")?;
        let timeout = args
            .next()
            .ok_or_else(|| "timeout not found")?
            .to_float()
            .ok_or_else(|| "timeout is not a float".to_string())?;

        Ok(BLPopCommand { key, timeout })
    }
}
