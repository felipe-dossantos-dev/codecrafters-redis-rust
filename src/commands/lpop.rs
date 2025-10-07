
use super::traits::ParseableCommand;
use crate::resp::RespDataType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct LPopCommand {
    pub key: String,
    pub count: i64,
}

impl ParseableCommand for LPopCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "LPOP command requires a key")?;

        let count_arg = args.next().unwrap_or(RespDataType::Integer(1));
        let count = count_arg
            .to_int()
            .ok_or_else(|| "cannot convert count to int".to_string())?;

        Ok(LPopCommand { key, count })
    }
}