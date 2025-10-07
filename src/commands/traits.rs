use crate::resp::RespDataType;
use std::vec::IntoIter;

pub trait ParseableCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String>
    where
        Self: Sized;

    fn get_arg_as_string(
        args: &mut IntoIter<RespDataType>,
        error_message: &str,
    ) -> Result<String, String> {
        if let Some(arg) = args.next().and_then(|f| f.to_string()) {
            Ok(arg)
        } else {
            Err(error_message.to_string())
        }
    }
}
