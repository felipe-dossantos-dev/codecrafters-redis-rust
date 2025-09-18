use crate::types::RedisType;
use std::vec::IntoIter;

pub fn get_arg_as_string(args: &mut IntoIter<RedisType>, error_message: &str) -> Result<String, String> {
    if let Some(arg) = args.next().and_then(|f| f.to_string()) {
        Ok(arg)
    } else {
        Err(error_message.to_string())
    }
}
