use super::traits::ParseableCommand;
use crate::types::RedisType;
use std::vec::IntoIter;

#[derive(Debug, PartialEq, Clone)]
pub struct EchoCommand {
    pub message: String,
}

impl ParseableCommand for EchoCommand {
    fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        if let Some(message) = args.next().and_then(|arg| arg.to_string()) {
            Ok(EchoCommand { message })
        } else {
            Err("ECHO command requires a message".to_string())
        }
    }
}
