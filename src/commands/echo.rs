use super::traits::{ParseableCommand, RunnableCommand};
use crate::{resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct EchoCommand {
    pub message: String,
}

impl ParseableCommand for EchoCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        if let Some(message) = args.next().and_then(|arg| arg.to_string()) {
            Ok(EchoCommand { message })
        } else {
            Err("ECHO command requires a message".to_string())
        }
    }
}

impl RunnableCommand for EchoCommand {
    async fn execute(
        &self,
        _client_id: &str,
        _store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        Some(RespDataType::bulk_string(&self.message))
    }
}
