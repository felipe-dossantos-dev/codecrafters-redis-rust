use super::traits::{ParseableCommand, RunnableCommand};
use crate::{resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct PingCommand;

impl ParseableCommand for PingCommand {
    fn parse(_args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        Ok(PingCommand)
    }
}

impl RunnableCommand for PingCommand {
    async fn execute(
        &self,
        _client_id: &str,
        _store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        Some(RespDataType::pong())
    }
}
