use super::traits::{ParseableCommand, RunnableCommand};
use crate::{resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

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

impl RunnableCommand for GetCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        let response = match store.get_key_value(&self.key).await {
            Some(val) if !val.is_expired() => RespDataType::bulk_string(&val.value),
            _ => RespDataType::Null,
        };
        Some(response)
    }
}
