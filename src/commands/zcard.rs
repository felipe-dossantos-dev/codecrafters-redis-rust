use tokio::sync::Notify;

use super::traits::ParseableCommand;
use crate::{commands::traits::RunnableCommand, resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};

#[derive(Debug, PartialEq, Clone)]
pub struct ZCardCommand {
    pub key: String,
}

impl ParseableCommand for ZCardCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZCARD command requires a key")?;

        Ok(ZCardCommand { key })
    }
}

impl RunnableCommand for ZCardCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        match store.get_sorted_set(&self.key).await {
            Some(ss) => return Some(RespDataType::Integer(ss.len())),
            None => Some(RespDataType::Integer(0)),
        }
    }
}
