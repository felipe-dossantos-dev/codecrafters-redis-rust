use std::collections::VecDeque;

use super::traits::{ParseableCommand, RunnableCommand};
use crate::{resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct LLenCommand {
    pub key: String,
}

impl ParseableCommand for LLenCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "LLEN command requires a key")?;
        Ok(LLenCommand { key })
    }
}

impl RunnableCommand for LLenCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        let mut lists_guard = store.lists.lock().await;
        let list = lists_guard
            .entry(self.key.clone())
            .or_insert_with(VecDeque::new);
        let len = list.len() as i64;
        Some(RespDataType::Integer(len))
    }
}
