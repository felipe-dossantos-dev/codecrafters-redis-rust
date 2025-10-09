use std::collections::VecDeque;

use super::traits::{ParseableCommand, RunnableCommand};
use crate::{resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;


#[derive(Debug, PartialEq, Clone)]
pub struct RPushCommand {
    pub key: String,
    pub values: Vec<String>,
}

impl ParseableCommand for RPushCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "RPUSH command requires a key")?;
        let values: Vec<String> = args.filter_map(|t| t.to_string()).collect();
        if values.is_empty() {
            return Err("RPUSH requires at least one value".to_string());
        }

        Ok(RPushCommand { key, values })
    }
}


impl RunnableCommand for RPushCommand {
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
        if list.is_empty() {
            store.notify_by_key(&self.key).await;
        }
        list.extend(self.values.clone());
        let len = list.len() as i64;

        Some(RespDataType::Integer(len))
    }
}
