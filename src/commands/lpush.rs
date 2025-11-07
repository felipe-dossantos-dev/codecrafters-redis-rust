use crate::{
    resp::RespDataType,
    store::{KeyResult, RedisStore},
    types::RedisType,
};
use std::{collections::VecDeque, sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

use super::traits::{ParseableCommand, RunnableCommand};

#[derive(Debug, PartialEq, Clone)]
pub struct LPushCommand {
    pub key: String,
    pub values: Vec<String>,
}

impl ParseableCommand for LPushCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "LPUSH command requires a key")?;
        let values: Vec<String> = args.filter_map(|t| t.to_string()).collect();
        if values.is_empty() {
            return Err("LPUSH requires at least one value".to_string());
        }

        Ok(LPushCommand { key, values })
    }
}

impl RunnableCommand for LPushCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        match store.get_list(&self.key).await {
            Some(mut list) => {
                for value in self.values.iter() {
                    list.push_front(value.clone());
                }
                store.notify_key_modified(&self.key).await;
                let len = list.len() as i64;
                return Some(RespDataType::Integer(len));
            }
            None => {
                let mut new_list = VecDeque::new();
                for value in self.values.iter() {
                    new_list.push_front(value.clone());
                }
                let len = new_list.len() as i64;
                let result = store.create(&self.key, RedisType::List(new_list)).await;

                match result {
                    KeyResult::Error(e) => Some(RespDataType::error(&e)),
                    _ => {
                        store.notify_key_modified(&self.key).await;
                        Some(RespDataType::Integer(len))
                    }
                }
            }
        }
    }
}
