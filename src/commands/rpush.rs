use std::collections::VecDeque;

use super::traits::{ParseableCommand, RunnableCommand};
use crate::{
    resp::RespDataType,
    store::{KeyResult, RedisStore},
    types::RedisType,
};
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
        match store.get_list(&self.key).await {
            Some(mut list) => {
                for value in self.values.iter() {
                    list.push_back(value.clone());
                }
                store.notify_key_modified(&self.key).await;
                let len = list.len() as i64;
                return Some(RespDataType::Integer(len));
            }
            None => {
                let mut new_list = VecDeque::new();
                for value in self.values.iter() {
                    new_list.push_back(value.clone());
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
