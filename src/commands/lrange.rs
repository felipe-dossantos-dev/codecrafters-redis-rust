use super::traits::{ParseableCommand, RunnableCommand};
use crate::{resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct LRangeCommand {
    pub key: String,
    pub start: i64,
    pub end: i64,
}

impl ParseableCommand for LRangeCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "LRANGE command requires a key")?;

        let start_arg = args
            .next()
            .ok_or_else(|| "Expected values for LRANGE start and end".to_string())?;
        let end_arg = args
            .next()
            .ok_or_else(|| "Expected values for LRANGE start and end".to_string())?;

        let start = start_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for LRANGE start and end".to_string())?;
        let end = end_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for LRANGE start and end".to_string())?;

        Ok(LRangeCommand { key, start, end })
    }
}

impl LRangeCommand {
    pub fn treat_bounds(&mut self, list_len: i64) -> Option<(usize, usize)> {
        if self.start > list_len {
            return None;
        }

        if self.start < 0 {
            self.start += list_len
        }

        let start = self.start.max(0) as usize;

        if self.end < 0 {
            self.end += list_len
        }

        if self.end > list_len {
            self.end = list_len - 1
        };

        let end = self.end.max(0) as usize;
        if self.start > self.end {
            return None;
        }
        Some((start, end))
    }
}

impl RunnableCommand for LRangeCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        match store.get_list(&self.key).await {
            Some(list_value) => {
                let list_len = list_value.len() as i64;
                let (start, end) = match self.clone().treat_bounds(list_len) {
                    Some(value) => value,
                    None => return Some(RespDataType::Array(vec![])),
                };
                let mut result_list: Vec<RespDataType> = Vec::new();
                for i in start..=end {
                    result_list.push(RespDataType::bulk_string(&list_value[i]));
                }
                return Some(RespDataType::Array(result_list));
            }
            _ => Some(RespDataType::Array(vec![])),
        }
    }
}
