use super::traits::ParseableCommand;
use crate::resp::RespDataType;
use std::vec::IntoIter;

use tokio::sync::Notify;

use crate::{commands::traits::RunnableCommand, store::RedisStore};
use std::sync::Arc;

#[derive(Debug, PartialEq, Clone)]
pub struct ZRangeCommand {
    pub key: String,
    pub start: i64,
    pub end: i64,
}

impl ParseableCommand for ZRangeCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZRANGE command requires a key")?;

        let start_arg = args
            .next()
            .ok_or_else(|| "Expected values for ZRANGE start and end".to_string())?;
        let end_arg = args
            .next()
            .ok_or_else(|| "Expected values for ZRANGE start and end".to_string())?;

        let start = start_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for ZRANGE start and end".to_string())?;
        let end = end_arg
            .to_int()
            .ok_or_else(|| "Expected integer values for ZRANGE start and end".to_string())?;

        Ok(ZRangeCommand { key, start, end })
    }
}

impl ZRangeCommand {
    pub fn treat_bounds(&mut self, set_size: i64) -> Option<(usize, usize)> {
        if self.start >= set_size {
            return None;
        }

        if self.start < 0 {
            self.start += set_size
        }

        let start = self.start.max(0) as usize;

        if self.end < 0 {
            self.end += set_size
        }

        if self.end > set_size {
            self.end = set_size - 1
        };

        let end = self.end.max(0) as usize;
        if self.start > self.end {
            return None;
        }
        Some((start, end))
    }
}

impl RunnableCommand for ZRangeCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        match store.get_sorted_set(&self.key).await {
            Some(ss) => {
                let (start, end) = match self.clone().treat_bounds(ss.len()) {
                    Some(value) => value,
                    None => return Some(RespDataType::Array(vec![])),
                };
                let result_list = ss
                    .range(start, end)
                    .map(|v| RespDataType::bulk_string(&v.member))
                    .collect();
                return Some(RespDataType::Array(result_list));
            }
            None => Some(RespDataType::Array(vec![])),
        }
    }
}
