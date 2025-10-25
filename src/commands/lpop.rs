use std::collections::{
    hash_map::Entry::{Occupied, Vacant},
    VecDeque,
};

use super::traits::{ParseableCommand, RunnableCommand};
use crate::{resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct LPopCommand {
    pub key: String,
    pub count: i64,
}

impl ParseableCommand for LPopCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "LPOP command requires a key")?;

        let count_arg = args.next().unwrap_or(RespDataType::Integer(1));
        let count = count_arg
            .to_int()
            .ok_or_else(|| "cannot convert count to int".to_string())?;

        Ok(LPopCommand { key, count })
    }
}

impl RunnableCommand for LPopCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        match store.get_list(&self.key).await {
            Some(mut list) => {
                let mut popped_elements: Vec<RespDataType> = Vec::new();
                for _i in 0..self.count {
                    if let Some(val) = list.pop_front() {
                        popped_elements.push(RespDataType::bulk_string(val.as_str()));
                    } else {
                        break;
                    }
                }
                if self.count == 1 && !popped_elements.is_empty() {
                    return Some(popped_elements.remove(0));
                } else if self.count > 1 {
                    return Some(RespDataType::Array(popped_elements));
                }
                Some(RespDataType::Null)
            }
            None => Some(RespDataType::Null),
        }
    }
}
