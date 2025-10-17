use super::traits::ParseableCommand;
use crate::{commands::traits::RunnableCommand, resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct ZRankCommand {
    pub key: String,
    pub member: String,
}

impl ParseableCommand for ZRankCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZADD command requires a key")?;
        let member = Self::get_arg_as_string(args, "ZADD command requires a member")?;

        Ok(ZRankCommand { key, member })
    }
}

impl RunnableCommand for ZRankCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        let ss = store.get_sorted_set(&self.key).await;
        match ss.get_rank_by_member(&self.member) {
            Some(val) => Some(RespDataType::Integer(val)),
            None => Some(RespDataType::Null),
        }
    }
}
