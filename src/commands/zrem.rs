use super::traits::ParseableCommand;
use crate::{commands::traits::RunnableCommand, resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct ZRemCommand {
    pub key: String,
    pub member: String,
}

impl ParseableCommand for ZRemCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZREM command requires a key")?;
        let member = Self::get_arg_as_string(args, "ZREM command requires a member")?;

        Ok(ZRemCommand { key, member })
    }
}

impl RunnableCommand for ZRemCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        let value = store
            .get_sorted_set_by_key(&self.key)
            .await
            .remove_by_member(&self.member);
        return Some(RespDataType::Integer(value));
    }
}
