use crate::{
    commands::traits::{ParseableCommand, RunnableCommand},
    resp::RespDataType,
    store::RedisStore,
};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct ZScoreCommand {
    pub key: String,
    pub member: String,
}

impl ParseableCommand for ZScoreCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "ZSCORE command requires a key")?;
        let member = Self::get_arg_as_string(args, "ZSCORE command requires a member")?;

        Ok(ZScoreCommand { key, member })
    }
}

impl RunnableCommand for ZScoreCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        match store.get_sorted_set(&self.key).await {
            Some(ss) => match ss.get_score_by_member(&self.member) {
                Some(value) => Some(RespDataType::bulk_string(&value.to_string())),
                None => Some(RespDataType::Null),
            },
            None => Some(RespDataType::Null),
        }
    }
}
