use tokio::sync::Notify;

use super::traits::ParseableCommand;
use crate::{
    commands::traits::RunnableCommand, resp::RespDataType, store::RedisStore, values::RedisValue,
};
use std::{sync::Arc, vec::IntoIter};

#[derive(Debug, PartialEq, Clone)]
pub struct KeyTypeCommand {
    pub key: String,
}

impl ParseableCommand for KeyTypeCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "TYPE command requires a key")?;

        Ok(KeyTypeCommand { key })
    }
}

impl RunnableCommand for KeyTypeCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        match store.get_key(&self.key).await {
            Some(key_type) => Some(key_type.to_type_resp()),
            None => Some(RedisValue::None.to_type_resp()), // "none" is the default for non-existent keys
        }
    }
}
