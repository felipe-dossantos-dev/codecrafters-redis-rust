use crate::{
    resp::RespDataType,
    store::RedisStore,
    values::{key_value::KeyValue, RedisValue},
};

use super::traits::{ParseableCommand, RunnableCommand};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct SetCommand {
    pub key: String,
    pub value: KeyValue,
}

impl ParseableCommand for SetCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "SET command requires a key")?;
        let value = KeyValue::parse(args)?;
        Ok(SetCommand { key, value })
    }
}

impl RunnableCommand for SetCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        store
            .pairs
            .lock()
            .await
            .insert(self.key.clone(), self.value.clone());
        // TODO
        // store
        //     .keys
        //     .lock()
        //     .await
        //     .insert(self.key.clone(), RedisValue::String);
        Some(RespDataType::ok())
    }
}
