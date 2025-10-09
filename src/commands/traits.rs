use tokio::sync::Notify;

use crate::{resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};

pub trait ParseableCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String>
    where
        Self: Sized;

    fn get_arg_as_string(
        args: &mut IntoIter<RespDataType>,
        error_message: &str,
    ) -> Result<String, String> {
        if let Some(arg) = args.next().and_then(|f| f.to_string()) {
            Ok(arg)
        } else {
            Err(error_message.to_string())
        }
    }
}

pub trait RunnableCommand: Send + Sync {
    async fn execute(
        &self,
        client_id: &str,
        store: &Arc<RedisStore>,
        client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType>;
}
