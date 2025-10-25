use super::traits::{ParseableCommand, RunnableCommand};
use crate::{resp::RespDataType, store::RedisStore};
use std::{sync::Arc, vec::IntoIter};
use tokio::sync::Notify;

#[derive(Debug, PartialEq, Clone)]
pub struct BLPopCommand {
    pub key: String,
    pub timeout: f64,
}

impl ParseableCommand for BLPopCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let key = Self::get_arg_as_string(args, "BLPOP command requires a key")?;
        let timeout = args
            .next()
            .ok_or_else(|| "timeout not found")?
            .to_float()
            .ok_or_else(|| "timeout is not a float".to_string())?;

        Ok(BLPopCommand { key, timeout })
    }
}

impl RunnableCommand for BLPopCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        let timeout_duration = if self.timeout > 0.0 {
            Some(std::time::Duration::from_secs_f64(self.timeout))
        } else {
            None
        };
        let start_time = std::time::Instant::now();

        loop {
            // Try to pop
            if let Some(mut list) = store.get_list(&self.key).await {
                if let Some(val) = list.pop_front() {
                    return Some(RespDataType::Array(vec![
                        RespDataType::bulk_string(&self.key),
                        RespDataType::bulk_string(&val),
                    ]));
                }
            }

            // Check timeout
            if let Some(timeout) = timeout_duration {
                if start_time.elapsed() >= timeout {
                    return Some(RespDataType::NullArray);
                }
            }

            // Wait for notification or timeout
            let mut receiver = store.subscribe_to_key(&self.key).await;

            if let Some(timeout) = timeout_duration {
                let remaining_timeout = timeout.saturating_sub(start_time.elapsed());
                tokio::select! {
                    _ = receiver.recv() => {
                        // Key was modified, loop again
                    }
                    _ = tokio::time::sleep(remaining_timeout) => {
                        return Some(RespDataType::NullArray); // Timeout
                    }
                }
            } else {
                // Block indefinitely
                let _ = receiver.recv().await;
            }
        }
    }
}
