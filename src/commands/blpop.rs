use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::VecDeque;

use super::traits::{ParseableCommand, RunnableCommand};
use crate::{
    resp::RespDataType,
    store::{RedisStore, WaitResult},
    utils,
};
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

async fn lpop(
    lists: &tokio::sync::Mutex<std::collections::HashMap<String, VecDeque<String>>>,
    key: String,
    count: i64,
) -> Option<RespDataType> {
    let mut lists_guard = lists.lock().await;
    let list = lists_guard.entry(key);
    match list {
        Occupied(mut occupied_entry) => {
            let mut popped_elements: Vec<RespDataType> = Vec::new();
            for _i in 0..count {
                if let Some(val) = occupied_entry.get_mut().pop_front() {
                    popped_elements.push(RespDataType::bulk_string(val.as_str()));
                } else {
                    break;
                }
            }
            if count == 1 && !popped_elements.is_empty() {
                return Some(popped_elements.remove(0));
            } else if count > 1 {
                return Some(RespDataType::Array(popped_elements));
            }
            Some(RespDataType::Null)
        }
        Vacant(_) => Some(RespDataType::Null),
    }
}

impl RunnableCommand for BLPopCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        let start_time = utils::now_millis();
        loop {
            let lpop_result = lpop(&store.lists, self.key.clone(), 1).await;
            if !matches!(lpop_result, Some(RespDataType::Null)) {
                return lpop_result.map(|val| {
                    RespDataType::Array(vec![RespDataType::bulk_string(self.key.as_str()), val])
                });
            }

            let elapsed = utils::now_millis() - start_time;
            if self.timeout > 0.0 && elapsed >= (self.timeout * 1000.0) as u128 {
                return Some(RespDataType::NullArray);
            }

            let remaining_timeout =
                std::time::Duration::from_millis((self.timeout * 1000.0) as u64 - elapsed as u64);
            match store
                .wait_until_timeout(&self.key, remaining_timeout, client_notifier)
                .await
            {
                WaitResult::Timeout => {
                    return Some(RespDataType::NullArray);
                }
                _ => {}
            };
        }
        //     let mut receiver = store.subscribe_to_key(&cmd.key, DataType::List).await; // Assumindo DataType::List
        // let start_time = utils::now_millis();
        // loop {
        //     // Tenta fazer o LPOP
        //     let lpop_result = lpop(&store.data, cmd.key.clone(), 1).await; // lpop precisaria ser adaptado para RedisValue
        //     if !matches!(lpop_result, Some(RespDataType::Null)) {
        //         return lpop_result.map(|val| {
        //             RespDataType::Array(vec![
        //                 RespDataType::bulk_string(cmd.key.as_str()),
        //                 val,
        //             ])
        //         });
        //     }

        //     let elapsed = utils::now_millis() - start_time;
        //     if cmd.timeout > 0.0 && elapsed >= (cmd.timeout * 1000.0) as u128 {
        //         return Some(RespDataType::NullArray);
        //     }

        //     let remaining_timeout = Duration::from_millis(
        //         (cmd.timeout * 1000.0) as u64 - elapsed as u64,
        //     );

        //     tokio::select! {
        //         _ = receiver.recv() => {
        //             // Notificado, tenta novamente
        //         }
        //         _ = tokio::time::sleep(remaining_timeout) => {
        //             return Some(RespDataType::NullArray); // Timeout
        //         }
        //     }
        // }
    }
}
