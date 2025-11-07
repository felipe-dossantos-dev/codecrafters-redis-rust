use crate::commands::traits::RunnableCommand;
use crate::resp::RespDataType;
use crate::store::RedisStore;
use crate::types::stream::{RedisStream, StreamEntry};
use crate::types::RedisType;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec::IntoIter;
use tokio::sync::Notify;

use super::traits::ParseableCommand;

#[derive(Debug, PartialEq, Clone)]
pub struct XAddCommand {
    pub stream_key: String,
    pub entry_key: String,
    pub values: HashMap<String, String>,
}

fn parse_map(args: &mut IntoIter<RespDataType>) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    loop {
        let name_arg = args.next();
        let value_arg = args.next();
        if name_arg.is_none() && value_arg.is_none() {
            break;
        } else if name_arg.is_none() || value_arg.is_none() {
            return Err("XADD command requires key-value pair".to_string());
        }

        let name_value = name_arg.unwrap().to_string().unwrap();
        let value_value = value_arg.unwrap().to_string().unwrap();
        map.insert(name_value, value_value);
    }

    if map.is_empty() {
        return Err("XADD command requires at least one key-value pair".to_string());
    } else {
        return Ok(map);
    }
}

impl ParseableCommand for XAddCommand {
    fn parse(args: &mut IntoIter<RespDataType>) -> Result<Self, String> {
        let stream_key = Self::get_arg_as_string(args, "XADD command requires a stream key")?;
        let entry_key = Self::get_arg_as_string(args, "XADD command requires a entry key")?;
        let values = parse_map(args)?;

        Ok(XAddCommand {
            stream_key,
            entry_key,
            values,
        })
    }
}

impl RunnableCommand for XAddCommand {
    async fn execute(
        &self,
        _client_id: &str,
        store: &Arc<RedisStore>,
        _client_notifier: &Arc<Notify>,
    ) -> Option<RespDataType> {
        let mut stream = match store.get_key(&self.stream_key).await {
            Some(value) => match *value {
                RedisType::Stream(ref s) => s.clone(),
                _ => {
                    return Some(RespDataType::error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    ));
                }
            },
            None => RedisStream::new(),
        };

        let entry = StreamEntry::new(self.values.clone());
        stream.add_entry(self.entry_key.clone(), entry);
        store
            .create_or_update_key(&self.stream_key, RedisType::Stream(stream))
            .await;

        Some(RespDataType::bulk_string(&self.entry_key))
    }
}
