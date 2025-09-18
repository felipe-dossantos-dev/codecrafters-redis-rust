use crate::types::RedisType;
use std::vec::IntoIter;
use super::command_utils;

#[derive(Debug, PartialEq, Clone)]
pub struct GetCommand {
    pub key: String,
}

impl GetCommand {
    pub fn parse(args: &mut IntoIter<RedisType>) -> Result<Self, String> {
        let key = command_utils::get_arg_as_string(args, "GET command requires a key")?;
        Ok(GetCommand { key })
    }
}

// pub async fn handle_get(command: GetCommand, store: &Arc<RedisStore>) -> Option<RedisType> {
//     let response = match store.pairs.lock().await.get(&command.key) {
//         Some(val) if !val.is_expired() => RedisType::bulk_string(&val.value),
//         _ => RedisType::Null,
//     };
//     Some(response)
// }
