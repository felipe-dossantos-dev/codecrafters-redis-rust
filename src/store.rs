use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::Arc,
};

use anyhow::Ok;
use tokio::sync::{
    broadcast::{self, Receiver, Sender},
    MappedMutexGuard, Mutex, MutexGuard,
};

use crate::types::RedisType;
use crate::types::{key_value::KeyValue, sorted_set::SortedSet, stream::RedisStream};

#[derive(Debug, PartialEq)]
pub enum KeyResult {
    /// The key was created sucessfully
    Created,
    /// The key already exists, has same type and was updated
    Updated,
    /// The key already exists and has different type
    Error(String),
}

#[derive(Debug)]
pub struct RedisStore {
    data: Mutex<HashMap<String, RedisType>>,
    pub key_notifiers: Mutex<HashMap<String, Sender<()>>>,
}

impl RedisStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            key_notifiers: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_key_value(&self, key: &String) -> Option<MappedMutexGuard<'_, KeyValue>> {
        let guard = self.data.lock().await;
        MutexGuard::try_map(guard, |map| {
            if let Some(RedisType::String(kv)) = map.get_mut(key) {
                Some(kv)
            } else {
                None
            }
        })
        .ok()
    }

    pub async fn create_or_update_key(&self, key: &String, value: RedisType) -> KeyResult {
        let mut guard = self.data.lock().await;
        let entry = guard.entry(key.to_string());
        match entry {
            Entry::Occupied(mut o) => {
                if !matches!(o.get(), _value) {
                    return KeyResult::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    );
                }
                o.insert(value);
                KeyResult::Updated
            }
            Entry::Vacant(v) => {
                v.insert(value);
                KeyResult::Created
            }
        }
    }

    pub async fn create(&self, key: &String, value: RedisType) -> KeyResult {
        let mut guard = self.data.lock().await;
        let entry = guard.entry(key.to_string());
        match entry {
            Entry::Occupied(_) => KeyResult::Error("Key already exists".to_string()),
            Entry::Vacant(v) => {
                v.insert(value);
                KeyResult::Created
            }
        }
    }

    pub async fn get_key(&self, key: &String) -> Option<MappedMutexGuard<'_, RedisType>> {
        let guard = self.data.lock().await;
        MutexGuard::try_map(guard, |map| match map.get_mut(key) {
            Some(val) => Some(val),
            _ => None,
        })
        .ok()
    }

    pub async fn get_list(&self, key: &String) -> Option<MappedMutexGuard<'_, VecDeque<String>>> {
        let guard = self.data.lock().await;
        MutexGuard::try_map(guard, |map| match map.get_mut(key) {
            Some(RedisType::List(list)) => Some(list),
            _ => None,
        })
        .ok()
    }

    pub async fn get_sorted_set(&self, key: &String) -> Option<MappedMutexGuard<'_, SortedSet>> {
        let guard = self.data.lock().await;
        MutexGuard::try_map(guard, |map| match map.get_mut(key) {
            Some(RedisType::ZSet(kv)) => Some(kv),
            _ => None,
        })
        .ok()
    }

    pub async fn get_stream(&self, key: &String) -> Option<MappedMutexGuard<'_, RedisStream>> {
        let guard = self.data.lock().await;
        MutexGuard::try_map(guard, |map| match map.get_mut(key) {
            Some(RedisType::Stream(stream)) => Some(stream),
            _ => None,
        })
        .ok()
    }

    pub async fn subscribe_to_key(&self, key: &String) -> Receiver<()> {
        let mut notifiers_guard = self.key_notifiers.lock().await;
        let sender = notifiers_guard
            .entry(key.clone())
            .or_insert_with(|| broadcast::channel(1).0);
        sender.subscribe()
    }

    pub async fn notify_key_modified(&self, key: &String) {
        let notifiers_guard = self.key_notifiers.lock().await;
        if let Some(sender) = notifiers_guard.get(key) {
            let _ = sender.send(());
        }
    }
}
