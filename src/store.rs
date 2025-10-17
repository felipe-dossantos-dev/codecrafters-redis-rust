use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use tokio::sync::{
    broadcast,
    broadcast::{Receiver, Sender},
    Mutex, MutexGuard, Notify, OwnedMutexGuard,
};

use crate::values::RedisValue;
use crate::values::{
    key_value::KeyValue,
    sorted_set::{SortedSet, SortedValue},
};

#[derive(Debug, PartialEq)]
pub enum WaitResult {
    Notified,
    Timeout,
}

#[derive(Debug)]
pub struct RedisStore {
    pub data: Mutex<HashMap<String, RedisValue>>,
    pub key_notifiers: Mutex<HashMap<String, Sender<()>>>,
}

impl RedisStore {
    pub fn new() -> Self {
        Self {
            data: Mutex::new(HashMap::new()),
            key_notifiers: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_key_value(
        &self,
        key: &String,
    ) -> Option<tokio::sync::MappedMutexGuard<'_, KeyValue>> {
        let guard = self.data.lock().await;
        MutexGuard::try_map(guard, |map| {
            if let Some(RedisValue::String(kv)) = map.get_mut(key) {
                Some(kv)
            } else {
                None
            }
        })
        .ok()
    }

    pub async fn get_sorted_set(
        &self,
        key: &String,
    ) -> tokio::sync::MappedMutexGuard<'_, SortedSet> {
        let guard = self.data.lock().await;
        MutexGuard::try_map(guard, |map| {
            if let Some(RedisValue::ZSet(kv)) = map.get_mut(key) {
                Some(kv)
            } else {
                None
            }
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
