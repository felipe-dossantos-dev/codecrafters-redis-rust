use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};

use tokio::sync::{Mutex, MutexGuard, Notify, OwnedMutexGuard};

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
    pub keys: Mutex<HashMap<String, RedisValue>>,
    pub pairs: Mutex<HashMap<String, KeyValue>>,
    pub lists: Mutex<HashMap<String, VecDeque<String>>>,
    pub sorted_sets: Mutex<HashMap<String, SortedSet>>,
    // para cada estrutura de dados com a key "X", tem vários clientes esperando ser notificados por algo
    // pode dar bug pq a chave pode ser não única, dai poderia ter uma list mylist e um sset mylist e ter comandos esperando os dois
    // TODO: transformar em channels
    // https://tokio.rs/tokio/tutorial/channels
    pub client_notifiers: Mutex<HashMap<String, Vec<Arc<Notify>>>>,
}

impl RedisStore {
    pub fn new() -> Self {
        Self {
            keys: Mutex::new(HashMap::new()),
            pairs: Mutex::new(HashMap::new()),
            lists: Mutex::new(HashMap::new()),
            sorted_sets: Mutex::new(HashMap::new()),
            client_notifiers: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get_sorted_set_by_key(
        &self,
        key: &String,
    ) -> tokio::sync::MappedMutexGuard<'_, SortedSet> {
        let guard = self.sorted_sets.lock().await;
        MutexGuard::map(guard, |ss| {
            ss.entry(key.clone()).or_insert_with(SortedSet::new)
        })
    }

    pub async fn notify_by_key(&self, key: &String) {
        if let Some(clients_notifiers) = self.client_notifiers.lock().await.get(key) {
            clients_notifiers.first().map(|f| f.notify_one());
        }
    }

    pub async fn wait_until_timeout(
        &self,
        key: &String,
        duration: Duration,
        notifier: &Arc<Notify>,
    ) -> WaitResult {
        {
            let notifier_clone = notifier.clone();
            let mut notifiers = self.client_notifiers.lock().await;
            notifiers
                .entry(key.clone())
                .or_insert_with(Vec::new)
                .push(notifier_clone);
        }

        let mut result = WaitResult::Notified;
        if duration > Duration::ZERO {
            tokio::select! {
                _ = notifier.notified() => {
                }
                _ = tokio::time::sleep(duration) => {
                    result = WaitResult::Timeout;
                }
            }
        } else {
            notifier.notified().await;
        }

        if let Some(vec) = self.client_notifiers.lock().await.get_mut(key) {
            vec.retain(|n| !Arc::ptr_eq(n, &notifier));
        }

        return result;
    }
}
