use std::collections::{BTreeMap, HashMap};

#[derive(Debug, PartialEq, Clone)]
pub struct StreamEntry {
    values: HashMap<String, String>,
}

impl StreamEntry {
    pub fn new(values: HashMap<String, String>) -> Self {
        Self { values }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct RedisStream {
    entries: BTreeMap<String, StreamEntry>,
}

impl RedisStream {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    pub fn add_entry(&mut self, key: String, entry: StreamEntry) {
        self.entries.insert(key, entry);
    }
}
