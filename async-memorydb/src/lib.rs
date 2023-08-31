pub use async_kvdb::*;
use async_lock::RwLock;
use std::collections::HashMap;

#[derive(Default)]
pub struct MenoryDb {
    mem: RwLock<HashMap<Key, Value>>,
}
impl MenoryDb {
    pub fn new(mem: HashMap<Key, Value>) -> Self {
        Self { mem: RwLock::new(mem) }
    }
}

#[async_trait]
impl Kvdb for MenoryDb {
    async fn scan_keys(&self, filter: &Filter) -> Vec<Key> {
        let mem = self.mem.read().await;
        mem.keys().filter(|k| filter(k)).map(Clone::clone).collect()
    }
    async fn get(&self, key: Key) -> Option<Value> {
        self.mem.read().await.get(&key).cloned()
    }
    async fn get_many(&self, keys: Vec<Key>) -> HashMap<Key, Value> {
        let mem = self.mem.read().await;
        mem.iter()
            .filter_map(|(k, v)| if keys.contains(k) { Some((k.clone(), v.clone())) } else { None })
            .collect()
    }
    async fn set(&self, key: Key, value: Value) {
        self.mem.write().await.insert(key, value);
    }
    async fn set_many(&self, data: HashMap<Key, Value>) {
        let mut mem = self.mem.write().await;
        mem.extend(data);
    }
    async fn delete(&self, key: Key) {
        self.mem.write().await.remove(&key);
    }
    async fn delete_many(&self, keys: Vec<Key>) {
        let mut mem = self.mem.write().await;
        mem.retain(|k, _| !keys.contains(k));
    }
    async fn delete_all(&self) {
        let mut mem = self.mem.write().await;
        *mem = HashMap::new();
    }
}
