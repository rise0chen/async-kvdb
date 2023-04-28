use async_kvdb::*;
use async_lock::RwLock;
use std::collections::HashMap;

#[derive(Default)]
pub struct MenoryDb {
    mem: RwLock<HashMap<Key, Value>>,
}
impl MenoryDb {
    pub fn new(mem: HashMap<Key, Value>) -> Self {
        Self {
            mem: RwLock::new(mem),
        }
    }
}
impl MenoryDb {
    pub async fn get(&self, key: Key) -> Option<Value> {
        self.mem.read().await.get(&key).cloned()
    }
    pub async fn get_all(&self) -> HashMap<Key, Value> {
        self.mem.read().await.clone()
    }
    pub async fn get_with_prefix(&self, prefix: Key) -> HashMap<Key, Value> {
        let mem = self.mem.read().await;
        let iter = mem.iter().filter_map(|(k, v)| {
            if k.starts_with(&*prefix) {
                Some((k.clone(), v.clone()))
            } else {
                None
            }
        });
        iter.collect()
    }
    pub async fn set(&self, key: Key, value: Value) {
        self.mem.write().await.insert(key, value);
    }
    pub async fn set_many(&self, data: HashMap<Key, Value>) {
        let mut mem = self.mem.write().await;
        data.into_iter().for_each(|(k, v)| {
            mem.insert(k, v);
        });
    }
    pub async fn delete(&self, key: Key) {
        self.mem.write().await.remove(&key);
    }
    pub async fn delete_all(&self) {
        let mut mem = self.mem.write().await;
        *mem = HashMap::new();
    }
    pub async fn delete_with_prefix(&self, prefix: Key) {
        let mut mem = self.mem.write().await;
        mem.retain(|k, _| !k.starts_with(&*prefix));
    }
}
