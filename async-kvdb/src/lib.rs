#[cfg(feature = "json")]
mod json;
#[cfg(feature = "proto")]
mod proto;

pub use async_trait::async_trait;
use bytes::Bytes;
use smol_str::SmolStr;
use std::collections::HashMap;

#[cfg(feature = "json")]
pub use json::KvdbJsonExt;
#[cfg(feature = "proto")]
pub use proto::KvdbProtoExt;

pub type Key = SmolStr;
pub type Value = Bytes;
pub type KeyValue = (Key, Value);
pub type Filter = dyn Fn(&Key) -> bool + Send + Sync;

pub enum DBOp {
    Insert { key: Key, value: Value },
    InsertMany { data: HashMap<Key, Value> },
    Delete { key: Key },
    DeleteMany { keys: Vec<Key> },
    DeleteAll,
}

#[async_trait]
pub trait Kvdb {
    async fn scan_keys(&self, filter: &Filter) -> Vec<Key>;
    async fn get(&self, key: Key) -> Option<Value> {
        let keys = Vec::from([key]);
        self.get_many(keys).await.into_values().next()
    }
    async fn get_many(&self, keys: Vec<Key>) -> HashMap<Key, Value>;
    async fn set(&self, key: Key, value: Value) {
        let data = HashMap::from([(key, value)]);
        self.set_many(data).await
    }
    async fn set_many(&self, data: HashMap<Key, Value>);
    async fn delete(&self, key: Key) {
        let keys = Vec::from([key]);
        self.delete_many(keys).await
    }
    async fn delete_many(&self, keys: Vec<Key>);
    async fn delete_all(&self);
}
