pub use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use smol_str::SmolStr;
use std::collections::HashMap;

pub type Key = SmolStr;
pub type Value = Bytes;
pub type KeyValue = (Key, Value);

pub enum DBOp {
    Insert { key: Key, value: Value },
    InsertMany { data: HashMap<Key, Value> },
    Delete { key: Key },
    DeleteAll,
    DeletePrefix { prefix: Key },
}

#[async_trait]
pub trait Kvdb {
    async fn get(&self, key: Key) -> Option<Value> {
        self.get_with_prefix(key.clone()).await.remove(&key)
    }
    async fn get_all(&self) -> HashMap<Key, Value>;
    async fn get_with_prefix(&self, prefix: Key) -> HashMap<Key, Value> {
        let mut data = self.get_all().await;
        data.retain(|k, _| k.starts_with(&*prefix));
        data
    }
    async fn set(&self, key: Key, value: Value) {
        let data = HashMap::from([(key, value)]);
        self.set_many(data).await
    }
    async fn set_many(&self, data: HashMap<Key, Value>);
    async fn delete(&self, key: Key);
    async fn delete_all(&self);
    async fn delete_with_prefix(&self, prefix: Key) {
        let data = self.get_with_prefix(prefix).await;
        let futs = data.into_keys().map(|k| self.delete(k));
        join_all(futs).await;
    }
}
