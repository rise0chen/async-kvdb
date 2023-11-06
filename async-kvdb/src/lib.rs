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

pub enum DbOp {
    Insert { key: Key, value: Value },
    InsertMany { data: HashMap<Key, Value> },
    Delete { key: Key },
    DeleteMany { keys: Vec<Key> },
    DeleteAll,
}

#[derive(Default)]
pub struct DbOps {
    pub clear: bool,
    pub insert: HashMap<Key, Value>,
    pub delete: Vec<Key>,
}

#[derive(Default)]
pub struct DbOpMerger {
    need_clear: bool,
    ops: HashMap<Key, Option<Value>>,
}
impl DbOpMerger {
    pub fn new() -> Self {
        Self {
            need_clear: false,
            ops: HashMap::new(),
        }
    }
    pub fn is_empty(&self) -> bool {
        !self.need_clear && self.ops.is_empty()
    }
    pub fn merge(&mut self, op: DbOp) {
        match op {
            DbOp::Insert { key, value } => {
                self.ops.insert(key, Some(value));
            }
            DbOp::InsertMany { data } => {
                for (key, value) in data {
                    self.ops.insert(key, Some(value));
                }
            }
            DbOp::Delete { key } => {
                self.ops.insert(key, None);
            }
            DbOp::DeleteMany { keys } => {
                for key in keys {
                    self.ops.insert(key, None);
                }
            }
            DbOp::DeleteAll => {
                self.need_clear = true;
                self.ops.clear();
            }
        }
    }
    pub fn into_ops(self) -> DbOps {
        let mut ops = DbOps::default();
        for (k, v) in self.ops {
            if let Some(v) = v {
                ops.insert.insert(k, v);
            } else {
                ops.delete.push(k);
            }
        }
        ops.clear = self.need_clear;
        ops
    }
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

#[test]
fn empty(){
    let op_merger = DbOpMerger::default();
    assert!(op_merger.is_empty());
}
