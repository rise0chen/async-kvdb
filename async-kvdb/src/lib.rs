use bytes::Bytes;
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
