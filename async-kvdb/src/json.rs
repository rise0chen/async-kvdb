use super::*;
use serde::{Deserialize, Serialize};

fn decode_json<T: for<'a> Deserialize<'a>>(data: &[u8]) -> Option<T> {
    serde_json::from_slice(data).ok()
}
fn encode_json<T: Serialize>(data: &T) -> Bytes {
    serde_json::to_vec_pretty(data).unwrap_or_default().into()
}

impl<T: ?Sized> KvdbJsonExt for T where T: Kvdb {}

#[async_trait]
pub trait KvdbJsonExt: Kvdb {
    async fn get_json<T: for<'a> Deserialize<'a>>(&self, key: Key) -> Option<T> {
        let val = self.get(key).await;
        val.and_then(|v| decode_json(&v))
    }
    async fn get_many_json<T: for<'a> Deserialize<'a>>(&self, keys: Vec<Key>) -> HashMap<Key, T> {
        let data = self.get_many(keys).await;
        data.into_iter().filter_map(|(k, v)| decode_json(&v).map(|v| (k, v))).collect()
    }
    async fn set_json<T: Serialize + Send + Sync>(&self, key: Key, value: &T) {
        let val = encode_json(value);
        self.set(key, val).await;
    }
    async fn set_many_json<T: Serialize + Send + Sync>(&self, data: HashMap<Key, T>) {
        let data = data.into_iter().map(|(k, v)| (k, encode_json(&v))).collect();
        self.set_many(data).await;
    }
}
