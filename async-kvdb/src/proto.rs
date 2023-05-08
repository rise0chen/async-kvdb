use super::*;
use prost::Message;

fn decode_proto<T: Message + Default>(data: &[u8]) -> Option<T> {
    Message::decode(data).ok()
}
fn encode_proto<T: Message>(data: &T) -> Bytes {
    data.encode_to_vec().into()
}

impl<T: ?Sized> KvdbProtoExt for T where T: Kvdb {}

#[async_trait]
pub trait KvdbProtoExt: Kvdb {
    async fn get_proto<T: Message + Default>(&self, key: Key) -> Option<T> {
        let val = self.get(key).await;
        val.and_then(|v| decode_proto(&v))
    }
    async fn get_proto_all<T: Message + Default>(&self) -> HashMap<Key, T> {
        let data = self.get_all().await;
        data.into_iter()
            .filter_map(|(k, v)| decode_proto(&v).map(|v| (k, v)))
            .collect()
    }
    async fn get_proto_with_prefix<T: Message + Default>(&self, prefix: Key) -> HashMap<Key, T> {
        let data = self.get_with_prefix(prefix).await;
        data.into_iter()
            .filter_map(|(k, v)| decode_proto(&v).map(|v| (k, v)))
            .collect()
    }
    async fn set_proto<T: Message>(&self, key: Key, value: &T) {
        let val = encode_proto(value);
        self.set(key, val).await;
    }
    async fn set_proto_many<T: Message>(&self, data: HashMap<Key, T>) {
        let data = data
            .into_iter()
            .map(|(k, v)| (k, encode_proto(&v)))
            .collect();
        self.set_many(data).await;
    }
}
