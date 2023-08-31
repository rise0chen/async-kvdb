use async_kvdb::KvdbProtoExt;
use async_memorydb::MenoryDb;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let db = MenoryDb::new(HashMap::new());

    db.set_proto("proto_1".into(), &()).await;
    assert_eq!(Some(()), db.get_proto("proto_1".into()).await);
    db.set_proto("proto_2".into(), &1).await;
    assert_eq!(Some(1), db.get_proto("proto_2".into()).await);
    db.set_proto("proto_3".into(), &String::from("3")).await;
    assert_eq!(Some(String::from("3")), db.get_proto("proto_3".into()).await);
}
