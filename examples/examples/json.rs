use async_kvdb::KvdbJsonExt;
use async_memorydb::MenoryDb;
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let db = MenoryDb::new(HashMap::new());

    db.set_json("json_1".into(), ()).await;
    assert_eq!(Some(()), db.get_json("json_1".into()).await);
    db.set_json("json_2".into(), 1).await;
    assert_eq!(Some(1), db.get_json("json_2".into()).await);
    db.set_json("json_3".into(), "3").await;
    assert_eq!(Some(String::from("3")), db.get_json("json_3".into()).await);
}
