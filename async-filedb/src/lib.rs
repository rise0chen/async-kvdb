use async_channel as mpsc;
use async_kvdb::*;
use async_memorydb::MenoryDb;
use std::collections::HashMap;
use std::path::PathBuf;
use std::{fs, io, thread};

fn key2file(key: Key) -> String {
    form_urlencoded::Serializer::new(String::new())
        .append_key_only(&key)
        .finish()
}
fn file2key(name: &str) -> Option<Key> {
    let mut parse = form_urlencoded::parse(name.as_bytes());
    let data = parse.next()?;
    Some(data.0.into())
}

fn fsdb_exec(path: &str, op: DBOp) -> io::Result<()> {
    let mut path = PathBuf::from(path);
    match op {
        DBOp::Insert { key, value } => {
            let name = key2file(key);
            path.push(name);
            fs::write(path, &*value)?;
        }
        DBOp::InsertMany { data } => {
            for (key, value) in data {
                let name = key2file(key);
                let path = path.join(name);
                fs::write(path, &*value)?;
            }
        }
        DBOp::Delete { key } => {
            let name = key2file(key);
            path.push(name);
            fs::remove_file(path)?;
        }
        DBOp::DeleteAll => {
            for entry in fs::read_dir(path)? {
                let file = entry?.path();
                if file.is_file() {
                    fs::remove_file(file)?;
                }
            }
        }
        DBOp::DeletePrefix { prefix } => {
            let prefix = key2file(prefix);
            if prefix.is_empty() {
                for entry in fs::read_dir(path)? {
                    let file = entry?.path();
                    if file.is_file() {
                        fs::remove_file(file)?;
                    }
                }
            } else {
                for entry in fs::read_dir(path)? {
                    let file = entry?.path();
                    if file.is_file() {
                        if let Some(name) = file.file_name() {
                            let name = name.to_string_lossy();
                            if name.starts_with(&prefix) {
                                fs::remove_file(file)?;
                            }
                        }
                    }
                }
            }
        }
    };
    Ok(())
}

pub struct FileDb {
    mem: MenoryDb,
    write_ch: mpsc::Sender<DBOp>,
}
impl FileDb {
    pub fn new(path: String) -> io::Result<Self> {
        let mut mem = HashMap::new();
        fs::create_dir_all(&path)?;
        for entry in fs::read_dir(&path)? {
            let file = entry?.path();
            if file.is_file() {
                if let Some(name) = file.file_name() {
                    let name = name.to_string_lossy().into_owned();
                    let key = if let Some(key) = file2key(&name) {
                        key
                    } else {
                        continue;
                    };
                    let value = fs::read(file)?;
                    mem.insert(key, value.into());
                }
            }
        }
        let (tx, receiver) = mpsc::bounded(128);
        thread::Builder::new()
            .name(format!("db-{}", path))
            .spawn(move || {
                while let Ok(op) = receiver.recv_blocking() {
                    if let Err(err) = fsdb_exec(&path, op) {
                        log::error!("db({}) failed exec: {:?}", path, err);
                    }
                }
                log::error!("fsdb exit");
            })?;
        Ok(Self {
            mem: MenoryDb::new(mem),
            write_ch: tx,
        })
    }
}
impl FileDb {
    pub async fn get(&self, key: Key) -> Option<Value> {
        self.mem.get(key).await
    }
    pub async fn get_all(&self) -> HashMap<Key, Value> {
        self.mem.get_all().await
    }
    pub async fn get_with_prefix(&self, prefix: Key) -> HashMap<Key, Value> {
        self.mem.get_with_prefix(prefix).await
    }
    pub async fn set(&self, key: Key, value: Value) {
        self.mem.set(key.clone(), value.clone()).await;
        let _ = self.write_ch.send(DBOp::Insert { key, value }).await;
    }
    pub async fn set_many(&self, data: HashMap<Key, Value>) {
        self.mem.set_many(data.clone()).await;
        let _ = self.write_ch.send(DBOp::InsertMany { data }).await;
    }
    pub async fn delete(&self, key: Key) {
        self.mem.delete(key.clone()).await;
        let _ = self.write_ch.send(DBOp::Delete { key }).await;
    }
    pub async fn delete_all(&self) {
        self.mem.delete_all().await;
        let _ = self.write_ch.send(DBOp::DeleteAll).await;
    }
    pub async fn delete_with_prefix(&self, prefix: Key) {
        self.mem.delete_with_prefix(prefix.clone()).await;
        let _ = self.write_ch.send(DBOp::DeletePrefix { prefix }).await;
    }
}
