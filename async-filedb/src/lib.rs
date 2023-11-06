use async_channel as mpsc;
pub use async_kvdb::*;
use async_memorydb::MenoryDb;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::{fs, io, thread};

fn key2file(key: Key) -> String {
    form_urlencoded::Serializer::new(String::new()).append_key_only(&key).finish()
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
            let mut f = fs::File::create(path)?;
            let _ = f.write_all(&value);
            #[cfg(feature = "auto_sync")]
            {
                let _ = f.sync_data();
            }
        }
        DBOp::InsertMany { data } => {
            for (key, value) in data {
                let name = key2file(key);
                let path = path.join(name);
                let mut f = fs::File::create(path)?;
                let _ = f.write_all(&value);
                #[cfg(feature = "auto_sync")]
                {
                    let _ = f.sync_data();
                }
            }
        }
        DBOp::Delete { key } => {
            let name = key2file(key);
            path.push(name);
            let _ = fs::remove_file(path);
        }
        DBOp::DeleteMany { keys } => {
            for entry in fs::read_dir(path)? {
                let file = entry?.path();
                if file.is_file() {
                    if let Some(name) = file.file_name() {
                        let name = name.to_string_lossy();
                        if keys.contains(&name.into()) {
                            let _ = fs::remove_file(file);
                        }
                    }
                }
            }
        }
        DBOp::DeleteAll => {
            for entry in fs::read_dir(path)? {
                let file = entry?.path();
                if file.is_file() {
                    let _ = fs::remove_file(file);
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
        let (tx, receiver) = mpsc::unbounded();
        thread::Builder::new().name(format!("db-{}", path)).spawn(move || {
            let mut op_merger = DbOpMerger::new();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(100));
                while let Ok(op) = receiver.try_recv() {
                    op_merger.merge(op);
                }
                if op_merger.is_empty(){
                    continue;
                }
                let ops = op_merger.into_ops();
                op_merger = DbOpMerger::new();
                if ops.clear {
                    let _ = fsdb_exec(&path, DBOp::DeleteAll);
                }
                let _ = fsdb_exec(&path, DBOp::InsertMany { data: ops.insert });
                let _ = fsdb_exec(&path, DBOp::DeleteMany { keys: ops.delete });
            }
        })?;
        Ok(Self {
            mem: MenoryDb::new(mem),
            write_ch: tx,
        })
    }
}

#[async_trait]
impl Kvdb for FileDb {
    async fn scan_keys(&self, filter: &Filter) -> Vec<Key> {
        self.mem.scan_keys(filter).await
    }
    async fn get(&self, key: Key) -> Option<Value> {
        self.mem.get(key).await
    }
    async fn get_many(&self, keys: Vec<Key>) -> HashMap<Key, Value> {
        self.mem.get_many(keys).await
    }
    async fn set(&self, key: Key, value: Value) {
        self.mem.set(key.clone(), value.clone()).await;
        let _ = self.write_ch.send(DBOp::Insert { key, value }).await;
    }
    async fn set_many(&self, data: HashMap<Key, Value>) {
        self.mem.set_many(data.clone()).await;
        let _ = self.write_ch.send(DBOp::InsertMany { data }).await;
    }
    async fn delete(&self, key: Key) {
        self.mem.delete(key.clone()).await;
        let _ = self.write_ch.send(DBOp::Delete { key }).await;
    }
    async fn delete_many(&self, keys: Vec<Key>) {
        self.mem.delete_many(keys.clone()).await;
        let _ = self.write_ch.send(DBOp::DeleteMany { keys }).await;
    }
    async fn delete_all(&self) {
        self.mem.delete_all().await;
        let _ = self.write_ch.send(DBOp::DeleteAll).await;
    }
}
