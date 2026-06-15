use async_channel as mpsc;
pub use async_kvdb::*;
use async_memorydb::MenoryDb;
use core::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{fs, io, thread};

fn key2file(key: &Key) -> String {
    form_urlencoded::Serializer::new(String::new()).append_key_only(key).finish()
}
fn file2key(name: &str) -> Option<Key> {
    let mut parse = form_urlencoded::parse(name.as_bytes());
    let data = parse.next()?;
    Some(data.0.into())
}

fn load_item(path: &Path, key: &Key) -> Option<Value> {
    let name = key2file(key);
    let path = path.join(name);
    let value: Value = fs::read(path).ok()?.into();
    Some(value)
}
fn load_all(path: &Path) -> HashMap<Key, Value> {
    let Ok(entrys) = fs::read_dir(path) else {
        return HashMap::new();
    };
    entrys
        .filter_map(|entry| {
            let file = entry.ok()?.path();
            if !file.is_file() {
                return None;
            }
            let name = file.file_name()?;
            let name = name.to_string_lossy().into_owned();
            let key = file2key(&name)?;
            let value = fs::read(file).ok()?;
            Some((key, value.into()))
        })
        .collect()
}

fn fsdb_exec(path: &Path, op: DbOp) -> io::Result<()> {
    match op {
        DbOp::Get { key, ch } => {
            let value = load_item(path, &key).unwrap_or_default();
            let _ = ch.send(value);
        }
        DbOp::GetMany { keys, ch } => {
            let values = keys
                .into_iter()
                .filter_map(|key| {
                    let value = load_item(path, &key)?;
                    Some((key, value))
                })
                .collect();
            let _ = ch.send(values);
        }
        DbOp::GetAll { ch } => {
            let value = load_all(path);
            let _ = ch.send(value);
        }
        DbOp::Insert { key, value } => {
            let name = key2file(&key);
            let path = path.join(name);
            let mut f = fs::File::create(path)?;
            let _ = f.write_all(&value);
            #[cfg(feature = "auto_sync")]
            {
                let _ = f.sync_data();
            }
        }
        DbOp::InsertMany { data } => {
            for (key, value) in data {
                let name = key2file(&key);
                let path = path.join(name);
                let mut f = fs::File::create(path)?;
                let _ = f.write_all(&value);
                #[cfg(feature = "auto_sync")]
                {
                    let _ = f.sync_data();
                }
            }
        }
        DbOp::Delete { key } => {
            let name = key2file(&key);
            let path = path.join(name);
            let _ = fs::remove_file(path);
        }
        DbOp::DeleteMany { keys } => {
            for entry in fs::read_dir(path)? {
                let file = entry?.path();
                if file.is_file() {
                    if let Some(name) = file.file_name() {
                        let name = name.to_string_lossy();
                        if keys.iter().any(|k| key2file(k) == name) {
                            let _ = fs::remove_file(file);
                        }
                    }
                }
            }
        }
        DbOp::DeleteAll => {
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
    cached_all: AtomicBool,
    mem: MenoryDb,
    write_ch: mpsc::Sender<DbOp>,
}
impl FileDb {
    /// 加载本地文件数据库
    /// path: 本地文件夹
    /// interval_ms: 周期性存到本地硬盘
    pub fn new(path: String, interval_ms: u64) -> io::Result<Self> {
        let thread_name = format!("db-{}", &path[path.len().saturating_sub(12)..]);
        let path = PathBuf::from(path);
        let mem = HashMap::new();
        fs::create_dir_all(&path)?;
        let (tx, receiver) = mpsc::unbounded();
        thread::Builder::new().name(thread_name).spawn(move || {
            let mut op_merger = DbOpMerger::new();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(interval_ms));
                if let Ok(op) = receiver.recv_blocking() {
                    op_merger.merge(op);
                }
                while let Ok(op) = receiver.try_recv() {
                    op_merger.merge(op);
                }
                if op_merger.is_empty() {
                    continue;
                }
                let ops = op_merger.into_ops();
                op_merger = DbOpMerger::new();
                if ops.clear {
                    let _ = fsdb_exec(&path, DbOp::DeleteAll);
                }
                // 增删 全删后的数据
                if !ops.insert.is_empty() {
                    let _ = fsdb_exec(&path, DbOp::InsertMany { data: ops.insert });
                }
                if !ops.delete.is_empty() {
                    let _ = fsdb_exec(&path, DbOp::DeleteMany { keys: ops.delete });
                }
                // 读取最新数据
                for (key, chs) in ops.get_one {
                    let value = load_item(&path, &key).unwrap_or_default();
                    for ch in chs {
                        let _ = ch.send(value.clone());
                    }
                }
                for op in ops.get_many {
                    let _ = fsdb_exec(&path, op);
                }
                for ch in ops.get_all {
                    let value = load_all(&path);
                    let _ = ch.send(value.clone());
                }
            }
        })?;
        Ok(Self {
            cached_all: AtomicBool::new(false),
            mem: MenoryDb::new(mem),
            write_ch: tx,
        })
    }
    pub async fn clear_cache(&self) {
        self.cached_all.store(false, Relaxed);
        self.mem.delete_all().await;
    }
    pub async fn load_all(&self) {
        let (tx, rx) = oneshot::async_channel();
        let _ = self.write_ch.send(DbOp::GetAll { ch: tx }).await;
        let Ok(data) = rx.await else { return };
        self.mem.set_many(data).await;
        self.cached_all.store(true, Relaxed);
    }
}

#[async_trait]
impl Kvdb for FileDb {
    async fn scan_keys(&self, filter: &Filter) -> Vec<Key> {
        if !self.cached_all.load(Relaxed) {
            self.load_all().await;
        }
        self.mem.scan_keys(filter).await
    }
    async fn get(&self, key: Key) -> Option<Value> {
        let ret = self.mem.get(key.clone()).await;
        if ret.is_some() {
            return ret;
        }
        let (tx, rx) = oneshot::async_channel();
        let _ = self.write_ch.send(DbOp::Get { key: key.clone(), ch: tx }).await;
        let value = rx.await.ok()?;
        self.mem.set(key, value.clone()).await;
        Some(value)
    }
    async fn get_many(&self, keys: Vec<Key>) -> HashMap<Key, Value> {
        if !self.cached_all.load(Relaxed) {
            self.load_all().await;
        }
        self.mem.get_many(keys).await
    }
    async fn set(&self, key: Key, value: Value) {
        self.mem.set(key.clone(), value.clone()).await;
        let _ = self.write_ch.send(DbOp::Insert { key, value }).await;
    }
    async fn set_many(&self, data: HashMap<Key, Value>) {
        self.mem.set_many(data.clone()).await;
        let _ = self.write_ch.send(DbOp::InsertMany { data }).await;
    }
    async fn delete(&self, key: Key) {
        self.mem.delete(key.clone()).await;
        let _ = self.write_ch.send(DbOp::Delete { key }).await;
    }
    async fn delete_many(&self, keys: Vec<Key>) {
        self.mem.delete_many(keys.clone()).await;
        let _ = self.write_ch.send(DbOp::DeleteMany { keys }).await;
    }
    async fn delete_all(&self) {
        self.mem.delete_all().await;
        let _ = self.write_ch.send(DbOp::DeleteAll).await;
    }
}
