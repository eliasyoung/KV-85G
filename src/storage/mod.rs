mod memory;
mod rocks;
mod sleddb;

use crate::{KvError, Kvpair, Value};
pub use memory::MemTable;
pub use rocks::RocksDB;
pub use sleddb::SledDb;

pub trait Storage {
    /// 从一个 HashTable 里获取一个 key 的 value
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 从一个 HashTable 里设置一个 key 的 value，返回旧的 value
    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError>;
    /// 查看 HashTable 中是否有 key
    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError>;
    /// 从 HashTable 中删除一个 key
    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError>;
    /// 遍历 HashTable，返回所有 kv pair（这个接口不好）
    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError>;
    /// 遍历 HashTable，返回 kv pair 的 Iterator
    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError>;
}

pub struct StorageIter<T> {
    data: T,
}

impl<T> StorageIter<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }
}

impl<T> Iterator for StorageIter<T>
where
    T: Iterator,
    T::Item: Into<Kvpair>,
{
    type Item = Kvpair;

    fn next(&mut self) -> Option<Self::Item> {
        self.data.next().map(|v| v.into())
    }
}

pub(crate) fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn memtable_basic_interface_should_work() {
        let store = MemTable::new();
        test_basic_interface(store);
    }

    #[test]
    fn memtable_get_all_should_work() {
        let store = MemTable::new();
        test_get_all(store);
    }

    #[test]
    fn memtable_iter_should_work() {
        let store = MemTable::new();
        test_get_iter(store);
    }

    #[test]
    fn sleddb_basic_interface_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_basic_interface(store);
    }

    #[test]
    fn sleddb_get_all_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_all(store);
    }

    #[test]
    fn sleddb_iter_should_work() {
        let dir = tempdir().unwrap();
        let store = SledDb::new(dir);
        test_get_iter(store);
    }

    #[test]
    fn rocksdb_basic_interface_should_work() {
        let dir = tempdir().unwrap();
        let store = RocksDB::new(dir);
        test_basic_interface(store);
    }

    #[test]
    fn rocksdb_get_all_should_work() {
        let dir = tempdir().unwrap();
        let store = RocksDB::new(dir);
        test_get_all(store);
    }

    #[test]
    fn rocksdb_iter_should_work() {
        let dir = tempdir().unwrap();
        let store = RocksDB::new(dir);
        test_get_iter(store);
    }

    fn test_basic_interface(store: impl Storage) {
        // Call set() first time will create table {{t1}}, insert the key and return None since there is no value before.
        // set() will return previous value of the key.
        let v = store.set("t1", "hello".into(), "world".into());
        assert!(v.unwrap().is_none());
        // Call set() again with same table and key will update the value of the key and return previous value.
        let v1 = store.set("t1", "hello".into(), "world1".into());
        assert_eq!(v1.unwrap(), Some("world".into()));

        // Call get() for existed key will return the latest value of the key.
        let v = store.get("t1", "hello");
        assert_eq!(v.unwrap(), Some("world1".into()));

        // Call get() for key/table not exist will return None.
        assert_eq!(None, store.get("t2", "hello").unwrap());
        assert_eq!(None, store.get("t1", "hello1").unwrap());
        assert!(store.get("t1", "hello1").unwrap().is_none());

        // Call contains() for key existed will return true, else will return false
        assert_eq!(store.contains("t1", "hello").unwrap(), true);
        assert_eq!(store.contains("t1", "hello1").unwrap(), false);
        assert_eq!(store.contains("t2", "hello").unwrap(), false);

        // Call del() to delete a existed key will return the deleted value.
        let v = store.del("t1", "hello");
        assert_eq!(v.unwrap(), Some("world1".into()));

        // Call del() to delete a non-existed key/table will return None
        assert_eq!(None, store.del("t1", "hello1").unwrap());
        assert_eq!(None, store.del("t2", "hello").unwrap());
    }

    fn test_get_all(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();
        store.set("t1", "k3".into(), "v3".into()).unwrap();
        let mut data = store.get_all("t2").unwrap();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        )
    }

    fn test_get_iter(store: impl Storage) {
        store.set("t2", "k1".into(), "v1".into()).unwrap();
        store.set("t2", "k2".into(), "v2".into()).unwrap();
        let mut data: Vec<_> = store.get_iter("t2").unwrap().collect();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap());
        assert_eq!(
            data,
            vec![
                Kvpair::new("k1", "v1".into()),
                Kvpair::new("k2", "v2".into())
            ]
        )
    }
}
