// implementation of using rocksdb

use crate::{flip, KvError, Kvpair, Storage, StorageIter, Value};
use rocksdb::{Direction, IteratorMode, ReadOptions, DB};
use std::{convert::TryInto, path::Path, str};

#[derive(Debug)]
pub struct RocksDB(DB);

impl RocksDB {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self(DB::open_default(path).unwrap())
    }

    fn get_full_key(table: &str, key: &str) -> String {
        format!("{}:{}", table, key)
    }

    fn get_table_prefix(table: &str) -> String {
        format!("{}:", table)
    }

    fn get_key_only(full_key: Box<[u8]>, table: &str) -> String {
        let key = &*full_key;
        let key = str::from_utf8(key).unwrap();
        let start_index = key.find(table).unwrap();
        let end_index = table.len();
        key[start_index + end_index..].to_string()
    }
}

impl Storage for RocksDB {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = RocksDB::get_full_key(table, key);
        let result = self
            .0
            .get(name.as_bytes())?
            .map(|v| v.as_slice().try_into());
        flip(result)
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let name = RocksDB::get_full_key(table, &key);
        let data: Vec<u8> = value.try_into()?;
        let previous_value: Option<Value> = match self.0.get(&name.as_bytes())? {
            Some(value) => Some(value.as_slice().try_into()?),
            None => None,
        };
        self.0.put(&name.as_bytes(), data)?;
        Ok(previous_value)
        // last value is the one before put, not the one currently putting
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let name = RocksDB::get_full_key(table, &key);
        let result = self.0.get(&name.as_bytes())?;
        match result {
            Some(_) => Ok(true),
            None => Ok(false),
        }
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = RocksDB::get_full_key(table, &key);

        let value = self.get(table, key)?;
        self.0.delete(&name.as_bytes())?;
        Ok(value)
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let prefix = RocksDB::get_table_prefix(table);
        let mut get_options = ReadOptions::default();
        get_options.set_prefix_same_as_start(true);

        let result = rocks_scan_prefix(&self, &prefix, get_options)?;
        Ok(result)
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        let prefix = RocksDB::get_table_prefix(table);
        let mut get_options = ReadOptions::default();
        get_options.set_prefix_same_as_start(true);

        let result = StorageIter::new(rocks_scan_prefix(&self, &prefix, get_options)?.into_iter());
        Ok(Box::new(result))
    }
}

impl<T> From<(T, Box<[u8]>)> for Kvpair
where
    T: Into<String>,
{
    fn from(value: (T, Box<[u8]>)) -> Self {
        let (key, value) = (value.0, value.1);
        Kvpair::new(key, (&*value).try_into().unwrap())
    }
}

fn rocks_scan_prefix(
    db: &RocksDB,
    prefix: &str,
    read_options: ReadOptions,
) -> Result<Vec<Kvpair>, KvError> {
    let db_iter = db.0.iterator_opt(
        IteratorMode::From(prefix.as_bytes(), Direction::Forward),
        read_options,
    );

    let mut vec: Vec<Kvpair> = Vec::new();

    for item in db_iter {
        let (key, value) = item.unwrap();
        vec.push((RocksDB::get_key_only(key, &prefix), value).into());
    }

    Ok(vec)
}
// impl From<(Box<[u8]>, Box<[u8]>)> for Kvpair {
//     fn from(value: (Box<[u8]>, Box<[u8]>)) -> Self {
//         let (key, value) = (&*value.0, &*value.1);
//         Kvpair::new(str::from_utf8(key).unwrap(), value.try_into().unwrap())
//     }
// }
