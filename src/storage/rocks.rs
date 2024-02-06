// implementation of using rocksdb

use rocksdb::{DB, Error};
use std::{convert::TryInto, path::Path};
use crate::{KvError, Kvpair, Storage, Value};

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
}

fn flip<T, E>(x: Option<Result<T, E>>) -> Result<Option<T>, E> {
    x.map_or(Ok(None), |v| v.map(Some))
}

impl Storage for RocksDB {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let name = RocksDB::get_full_key(table, key);
        let result: Option<Result<Value, KvError>> = self.0.get(name.as_bytes())?.map(|v| v.as_slice().try_into());
        flip(result)
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let name = RocksDB::get_full_key(table, &key);
        let data: Vec<u8> = value.try_into()?;
        let result = self.0.put(&name.as_bytes(), data).is_ok();
        todo!()
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        todo!()
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        todo!()
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        todo!()
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item=Kvpair>>, KvError> {
        todo!()
    }
}