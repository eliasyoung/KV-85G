// implementation of using rocksdb

use crate::{flip, KvError, Kvpair, Storage, Value};
use rocksdb::{Error, DB, ReadOptions, IteratorMode, Direction};
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
            Some(value) => {
                Some(value.as_slice().try_into()?)
            },
            None => None
        };
        self.0.put(&name.as_bytes(), data)?;
        // let result: Value = self.0.get(&name.as_bytes())?.map(|v| v.as_slice().try_into()).unwrap()?;
        Ok(previous_value)
        // last value 为之前的值，不是当前值。
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

        let iter= self.0.iterator_opt(IteratorMode::From(&prefix.as_bytes(), Direction::Forward), get_options);
        let mut result: Vec<Kvpair> = Vec::new();
        for item in iter {
            result.push(item.unwrap().into())
        }
        Ok(result)
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        todo!()
    }
}

impl From<(Box<[u8]>, Box<[u8]>)> for Kvpair {
    fn from(value: (Box<[u8]>, Box<[u8]>)) -> Self {
        Self::new(str::from_utf8(&*value.0).unwrap(), str::from_utf8(&*value.1).unwrap().into())
    }
}