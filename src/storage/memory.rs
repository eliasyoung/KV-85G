use crate::{KvError, Kvpair, Storage, Value};
use dashmap::{mapref::one::Ref, DashMap};

// Use Dashmap build MemTable, which impled Storage trait
#[derive(Clone, Debug, Default)]
pub struct MemTable {
    tables: DashMap<String, DashMap<String, Value>>,
}

impl MemTable {
    // Create a default MemTable
    pub fn new() -> Self {
        Self::default()
    }

    // If hash table {{ name }} not existed, create it. Else return the {{ name }} hash table
    fn get_or_create_table(&self, name: &str) -> Ref<String, DashMap<String, Value>> {
        if let Some(table) = self.tables.get(name) {
            table
        } else {
            let entry = self.tables.entry(name.into()).or_default();
            entry.downgrade()
        }
    }
}

impl Storage for MemTable {
    fn get(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.get(key).map(|v| v.value().clone()))
    }

    fn set(&self, table: &str, key: String, value: Value) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.insert(key, value))
    }

    fn contains(&self, table: &str, key: &str) -> Result<bool, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table.contains_key(key))
    }

    fn del(&self, table: &str, key: &str) -> Result<Option<Value>, KvError> {
        let table = self.get_or_create_table(table);
        // Ok(table.remove(key).map(|v| v.1))
        Ok(table.remove(key).map(|(_key, value)| value))
    }

    fn get_all(&self, table: &str) -> Result<Vec<Kvpair>, KvError> {
        let table = self.get_or_create_table(table);
        Ok(table
            .iter()
            .map(|v| Kvpair::new(v.key(), v.value().clone()))
            .collect())
    }

    fn get_iter(&self, table: &str) -> Result<Box<dyn Iterator<Item = Kvpair>>, KvError> {
        todo!()
    }
}
