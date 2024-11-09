use crate::error::Result;
use crate::sql::engine::{Engine, Transaction};
use crate::sql::schema::Table;
use crate::sql::types::Row;
use crate::storage;

// KV engine 定义
pub struct KVEngine {
    pub kv : storage::Mvcc
}

impl Clone for KVEngine {
    fn clone(&self) -> Self {
        Self{kv: self.kv.clone()}
    }
}

impl Engine for KVEngine {
    type Transaction = KVTransaction;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(
            Self::Transaction::new(self.kv.begin()?)
        )
    }
}

// 封装存储引擎中的MvccTransaction
pub struct KVTransaction{
    transaction : storage::MvccTransaction
}

impl KVTransaction{
    pub fn new(transaction: storage::MvccTransaction) -> Self {
        Self{transaction}
    }
}

impl Transaction for KVTransaction {
    fn commit(&self) -> Result<()> {
        todo!()
    }

    fn rollback(&self) -> Result<()> {
        todo!()
    }

    fn create_row(&mut self, table: String, row: Row) -> Result<()> {
        todo!()
    }

    fn scan(&self, table_name: String) -> Result<Vec<Row>> {
        todo!()
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        todo!()
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        todo!()
    }
}