use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};
use crate::sql::engine::{Engine, Transaction};
use crate::sql::schema::Table;
use crate::sql::types::Row;
use crate::storage::{self,engine::Engine as storageEngine};  // self 即指 crate::storage

// KV engine 定义
pub struct KVEngine<E:storageEngine> {
    pub kv : storage::mvcc::Mvcc<E>
}

impl<E:storageEngine> Clone for KVEngine<E> {
    fn clone(&self) -> Self {
        Self{kv: self.kv.clone()}
    }
}

impl<E:storageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(
            Self::Transaction::new(self.kv.begin()?)
        )
    }
}

// 封装存储引擎中的MvccTransaction
pub struct KVTransaction<E:storageEngine>{
    transaction : storage::mvcc::MvccTransaction<E>
}

impl<E:storageEngine> KVTransaction<E>{
    pub fn new(transaction: storage::mvcc::MvccTransaction<E>) -> Self {
        Self{transaction}
    }
}

impl<E:storageEngine> Transaction for KVTransaction<E> {
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
        // 1. 判断表是否存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" already exists", table.name.clone())))
        }

        // 2. 判断表的有效性
        if table.columns.is_empty() {
            return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" has no columns", table.name.clone())))
        }

        // 创建表成功，调用存储引擎存储
        // String -> 序列化 -> bincode
        let key = Key::Table(table.name.clone());
        let bin_code_key = bincode::serialize(&key)?;
        let value = bincode::serialize(&table)?;
        self.transaction.set(bin_code_key, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        let bin_code_key = bincode::serialize(&key)?;
        let value = self.transaction.get(bin_code_key)?.map(
            |value| bincode::deserialize(&value)
        ).transpose()?;
        Ok(value)
    }
}

// 辅助方法：由于底层的存储的传入参数都是 u8, 用户给的字符串需要进行转换
#[derive(Debug,Serialize,Deserialize)]
enum Key{
    Table(String),
    Row(String,String),   // (table_name, primary_key)
}