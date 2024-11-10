use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};
use crate::sql::engine::{Engine, Transaction};
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
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
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    fn create_row(&mut self, table: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table)?;
        // 插入行数据的数据类型检查
        for (i,col) in table.columns.iter().enumerate() {
            match row[i].get_datatype() {
                None if col.nullable => continue,
                None => return Err(Error::Internal(format!("[Insert Table] Column \" {} \" cannot be null",col.name))),
                Some(datatype) if datatype != col.datatype => return Err(Error::Internal(format!("[Insert Table] Column \" {} \" mismatched data type",col.name))),
                _ => continue,
            }
        }
        // 存放数据，这里暂时以第一列为主键
        let key = Key::Row(table.name.clone(), row[0].clone());
        let bin_code_key = bincode::serialize(&key)?;
        let value = bincode::serialize(&row)?;
        self.transaction.set(bin_code_key, value)?;
        Ok(())
    }

    fn scan(&self, table_name: String) -> Result<Vec<Row>> {
        // 根据前缀扫描表
        let prefix = PrefixKey::Row(table_name.clone());
        let results = self.transaction.prefix_scan(bincode::serialize(&prefix)?)?;

        let mut rows = Vec::new();
        for res in results {
            let row: Row = bincode::deserialize(&res.value)?;
            rows.push(row);
        }
        Ok(rows)
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
    Row(String,Value),   // (table_name, primary_key)
}

// 辅助枚举，用于前缀扫描
#[derive(Debug,Serialize,Deserialize)]
enum PrefixKey {
    Table,  // 存的时候Table是第0个枚举，Row是第一个枚举，如果这里没有Table的话，扫描的时候是对不上的，所以要Table进行占位
    Row(String)
}


// new方法定义
impl<E:storageEngine> KVEngine<E>{
    pub fn new(engine:E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute("create table t1 (a int, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        s.execute("select * from t1;")?;

        Ok(())
    }
}
