use std::os::unix::fs::lchown;
use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};
use crate::sql::engine::{Engine, Transaction};
use crate::sql::parser::ast::Expression;
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
use crate::storage::{self,engine::Engine as storageEngine};
use crate::storage::keyencode::serialize_key;
// self 即指 crate::storage

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
        self.transaction.commit()
    }

    fn rollback(&self) -> Result<()> {
        self.transaction.rollback()
    }

    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table_name.clone())?;
        // 插入行数据的数据类型检查
        for (i,col) in table.columns.iter().enumerate() {
            match row[i].get_datatype() {
                None if col.nullable => continue,
                None => return Err(Error::Internal(format!("[Insert Table] Column \" {} \" cannot be null",col.name))),
                Some(datatype) if datatype != col.datatype => return Err(Error::Internal(format!("[Insert Table] Column \" {} \" mismatched data type",col.name))),
                _ => continue,
            }
        }

        let primary_key = table.get_primary_key(&row)?;
        let key = Key::Row(table.name.clone(), primary_key.clone()).encode()?;

        // 如果主键已经存在，则报冲突
        if self.transaction.get(key.clone())?.is_some(){
            return Err(Error::Internal(format!("[Insert Table] Primary Key \" {} \" conflicted in table \" {} \"", primary_key, table_name)));
        }

        // 存放数据
        let value = bincode::serialize(&row)?;
        self.transaction.set(key, value)?;
        Ok(())
    }

    fn update_row(&mut self, table: &Table, primary_key: &Value, row: Row) -> Result<()> {
        // 对比主键是否修改，是则删除原key，建立新key
        let new_primary_key = table.get_primary_key(&row)?;
        if new_primary_key != *primary_key{
            let key = Key::Row(table.name.clone(), primary_key.clone()).encode()?;
            self.transaction.delete(key)?;
        }

        let key = Key::Row(table.name.clone(), new_primary_key.clone()).encode()?;
        let value = bincode::serialize(&row)?;
        self.transaction.set(key, value)?;
        Ok(())
    }

    fn scan(&self, table_name: String, filter: Option<(String, Expression)>) -> Result<Vec<Row>> {
        let table = self.must_get_table(table_name.clone())?;
        // 根据前缀扫描表
        let prefix = PrefixKey::Row(table_name.clone()).encode()?;
        let results = self.transaction.prefix_scan(prefix)?;

        let mut rows = Vec::new();
        for res in results {
            // 根据filter过滤数据
            let row: Row = bincode::deserialize(&res.value)?;
            if let Some((col, expression)) = &filter {
                let col_index = table.get_col_index(col)?;
                if Value::from_expression_to_value(expression.clone()) == row[col_index].clone(){
                    // 过滤where的条件和这里的列数据是否一致
                    rows.push(row);
                }
            }else{
                // filter不存在，查找所有数据
                rows.push(row);
            }
        }
        Ok(rows)
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        // 判断表是否存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" already exists", table.name.clone())))
        }

        // 判断表是否有效
        table.is_valid()?;

        // 创建表成功，调用存储引擎存储
        let key = Key::Table(table.name.clone()).encode()?;
        let value = bincode::serialize(&table)?;
        self.transaction.set(key, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name).encode()?;
        let value = self.transaction.get(key)?.map(
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

impl Key{
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

// 辅助枚举，用于前缀扫描
#[derive(Debug,Serialize,Deserialize)]
enum PrefixKey {
    Table,  // 存的时候Table是第0个枚举，Row是第一个枚举，如果这里没有Table的话，扫描的时候是对不上的，所以要Table进行占位
    Row(String)
}

impl PrefixKey{
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
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

        s.execute("create table t1 (a int primary key, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        s.execute("select * from t1;")?;

        Ok(())
    }

    #[test]
    fn test_update() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute(
            "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
        )?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b', 2);")?;
        s.execute("insert into t1 values(3, 'c', 3);")?;

        let v = s.execute("update t1 set b = 'aa' where a = 1;")?;
        let v = s.execute("update t1 set a = 33 where a = 3;")?;
        println!("{:?}", v);

        match s.execute("select * from t1;")? {
            crate::sql::executor::ResultSet::Scan { columns, rows } => {
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
