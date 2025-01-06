use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};
use crate::sql::engine::{Engine, Transaction};
use crate::sql::parser::ast::{parse_expression, Expression};
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

    fn delete_row(&mut self, table: &Table, primary_key: &Value) -> Result<()> {
        let key = Key::Row(table.name.clone(), primary_key.clone()).encode()?;
        self.transaction.delete(key)
    }

    fn scan(&self, table_name: String, filter: Option<Expression>) -> Result<Vec<Row>> {
        let table = self.must_get_table(table_name.clone())?;
        // 根据前缀扫描表
        let prefix = PrefixKey::Row(table_name.clone()).encode()?;
        let results = self.transaction.prefix_scan(prefix)?;

        let mut rows = Vec::new();
        for res in results {
            // 根据filter过滤数据
            let row: Row = bincode::deserialize(&res.value)?;
            if let Some( expression) = &filter {
                let cols = table.columns.iter().map(|c| c.name.clone()).collect();
                match parse_expression(expression, &cols, &row, &cols, &row)? {
                    Value::Null => {}
                    Value::Boolean(false) => {}
                    Value::Boolean(true) => {
                        rows.push(row);
                    }
                    _ => return Err(Error::Internal("[KV Engine Scan] Unexpected expression".into())),
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

    use super::KVEngine;
    use crate::storage::engine::Engine as StorageEngine;
    use crate::{
        error::Result,
        sql::{
            engine::{Engine, Session},
            executor::ResultSet,
            types::{Row, Value},
        },
        storage::disk::DiskEngine,
    };

    fn setup_table<E: StorageEngine + 'static>(s: &mut Session<KVEngine<E>>) -> Result<()> {
        s.execute(
            "create table t1 (
                     a int primary key,
                     b text default 'vv',
                     c integer default 100
                 );",
        )?;

        s.execute(
            "create table t2 (
                     a int primary key,
                     b integer default 100,
                     c float default 1.1,
                     d bool default false,
                     e boolean default true,
                     f text default 'v1',
                     g string default 'v2',
                     h varchar default 'v3'
                 );",
        )?;

        s.execute(
            "create table t3 (
                     a int primary key,
                     b int default 12 null,
                     c integer default NULL,
                     d float not NULL
                 );",
        )?;

        s.execute(
            "create table t4 (
                     a bool primary key,
                     b int default 12,
                     d boolean default true
                 );",
        )?;
        Ok(())
    }

    fn scan_table_and_compare<E: StorageEngine + 'static>(
        s: &mut Session<KVEngine<E>>,
        table_name: &str,
        expect: Vec<Row>,
    ) -> Result<()> {
        match s.execute(&format!("select * from {};", table_name))? {
            ResultSet::Scan { columns: _, rows } => {
                assert_eq!(rows, expect);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn scan_table_and_print<E: StorageEngine + 'static>(
        s: &mut Session<KVEngine<E>>,
        table_name: &str,
    ) -> Result<()> {
        match s.execute(&format!("select * from {};", table_name))? {
            ResultSet::Scan { columns: _, rows } => {
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[test]
    fn test_create_table() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_insert() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        // t1
        s.execute("insert into t1 (a) values (1);")?;
        s.execute("insert into t1 values (2, 'a', 2);")?;
        s.execute("insert into t1(b,a) values ('b', 3);")?;

        scan_table_and_compare(
            &mut s,
            "t1",
            vec![
                vec![
                    Value::Integer(1),
                    Value::String("vv".to_string()),
                    Value::Integer(100),
                ],
                vec![
                    Value::Integer(2),
                    Value::String("a".to_string()),
                    Value::Integer(2),
                ],
                vec![
                    Value::Integer(3),
                    Value::String("b".to_string()),
                    Value::Integer(100),
                ],
            ],
        )?;

        // t2
        s.execute("insert into t2 (a) values (1);")?;
        scan_table_and_compare(
            &mut s,
            "t2",
            vec![vec![
                Value::Integer(1),
                Value::Integer(100),
                Value::Float(1.1),
                Value::Boolean(false),
                Value::Boolean(true),
                Value::String("v1".to_string()),
                Value::String("v2".to_string()),
                Value::String("v3".to_string()),
            ]],
        )?;

        // t3
        s.execute("insert into t3 (a, d) values (1, 1.1);")?;
        scan_table_and_compare(
            &mut s,
            "t3",
            vec![vec![
                Value::Integer(1),
                Value::Integer(12),
                Value::Null,
                Value::Float(1.1),
            ]],
        )?;

        // t4
        s.execute("insert into t4 (a) values (true);")?;
        scan_table_and_compare(
            &mut s,
            "t4",
            vec![vec![
                Value::Boolean(true),
                Value::Integer(12),
                Value::Boolean(true),
            ]],
        )?;

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_update() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t2 values (1, 1, 1.1, true, true, 'v1', 'v2', 'v3');")?;
        s.execute("insert into t2 values (2, 2, 2.2, false, false, 'v4', 'v5', 'v6');")?;
        s.execute("insert into t2 values (3, 3, 3.3, true, false, 'v7', 'v8', 'v9');")?;
        s.execute("insert into t2 values (4, 4, 4.4, false, true, 'v10', 'v11', 'v12');")?;

        let res = s.execute("update t2 set b = 100 where a = 1;")?;
        assert_eq!(res, ResultSet::Update { count: 1 });
        let res = s.execute("update t2 set d = false where d = true;")?;
        assert_eq!(res, ResultSet::Update { count: 2 });

        scan_table_and_compare(
            &mut s,
            "t2",
            vec![
                vec![
                    Value::Integer(1),
                    Value::Integer(100),
                    Value::Float(1.1),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::String("v1".to_string()),
                    Value::String("v2".to_string()),
                    Value::String("v3".to_string()),
                ],
                vec![
                    Value::Integer(2),
                    Value::Integer(2),
                    Value::Float(2.2),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::String("v4".to_string()),
                    Value::String("v5".to_string()),
                    Value::String("v6".to_string()),
                ],
                vec![
                    Value::Integer(3),
                    Value::Integer(3),
                    Value::Float(3.3),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::String("v7".to_string()),
                    Value::String("v8".to_string()),
                    Value::String("v9".to_string()),
                ],
                vec![
                    Value::Integer(4),
                    Value::Integer(4),
                    Value::Float(4.4),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::String("v10".to_string()),
                    Value::String("v11".to_string()),
                    Value::String("v12".to_string()),
                ],
            ],
        )?;

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t2 values (1, 1, 1.1, true, true, 'v1', 'v2', 'v3');")?;
        s.execute("insert into t2 values (2, 2, 2.2, false, false, 'v4', 'v5', 'v6');")?;
        s.execute("insert into t2 values (3, 3, 3.3, true, false, 'v7', 'v8', 'v9');")?;
        s.execute("insert into t2 values (4, 4, 4.4, false, true, 'v10', 'v11', 'v12');")?;

        let res = s.execute("delete from t2 where a = 1;")?;
        assert_eq!(res, ResultSet::Delete { count: 1 });
        scan_table_and_compare(
            &mut s,
            "t2",
            vec![
                vec![
                    Value::Integer(2),
                    Value::Integer(2),
                    Value::Float(2.2),
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::String("v4".to_string()),
                    Value::String("v5".to_string()),
                    Value::String("v6".to_string()),
                ],
                vec![
                    Value::Integer(3),
                    Value::Integer(3),
                    Value::Float(3.3),
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::String("v7".to_string()),
                    Value::String("v8".to_string()),
                    Value::String("v9".to_string()),
                ],
                vec![
                    Value::Integer(4),
                    Value::Integer(4),
                    Value::Float(4.4),
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::String("v10".to_string()),
                    Value::String("v11".to_string()),
                    Value::String("v12".to_string()),
                ],
            ],
        )?;

        let res = s.execute("delete from t2 where d = false;")?;
        assert_eq!(res, ResultSet::Delete { count: 2 });
        scan_table_and_compare(
            &mut s,
            "t2",
            vec![vec![
                Value::Integer(3),
                Value::Integer(3),
                Value::Float(3.3),
                Value::Boolean(true),
                Value::Boolean(false),
                Value::String("v7".to_string()),
                Value::String("v8".to_string()),
                Value::String("v9".to_string()),
            ]],
        )?;

        let res = s.execute("delete from t2;")?;
        assert_eq!(res, ResultSet::Delete { count: 1 });
        scan_table_and_compare(&mut s, "t2", vec![])?;

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_sort() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        setup_table(&mut s)?;

        s.execute("insert into t3 values (1, 34, 22, 1.22);")?;
        s.execute("insert into t3 values (4, 23, 65, 4.23);")?;
        s.execute("insert into t3 values (3, 56, 22, 2.88);")?;
        s.execute("insert into t3 values (2, 87, 57, 6.78);")?;
        s.execute("insert into t3 values (5, 87, 14, 3.28);")?;
        s.execute("insert into t3 values (7, 87, 82, 9.52);")?;

        match s.execute("select a, b as col2 from t3 order by c, a desc limit 100;")? {
            ResultSet::Scan { columns, rows } => {
                for col in columns {
                    print!("{} ", col);
                }
                println!();
                println!("-----------");
                for r in rows {
                    println!("{:?}", r);
                }
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_cross_join() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key);")?;
        s.execute("create table t2 (b int primary key);")?;
        s.execute("create table t3 (c int primary key);")?;

        s.execute("insert into t1 values (1), (2), (3);")?;
        s.execute("insert into t2 values (4), (5), (6);")?;
        s.execute("insert into t3 values (7), (8), (9);")?;

        match s.execute("select * from t1 cross join t2 cross join t3;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(3, columns.len());
                assert_eq!(27, rows.len());
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_join() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key);")?;
        s.execute("create table t2 (b int primary key);")?;
        s.execute("create table t3 (c int primary key);")?;

        s.execute("insert into t1 values (1), (2), (3);")?;
        s.execute("insert into t2 values (2), (3), (4);")?;
        s.execute("insert into t3 values (3), (8), (9);")?;

        match s.execute("select * from t1 left join t2 on a = b join t3 on a = c;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(3, columns.len());
                assert_eq!(1, rows.len());
                for row in rows {
                    println!("{:?}", row);
                }
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_agg() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key, b text, c float);")?;

        s.execute("insert into t1 values (1, 'aa', 3.1);")?;
        s.execute("insert into t1 values (2, 'cc', 5.3);")?;
        s.execute("insert into t1 values (3, null, NULL);")?;
        s.execute("insert into t1 values (4, 'dd', 4.6);")?;

        match s.execute("select count(a) as total, max(b), min(a), sum(c), avg(c) from t1;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns, vec!["total", "max", "min", "sum", "avg"]);
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Integer(4),
                        Value::String("dd".to_string()),
                        Value::Integer(1),
                        Value::Float(13.0),
                        Value::Float(13.0 / 3.0)
                    ]]
                );
            }
            _ => unreachable!(),
        }

        s.execute("create table t2 (a int primary key, b text, c float);")?;
        s.execute("insert into t2 values (1, NULL, NULL);")?;
        s.execute("insert into t2 values (2, NULL, NULL);")?;
        match s.execute("select count(a) as total, max(b), min(a), sum(c), avg(c) from t2;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns, vec!["total", "max", "min", "sum", "avg"]);
                assert_eq!(
                    rows,
                    vec![vec![
                        Value::Integer(2),
                        Value::Null,
                        Value::Integer(1),
                        Value::Null,
                        Value::Null
                    ]]
                );
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_group_by() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key, b text, c float);")?;

        s.execute("insert into t1 values (1, 'aa', 3.1);")?;
        s.execute("insert into t1 values (2, 'bb', 5.3);")?;
        s.execute("insert into t1 values (3, null, NULL);")?;
        s.execute("insert into t1 values (4, null, 4.6);")?;
        s.execute("insert into t1 values (5, 'bb', 5.8);")?;
        s.execute("insert into t1 values (6, 'dd', 1.4);")?;

        match s.execute("select b, min(c), max(a), avg(c) from t1 group by b order by avg;")? {
            ResultSet::Scan { columns, rows } => {
                assert_eq!(columns, vec!["b", "min", "max", "avg"]);
                assert_eq!(
                    rows,
                    vec![
                        vec![
                            Value::String("dd".to_string()),
                            Value::Float(1.4),
                            Value::Integer(6),
                            Value::Float(1.4)
                        ],
                        vec![
                            Value::String("aa".to_string()),
                            Value::Float(3.1),
                            Value::Integer(1),
                            Value::Float(3.1)
                        ],
                        vec![
                            Value::Null,
                            Value::Float(4.6),
                            Value::Integer(4),
                            Value::Float(4.6)
                        ],
                        vec![
                            Value::String("bb".to_string()),
                            Value::Float(5.3),
                            Value::Integer(5),
                            Value::Float(5.55)
                        ],
                    ]
                );
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_filter() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut s = kvengine.session()?;
        s.execute("create table t1 (a int primary key, b text, c float, d bool);")?;

        s.execute("insert into t1 values (1, 'aa', 3.1, true);")?;
        s.execute("insert into t1 values (2, 'bb', 5.3, true);")?;
        s.execute("insert into t1 values (3, null, NULL, false);")?;
        s.execute("insert into t1 values (4, null, 4.6, false);")?;
        s.execute("insert into t1 values (5, 'bb', 5.8, true);")?;
        s.execute("insert into t1 values (6, 'dd', 1.4, false);")?;

        match s.execute("select * from t1 where d < true;")? {
            ResultSet::Scan { columns, rows } => {
                // for row in rows {
                //     println!("{:?}", row);
                // }
                assert_eq!(4, columns.len());
                assert_eq!(3, rows.len());
            }
            _ => unreachable!(),
        }

        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
}