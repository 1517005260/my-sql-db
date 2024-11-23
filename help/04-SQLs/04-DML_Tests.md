# 基础DML语句测试 (select, insert, update, delete)

先修复两个bug：

1. 当bool为主键时，我们没有实现key_encode的自定义序列化，这里在storage/keyencode.rs中修改：

```rust
fn serialize_bool(self, v: bool) -> Result<()> {
        self.output.push(v as u8);
        Ok(())
}

fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
where
    V: Visitor<'de>
{
    let v = self.take_bytes(1)[0];
    // v == 0 false
    // v == 1 true
    visitor.visit_bool(v != 0)  // v=0 则 v!=0 == false，反之 v!=0 == true
}
```

2. 创建表时的有效性判断不足，没有判断列有效性
- 主键不能为空 
- 列默认值需要和列数据类型匹配

在sql/schema.rs中：

```rust
impl Table{
    pub fn is_valid(&self) -> Result<()>{
        // 判断列是否为空
        if self.columns.is_empty() {
            return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" has no columns", self.name)));
        }

        // 判断主键信息
        match self.columns.iter().filter(|c| c.is_primary_key).count() {
            1 => {},
            0 => return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" has no primary key", self.name))),
            _ => return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" has multiple primary keys", self.name))),
        }

        // 判断列是否有效
        for column in &self.columns {
            // 主键不能空
            if column.is_primary_key && column.nullable {
                return Err(Error::Internal(format!("[CreateTable] Failed, primary key \" {} \" cannot be nullable in table \" {} \"", column.name, self.name)));
            }

            // 列默认值需要和列数据类型匹配
            if let Some(default_value) = &column.default {
                match default_value.get_datatype() {
                    Some(datatype) => {
                        if datatype != column.datatype {
                            return Err(Error::Internal(format!("[CreateTable] Failed, default value type for column \" {} \" mismatch in table \" {} \"", column.name, self.name)))
                        }
                    },
                    None =>{}
                }
            }
        }

        Ok(())
    }
}
```

3. 在kv.rs中测试:

```rust
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
}
```

注意需要在executor/mod.rs中为ResultSet添加注解：

```rust
#[derive(Debug, PartialEq, Clone)]
pub enum ResultSet{}
```

测试发现bug：`Error: Internal("[CreateTable] Failed, primary key \" a \" cannot be nullable in table \" t1 \"")`，查看原sql语句是：`create table t1 (a int primary key);`。我们没有指定a是否为空，而在planner/planner.rs中：

```rust
impl Planner {
    fn build_sentence(&mut self, sentence: Sentence) -> Node{
        match sentence {
            Sentence::CreateTable {name,columns} =>
                Node::CreateTable {
                    schema:Table{
                        name,
                        columns:
                        columns.into_iter().map(|c| {
                            let nullable = c.nullable.unwrap_or(true); // nullable解包出来是None，说明可以为空
                            let default = match c.default {
                                Some(expression) => Some(Value::from_expression_to_value(expression)),
                                None if nullable => Some(Value::Null),  // 如果没写default且可为null，则默认null
                                None => None,
                            };

                            schema::Column{
                                name: c.name,
                                datatype: c.datatype,
                                nullable,
                                default,
                                is_primary_key: c.is_primary_key,
                            }
                        }).collect(),
                    }
                },
            ...
        }
    }
}
```

代码的建表逻辑是：我们如果没有指定列可否为空，则可为空，而主键是不能为空的，所以产生了bug。

所以修改为：

```rust
let nullable = c.nullable.unwrap_or(!c.is_primary_key);  // 如果是主键，则!c.is_primary_key == false，不能为空
```