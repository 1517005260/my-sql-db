# 索引

## 索引简介

索引最适合点读的情况。比如一个表里有1w条数据，但是最后`select`出来的结果只有很少的条数。如果说本来结果就有6、7千条，那么没有索引扫描全表时可以接受的。但是寥寥几条的数据如果没有索引支持，那么就是在大海捞针。

在关系型数据库中，常见的索引有B+树索引，Hash索引等。

B+树数据在存储的时候是天然有序的，能够很好的支持索引顺序扫描，可以在索引的基础上快速实现范围查找。而 Hash 数据结构则在点读的时候能够更加快速的定位到某条记录。

本数据库将基于现有的简单存储引擎，实现类似于Hash的简单索引。

在engine/kv.rs中，我们是这样存储表的：

```rust
#[derive(Debug,Serialize,Deserialize)]
enum Key{
    Table(String),
    Row(String,Value),   // (table_name, primary_key)
}

fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
    // ...
    let primary_key = table.get_primary_key(&row)?;
    let key = Key::Row(table.name.clone(), primary_key.clone()).encode()?;
    // ...
}
```

我们存储表数据的时候，唯一标识是 `Table_name + Primary Key`。

例如：现在有表t，有a、b、c三个字段，a是主键

```
a |b | c
--+--+---
1 |2 |'a'
2 |2 |'b'
3 |3 |'c'
4 |3 |'d'
```

根据我们的存储结构，四行数据的存储形式如下：

```
key        value
[1,t,1]   [1,2,'a']
[1,t,2]   [2,2,'b']
[1,t,3]   [3,3,'c']
[1,t,4]   [4,3,'d']
```

其中，value即一行的内容，key的三个位置分别代表：`[Key枚举中Row的位置, 表名, 主键值]`

若现在有查询：`select * from t where b = 2;`，在没有索引时，只能扫描全表，依次找到b=2的行。

若现在有索引，结构如下：

```rust
#[derive(Debug,Serialize,Deserialize)]
enum Key{
    Table(String),
    Row(String,Value),   
    Index,
}
```

现在数据的存储形式如下：

```
Index_key     Index_value
                  key         value
[2,t,b,2]       [1,t,1]     [1,2,'a']
[2,t,b,2]       [1,t,2]     [2,2,'b']
[2,t,b,3]       [1,t,3]     [3,3,'c']
[2,t,b,3]       [1,t,4]     [4,3,'d'] 
```

其中，Index_Key的元素分别代表 `[Key枚举中Index的位置, 表名, 建立索引的列名, 索引列的值]`

又由于KV对的Hash结构是不允许key重复的，我们可以用Set来维护一个Index_Key中的所有元素。即 `[2,t,b,2]` Key中的元素有：`[1,t,1], [1,t,2]`

Set里的数据肯定是不重复的，因为存储表数据时的Key一定是唯一标识的。

有了索引之后，再去找 `b=x` 的所有行，只要找到索引 `[2,t,b,x]` 即可。从Index_value中，我们可以快速找到 `b=x` 的key，从而不用扫描全表，加速了扫描。

索引的优缺点如下：

- 优点:显著提升查询性能
- 缺点:需要额外存储空间,插入/更新时需要维护索引

## 索引维护

1. 插入行：插入索引即可
2. 删除行：删除索引即可
3. 更新行：

- 如果是主键被更新，那么就可被视为，删除旧Index_value，插入新Index_value，Index_key是不变的
- 如果是索引列被更新，那么比较复杂，需要更新Index_key，见下例：

```
update t set b=3 where a=2;


Index_key     Index_value                             Index_key       Index_value
                  key         value                                       key         value
[2,t,b,2]       [1,t,1]     [1,2,'a']                 [2,t,b,2]         [1,t,1]     [1,2,'a']
[2,t,b,2]       [1,t,2]     [2,2,'b']         ==>     [2,t,b,3(update)] [1,t,2]     [2,3(update),'b'] 
[2,t,b,3]       [1,t,3]     [3,3,'c']                 [2,t,b,3]         [1,t,3]     [3,3,'c']
[2,t,b,3]       [1,t,4]     [4,3,'d']                 [2,t,b,3]         [1,t,4]     [4,3,'d'] 
```

对于更新索引列的情况:
- 从原来的Index_key (例如[2,t,b,2])中移除该行的Index_value 
- 在新的Index_key (例如[2,t,b,3])中添加更新后的Index_value 
- 同时更新原始数据表中的value

## 代码实现

1. 修改types/mod.rs中的列定义：

```rust
#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub struct Column{
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub is_primary_key: bool,
    pub is_index: bool, 
}
```

2. 先暂时将planner/planner.rs中的is_index置为true，防止报错：

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Result<Node>{
    Ok(match sentence {
        Sentence::CreateTable {name,columns} =>
            Node::CreateTable {
                schema:Table{
                    name,
                    columns:
                        columns.into_iter().map(|c| {
                            let nullable = c.nullable.unwrap_or(!c.is_primary_key);
                            let default = match c.default {
                                Some(expression) => Some(Value::from_expression_to_value(expression)),
                                None if nullable => Some(Value::Null),
                                None => None,
                            };

                            schema::Column{
                                name: c.name,
                                datatype: c.datatype,
                                nullable,
                                default,
                                is_primary_key: c.is_primary_key,
                                is_index: true,   // 暂时为true
                            }
                        }).collect(),
                }
            },
    })
}
```

3. 在kv.rs中增加维护索引的方法：

```rust
impl<E:storageEngine> Transaction for KVTransaction<E> {
    fn create_row(&mut self, table_name: String, row: Row) -> Result<()> {
        // ...
        // 存放数据后维护索引
        // 找出有索引的列
        let index_cols = table.columns.iter().enumerate().filter(|(_,c)| c.is_index).collect::<Vec<_>>();
        for (i, index_col) in index_cols {
            let mut index = self.load_index(&table_name, &index_col.name, &row[i])?;
            index.insert(primary_key.clone());  // Index_key已经包含表信息了，而主键是不重复的，所以这里不用存表名
            self.save_index(&table_name, &index_col.name, &row[i] ,index)?
        }
        Ok(())
    }

    fn update_row(&mut self, table: &Table, primary_key: &Value, row: Row) -> Result<()> {
        // 传入的是新row
        // 对比主键是否修改，是则删除原key，建立新key
        let new_primary_key = table.get_primary_key(&row)?;
        if new_primary_key != *primary_key{
            // delete_row和create_row本身就有索引的操作，这里直接调
            self.delete_row(table, primary_key)?;
            self.create_row(table.name.clone(), row)?;
            return Ok(())
        }

        // 修改的不是主键，需要手动维护索引
        let index_cols = table.columns.iter().enumerate().filter(|(_,c)| c.is_index).collect::<Vec<_>>();
        for (i, index_col) in index_cols {
            // 加载旧row
            if let Some(old_row) = self.read_row_by_pk(&table.name, primary_key)?{
                if old_row[i] == row[i] {continue;} // 没有更新索引列

                // 更新了索引列
                // 需要先从旧集合中删除，再加入新集合
                let mut old_index = self.load_index(&table.name, &index_col.name, &old_row[i])?;
                old_index.remove(primary_key);
                self.save_index(&table.name, &index_col.name, &old_row[i], old_index)?;

                let mut new_index = self.load_index(&table.name, &index_col.name, &row[i])?;
                new_index.insert(primary_key.clone());
                self.save_index(&table.name, &index_col.name, &row[i], new_index)?;
            }
        }

        let key = Key::Row(table.name.clone(), new_primary_key.clone()).encode()?;
        let value = bincode::serialize(&row)?;
        self.transaction.set(key, value)?;
        Ok(())
    }

    fn delete_row(&mut self, table: &Table, primary_key: &Value) -> Result<()> {
        // 删除数据之前先删索引
        let index_cols = table.columns.iter().enumerate().filter(|(_,c)| c.is_index).collect::<Vec<_>>();
        for (i, index_col) in index_cols {
            if let Some(row) = self.read_row_by_pk(&table.name, primary_key)?{
                let mut index = self.load_index(&table.name, &index_col.name, &row[i])?;
                index.remove(primary_key);
                self.save_index(&table.name, &index_col.name, &row[i] ,index)?; // 修改后的索引重新存储
            }
        }

        let key = Key::Row(table.name.clone(), primary_key.clone()).encode()?;
        self.transaction.delete(key)
    }
}

impl<E:storageEngine> KVTransaction<E> {
    fn load_index(&self, table_name: &str, col_name: &str, col_value: &Value) -> Result<HashSet<Value>>{
        // 加载Index_key，并进行反序列化
        let key = Key::Index(table_name.into(), col_name.into(), col_value.clone()).encode()?;
        Ok(
            self.transaction.get(key)?
                .map(|v| bincode::deserialize(&v)).transpose()?.unwrap_or_default()
        )
    }

    fn save_index(&mut self, table_name: &str, col_name: &str, col_value: &Value, index: HashSet<Value>) -> Result<()>{
        // 存储索引，如果整个Index_set都空了，那么删除Index
        let key = Key::Index(table_name.into(), col_name.into(), col_value.clone()).encode()?;
        if index.is_empty(){
            self.transaction.delete(key)
        }else{
            self.transaction.set(key, bincode::serialize(&index)?)
        }
    }

    fn read_row_by_pk(&self, table_name: &str, pk: &Value) -> Result<Option<Row>>{
        let res = self.transaction.get(
            Key::Row(table_name.into(), pk.clone()).encode()?
        )?.map(|v| bincode::deserialize(&v)).transpose()?;
        Ok(res)
    }
}

#[derive(Debug,Serialize,Deserialize)]
enum Key{
    Table(String),
    Row(String,Value),   // (table_name, primary_key)
    Index(String, String, Value),   // [table_name, index_col_name, index_col_value]
}
```