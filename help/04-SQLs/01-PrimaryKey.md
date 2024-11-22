# 主键完善

接下来的sql语句的实现涉及主键的完善，之前的代码中：

```rust
// sql/engine/kv.rs
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
```

我们在建立行数据的时候，默认以第一列为主键，我们现在需要给表属性加入主键，作为行数据的唯一标识。

例如，表`tbl`主键是id，从100开始，那么我们可以这样存储：

```rust
Key::Row::("tbl",100);
Key::Row::("tbl",101);
Key::Row::("tbl",102);
...
```

在MySQL等关系型数据库中，如果用户不指定，那么默认主键就是行id，这里为了实现方便，我们需要用户指定主键，否则报错。

### 代码实现

1. 在sql/parser/ast.rs中修改列的定义：

```rust
// 列定义
#[derive(Debug,PartialEq)]
pub struct Column{            // 列的各种属性
    pub name: String,         // 列名
    pub datatype: DataType,   // 列数据类型
    pub nullable: Option<bool>, // 列是否为空
    pub default: Option<Expression>, // 列的默认值
    pub is_primary_key: bool,       // 本列是否为主键
}
```

2. 随后在parser/mod.rs中增加建表时对主键的解析：

```rust
// 解析column
    fn parse_ddl_column(&mut self) -> Result<Column>{
        let mut column: Column = Column{
            name: self.expect_next_is_ident()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => DataType::Integer,
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => DataType::Boolean,
                Token::Keyword(Keyword::String) | Token::Keyword(Keyword::Text) | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parser] Unexpected token {}",token))),
            },
            nullable: None,
            default: None,
            is_primary_key: false,
        };

        // 解析是否为空，是否有默认值
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.expect_next_token_is(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                },
                Keyword::Default => column.default = Some(self.parse_expression()?),
                Keyword::Primary => {
                    self.expect_next_token_is(Token::Keyword(Keyword::Key))?;  // 关键字为primary key
                    column.is_primary_key = true;
                },
                keyword => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}",keyword))),
            }
        }
        Ok(column)
    }
```

3. 在sql/schema.rs中增加列主键的定义，前面的定义是给parser用的，这里的定义是给数据库内部用的：

```rust
#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub struct Column{
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub is_primary_key: bool,
}
```

4. 在sql/planner/planner.rs中修改将sql语句转换为执行节点的代码：

```rust
// 将parser得到的sql-sentence转换为node节点
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

            Sentence::Insert { table_name, columns, values, } =>
                Node::Insert {
                    table_name,
                    columns:columns.unwrap_or_default(),  // columns 是 None 时，则使用 Vec::default()，即一个空的 Vec 列表，作为默认值返回。
                    values,
                },

            Sentence::Select {table_name} =>
                Node::Scan {table_name},
            }
        }
}
```

5. 在sql/engine/kv.rs中修改sql建表语句的主键定义：

对于如下的旧方法：

```rust
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
```

我们先将“判断表的有效性”单独摘出来，减少代码的复杂性。在sql/schema.rs中：

```rust
impl Table{
    // 判断表的有效性
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

        Ok(())
    }
}
```

则原create_table方法变为：

```rust
fn create_table(&mut self, table: Table) -> Result<()> {
        // 判断表是否存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" already exists", table.name.clone())))
        }

        // 判断表是否有效
        table.is_valid()?;

        // 创建表成功，调用存储引擎存储
        // String -> 序列化 -> bincode
        let key = Key::Table(table.name.clone());
        let bin_code_key = bincode::serialize(&key)?;
        let value = bincode::serialize(&table)?;
        self.transaction.set(bin_code_key, value)?;

        Ok(())
    }
```

6. 在相同文件中修改create_row函数，使得不再默认以行数据第一列作为主键

继续对Table结构体新增方法，即获取主键。又由于column是一个Vec，那么实际上就是要找向量的第i列，然后找到某行的第i列，这就是一行的主键：

```rust
impl Table{
    // 获取主键
    pub fn get_primary_key(&self, row: &Row) -> Result<Value> {
        let index = self.columns.iter().position(|c| c.is_primary_key).unwrap();  // 由于建表时已经判断了主键信息，所以这里直接解包即可
        Ok(row[index].clone())
    }
}
```

此外，这里又涉及到了存储时的编码问题，之前都是用bincode而非自定义的编码，对于`Row(String,Value)`等的变长编码是不匹配的，所以这里需要先修改一下，即修改kv.rs中的Key-enum：

```rust
impl Key{
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}

impl PrefixKey{
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(self)
    }
}
```

create_row函数变为：

```rust
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
```

这里还需要为type/mod.rs中的Value类型实现Display方法：

```rust
impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "{}", "NULL"),
            Value::Boolean(b) if *b => write!(f, "{}", "TRUE"),
            Value::Boolean(_) => write!(f, "{}", "FALSE"),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
        }
    }
}
```

7. 对kv.rs中剩余的bincode-key编码全部改为自定义实现的编码方法：

```rust
// scan 方法下
let prefix = PrefixKey::Row(table_name.clone()).encode()?;
let results = self.transaction.prefix_scan(prefix)?;

// create_table 方法下
let key = Key::Table(table.name.clone()).encode()?;
let value = bincode::serialize(&table)?;
self.transaction.set(key, value)?;

// get_table 方法下
let key = Key::Table(table_name).encode()?;
let value = self.transaction.get(key)?.map(
|value| bincode::deserialize(&value)
).transpose()?;
```

8. 在storage/keyencode.rs中实现对String类型，i64类型的序列化方法：

```rust
fn serialize_str(self, v: &str) -> Result<()> {
        self.output.extend(v.as_bytes());
        Ok(())
    }

fn serialize_i64(self, v: i64) -> Result<()> {
    self.output.extend(v.to_be_bytes());
    Ok(())
}

fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
where
    V: Visitor<'de>
{
    let bytes = self.next_bytes()?;
    visitor.visit_str(&String::from_utf8(bytes)?)
}

fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
where
    V: Visitor<'de>
{
    let bytes = self.take_bytes(8);
    let v = i64::from_be_bytes(bytes.try_into()?);
    visitor.visit_i64(v)
}
```

这里出现了新的error，需要我们自定义补充，在error.rs中：

```rust
// String from utf 8 错误
impl From<FromUtf8Error> for Error {
    fn from(value: FromUtf8Error) -> Self {
        Error::Internal(value.to_string())
    }
}
```

9. 修改kv.rs中的测试并运行：

```rust
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
}
```