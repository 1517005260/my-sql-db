# 查询索引优化

在底层存储支持索引后，我们现在从parser开始，让数据库支持索引查询

例如：

```sql
create table t (a int primary key , b float index, c float);
select * from t where b = 2.5;  -- 点读可以走索引
select * from t where b = 3.3;  -- 不能走索引
```

## 代码实现

1. 修改parser

在lexer.rs中新增关键字

```rust
#[derive(Debug, Clone, PartialEq, EnumIter)]
pub enum Keyword {
    Index,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                "INDEX" => Keyword::Index,
            })
    }
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Index => "INDEX",
        }
    }
}
```

在ast.rs中更改列的定义：

```rust
#[derive(Debug,PartialEq)]
pub struct Column{            // 列的各种属性
    pub name: String,         // 列名
    pub datatype: DataType,   // 列数据类型
    pub nullable: Option<bool>, // 列是否为空
    pub default: Option<Expression>, // 列的默认值
    pub is_primary_key: bool,       // 本列是否为主键
    pub is_index: bool,             // 本列是否为索引
}
```

在mod.rs中更新解析列的方法：

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
        is_index: false,
    };

    // 解析是否为空，是否有默认值，是否为主键，是否有索引
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
            Keyword::Index => column.is_index = true,
            keyword => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}",keyword))),
        }
    }
    Ok(column)
}
```

2. 修改planner

在planner.rs中修改之前的默认true值：

```rust
schema::Column{
    name: c.name,
    datatype: c.datatype,
    nullable,
    default,
    is_primary_key: c.is_primary_key,
    is_index: c.is_index && !c.is_primary_key,  // 主键不能建索引
}
```

注意，主键本身就有一个索引，因此不需要再为主键列显式创建索引。但是目前我们没有为主键实现索引。

再回顾下点读的条件：`select * from t where b=2;`，我们需要进行判断，如果符合条件，并且本列有索引，那么就将原本的Scan节点升级成Index节点，走索引查询。

在mod.rs中新增节点：

```rust
#[derive(Debug,PartialEq)]
pub enum Node{
    ScanIndex{
        table_name: String,
        col_name: String,
        value: Value,
    },
}
```

继续修改planner.rs。这里由于我们需要拿到表的信息，而`must_get_table()`方法是在事务层实现的，按理来说只有executor才能进行调用，但是这里避免不了得成为了屎山代码。

```rust
pub struct Planner<'a, T: Transaction>{   // 辅助Plan的结构体
    transaction: &'a mut T,
}

impl<'a, T:Transaction> Planner<'a, T> {
    pub fn new(transaction: &'a mut T) -> Self {
        Self{transaction }
    }
}
```

通过将生命周期 'a 作为 Planner 结构体的一个参数，Rust 编译器能够确保 Planner 实例的生命周期不会超过它所引用的 transaction 的生命周期。本例可以被视为**标注活得更短的一方**。

这里修改之后，接下来的地方都需要连带修改：

```rust
// planner/mod.rs
impl Plan{
    pub fn build<T: Transaction>(sentence: Sentence, transaction: &mut T) -> Result<Self>{
        Ok(Planner::new(transaction).build(sentence)?)
    }
}

// engine/mod.rs
impl<E:Engine + 'static> Session<E> {
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            sentence if self.transaction.is_some() =>{
                Plan::build(sentence, self.transaction.as_mut().unwrap())?.execute(self.transaction.as_mut().unwrap())
            },
            sentence => {
                let mut transaction = self.engine.begin()?;  
                
                match Plan::build(sentence, &mut transaction)?.  
                    execute(&mut transaction) {
                    Ok(res) => {
                        transaction.commit()?;  
                        Ok(res)
                    },
                    Err(e) => {
                        transaction.rollback()?; 
                        Err(e)
                    }
                }
            },
        }
    }
}

// planner/mod.rs - test
#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        sql::{
            parser::{
                ast::{self, Expression},
                Parser,
            },
            planner::{Node, Plan},
        },
    };
    use crate::sql::engine::Engine;
    use crate::sql::engine::kv::KVEngine;
    use crate::storage::disk::DiskEngine;

    #[test]
    fn test_plan_create_table() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut transaction = kvengine.begin()?;

        let sql1 = "
        create table tbl1 (
            a int default 100,
            b float not null,
            c varchar null,
            d bool default true
        );
        ";
        let sentence1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(sentence1, &mut transaction);
        println!("{:?}",p1);

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let sentence2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(sentence2, &mut transaction);
        assert_eq!(p1, p2);
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_plan_insert() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut transaction = kvengine.begin()?;

        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let sentence1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(sentence1,&mut transaction)?;
        assert_eq!(
            p1,
            Plan(Node::Insert {
                table_name: "tbl1".to_string(),
                columns: vec![],
                values: vec![vec![
                    Expression::Consts(ast::Consts::Integer(1)),
                    Expression::Consts(ast::Consts::Integer(2)),
                    Expression::Consts(ast::Consts::Integer(3)),
                    Expression::Consts(ast::Consts::String("a".to_string())),
                    Expression::Consts(ast::Consts::Boolean(true)),
                ]],
            })
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let sentence2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(sentence2, &mut transaction)?;
        assert_eq!(
            p2,
            Plan(Node::Insert {
                table_name: "tbl2".to_string(),
                columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
                values: vec![
                    vec![
                        Expression::Consts(ast::Consts::Integer(3)),
                        Expression::Consts(ast::Consts::String("a".to_string())),
                        Expression::Consts(ast::Consts::Boolean(true)),
                    ],
                    vec![
                        Expression::Consts(ast::Consts::Integer(4)),
                        Expression::Consts(ast::Consts::String("b".to_string())),
                        Expression::Consts(ast::Consts::Boolean(false)),
                    ],
                ],
            })
        );
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_plan_select() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut transaction = kvengine.begin()?;

        let sql = "select * from tbl1;";
        let sentence = Parser::new(sql).parse()?;
        let plan = Plan::build(sentence, &mut transaction)?;
        assert_eq!(
            plan,
            Plan(Node::Scan {
                table_name: "tbl1".to_string(),
                filter: None,
            })
        );
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
}
```

回到planner.rs：

```rust
impl<'a, T:Transaction> Planner<'a, T> {
    fn build_sentence(&mut self, sentence: Sentence) -> Result<Node>{
        Ok(match sentence {
            Sentence::Update {table_name, columns, condition} =>
                Node::Update {
                    table_name: table_name.clone(),
                    scan: Box::new(self.build_scan_or_index(table_name, condition)?),
                    columns,
                },

            Sentence::Delete {table_name, condition} =>
                Node::Delete {
                    table_name:table_name.clone(),
                    scan: Box::new(self.build_scan_or_index(table_name, condition)?)
                },
        })
    }
    
    fn build_from_item(&mut self, item: FromItem, filter: &Option<Expression>) -> Result<Node>{
        let node = match item {
            FromItem::Table { name } => self.build_scan_or_index(name, filter.clone())?,
        };
    }
    
    // 根据filter条件判断是否可以走索引
    fn build_scan_or_index(&self, table_name: String, filter: Option<Expression>) -> Result<Node>{
        let node = match Self::parse_filter(filter.clone()) {
            Some((col, val)) => {
                // 即使条件是 b=2，但是若不是索引列，也不能走索引
                let table = self.transaction.must_get_table(table_name.clone())?;
                match table.columns.iter().position(|c| *c.name == col && c.is_index){
                    Some(_) => {
                        // 本列有索引
                        Node::ScanIndex{table_name, col_name: col, value: val}
                    },
                    None => Node::Scan {table_name, filter},
                }
            },
            None => Node::Scan {table_name, filter},
        };
        Ok(node)
    }

    // 解析上个函数的filter表达式
    // 实际上我们的hash索引仅支持 b=2 的条件，也即Expression::Operation::Equal
    fn parse_filter(filter: Option<Expression>) -> Option<(String, Value)>{
        match filter {
            Some(expr) => {
                match expr {
                    // 解析右边的常数
                    Expression::Consts(val) => Some(("".into(), Value::from_expression_to_value(Expression::Consts(val)))),
                    // 解析左边的列名
                    Expression::Field(col) => Some((col, Value::Null)),
                    Expression::Operation(operation) => {
                        match operation {
                            Operation::Equal(col, val) => {
                                // 递归调用进行解析
                                let left = Self::parse_filter(Some(*col));
                                let right = Self::parse_filter(Some(*val));

                                // 左边为(col, null)，右边为("", val)，现在进行组合
                                Some((left.unwrap().0, right.unwrap().1))
                            },
                            _ => None,
                        }
                    },
                    _ => None,
                }
            },
            None => None,
        }
    }
}
```

3. 修改executor

在mod.rs中新增：

```rust
impl<T:Transaction + 'static> dyn Executor<T>{
    pub fn build(node: Node) -> Box<dyn Executor<T>>{
        match node {
            Node::ScanIndex { table_name, col_name, value} => ScanIndex::new(table_name, col_name, value),
        }
    }
}
```

在query.rs中新增：

```rust
pub struct ScanIndex{
    table_name: String,
    col_name: String,
    value: Value,
}

impl ScanIndex{
    pub fn new(table_name: String, col_name: String, value: Value) -> Box<Self>{
        Box::new(Self{ table_name, col_name, value })
    }
}
```

为了方便起见，我们将[之前](./01-Index.md)写在外面的索引方法写进transaction里面

```rust
// engine/mod.rs
pub trait Transaction {
    // 索引相关方法
    fn load_index(&self, table_name: &str, col_name: &str, col_value: &Value) -> Result<HashSet<Value>>;
    fn save_index(&mut self, table_name: &str, col_name: &str, col_value: &Value, index: HashSet<Value>) -> Result<()>;
    fn read_row_by_pk(&self, table_name: &str, pk: &Value) -> Result<Option<Row>>;
}

// engine/kv.rs
// 将之前写在外面的方法挪进来
impl<E:storageEngine> Transaction for KVTransaction<E> {
    fn load_index(&self, table_name: &str, col_name: &str, col_value: &Value) -> Result<HashSet<Value>>{
        // ...
    }
    
    fn save_index(&mut self, table_name: &str, col_name: &str, col_value: &Value, index: HashSet<Value>) -> Result<()>{
        // ...
    }
    fn read_row_by_pk(&self, table_name: &str, pk: &Value) -> Result<Option<Row>>{
        //...
    }
}
```

继续在query.rs中修改：

```rust
impl<T:Transaction> Executor<T> for ScanIndex{
    fn execute(self:Box<Self>,trasaction: &mut T) -> Result<ResultSet> {
        let table = trasaction.must_get_table(self.table_name.clone())?;

        // 加载 col_name, value 对应的索引情况
        let index = trasaction.load_index(&self.table_name, &self.col_name, &self.value)?;

        // 由于拿到的是Set，是无序的，我们尽量让它有序
        // 先转为列表
        let mut pks = index.iter().collect::<Vec<_>>();
        pks.sort_by(|v1, v2| v1.partial_cmp(v2).unwrap_or_else(|| Ordering::Equal));

        let mut rows = Vec::new();
        for pk in pks{
            if let Some(row) = trasaction.read_row_by_pk(&self.table_name, &pk)? {
                rows.push(row);
            }
        }
        Ok(ResultSet::Scan {
            columns: table.columns.into_iter().map(|c| c.name.clone()).collect(),
            rows,
        })
    }
}
```

4. 在kv.rs中测试：

```rust
#[test]
fn test_index() -> Result<()> {
    let p = tempfile::tempdir()?.into_path().join("sqldb-log");
    let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
    let mut s = kvengine.session()?;
    s.execute("create table t (a int primary key, b text index, c float index, d bool);")?;
    s.execute("insert into t values (1, 'a', 1.1, true);")?;
    s.execute("insert into t values (2, 'b', 2.1, true);")?;
    s.execute("insert into t values (3, 'a', 3.2, false);")?;
    s.execute("insert into t values (4, 'c', 1.1, true);")?;
    s.execute("insert into t values (5, 'd', 2.1, false);")?;

    s.execute("delete from t where a = 4;")?;

    match s.execute("select * from t where c = 1.1;")? {
        ResultSet::Scan { columns, rows } => {
            assert_eq!(columns.len(), 4);
            assert_eq!(rows.len(), 1);
        }
        _ => unreachable!(),
    }

    std::fs::remove_dir_all(p.parent().unwrap())?;
    Ok(())
}
```

测试报错：

```bash
not yet implemented
thread 'sql::engine::kv::tests::test_index' panicked at src/storage/keyencode.rs:81:9:
not yet implemented
```

我们前往对应位置实现代码进行修复：

```rust
fn serialize_f64(self, v: f64) -> Result<()> {
    self.output.extend(v.to_be_bytes());
    Ok(())
}

fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
where
    V: Visitor<'de>
{
    let bytes = self.take_bytes(8);
    let v = f64::from_be_bytes(bytes.try_into()?);
    visitor.visit_f64(v)
}
```