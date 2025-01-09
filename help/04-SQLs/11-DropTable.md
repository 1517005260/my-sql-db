# 删除表实现

**语法规定**：

```sql
DROP TABLE table_name;
```

## 代码实现

1. 修改parser

在lexer.rs中新增关键字

```rust
#[derive(Debug, Clone, PartialEq, EnumIter)]
pub enum Keyword {
    Drop,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                "DROP" => Keyword::Drop,
            })
    }
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Drop => "DROP",            
        }
    }
}
```

ast.rs中新增：

```rust
#[derive(Debug,PartialEq)]
pub enum Sentence{
    DropTable{
        name: String,
    },
}
```

mod.rs中新增解析方法：

```rust
fn parse_sentence(&mut self) -> Result<Sentence>{
        match self.peek()? {
            Some(Token::Keyword(Keyword::Drop)) => self.parse_ddl(),
        }
}

fn parse_ddl(&mut self) -> Result<ast::Sentence>{
    match self.next()? {  // 这里要消耗token
        Token::Keyword(Keyword::Create) => match self.next()? {
            Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),  // CREATE TABLE
            token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),  // 语法错误
        },
        Token::Keyword(Keyword::Drop) => match self.next()? {
            Token::Keyword(Keyword::Table) => self.parse_ddl_drop_table(),  // DROP TABLE
            token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
        },
        token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),
    }
}

// 解析Drop Table 语句
fn parse_ddl_drop_table(&mut self) -> Result<Sentence>{
    let table_name = self.expect_next_is_ident()?;
    Ok(Sentence::DropTable {
        name: table_name,
    })
}
```

2. 修改Planner

mod.rs新增：

```rust
#[derive(Debug,PartialEq)]
pub enum Node{
    DropTable{
        name: String,
    },
}
```

planner.rs新增：

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Result<Node>{
    Ok(match sentence {
        Sentence::DropTable {name} => Node::DropTable {name},
    })
}
```

3. 修改executor

schema.rs新增：

```rust
pub struct DropTable{
    name: String,
}

impl DropTable{
    pub fn new(name: String) -> Box<Self> {
        Box::new(Self {name})
    }
}
```

mod.rs新增：

```rust
#[derive(Debug, PartialEq, Clone)]
pub enum ResultSet{
    DropTable{
        table_name: String,
    },
}

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::DropTable { table_name } => format!("DROP TABLE {}", table_name),            
        }
    }
}

impl<T:Transaction + 'static> dyn Executor<T>{
    pub fn build(node: Node) -> Box<dyn Executor<T>>{
        match node {
            Node::DropTable { name } => DropTable::new(name),            
        }
    }
}
```

在事务接口中新增删除表的操作，并且在kv.rs中实现：

```rust
// engine/mod.rs
pub trait Transaction {
    fn drop_table(&mut self, name: String)-> Result<()>;
}

// engine/kv.rs
impl<E:storageEngine> Transaction for KVTransaction<E> {
    fn drop_table(&mut self, name: String)-> Result<()>{
        // 获取表
        let table = self.must_get_table(name.clone())?;
        // 获取表的数据
        let rows = self.scan(name, None)?;
        // 删除表的数据
        for row in rows {
            self.delete_row(&table, &table.get_primary_key(&row)?)?;
        }
        // 删除表结构定义
        let key = Key::Table(table.name).encode()?;
        self.transaction.delete(key)
    }
}
```

继续在schema.rs里修改：

```rust
impl<T:Transaction> Executor<T> for DropTable{
    fn execute(self:Box<Self>,transaction:&mut T) -> crate::error::Result<ResultSet> {
        transaction.drop_table(self.name.clone())?;
        Ok(ResultSet::DropTable {table_name: self.name})
    }
}
```

4. 测试：

```bash
sql-db>> show tables;
t1
t2
t3
[Execution time: 1.560834ms]
sql-db>> DROP TABLE t;
Internal Error: [Get Table] Table " t " does not exist
[Execution time: 688.584µs]
sql-db>> drop table t1;
DROP TABLE t1
[Execution time: 704.577µs]
sql-db>> show tables;
t2
t3
[Execution time: 496.059µs]
sql-db>> select * from t1;
Internal Error: [Get Table] Table " t1 " does not exist
[Execution time: 1.969825ms]
```