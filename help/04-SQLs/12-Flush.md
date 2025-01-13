# Flush 清除所有表

使用`flush`来清除所有表。一般这是个刷新一个数据库的命令，但是我们没有实现`use database;`语法，所以直接用删除所有表来实现。

## 代码实现

1. 修改parser

在lexer中新增关键字：

```rust
#[derive(Debug, Clone, PartialEq, EnumIter)]
pub enum Keyword {
    Flush,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(match input.to_uppercase().as_ref() {
            "FLUSH" => Keyword::Flush,    
        })
    }
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Flush => "FLUSH",
        }
    }
}
```

在ast.rs中新增语法树：

```rust
#[derive(Debug, PartialEq)]
pub enum Sentence {
    Flush{},
}
```

在mod.rs中新增处理：

```rust
fn parse_sentence(&mut self) -> Result<Sentence> {
    match self.peek()? {
        Some(Token::Keyword(Keyword::Flush)) => self.parse_flush(),        
    }
}

fn parse_flush(&mut self) -> Result<Sentence> {
    self.expect_next_token_is(Token::Keyword(Keyword::Flush))?;
    Ok(Sentence::Flush{})
}
```

2. 修改planner

planner.rs中：

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Result<Node> {
    Ok(match sentence {
        Sentence::Flush { } => {
            return Err(Error::Internal("[Planner] Unexpected flush command".into()))
        },        
    })
}
```

这里和事务命令等一样，交由engine层执行，planner只是形式上接收

3. 修改executor

mod.rs中：

```rust
#[derive(Debug, PartialEq, Clone)]
pub enum ResultSet {
    Flush{},
}

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::Flush { } => "FLUSH DB".to_string(),
        }
    }
}
```

4. 在engine/mod.rs中：

这里和Plan Node一样的处理方法，如果没有事务需要手动构建

```rust
impl<E: Engine + 'static> Session<E> {
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        ast::Sentence::Flush{} => {
            let res  = match  self.transaction.as_ref() {
                Some(_) => {
                    let transaction = self.transaction.as_mut().unwrap();
                    let names = transaction.get_all_table_names()?;
                    for name in names{
                        transaction.drop_table(name)?;
                    }
                    ResultSet::Flush {}
                }
                None => {
                    // 手动构建事务
                    let mut transaction = self.engine.begin()?;
                    let names = transaction.get_all_table_names()?;
                    for name in names{
                        transaction.drop_table(name)?;
                    }
                    transaction.commit()?;
                    ResultSet::Flush {}
                }
            };
            Ok(res)
        }
    }
}
```

5. 测试：

```bash
sql-db>> show tables;
t
t1
[Execution time: 547.704µs]
sql-db>> flush;
FLUSH DB
[Execution time: 599.205µs]
sql-db>> show tables;
No tables found.
[Execution time: 683.566µs]
```