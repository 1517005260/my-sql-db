# Limit 与 Offset

**基本语法**：

```sql
select * from table_name limit 10 offset 10;
```

- limit限制输出条数
- offset设置输出偏移，即从offset位置开始输出结果

抽象出来为：

```sql
SELECT * FROM table_name
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]
```

## 代码实现

1. 在parser/lexer.rs中新增关键字解析：

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    // 新增
    Limit,
    Offset,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                "LIMIT" => Keyword::Limit,
                "OFFSET" => Keyword::Offset,
            }
        )}
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Limit => "LIMIT",
            Keyword::Offset => "OFFSET",
        }
    }
}
```

在ast中增加limit和offset的抽象语法树：

```rust
#[derive(Debug,PartialEq)]
pub enum Sentence{
    // 修改
    Select{
        table_name: String,
        order_by: Vec<(String, OrderBy)>, // 例如，order by col_a desc
        limit: Option<Expression>,  // 可以 limit 1+2
        offset: Option<Expression>,
    },
}
```

在mod.rs中添加具体解析的方法：

```rust
// 分类二：Select语句
fn parse_select(&mut self) -> Result<Sentence>{
    // 先只实现select *
    self.expect_next_token_is(Token::Keyword(Keyword::Select))?;
    self.expect_next_token_is(Token::Asterisk)?;
    self.expect_next_token_is(Token::Keyword(Keyword::From))?;

    // 识别完关键字之后为表名
    let table_name = self.expect_next_is_ident()?;
    Ok(Sentence::Select {
        table_name,
        order_by: self.parse_order_by_condition()?,
        limit: {
            if self.next_if_is_token(Token::Keyword(Keyword::Limit)).is_some(){
                Some(self.parse_expression()?)
            }else{
                None
            }
        },
        offset:{
            if self.next_if_is_token(Token::Keyword(Keyword::Offset)).is_some(){
                Some(self.parse_expression()?)
            }else{
                None
            }
        },
    })
}

// 测试
#[test]
fn test_parser_select() -> Result<()> {
    let sql = "select * from tbl1 limit 10 offset 20;";
    let sentence = Parser::new(sql).parse()?;
    assert_eq!(
        sentence,
        ast::Sentence::Select {
            table_name: "tbl1".to_string(),
            order_by: vec![],
            limit: Some(Expression::Consts(Integer(10))),
            offset: Some(Expression::Consts(Integer(20))),
        }
    );

    let sql = "select * from tbl1 order by a, b asc, c desc;";
    let sentence = Parser::new(sql).parse()?;
    assert_eq!(
        sentence,
        ast::Sentence::Select {
            table_name: "tbl1".to_string(),
            order_by: vec![
                ("a".to_string(), Asc),
                ("b".to_string(), Asc),
                ("c".to_string(), Desc),
            ],
            limit: None,
            offset: None,
        }
    );
    Ok(())
}
```

2. 修改planner

首先在mod.rs中修改执行器节点，这里我们仿照order by的做法，即如果没有附加条件，则返回普通scan节点或者order节点，有附加条件，则返回limit/offset节点

```rust
// 我们可以对scan节点limit，也可以对order by节点limit
#[derive(Debug,PartialEq)]
pub enum Node{
    // 新增
    Limit{
        source: Box<Node>,
        limit: usize,
    },
    Offset{
        source: Box<Node>,
        offset: usize,
    },
}
```

接着修改planner.rs：

这里需要注意：`select * from table_A limit 10 offset 10`中，同时指定了limit和offset，先执行谁？答案显然是offset，因为这句话可以解释为，“从偏移为10的地方取10条数据”

此外，由于解析Expression时需要解决error，所以整个调用链路都需要加上Result

```rust
impl Planner {
    pub fn build(&mut self, sentence: Sentence) -> Result<Plan>{
        Ok(Plan(self.build_sentence(sentence)?))
    }
    
    fn build_sentence(&mut self, sentence: Sentence) -> Result<Node>{
        Ok(match sentence {
            // 修改
            Sentence::Select {table_name, order_by, limit, offset} =>
                {
                    let mut node = Node::Scan {table_name, filter:None};
                    // 如果有order by，那么这里就返回OrderBy节点而不是Scan节点
                    if !order_by.is_empty() {
                        node = Node::OrderBy {
                            scan: Box::new(node),
                            order_by,
                        }; // 更新 scan_node 为 order_by_node
                    }

                    // offset
                    if let Some(expr) = offset {
                        node = Node::Offset {
                            source: Box::new(node),
                            offset: match Value::from_expression_to_value(expr) {
                                Value::Integer(i) => i as usize,
                                _ => return Err(Error::Internal("invalid offset".into())),
                            },
                        }
                    }

                    // limit
                    if let Some(expr) = limit {
                        node = Node::Limit {
                            source: Box::new(node),
                            limit: match Value::from_expression_to_value(expr) {
                                Value::Integer(i) => i as usize,
                                _ => return Err(Error::Internal("invalid offset".into())),
                            },
                        }
                    }
                    node
                },
        })
    }
}

// mod.rs中修改为Result调用
impl Plan{
    pub fn build(sentence: Sentence) -> Result<Self>{
        Ok(Planner::new().build(sentence)?)
    }
}

#[cfg(test)]
// ...
let p1 = Plan::build(sentence1)?;
let p2 = Plan::build(sentence2)?;
let p = Plan::build(sentence)?;

// engine/mod.rs中需要连带修改：
match Plan::build(sentence)?.    
    execute(&mut transaction){
    Ok(res) => {
        transaction.commit()?;  
        Ok(res)
    },
    Err(e) => {
        transaction.rollback()?;
    Err(e)
    }
}
```

3. 修改Executor

在mod.rs中新增：

```rust
impl<T:Transaction + 'static> dyn Executor<T>{
    pub fn build(node: Node) -> Box<dyn Executor<T>>{
        match node {
            // 新增
            Node::Limit {source, limit} => Limit::new(Self::build(*source), limit),
            Node::Offset {source, offset} => Offset::new(Self::build(*source), offset),
        }
    }
}
```

在query.rs中新增：

```rust
pub struct Limit<T: Transaction>{
    source: Box<dyn Executor<T>>,
    limit: usize,
}

impl<T:Transaction> Limit<T> {
    pub fn new(source: Box<dyn Executor<T>>, limit: usize) -> Box<Self>{
        Box::new(Self{ source, limit })
    }
}

impl<T:Transaction> Executor<T> for Limit<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        match self.source.execute(transaction){
            Ok(ResultSet::Scan {columns, mut rows}) => {
                // 对输出的rows截断即可
                Ok(
                    ResultSet::Scan {
                        columns,
                        rows: rows.into_iter().take(self.limit).collect(),
                    }
                )
            },
            _ => return Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string())),
        }
    }
}

pub struct Offset<T: Transaction>{
    source: Box<dyn Executor<T>>,
    offset: usize,
}

impl<T:Transaction> Offset<T> {
    pub fn new(source: Box<dyn Executor<T>>, offset: usize) -> Box<Self>{
        Box::new(Self{ source, offset })
    }
}

impl<T:Transaction> Executor<T> for Offset<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        match self.source.execute(transaction){
            Ok(ResultSet::Scan {columns, mut rows}) => {
                // 对输出rows跳过即可
                Ok(
                    ResultSet::Scan {
                        columns,
                        rows: rows.into_iter().skip(self.offset).collect(),
                    }
                )
            },
            _ => return Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string())),
        }
    }
}
```

4. 在engine/kv.rs中测试：

```rust
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

    match s.execute("select * from t3 order by b, c desc limit 1 offset 3;")? {
        ResultSet::Scan { columns, rows } => {
            for r in rows {
                println!("{:?}", r);
            }
        }
        _ => unreachable!(),
    }

    std::fs::remove_dir_all(p.parent().unwrap())?;
    Ok(())
}
```