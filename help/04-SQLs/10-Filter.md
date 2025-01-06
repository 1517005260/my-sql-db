# Filter 过滤选择的数据

完善Select的where条件

之前在[update](./02-Update.md)实现时的Where逻辑可以复用，但是当时只有`=`，现在可以补充`>、<、<=、>=、!=`

**语法规定**：

```sql
SELECT [* | col_name | function [ [ AS ] output_name [, ...] ]]
FROM from_item
[WHERE expr]
[GROUP BY col_name]
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]
```

## 代码实现——Select Where

1. 修改parser

首先在ast中修改：

```rust
#[derive(Debug, PartialEq, Clone)]
pub enum Operation{
    Equal(Box<Expression>, Box<Expression>),
    Greater(Box<Expression>, Box<Expression>),  // a > b，下同
    GreaterEqual(Box<Expression>, Box<Expression>),
    Less(Box<Expression>, Box<Expression>),
    LessEqual(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
}

#[derive(Debug,PartialEq)]
pub enum Sentence{
    Select{
        select_condition: Vec<(Expression, Option<String>)>,  // 列名，可选的别名
        from_item: FromItem,
        where_condition: Option<Expression>,
        group_by: Option<Expression>,
        order_by: Vec<(String, OrderBy)>, // 例如，order by col_a desc
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    Update{
        table_name: String,
        columns: BTreeMap<String, Expression>,
        condition: Option<Expression>
    },
    Delete{
        table_name: String,
        condition: Option<Expression>,
    },
}
```

这里由于现在的Expression解析本身就包含列名了，所以原先的Update和Delete的定义也可以修改了。

在lexer.rs中增加表达式：

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // 新增 
    Greater,            // >
    GreaterEqual,       // >=
    Less,               // <
    LessEqual,          // <=
    NotEqual,           // !=
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Greater => ">",
            Token::GreaterEqual => ">=",
            Token::Less => "<",
            Token::LessEqual => "<=",
            Token::NotEqual => "!=",
        })
    }
}

impl<'a> Lexer<'a> {
    // 需要注意，原来的处理方法，只能处理char，而 >= 算作String
    fn scan_symbol(&mut self) -> Option<Token> {
        match self.iter.peek()? {
            '>' => {
                self.iter.next(); // 消费 '>'
                if self.iter.peek() == Some(&'=') {
                    self.iter.next(); // 消费 '='
                    Some(Token::GreaterEqual)
                } else {
                    Some(Token::Greater)
                }
            },
            '<' => {
                self.iter.next(); // 消费 '<'
                if self.iter.peek() == Some(&'=') {
                    self.iter.next(); // 消费 '='
                    Some(Token::LessEqual)
                } else {
                    Some(Token::Less)
                }
            },
            '!' => {
                self.iter.next();
                if self.iter.peek() == Some(&'=') {
                    self.iter.next();
                    Some(Token::NotEqual)
                }else{
                    None
                }
            },
            _ => self.next_if_token(|c|
                match c {
                    '*' => Some(Token::Asterisk),
                    '(' => Some(Token::OpenParen),
                    ')' => Some(Token::CloseParen),
                    ',' => Some(Token::Comma),
                    ';' => Some(Token::Semicolon),
                    '+' => Some(Token::Plus),
                    '-' => Some(Token::Minus),
                    '/' => Some(Token::Slash),
                    '=' => Some(Token::Equal),
                    _ => None,
                })
        }
    }
}
```

然后在mod.rs中修改：

```rust
// 解析表达式当中的Operation类型
fn parse_operation(&mut self) -> Result<Expression>{
    let left = self.parse_expression()?;
    let token = self.next()?;
    let res = match token{
        Token::Equal => Expression::Operation(Operation::Equal(
            Box::new(left),
            Box::new(self.parse_expression()?),
        )),
        Token::Greater => Expression::Operation(Operation::Greater(
            Box::new(left),
            Box::new(self.parse_expression()?),
        )),
        Token::GreaterEqual => Expression::Operation(Operation::GreaterEqual(
            Box::new(left),
            Box::new(self.parse_expression()?),
        )),
        Token::Less=> Expression::Operation(Operation::Less(
            Box::new(left),
            Box::new(self.parse_expression()?),
        )),
        Token::LessEqual=> Expression::Operation(Operation::LessEqual(
            Box::new(left),
            Box::new(self.parse_expression()?),
        )),
        Token::NotEqual => Expression::Operation(Operation::NotEqual(
            Box::new(left),
            Box::new(self.parse_expression()?),
        )),
        _ => return Err(Error::Internal(format!("[Parser] Unexpected token {}",token))),
    };
    Ok(res)
}

fn parse_where_condition(&mut self) -> Result<Option<Expression>>{
    if self.next_if_is_token(Token::Keyword(Keyword::Where)).is_none(){
        return Ok(None);  // 没有指定where条件
    }
    Ok(Some(self.parse_operation()?))
}

// 分类二：Select语句
fn parse_select(&mut self) -> Result<Sentence>{
    Ok(Sentence::Select {
        select_condition: self.parse_select_condition()?,
        from_item: self.parse_from_condition()?,
        where_condition: self.parse_where_condition()?,
        group_by: self.parse_group_by()?,
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
    let sql = "select * from tbl1 where a <= 100 limit 10 offset 20;";
    let sentence = Parser::new(sql).parse()?;
    assert_eq!(
        sentence,
        ast::Sentence::Select {
            select_condition:vec![],
            from_item: Table { name:"tbl1".into() },
            where_condition: Some(ast::Expression::Operation(ast::Operation::LessEqual(
                Box::new(ast::Expression::Field("a".into())),
                Box::new(ast::Expression::Consts(Consts::Integer(100)))
            ))),
            group_by: None,
            order_by: vec![],
            limit: Some(Expression::Consts(Integer(10))),
            offset: Some(Expression::Consts(Integer(20))),
        }
    );
    // 其余省略
    Ok(())
}

#[test]
fn test_parser_update() -> Result<()> {
    let sql = "update tbl set a = 1, b = 2.0 where c = 'a';";
    let sentence = Parser::new(sql).parse()?;
    println!("{:?}",sentence);
    assert_eq!(
        sentence,
        Sentence::Update {
            table_name: "tbl".into(),
            columns: vec![
                ("a".into(), ast::Consts::Integer(1).into()),
                ("b".into(), ast::Consts::Float(2.0).into()),
            ]
                .into_iter()
                .collect(),
            condition: Some(ast::Expression::Operation(ast::Operation::Equal(
                Box::new(ast::Expression::Field("c".into())),
                Box::new(ast::Expression::Consts(Consts::String("a".into())))
            ))),
        }
    );

    Ok(())
}
```

2. 修改planner

首先在mod.rs中修改Scan节点的定义：

```rust
#[derive(Debug,PartialEq)]
pub enum Node{
    Scan{
        // select
        table_name: String,
        // 过滤条件
        filter: Option<Expression>,
    },
}
```

在planner.rs中，更改from_item的处理，增加了数据的过滤：

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Result<Node>{
        Ok(match sentence {
            Sentence::Select {select_condition,from_item,where_condition, group_by , order_by, limit, offset} =>
                {
                    // from
                    let mut node = self.build_from_item(from_item, &where_condition)?;
                    
                    // ...
                }
        })
}

fn build_from_item(&mut self, item: FromItem, filter: &Option<Expression>) -> Result<Node>{
    let node = match item {
        FromItem::Table { name } => Node::Scan {table_name:name, filter: filter.clone()},
        FromItem::Join { left, right, join_type, condition } => {
            let (left, right) = match join_type {
                JoinType::Right => (right, left),
                _ => (left, right),
            };

            let outer = match join_type  {
                JoinType::Cross | JoinType::Inner => false,
                _ => true,
            };

            Node::NestedLoopJoin {
                left: Box::new(self.build_from_item(*left, filter)?),
                right: Box::new(self.build_from_item(*right, filter)?),
                condition,
                outer,
            }
        },
    };
    Ok(node)
}
```

3. 修改executor

在query.rs中修改Scan节点的定义：

```rust
pub struct Scan{
    table_name: String,
    filter: Option<Expression>
}

impl Scan{
    pub fn new(table_name: String, filter: Option<Expression>) -> Box<Self>{
        Box::new(Self{ table_name, filter })
    }
}
```

连带修改engine：

```rust
// mod.rs
fn scan(&self,table_name: String, filter: Option<Expression>)-> Result<Vec<Row>>;
```

之前是在kv.rs中过滤数据的，但是只过滤了等号，比较简单，这里我们继续沿用之前判断join条件的函数，将其挪到parser/ast.rs中，作为公用方法被调用。

```rust
// 原executor/join.rs下的方法
fn parse_expression(expr: &Expression,
                    left_cols: &Vec<String>, left_row: &Vec<Value>,
                    right_cols: &Vec<String>, right_row: &Vec<Value>) -> Result<Value> {
    match expr {
        Expression::Field(col_name) => {
            // 根据列名，取对应行的数据
            let pos = match left_cols.iter().position(|col| *col == *col_name){
                Some(pos) => pos,
                None => return Err(Internal(format!("[Executor] Column {} does not exist", col_name))),
            };
            Ok(left_row[pos].clone())
        },
        Expression::Operation(operation) =>{
            match operation {
                Operation::Equal(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;

                    // 取到两张表同名列的值，如果相等则可以连接
                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l == r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l == r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 == r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l == r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l == r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l == r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                }
            }
        },
        _ => return Err(Internal(format!("[Executor] Unexpected Expression {:?}", expr)))
    }
}

// 现将其放在parser/ast.rs下并进行修改
// 解析表达式
pub fn parse_expression(expr: &Expression,
                        left_cols: &Vec<String>, left_row: &Vec<Value>,
                        right_cols: &Vec<String>, right_row: &Vec<Value>) -> crate::error::Result<Value> {
    match expr {
        Expression::Field(col_name) => {
            // 根据列名，取对应行的数据
            let pos = match left_cols.iter().position(|col| *col == *col_name){
                Some(pos) => pos,
                None => return Err(Internal(format!("[Executor] Column {} does not exist", col_name))),
            };
            Ok(left_row[pos].clone())
        },
        Expression::Consts(c) => {
            // 解析诸如 a = 3 中的常量
            let value = match c {
                Consts::Null => Value::Null,
                Consts::Boolean(v) => Value::Boolean(*v),
                Consts::Integer(v) => Value::Integer(*v),
                Consts::Float(v) => Value::Float(*v),
                Consts::String(v) => Value::String(v.clone()),
            };
            Ok(value)
        },
        Expression::Operation(operation) =>{
            match operation {
                Operation::Equal(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;
                    
                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l == r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l == r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 == r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l == r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l == r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l == r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
                Operation::Greater(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;
                    
                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l > r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l > r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 > r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l > r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l > r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l > r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
                Operation::GreaterEqual(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;
                    
                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l >= r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l >= r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 >= r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l >= r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l >= r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l >= r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
                Operation::Less(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;
                    
                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l < r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l < r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean((l as f64) < r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l < r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l < r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l < r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
                Operation::LessEqual(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;
                    
                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l <= r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l <= r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 <= r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l <= r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l <= r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l <= r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
                Operation::NotEqual(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;

                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l != r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l != r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 != r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l != r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l != r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l != r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
            }
        },
        _ => return Err(Internal(format!("[Executor] Unexpected Expression {:?}", expr)))
    }
}
```

我们继续仿照executor/join.rs的判断逻辑，在kv.rs中修改判断的逻辑：

与join不同的是，join会分左右两表，而scan-filter是不分左右的（自己与自己比较）

```rust
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
```

继续在kv.rs中测试：

```rust
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
```