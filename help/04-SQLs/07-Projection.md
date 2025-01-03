# Projection投影

前面只实现了`select *`，现在来实现投影。

语法：

```sql
SELECT [* | col_name [ [ AS ] output_name [, ...] ]]
FROM table_name
[ORDER BY col_name [asc | desc] [, ...]]
[LIMIT count]
[OFFSET count]
```

## 代码实现

1. 仍然从parser/lexer.rs开始，本次关键字只有As

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    // 新增
    As,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                // 新增
                "AS" => Keyword::As,
            })
    }
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::As => "AS",
        }
    }
}
```

抽象语法树ast.rs补充：

```rust
// 目前表达式为了简单，仅支持常量，不支持：insert into Table_A value(11 * 11 + 2) 等
// 更新：select的列名算作Expression
#[derive(Debug,PartialEq,Clone)]
pub enum Expression{
    Consts(Consts),
    Field(String),
}

#[derive(Debug,PartialEq)]
pub enum Sentence{
    Select{
        table_name: String,
        select_condition: Vec<(Expression, Option<String>)>,  // 列名，可选的别名
        order_by: Vec<(String, OrderBy)>, // 例如，order by col_a desc
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
}
```

由于修改了Expression的定义，types/mod.rs需要连带修改：

```rust
impl Value {
    pub fn from_expression_to_value(expression: Expression) -> Self{
        match expression {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(bool)) => Self::Boolean(bool),
            Expression::Consts(Consts::Integer(int)) => Self::Integer(int),
            Expression::Consts(Consts::Float(float)) => Self::Float(float),
            Expression::Consts(Consts::String(string)) => Self::String(string),
            _ => unreachable!(), // 列名不会涉及变成Value的转换
        }
    }
}
```

`unreachable!()` 宏在 Rust 中表示“这个代码块永远不应该被执行”。它的作用是告诉编译器和开发者，在某个特定位置，如果代码执行到了这个地方，那说明程序出现了异常或错误，因为按理说这个地方的代码是不应该被到达的。

之后需要修改mod.rs中解析select的逻辑，之前我们是默认解析为 `select *` 的，现在需要修改。

```rust
// 解析表达式，目前有常量和列名
fn parse_expression(&mut self) -> Result<ast::Expression>{
    Ok(
        match self.next()? {
            // 新增
            Token::Ident(ident) =>{
                // 列名
                ast::Expression::Field(ident)
            },
        }
    )
}

// 分类二：Select语句
fn parse_select(&mut self) -> Result<Sentence>{
    // 首先解析select的列信息
    let selects = self.parse_select_condition()?;

    self.expect_next_token_is(Token::Keyword(Keyword::From))?;

    // 识别完关键字之后为表名
    let table_name = self.expect_next_is_ident()?;
    Ok(Sentence::Select {
        table_name,
        select_condition: selects,
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

fn parse_select_condition(&mut self) -> Result<Vec<(Expression, Option<String>)>>{
    self.expect_next_token_is(Token::Keyword(Keyword::Select))?;

    let mut selects = Vec::new();
    // 如果是select *
    if self.next_if_is_token(Token::Asterisk).is_some(){
        return Ok(selects);
    }

    // 处理多个select的列
    loop{
        let col_name = self.parse_expression()?;
        // 查看是否有别名，比如 select user_name as a
        let nick_name= match self.next_if_is_token(Token::Keyword(Keyword::As)) {
            Some(_) => Some(self.expect_next_is_ident()?),
            None => None,
        };
        selects.push((col_name, nick_name));
        // 没有逗号，解析完毕
        if self.next_if_is_token(Token::Comma).is_none(){
            break;
        }
    }

    Ok(selects)
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
            select_condition:vec![],
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
            select_condition:vec![],
            order_by: vec![
                ("a".to_string(), Asc),
                ("b".to_string(), Asc),
                ("c".to_string(), Desc),
            ],
            limit: None,
            offset: None,
        }
    );

    let sql = "select a as col1, b as col2, c from tbl1 order by a, b asc, c desc;";
    let sentence = Parser::new(sql).parse()?;
    assert_eq!(
        sentence,
        ast::Sentence::Select {
            table_name: "tbl1".to_string(),
            select_condition: vec![
                (Expression::Field("a".into()), Some("col1".into())),
                (Expression::Field("b".into()), Some("col2".into())),
                (Expression::Field("c".into()), None),
            ],
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

在mod.rs中新增投影节点：

```rust
#[derive(Debug,PartialEq)]
pub enum Node{
    Projection{
        source: Box<Node>,
        expressions: Vec<(Expression, Option<String>)>,
    },
}
```

在planner.rs中接收前置node的信息，进行Projection_node的构建

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Result<Node>{
        Ok(match sentence {
            Sentence::Select {table_name,select_condition, order_by, limit, offset} =>
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

                    // projection
                    if !select_condition.is_empty(){
                        node = Node::Projection {
                            source: Box::new(node),
                            expressions: select_condition,
                        }
                    }

                    node
                },            
        })
}
```

3. 修改执行器

在mod.rs中新增：

```rust
impl<T:Transaction + 'static> dyn Executor<T>{
    pub fn build(node: Node) -> Box<dyn Executor<T>>{
        match node {
            Node::Projection {source, expressions} => Projection::new(Self::build(*source), expressions),            
        }
    }
}
```

在query.rs中新增Projection执行节点：

```rust
pub struct Projection<T: Transaction>{
    source: Box<dyn Executor<T>>,
    expressions: Vec<(Expression, Option<String>)>,
}

impl<T:Transaction> Projection<T>{
    pub fn new(source: Box<dyn Executor<T>>, expressions: Vec<(Expression, Option<String>)>) -> Box<Self>{
        Box::new(Self{ source, expressions })
    }
}

impl<T:Transaction> Executor<T> for Projection<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        match self.source.execute(transaction){
            Ok(ResultSet::Scan {columns, rows}) => {
                // 处理投影逻辑，我们需要根据expressions构建新的“表”
                let mut select_index = Vec::new(); // 选择的列的下标
                let mut new_columns = Vec::new();  // 选择的列

                for (expr, nick_name) in self.expressions{
                    if let Expression::Field(col_name) = expr{
                        // 找到col_name在原表中的下标
                        let position = match columns.iter().position(|c| *c == col_name){
                            Some(position) => position,
                            None => return Err(Internal(format!("[Executor] Projection column {} does not exist", col_name)))
                        };
                        select_index.push(position);
                        new_columns.push(if nick_name.is_some(){ nick_name.unwrap() } else { col_name});
                    };
                }

                // 根据选择的列，对每行内容进行过滤
                let mut new_rows = Vec::new();
                for row in rows{
                    let mut new_row = Vec::new();
                    for i in select_index.iter(){
                        new_row.push(row[*i].clone());
                    }
                    new_rows.push(new_row);
                }
                
                Ok(ResultSet::Scan { columns: new_columns, rows: new_rows })
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
```