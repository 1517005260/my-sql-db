# Explain SQL 执行计划

执行计划，即打印整个SQL语句的执行链路。

**语法规定**：

```sql
EXPLAIN SQL;
```

## 代码实现

1. 修改parser

在lexer.rs中新增关键字：

```rust
#[derive(Debug, Clone, PartialEq, EnumIter)]
pub enum Keyword {
    Explain,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                "EXPLAIN" => Keyword::Explain,
            })
    }
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Explain => "EXPLAIN",            
        }
    }
}
```

在ast.rs中新增执行计划节点：

```rust
#[derive(Debug,PartialEq)]
pub enum Sentence{
    Explain{
        sentence: Box<Sentence>,
    },
}
```

在mod.rs中新增解析：

```rust
fn parse_sentence(&mut self) -> Result<Sentence>{
    match self.peek()? {
        Some(Token::Keyword(Keyword::Explain)) => self.parse_explain(),
    }
}

fn parse_explain(&mut self) -> Result<Sentence>{
    self.expect_next_token_is(Token::Keyword(Keyword::Explain))?;
    // 不支持对Explain语句进行Explain
    if let Some(Token::Keyword(Keyword::Explain)) = self.peek()? {
        return Err(Parse("[Parser] Cannot explain the explain sql".to_string()));
    }
    // 拿到explain后面的sql语句
    Ok(Sentence::Explain{
        sentence: Box::new(self.parse_sentence()?)
    })
}
```

2. 修改Planner

由于Explain语句其实不涉及真正的执行，所以和事务命令一样，Planner只是形式上接收，真正的处理交给SQLEngine层。

planner.rs中：

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Result<Node>{
    Ok(match sentence {
        Sentence::Explain {sentence:_} => {  // 不使用字段sentence
            return Err(Error::Internal("[Planner] Unexpected explain command".into()));
        },        
    })
}
```

mod.rs中新增对PlanNode的格式化输出方法，方便后续调用：

```rust
// Plan Node 的格式化输出方法
impl Display for Node{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.format(f, "", true)
    }
}

impl Node{
    fn format(&self, f: &mut Formatter<'_>,  // formatter进行输出
              prefix: &str,                  // 换行前缀
              is_first: bool,                // 是否是第一个节点
    ) -> std::fmt::Result {
        if is_first{
            writeln!(f, "           SQL PLAN           ")?;
            writeln!(f, "------------------------------")?;
        }else {
            writeln!(f)?;
        }

        let prefix =
            if prefix.is_empty() {
            " -> ".to_string()
        } else {
            write!(f, "{}", prefix)?;
            format!(" {}", prefix)  // 下一个prefix需要有层次感
        };

        match self {
            Node::CreateTable {schema} => {
                write!(f, "Create Table {}", schema.name)
            },
            Node::DropTable {name} => {
                write!(f, "Drop Table {}", name)
            },
            Node::Insert {table_name, columns:_, values:_} => {
                write!(f, "Insert Into Table {}", table_name)
            },
            Node::Scan {table_name, filter} => {
                write!(f, "Sequence Scan On Table {}", table_name)?;
                if let Some(filter) = filter {
                    write!(f, " ( Filter: {} )", filter)?;
                }
                Ok(())
            },
            Node::ScanIndex { table_name, col_name, value:_ } => {
                write!(f, "Index Scan On Table {}.{}", table_name, col_name)
            },
            Node::PkIndex { table_name, value } => {
                write!(f, "Primary Key Scan On Table {}({})", table_name, value)
            },
            Node::Update {table_name, scan, columns:_} => {
                write!(f, "Update On Table {}", table_name)?;
                (*scan).format(f, &prefix, false)
            },
            Node::Delete {table_name, scan} => {
                write!(f, "Delete On Table {}", table_name)?;
                (*scan).format(f, &prefix, false)
            },
            Node::OrderBy {scan, order_by} => {
                let condition = order_by.iter().
                    map(|c| {
                        format!("{} {}", c.0, if c.1 == Asc {"Asc"} else {"Desc"})
                    }).collect::<Vec<_>>().join(", ");
                write!(f, "Order By {}", condition)?;
                (*scan).format(f, &prefix, false)
            },
            Node::Limit {source, limit} => {
                write!(f, "Limit {}", limit)?;
                (*source).format(f, &prefix, false)
            }
            Node::Offset {source, offset} => {
                write!(f, "Offset {}", offset)?;
                (*source).format(f, &prefix, false)
            }
            Node::Projection {source, expressions} => {
                let selects = expressions.iter().map(|(col_name, nick_name)|{
                    format!("{} {}", col_name, if nick_name.is_some() {format!(" As {}", nick_name.clone().unwrap())} else {"".to_string()})
                }).collect::<Vec<_>>().join(", ");
                write!(f, "Projection {}", selects)?;
                (*source).format(f, &prefix, false)
            },
            Node::NestedLoopJoin {left, right, condition, outer:_} => {
                write!(f, "Nested Loop Join")?;
                if let Some(expr) = condition {
                    write!(f, "( {} )", expr)?;
                }
                (*left).format(f, &prefix, false)?;
                (*right).format(f, &prefix, false)
            },
            Node::HashJoin {left, right, condition, outer:_} => {
                write!(f, "Hash Join")?;
                if let Some(expr) = condition {
                    write!(f, "( {} )", expr)?;
                }
                (*left).format(f, &prefix, false)?;
                (*right).format(f, &prefix, false)
            },
            Node::Aggregate { source, expression, group_by} => {
                let agg = expression.iter().map(|(col_name, nick_name)|{
                    format!("{} {}", col_name, if nick_name.is_some() {format!(" As {}", nick_name.clone().unwrap())} else {"".to_string()})
                }).collect::<Vec<_>>().join(", ");
                write!(f, "Aggregate {} ", agg)?;
                if let Some(Expression::Field(col_name)) = group_by {
                    write!(f, "Group By {}", col_name)?;
                }
                (*source).format(f, &prefix, false)
            },
            Node::Having { source, condition} => {
                write!(f, "Filter: {}", condition)?;
                (*source).format(f, &prefix, false)
            },
            Node::TableSchema { name } => {
                write!(f, "Show Table Schema: {}", name)
            },
            Node::TableNames {} => {
                write!(f, "Show Table Names")
            },
        }
    }
}
```

由于对Filter也需要打印，而它本身是Expression类型，所以我们也需要为Expression实现Display方法：

```rust
// parser/ast.rs
impl Display for Expression{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Expression::Consts(c) => write!(f, "{}", Value::from_expression_to_value(Expression::Consts(c.clone()))),
            Expression::Field(col_name) => write!(f, "{}", col_name),
            Expression::Operation(op) => match op {
                Operation::Equal(l, r) => write!(f, "{} = {}", l, r),
                Operation::Greater(l, r) => write!(f, "{} > {}", l, r),
                Operation::GreaterEqual(l, r) => write!(f, "{} >= {}", l, r),
                Operation::Less(l, r) => write!(f, "{} < {}", l, r),
                Operation::LessEqual(l, r) => write!(f, "{} <= {}", l, r),
                Operation::NotEqual(l, r) => write!(f, "{} != {}", l, r),
            },
            Expression::Function(func_name, col_name) => write!(f, "{}({})", func_name, col_name),
        }
    }
}
```

3. 修改Executor

mod.rs新增ResultSet

```rust
#[derive(Debug, PartialEq, Clone)]
pub enum ResultSet{
    Explain{
        plan: String,
    },
}

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::Explain {plan} => plan.to_string(),            
        }
    }
}
```

4. 修改SQLEngine层：

mod.rs中：

```rust
impl<E:Engine + 'static> Session<E> {
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            ast::Sentence::Explain {sentence} => {
                let plan = match self.transaction.as_ref() {
                    Some(_) => {
                        // 如果处在事务中
                        // 直接构建Plan节点返回即可
                        Plan::build(*sentence, self.transaction.as_mut().unwrap())?
                    },
                    None => {
                        // 手动构建事务
                        let mut transaction = self.engine.begin()?;
                        let plan = Plan::build(*sentence, &mut transaction)?;
                        transaction.commit()?;
                        plan
                    }
                };
                Ok(ResultSet::Explain { plan: plan.0.to_string()})  // to_string 实际上就是 node 的Display方法
            },
        }
    }
}
```

5. 测试：

```bash
sql-db>> show tables;
No tables found.
[Execution time: 861.295µs]
sql-db>> explain show tables;
           SQL PLAN           
------------------------------
Show Table Names
[Execution time: 554.091µs]
sql-db>> create table t (a int PRIMARY KEY, b int);
CREATE TABLE t
[Execution time: 613.482µs]
sql-db>> explain create table t (a int PRIMARY KEY, b int);
           SQL PLAN
------------------------------
Create Table t
[Execution time: 626.325µs]
sql-db>> explain insert into t values (1,2);
           SQL PLAN
------------------------------
Insert Into Table t
[Execution time: 1.444638ms]
sql-db>> explain select a, min(b) from t group by a limit 10 offset 10;
           SQL PLAN
------------------------------
Limit 10
 -> Offset 10
  -> Aggregate a , min(b)  Group By a
   -> Sequence Scan On Table t
[Execution time: 793.956µs]
sql-db>> explain select a from t where a = 1;
           SQL PLAN
------------------------------
Projection a
 -> Primary Key Scan On Table t(1)
[Execution time: 889.718µs]
```

执行是自下而上执行的，也可以看作上层节点调用了下层节点