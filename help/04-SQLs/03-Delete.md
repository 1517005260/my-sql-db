# Delete

Delete的实现和Update类似，它的语法如下：

```sql
DELETE FROM table_name
[WHERE condition];

-- 例如
DELETE FROM employees
WHERE department_id = 5;
```

新增关键字为：`DELETE`。

为了简单，condition部分我们仅先实现：`where column_name = xxx`。

Delete语句的抽象语法树：

```
Delete{
    table_name: String,
    condition: Option<(String, Expression)>
}
```

## 代码实现

流程基本和Update一致，而且更简单，并且可以服用Update里解析condition的逻辑

1. 在sql/parser/lexer.rs中，新增：

```rust
pub enum Keyword {
    ...,
    Delete,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                ...,
                "DELETE" => Keyword::Delete,
            })
    }

    pub fn to_str(&self) -> &str {
        match self {
            ...,
            Keyword::Delete => "DELETE",
        }
    }
}
```

在ast.rs中新增语法树：

```rust
pub enum Sentence{
    ...,
    Delete{
        table_name: String,
        condition: Option<(String, Expression)>,
    },
}
```

在parser/mod.rs中新增解析Delete：

```rust
impl<'a> Parser<'a> {
    fn parse_sentence(&mut self) -> Result<Sentence>{
        match self.peek()? {
            ...,
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
        }
    }

    // 分类：Delete语句
    fn parse_delete(&mut self) -> Result<Sentence>{
        self.expect_next_token_is(Token::Keyword(Keyword::Delete))?;
        self.expect_next_token_is(Token::Keyword(Keyword::From))?;
        let table_name = self.expect_next_is_ident()?;
        Ok(Sentence::Delete {
            table_name,
            condition: self.parse_where_condition()?,
        })
    }
}
```

2. 在planner中修改：

这里也涉及递归定义，即扫描出符合条件的节点后才能进行删除

在mod.rs中：

```rust
pub enum Node{
    ...,
    Delete{
        table_name: String,
        scan: Box<Node>,
    },
}
```

在planner.rs中：

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Node{
        match sentence {
         ...,
        Sentence::Delete {table_name, condition} =>
            Node::Delete {
                table_name:table_name.clone(),
                scan: Box::new(Node::Scan {table_name, filter: condition})
            },   
        }
}
```

3. 修改executor层

在mutation.rs中：

```rust
pub struct Delete<T: Transaction>{
    table_name:String,
    scan: Box<dyn Executor<T>>,
}

impl<T:Transaction> Delete<T>{
    pub fn new(table_name:String,scan:Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self{
            table_name,scan
        })
    }
}

impl<T:Transaction> Executor<T> for Delete<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        let mut count = 0;
        match self.scan.execute(transaction)? {
            ResultSet::Scan {columns:_, rows} => {  // columns 参数未用到
                let table = transaction.must_get_table(self.table_name)?;
                for row in rows{
                    // 删除行，而行定位的key为(table_name, primary_key)，所以还需要主键
                    let primary_key = table.get_primary_key(&row)?;
                    transaction.delete_row(&table, &primary_key)?;
                    count +=1;
                }
                Ok(ResultSet::Delete {count})
            },
            _ => Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string())),
        }
    }
}
```

在mod.rs中：

```rust
pub enum ResultSet{
    ...,
    Delete{
        count: usize,   // 删除了多少条数据  
    },
}

pub fn build(node: Node) -> Box<dyn Executor<T>>{
    match node {
        ...,
        Node::Delete {table_name, scan} => Delete::new(table_name, Self::build(*scan)),
    }
}
```

4. 增加engine层的事务定义：

在mod.rs中更新接口：

```rust
pub trait Transaction {
    ...,
    fn delete_row(&mut self,table:&Table, primary_key:&Value)-> Result<()>;
}
```

在kv.rs中实现：

```rust
impl<E:storageEngine> Transaction for KVTransaction<E> {
    fn delete_row(&mut self, table: &Table, primary_key: &Value) -> Result<()> {
        let key = Key::Row(table.name.clone(), primary_key.clone()).encode()?;
        self.transaction.delete(key)
    }
}
```

5. 在kv.rs中简单测试：

```rust
#[test]
fn test_delete() -> Result<()> {
    let kvengine = KVEngine::new(MemoryEngine::new());
    let mut s = kvengine.session()?;

    s.execute(
        "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
    )?;
    s.execute("insert into t1 values(1, 'a', 1);")?;
    s.execute("insert into t1 values(2, 'b', 2);")?;
    s.execute("insert into t1 values(3, 'c', 3);")?;

    s.execute("delete from t1 where a = 3;")?;
    s.execute("delete from t1 where a = 2;")?;

    match s.execute("select * from t1;")? {
        crate::sql::executor::ResultSet::Scan { columns, rows } => {
            for row in rows {
                println!("{:?}", row);
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
```