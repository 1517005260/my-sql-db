# Update

update的语法如下：

```sql
UPDATE table_name
SET column_name = expression [, ...]
[WHERE condition];

--例如
UPDATE employees
SET salary = salary * 1.1
WHERE department = 'Sales';
```

关键字为：`UPDATE  SET  WHERE`

为了简单，condition部分我们仅先实现：`where column_name = xxx`

实现时，还是根据[基本架构](../01-BasicStructure)的思路，自顶向下实现语句。

Update的抽象语法树：

```
Update{
    table_name: String,
    columns: BTreeMap<String, Expression>,
    condition: Option<(String, Expression)>
}
```

### 代码实现

我们需要从Lexer开始一步步向下修改：

1. 首先在parser/lexer.rs中扩充Keyword的定义：

```rust
pub enum Keyword {
    ...,
    Update,
    Set,
    Where,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                ...,
                "UPDATE" => Keyword::Update,
                "SET" => Keyword::Set,
                "WHERE" => Keyword::Where,
            })
    }
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            ...,
            Keyword::Update => "UPDATE",
            Keyword::Set => "SET",
            Keyword::Where => "WHERE",
        }
    }
}

impl<'a> Lexer<'a> {
    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c{
            ...,
            '=' => Some(Token::Equal),
        })
    }
}
```

接着我们发现，update语句中有新的token`=`，所以也需要在lexer中修改：

```rust
pub enum Token {
    ...,
    Equal,    // =
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ...,
            Token::Equal => "=",
        })
    }
}
```

2. 接着在parser/mod.rs中修改相应的解析代码：

```rust
impl<'a> Parser<'a> {
    fn parse_sentence(&mut self) -> Result<ast::Sentence>{
        match self.peek()? {
            ...,
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
        }
    }

    // 分类：Update语句
    fn parse_update(&mut self) -> Result<Sentence>{
        self.expect_next_token_is(Token::Keyword(Keyword::Update))?;
        let table_name = self.expect_next_is_ident()?;
        self.expect_next_token_is(Token::Keyword(Keyword::Set))?;

        // loop 更新 columns
        // 又由于Set时不能出现重复，即 set a=1, a=2，所以需要去重
        let mut columns = BTreeMap::new();
        loop{
            let col = self.expect_next_is_ident()?;
            self.expect_next_token_is(Token::Equal)?;
            let value = self.parse_expression()?;
            if columns.contains_key(&col){
                return Err(Error::Parse(format!("[Parser] Update column {} conflicted",col)));
            }
            columns.insert(col, value);
            // 如果后续没有逗号，说明解析完成，退出循环
            if self.next_if_is_token(Token::Comma).is_none(){
                break;
            }
        }
        Ok(Sentence::Update {
            table_name,
            columns,
            condition: self.parse_where_condition()?,
        })
    }

    fn parse_where_condition(&mut self) -> Result<Option<(String, Expression)>>{
        if self.next_if_is_token(Token::Keyword(Keyword::Where)).is_none(){
            return Ok(None);  // 没有指定where条件
        }
        let col = self.expect_next_is_ident()?;
        self.expect_next_token_is(Token::Equal)?;
        let value = self.parse_expression()?;
        Ok(Some((col, value)))
    }
}
```

这里涉及了ast.rs中新的update抽象语法树的定义：

```rust
pub enum Sentence{
    ...,
    Update{
    table_name: String,
    columns: BTreeMap<String, Expression>,
    condition: Option<(String, Expression)>
    },
}
```

parser/mod.rs中新增测试：

```rust
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
            condition: Some(("c".into(), ast::Consts::String("a".into()).into())),
        }
    );

    Ok(())
}
```

可以看到输出：

```
Update { table_name: "tbl", columns: {"a": Consts(Integer(1)), "b": Consts(Float(2.0))}, condition: Some(("c", Consts(String("a")))) }
```

3. 接着修改planner/mod.rs中的执行节点定义：

顺便对Scan节点新增属性过滤条件：

```rust
pub enum Node{
    ...,
    Scan{
    // select
    table_name: String,
    // 过滤条件
    filter: Option<(String, Expression)>,
    },

    Update{
        table_name: String,
        scan: Box<Node>,
        columns: BTreeMap<String, Expression>,
    },
}
```

需要注意的是，update实际上是先扫描符合要求的数据，然后重写，所以Update节点里递归地定义了一个扫描Scan节点，它会先于Update执行。

之后修改planner.rs中的匹配：

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Node{
        match sentence {
            ...,
            Sentence::Select {table_name} =>
                Node::Scan {table_name, filter:None},

            Sentence::Update {table_name, columns, condition} =>
                Node::Update {
                    table_name: table_name.clone(),
                    scan: Box::new(Node::Scan {table_name, filter: condition}),
                    columns,
                },
        }
}
```

还需要对mod.rs中的测试方法进行修改：

```rust
#[test]
fn test_plan_select() -> Result<()> {
    let sql = "select * from tbl1;";
    let sentence = Parser::new(sql).parse()?;
    let p = Plan::build(sentence);
    assert_eq!(
        p,
        Plan(Node::Scan {
            table_name: "tbl1".to_string(),
            filter: None,
        })
    );

    Ok(())
}
```

4. 对executor部分进行修改：

在mod.rs中:

```rust
impl<T:Transaction> dyn Executor<T>{
    pub fn build(node: Node) -> Box<dyn Executor<T>>{
        match node {
            Node::CreateTable {schema} => CreateTable::new(schema),
            Node::Insert {table_name,columns,values} => Insert::new(table_name, columns, values),
            Node::Scan {table_name,filter} => Scan::new(table_name,filter),
            Node::Update {table_name, scan, columns} =>
                Update::new(table_name,
                            Self::build(*scan),
                            columns),
        }
    }
}
```

修改query.rs：

```rust
pub struct Scan{
    table_name: String,
    filter: Option<(String, Expression)>
}

impl Scan{
    pub fn new(table_name: String, filter: Option<(String, Expression)>) -> Box<Self>{
        Box::new(Self{ table_name, filter })
    }
}

impl<T:Transaction> Executor<T> for Scan{
    fn execute(self:Box<Self>,trasaction:&mut T) -> crate::error::Result<ResultSet> {
        ...
        let rows = trasaction.scan(self.table_name.clone(), self.filter)?;
    }
}
```

修改mutation.rs：

```rust
pub struct Update<T: Transaction>{
    table_name:String,
    scan: Box<dyn Executor<T>>,   // scan 是一个执行节点，这里是递归的定义。执行节点又是Executor<T>接口的实现，在编译期不知道类型，需要Box包裹
    columns: BTreeMap<String, Expression>,
}

impl<T:Transaction> Update<T>{
    pub fn new(table_name:String,scan:Box<dyn Executor<T>>,columns:BTreeMap<String,Expression>) -> Box<Self> {
        Box::new(Self{
            table_name,scan,columns
        })
    }
}

impl<T:Transaction> Executor<T> for  Update<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        let mut count = 0;
        // 先获取到扫描的结果，这是我们需要更新的数据
        match self.scan.execute(transaction)? {
            ResultSet::Scan {columns, rows} => {
                // 处理更新流程
                let table = transaction.must_get_table(self.table_name.clone())?;
                // 遍历每行，更新列数据
                for row in rows{
                    let mut new_row = row.clone();
                    let mut primary_key = table.get_primary_key(&row)?;
                    for (i ,col) in columns.iter().enumerate(){
                        if let Some(expression) = self.columns.get(col) {
                            // 如果本列需要修改
                            new_row[i] = Value::from_expression_to_value(expression.clone());
                        }
                    }
                    // 如果涉及了主键的更新，由于我们存储时用的是表名和主键一起作为key，所以这里需要删了重新建key
                    // 否则，key部分(table_name, primary_key) 不动，直接变value即可
                    transaction.update_row(&table, &primary_key, new_row)?;
                    count += 1;
                }
            },
            _ => return Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string())),
        }

        Ok(ResultSet::Update {count})
    }
}
```

这里还需要为ast.rs中的enum实现Clone注解：

```rust
#[derive(Debug,PartialEq,Clone)]
pub enum Expression{ }

#[derive(Debug, PartialEq, Clone)]
pub enum Consts{}
```

在mod.rs中新增ResultSet：

```rust
pub enum ResultSet{
    ...,
    Update{
    count: usize, // 更新了多少条数据
    },
}
```

这里需要注意，由于这里涉及到了递归调用，编译器不知道Scan未运行前是什么类型，要确保调用者活得比被调用者长，根据编译器提示，我们直接强制生命周期为static无限长即可，防止报错。即T的生命周期一定大于等于Scan节点的生命周期。

```rust
// executor/mod.rs
impl<T:Transaction + 'static> dyn Executor<T>{}

// planner/mod.rs
pub fn execute<T:Transaction + 'static>(self, transaction :&mut T) -> Result<ResultSet>{}

// engine/mod.rs
impl<E:Engine + 'static> Session<E>{}
```

5. 为Transaction实现更新行的操作，以便executor层调用，并且需要更新扫描操作，增加过滤条件

在engine/mod.rs中：

```rust
pub trait Transaction {
    ...,
    // 更新行
    fn update_row(&mut self,table:&Table, primary_key:&Value, row: Row)-> Result<()>;

    // 扫描表
    fn scan(&self,table_name: String, filter: Option<(String, Expression)>)-> Result<Vec<Row>>;
}
```

在kv.rs中实现接口：

```rust
impl<E:storageEngine> Transaction for KVTransaction<E> {
    ...,
    fn update_row(&mut self, table: &Table, primary_key: &Value, row: Row) -> Result<()> {
        // 对比主键是否修改，是则删除原key，建立新key
        let new_primary_key = table.get_primary_key(&row)?;
        if new_primary_key != *primary_key{
            let key = Key::Row(table.name.clone(), primary_key.clone()).encode()?;
            self.transaction.delete(key)?;
        }

        let key = Key::Row(table.name.clone(), new_primary_key.clone()).encode()?;
        let value = bincode::serialize(&row)?;
        self.transaction.set(key, value)?;
        Ok(())
    }

    fn scan(&self, table_name: String, filter: Option<(String, Expression)>) -> Result<Vec<Row>> {
        let table = self.must_get_table(table_name.clone())?;
        // 根据前缀扫描表
        let prefix = PrefixKey::Row(table_name.clone()).encode()?;
        let results = self.transaction.prefix_scan(prefix)?;

        let mut rows = Vec::new();
        for res in results {
            // 根据filter过滤数据
            let row: Row = bincode::deserialize(&res.value)?;
            if let Some((col, expression)) = &filter {
                let col_index = table.get_col_index(col)?;
                if Value::from_expression_to_value(expression.clone()) == row[col_index].clone(){
                    // 过滤where的条件和这里的列数据是否一致
                    rows.push(row);
                }
            }else{
                // filter不存在，查找所有数据
                rows.push(row);
            }
        }
        Ok(rows)
    }
}
```

为了方便找到列索引，我们在sql/schema.rs中新增：

```rust
impl Table{
    // 获取列索引
    pub fn get_col_index(&self, col_name: &str) -> Result<usize> {
        self.columns.iter().position(|c| c.name == col_name)
            .ok_or(Error::Internal(format!("[Get Column Index Failed] Column {} not found", col_name)))
    }
}
```

6. 测试，在kv.rs中：

```rust
#[test]
fn test_update() -> Result<()> {
    let kvengine = KVEngine::new(MemoryEngine::new());
    let mut s = kvengine.session()?;

    s.execute(
        "create table t1 (a int primary key, b text default 'vv', c integer default 100);",
    )?;
    s.execute("insert into t1 values(1, 'a', 1);")?;
    s.execute("insert into t1 values(2, 'b', 2);")?;
    s.execute("insert into t1 values(3, 'c', 3);")?;

    let v = s.execute("update t1 set b = 'aa' where a = 1;")?;
    let v = s.execute("update t1 set a = 33 where a = 3;")?;
    println!("{:?}", v);

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

ResultSet需要实现Debug注解，在executor/mod.rs中：

```rust
#[derive(Debug)]
pub enum ResultSet{}
```

输出：

```
Update { count: 1 }
[Integer(1), String("aa"), Integer(1)]
[Integer(2), String("b"), Integer(2)]
[Integer(33), String("c"), Integer(3)]
```