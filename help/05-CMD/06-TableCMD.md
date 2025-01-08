# 表命令完善

完善命令

- `show tables` 查看所有表名
- `show table table_name` 查看单表结构

## 代码实现

1. 优化

之前的client.rs中：

```rust
pub async fn exec_cmd(&self, cmd: &str) -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect(self.address).await?;
    // ...
    Ok(())
}
```

stream写在exec_cmd里会导致每次执行命令都需要进行一次连接，这里进行优化：

```rust
async fn main() -> Result<(), Box<dyn Error>> {
    //...
    let mut client = Client::new(addr).await?;
}

pub struct Client {
    stream: TcpStream,
}

impl Client {
    pub async fn new(address: SocketAddr) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self { stream })
    }

    pub async fn exec_cmd(&mut self, cmd: &str) -> Result<(), Box<dyn Error>> {
        let (r, w) = self.stream.split();
        // ...
    }
}
```

现在在new的时候就可以建立连接，后续就不会一直连接了。

2. 增加执行方法

由于我们需要table的metadata，所以在sql/schema.rs中为Table和Column增加Display方法，使得可以被打印成字符串：

```rust
impl Display for Table{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let column_description = self.columns.iter()
            .map(|c| format!("{}", c))  // 分别展示每列的信息
            .collect::<Vec<_>>().join(",\n");
        write!(f, "TABLE NAME: {} (\n{}\n)", self.name, column_description)
    }
}

impl Display for Column{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut column_description = format!("  {} {:?} ", self.name, self.datatype);
        if self.is_primary_key {
            column_description += "PRIMARY KEY ";
        }
        if !self.nullable && !self.is_primary_key {
            column_description += "NOT NULL ";
        }
        if let Some(v) = &self.default {
            column_description += &format!("DEFAULT {}", v.to_string());
        }
        write!(f, "{}", column_description)
    }
}
```

接着发现，我们没有写获取所有表名的方法，于是修改：

```rust
// engine/mod.rs新增接口
pub trait Transaction {
    fn get_all_table_names(&self)-> Result<Vec<String>>;
}

// kv.rs实现
impl<E:storageEngine> Transaction for KVTransaction<E> {
    fn get_all_table_names(&self) -> Result<Vec<String>> {
        let prefix = PrefixKey::Table.encode()?;
        let results = self.transaction.prefix_scan(prefix)?;
        let mut names = Vec::new();
        for result in results {
            let table: Table = bincode::deserialize(&result.value)?;
            names.push(table.name);
        }
        Ok(names)
    }
}
```

另外测试代码时，发现storage/engine.rs中有报错：

```rust
// 前缀扫描
fn prefix_scan(&mut self, prefix: Vec<u8>) -> Self::EngineIter<'_>{
    // 特定条件下的scan
    let start = Bound::Included(prefix.clone());
    let mut bound_prefix = prefix.clone();
    if let Some(last) = bound_prefix.iter_mut().last() {
        *last += 1;
    }
    let end = Bound::Excluded(bound_prefix);
    self.scan((start,end))
}
```

这里的 prefix 是u8，即最大为255，如果本身last就是255则会溢出。所以我们做出如下修改：

```rust
// 前缀扫描
fn prefix_scan(&mut self, prefix: Vec<u8>) -> Self::EngineIter<'_>{
    // abc,abd,abe, 均在 < abf的范围内，即[abc, ab (e+1) )
    let start = Bound::Included(prefix.clone());
    let mut bound_prefix = prefix.clone();
    let end = match bound_prefix.iter().rposition(|b| *b != 255) {  // 从后往前找第一个不是255的
        Some(pos) => {
            bound_prefix[pos] += 1;
            bound_prefix.truncate(pos + 1);  // 从255开始向后丢弃
            Bound::Excluded(bound_prefix)
        }
        None => Bound::Unbounded,
    };
    self.scan((start,end))
}
```

3. 修改parser

在lexer.rs中新增关键字

```rust
#[derive(Debug, Clone, PartialEq, EnumIter)]
pub enum Keyword {
    Show,
    Tables,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                "SHOW" => Keyword::Show,
                "TABLES" => Keyword::Tables,
            }
        )
    }
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Show => "SHOW",
            Keyword::Tables => "TABLES",
        }
    }
}
```

在ast.rs中新增show结构：

```rust
#[derive(Debug,PartialEq)]
pub enum Sentence{
    TableSchema{
        table_name: String,
    },
    TableNames{
        // 没有参数，因为是全体表
    },
}
```

修改mod.rs:

```rust
fn parse_sentence(&mut self) -> Result<Sentence>{
    // 我们尝试查看第一个Token以进行分类
    match self.peek()? {
        Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
        Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
        Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
        Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
        Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
        Some(Token::Keyword(Keyword::Show)) => self.parse_show(),
        Some(token) => Err(Error::Parse(format!("[Parser] Unexpected token {}",token))),  // 其他token
        None => Err(Error::Parse("[Parser] Unexpected EOF".to_string()))
    }
}

// 分类：show语句
fn parse_show(&mut self) -> Result<Sentence>{
    self.expect_next_token_is(Token::Keyword(Keyword::Show))?;
    match self.next()? {
        Token::Keyword(Keyword::Tables) => Ok(TableNames {}),
        Token::Keyword(Keyword::Table) => Ok(TableSchema {table_name: self.expect_next_is_ident()?}),
        _ => Err(Error::Internal("[Parser] Unexpected token".to_string()))
    }
}
```

4. 修改planner

在mod.rs中新增：

```rust
#[derive(Debug,PartialEq)]
pub enum Node{
    ableSchema{
        name: String,
    },
    TableNames{

    },
}
```

修改planner.rs:

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Result<Node>{
    Ok(match sentence {
        Sentence::TableSchema {table_name} => Node::TableSchema {name: table_name},
        Sentence::TableNames { } => Node::TableNames {},      
    })
}
```

5. 修改Executor

在mod.rs中修改ResultSet的定义：

```rust
mod show;

#[derive(Debug, PartialEq, Clone)]
pub enum ResultSet{
    TableSchema{
        schema: String,
    },
    TableNames{
        names: Vec<String>,
    },
}

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::TableSchema { schema } => format!("{}", schema),
            ResultSet::TableNames { names } => {
                if names.is_empty() {
                    "No tables found.".to_string()
                } else {
                    names.join("\n")
                }
            },    
        }
    }
}

pub fn build(node: Node) -> Box<dyn Executor<T>>{
    match node {
        Node::TableSchema {name} => TableSchema::new(&name),
        Node::TableNames { } => TableNames::new(),        
    }
}
```

新建show.rs处理逻辑

```rust
use std::marker::PhantomData;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};

pub struct TableSchema<T: Transaction> {
    name: String,
    _marker: PhantomData<T>,  // 通过添加 _marker: PhantomData<T>，我们告诉编译器该结构体实际上与 T 相关联，尽管它不直接使用 T
}

impl<T: Transaction> TableSchema<T> {
    pub fn new(name: &str) -> Box<Self> {
        Box::new(TableSchema {
            name: name.into(),
            _marker: PhantomData,
        })
    }
}

impl<T: Transaction> Executor<T> for TableSchema<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        let table = transaction.must_get_table(self.name.clone())?;
        let schema = table.to_string();

        Ok(ResultSet::TableSchema { schema })
    }
}

pub struct TableNames<T: Transaction> {
    _marker: PhantomData<T>,
}

impl<T: Transaction> TableNames<T> {
    pub fn new() -> Box<Self> {
        Box::new(TableNames {
            _marker: PhantomData,
        })
    }
}

impl<T: Transaction> Executor<T> for TableNames<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        let names = transaction.get_all_table_names()?;
        Ok(ResultSet::TableNames { names })
    }
}
```

6. 修改server：

```rust
enum Request{
    // 客户端的请求类型
    SQL(String),   // SQL命令
}

pub fn new(engine: MutexGuard<'_, E>) -> Result<Self>{  // 根据编译器提示修改生命周期
    Ok(Self{session: engine.session()?})
}

// 执行request命令
let response = match request {
SQL(sql) => self.session.execute(&sql),
};

// 开启一个tokio任务
tokio::spawn(async move {
    match server_session.handle_request(socket).await{
    Ok(_) => {}
    Err(e) => {
        println!("Internal server error {:?}", e);
        }
    }
});
```

7. bug修复

发现在outer join一张空表时报错，排查发现，在join.rs中：

```rust
for _ in 0..right_rows[0].len() {
    row.push(Value::Null);
}
```

在右表全空时会报错，需要改为：

```rust
for _ in 0..right_cols.len() {
    row.push(Value::Null);
}
```

8. 测试命令行：

```bash
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.03s
     Running `target/debug/client`
sql-db>> show tables;
No tables found.
[Execution time: 725.231µs]
sql-db>> create table t1 (a int PRIMARY KEY, b float not null);
CREATE TABLE t1
[Execution time: 606.286µs]
sql-db>> CREATE TABLE t2 (c int PRIMARY KEY, d bool default true);
CREATE TABLE t2
[Execution time: 672.934µs]
sql-db>> show tables;
t1
t2
[Execution time: 637.397µs]
sql-db>> show table t1;
TABLE NAME: t1 (
  a Integer PRIMARY KEY ,
  b Float NOT NULL
)
[Execution time: 495.706µs]
sql-db>> show table t2;
TABLE NAME: t2 (
  c Integer PRIMARY KEY ,
  d Boolean DEFAULT TRUE
)
[Execution time: 627.665µs]
sql-db>> insert into t1 VALUES(1, 1.1),(2,2.2),(3,3.3);
INSERT 3 rows
[Execution time: 757.297µs]
sql-db>> select * from t1;
a |b
--+----
1 |1.1
2 |2.2
3 |3.3
(3 rows)
[Execution time: 744.872µs]
sql-db>> insert into t2 values(1,false),(2),(3,false);
INSERT 3 rows
[Execution time: 681.942µs]
sql-db>> select * from t2;
c |d
--+------
1 |FALSE
2 |TRUE
3 |FALSE
(3 rows)
[Execution time: 740.645µs]
sql-db>> select * from t1 join t2 on a=c;
a |b   |c |d
--+----+--+------
1 |1.1 |1 |FALSE
2 |2.2 |2 |TRUE
3 |3.3 |3 |FALSE
(3 rows)
[Execution time: 718.753µs]
sql-db>> select max(a) from t1;
max
----
3
(1 rows)
[Execution time: 604.034µs]
sql-db>>
```