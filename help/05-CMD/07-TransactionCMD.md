# 显式事务命令

完善显式的事务命令：Begin, Rollback, Commit

之前在执行sql时：

```rust
// engine/mod.rs
pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
    match Parser::new(sql).parse()? {    // 传进来的sql直接扔给parser解析
        sentence => {         //  获取到了一句sql
            let mut transaction = self.engine.begin()?;  // 开启事务

            // 开始构建plan
            match Plan::build(sentence)?.    // 这里获得一个node
                execute(&mut transaction) {
                Ok(res) => {
                    transaction.commit()?;  // 成功，事务提交
                    Ok(res)
                },
                Err(e) => {
                    transaction.rollback()?;  // 失败，事务回滚
                    Err(e)
                }
            }
        }
    }
}
```

我们实际上也构建了事务，但是这对用户来说是不可见的。

## 代码实现

1. 修改parser

在lexer.rs中新增关键字：

```rust
#[derive(Debug, Clone, PartialEq, EnumIter)]
pub enum Keyword {
    Begin,
    Commit,
    Rollback,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                "BEGIN" => Keyword::Begin,
                "COMMIT" => Keyword::Commit,
                "ROLLBACK" => Keyword::Rollback,
            })
    }
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Begin => "BEGIN",
            Keyword::Commit => "COMMIT",
            Keyword::Rollback => "ROLLBACK",            
        }
    }
}
```

在ast.rs中新增事务语句的定义：

```rust
#[derive(Debug,PartialEq)]
pub enum Sentence{
    Begin{
        //  没有参数，因为事务号是底层mvcc自动增加的
    },
    Commit{
    },
    Rollback{
    },
}
```

在mod.rs中解析事务命令：

```rust
// 解析语句
fn parse_sentence(&mut self) -> Result<Sentence>{
    // 我们尝试查看第一个Token以进行分类
    match self.peek()? {
        Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
        Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
        Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
        Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
        Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
        Some(Token::Keyword(Keyword::Show)) => self.parse_show(),
        Some(Token::Keyword(Keyword::Begin)) => self.parse_transaction(),
        Some(Token::Keyword(Keyword::Commit)) => self.parse_transaction(),
        Some(Token::Keyword(Keyword::Rollback)) => self.parse_transaction(),
        Some(token) => Err(Error::Parse(format!("[Parser] Unexpected token {}",token))),  // 其他token
        None => Err(Error::Parse("[Parser] Unexpected EOF".to_string()))
    }
}

// 分类：事务命令
fn parse_transaction(&mut self) -> Result<Sentence>{
    let sentence = match self.next()? {
        Token::Keyword(Keyword::Begin) => Sentence::Begin{},
        Token::Keyword(Keyword::Commit) => Sentence::Commit{},
        Token::Keyword(Keyword::Rollback) => Sentence::Rollback{},
        _ => return Err(Error::Internal("[Parser] Unknown transaction command".to_string()))
    };
    Ok(sentence)
}
```

2. 修改planner

底层的逻辑和之前的命令都不一样。由于我们是在session处开启与处理事务的，所以planner只是形式上接收，并不会再向executor传递

```rust
// mod.rs
fn build_sentence(&mut self, sentence: Sentence) -> Result<Node>{
    Ok(match sentence {
        Sentence::Begin{} | Sentence::Commit{} | Sentence::Rollback{} => {
            return Err(Error::Internal("[Planner] Unexpected transaction command".into()));
        },        
    })
}
```
3. 修改executor中ResultSet的定义，我们希望用户能得知当前显式事务命令的版本号

```rust
// mod.rs
#[derive(Debug, PartialEq, Clone)]
pub enum ResultSet{
    Begin{
        version: u64,
    },
    Commit{
        version: u64,
    },
    Rollback{
        version: u64,
    },
}

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::Begin {version} => format!("TRANSACTION {} BEGIN", version),
            ResultSet::Commit {version} => format!("TRANSACTION {} COMMIT", version),
            ResultSet::Rollback {version} => format!("TRANSACTION {} ROLLBACK", version),   
        }
    }
}
```

4. 在engine层实现事务逻辑

mod.rs中：

```rust
pub trait Transaction {
    // 获取事务版本号
    fn get_version(&self) -> u64;
}

pub struct Session<E:Engine>{
    engine: E,  // 存储当前的 SQL 引擎实例
    transaction: Option<E::Transaction>,   // 显式事务命令
}
```

由于事务的版本号是在storage/mvcc.rs中的TransactionState维护的，所以需要自底向上修改：

```rust
// mvcc.rs
impl<E:Engine> MvccTransaction<E> {
    // 获取事务版本号
    pub fn get_version(&self) -> u64{
        self.state.version
    }
}

// engine/kv.rs
impl<E:storageEngine> Transaction for KVTransaction<E> {
    fn get_version(&self) -> u64 {
        self.transaction.get_version()
    }
}
```

继续在engine/mod.rs中：

```rust
pub trait Engine: Clone{
    fn session(&self) -> Result<Session<Self>>{    // 客户端与sql服务端的连接靠session来维持
        Ok(Session{
            engine: self.clone(),     // 确保 Session 拥有当前引擎的一个副本
            transaction: None,       // 初始化为None，直到有显式事务
        })
    }
}

impl<E:Engine + 'static> Session<E> {
    // 执行客户端传来的sql语句
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {
            ast::Sentence::Begin{} => {        // 处理事务命令
                let transaction = self.engine.begin()?;
                let version = transaction.get_version();
                self.transaction = Some(transaction);
                Ok(ResultSet::Begin { version })
            },
            ast::Sentence::Commit{} => {
                let transaction = self.transaction.take()  // take() 会将 Option 取出，同时将原来的 Option 设置为 None
                    .unwrap();
                let version = transaction.get_version();
                transaction.commit()?;
                Ok(ResultSet::Commit { version })
            },
            ast::Sentence::Rollback{} => {
                let transaction = self.transaction.as_ref().unwrap();
                let version = transaction.get_version();
                transaction.rollback()?;
                Ok(ResultSet::Rollback { version })
            },
        }
    }
}
```

### 出现了所有权转移问题

#### take()方法解决

这里需要着重了解take()方法：在Rust中，不允许部分移动结构体的字段，因为：

- 结构体必须在任何时候都是完整且有效的
- 如果允许移动字段，会留下一个部分未初始化的结构体，这在 Rust 的内存安全模型中是不允许的

如果仅用unwrap()，就像从房子里偷东西一样，所有权直接被转移了。代码中，self.transaction是个结构体的字段，是不允许被移动的。

而take()方法，会：

- 将 Option 中的值取出
- 同时将原 Option 设置为 None
- 这样保持了结构体的有效性，因为 None 是一个有效的 Option 值

这样就像从房子里拿走一个有东西的盒子，接着再通过unwrap()取出盒子里的东西，同时take()还会在房子里放一个新的空盒子

take的源码如下：

```rust
#[inline]
#[stable(feature = "rust1", since = "1.0.0")]
#[rustc_const_unstable(feature = "const_option", issue = "67441")]
pub const fn take(&mut self) -> Option<T> {
    mem::replace(self, None)
}
```

#### as_ref()解决

或者我们可以直接`self.transaction.as_ref().unwrap();`，转换成对transaction字段的引用，这样也不会出现所有权转移问题。

5. engine/mod.rs中的逻辑继续修改

如果我们begin了一个显示事务，我们就需要在这个事务中执行sql语句，直到这个事务提交，所以完成的match逻辑为：

```rust
pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
    match Parser::new(sql).parse()? {    // 传进来的sql直接扔给parser解析
        ast::Sentence::Begin{} if self.transaction.is_some() =>{
            return Err(Internal("[Exec Transaction] Already in transaction".into()))
        },
        ast::Sentence::Commit{} | ast::Sentence::Rollback{}  if self.transaction.is_none()=> {
            return Err(Internal("[Exec Transaction] Not in transaction".into()))
        },
        ast::Sentence::Begin{} => {        // 处理事务命令
            let transaction = self.engine.begin()?;
            let version = transaction.get_version();
            self.transaction = Some(transaction);
            Ok(ResultSet::Begin { version })
        },
        ast::Sentence::Commit{} => {
            let transaction = self.transaction.take()  // take() 会将 Option 取出，同时将原来的 Option 设置为 None
                .unwrap();
            // let transaction = self.transaction.as_ref().unwrap();
            let version = transaction.get_version();
            transaction.commit()?;
            Ok(ResultSet::Commit { version })
        },
        ast::Sentence::Rollback{} => {
            let transaction = self.transaction.take().unwrap();
            // let transaction = self.transaction.as_ref().unwrap();
            let version = transaction.get_version();
            transaction.rollback()?;
            Ok(ResultSet::Rollback { version })
        },
        sentence if self.transaction.is_some() =>{
            // 在事务内的sql
            Plan::build(sentence)?.execute(self.transaction.as_mut().unwrap())
        },
        sentence => {         //  获取到了一句无显式事务的sql
            let mut transaction = self.engine.begin()?;  // 开启事务

            // 开始构建plan
            match Plan::build(sentence)?.    // 这里获得一个node
                execute(&mut transaction) {
                Ok(res) => {
                    transaction.commit()?;  // 成功，事务提交
                    Ok(res)
                },
                Err(e) => {
                    transaction.rollback()?;  // 失败，事务回滚
                    Err(e)
                }
            }
        },
    }
}
```

6. 在client.rs中优化命令行显示。如果用户输入了事务命令，则要提示用户当前的事务号

```rust
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // ...
    loop {
        let prompt = if multiline.is_empty() {
            match client.transaction_version {
                Some(version) => format!("transaction#{}>> ", version),
                None => "sql-db>> ".to_string(),
            }
        } else {
            ".......> ".to_string()
        };
        let readline = editor.readline(&prompt);
        // ...
    }
}

pub struct Client {
    stream: TcpStream,
    transaction_version: Option<u64>,
}

impl Client {
    pub async fn new(address: SocketAddr) -> Result<Self, Box<dyn Error>> {
        let stream = TcpStream::connect(address).await?;
        Ok(Self { stream , transaction_version: None })
    }

    pub async fn exec_cmd(&mut self, cmd: &str) -> Result<(), Box<dyn Error>> {
        // ...
        // 接收执行结果
        while let Some(val) = stream.try_next().await? {
            if val == RESPONSE_END {
                break;
            }
            // 解析事务命令
            if val.starts_with("TRANSACTION"){
                let args = val.split(" ").collect::<Vec<_>>();
                if args[2] == "COMMIT" || args[2] == "ROLLBACK" {
                    self.transaction_version = None;
                }
                if args[2] == "BEGIN" {
                    let version = args[1].parse::<u64>().unwrap();
                    self.transaction_version = Some(version);
                }
            }
            // 打印执行结果
            println!("{}", val);
        }
        Ok(())
    }
}
```

7. 命令行测试：

```bash
sql-db>> begin;
TRANSACTION 13 BEGIN
[Execution time: 703.845µs]
transaction#13>> show tables;
t1
t2
[Execution time: 718.996µs]
transaction#13>> create table t3(t int primary key);
CREATE TABLE t3
[Execution time: 633.902µs]
transaction#13>> show tables;
t1
t2
t3
[Execution time: 457.742µs]
transaction#13>>
```

此时另起一个终端：

```bash
sql-db>> show tables;
t1
t2
[Execution time: 661.643µs]
sql-db>>
```

发现事务是满足隔离性的。