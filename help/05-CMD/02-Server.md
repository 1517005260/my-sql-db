# Server 实现

启动一个TCP服务，监听对应端口，处理TCP请求，向客户端返回结果

## 代码实现

项目依赖：

```toml
tokio = { version = "1.41.1", features = ["full"] }
tokio-util = { version = "0.7.12", features = ["full"] }
tokio-stream = "0.1.16"
futures = "0.3.31"
```

1. 新建目录bin，即可执行程序，新建server.rs

这是官方的示例程序：

```rust
#![warn(rust_2018_idioms)]

use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use futures::SinkExt;
use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};

/// The in-memory database shared amongst all clients.
///
/// This database will be shared via `Arc`, so to mutate the internal map we're
/// going to use a `Mutex` for interior mutability.
struct Database {
    map: Mutex<HashMap<String, String>>,
}

/// Possible requests our clients can send us
enum Request {
    Get { key: String },
    Set { key: String, value: String },
}

/// Responses to the `Request` commands above
enum Response {
    Value {
        key: String,
        value: String,
    },
    Set {
        key: String,
        value: String,
        previous: Option<String>,
    },
    Error {
        msg: String,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Parse the address we're going to run this server on
    // and set up our TCP listener to accept connections.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {addr}");

    // Create the shared state of this server that will be shared amongst all
    // clients. We populate the initial database and then create the `Database`
    // structure. Note the usage of `Arc` here which will be used to ensure that
    // each independently spawned client will have a reference to the in-memory
    // database.
    let mut initial_db = HashMap::new();
    initial_db.insert("foo".to_string(), "bar".to_string());
    let db = Arc::new(Database {
        map: Mutex::new(initial_db),
    });

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                // After getting a new connection first we see a clone of the database
                // being created, which is creating a new reference for this connected
                // client to use.
                let db = db.clone();

                // Like with other small servers, we'll `spawn` this client to ensure it
                // runs concurrently with all other clients. The `move` keyword is used
                // here to move ownership of our db handle into the async closure.
                tokio::spawn(async move {
                    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
                    // to convert our stream of bytes, `socket`, into a `Stream` of lines
                    // as well as convert our line based responses into a stream of bytes.
                    let mut lines = Framed::new(socket, LinesCodec::new());

                    // Here for every line we get back from the `Framed` decoder,
                    // we parse the request, and if it's valid we generate a response
                    // based on the values in the database.
                    while let Some(result) = lines.next().await {
                        match result {
                            Ok(line) => {
                                let response = handle_request(&line, &db);

                                let response = response.serialize();

                                if let Err(e) = lines.send(response.as_str()).await {
                                    println!("error on sending response; error = {e:?}");
                                }
                            }
                            Err(e) => {
                                println!("error on decoding from socket; error = {e:?}");
                            }
                        }
                    }

                    // The connection will be closed at this point as `lines.next()` has returned `None`.
                });
            }
            Err(e) => println!("error accepting socket; error = {e:?}"),
        }
    }
}

fn handle_request(line: &str, db: &Arc<Database>) -> Response {
    let request = match Request::parse(line) {
        Ok(req) => req,
        Err(e) => return Response::Error { msg: e },
    };

    let mut db = db.map.lock().unwrap();
    match request {
        Request::Get { key } => match db.get(&key) {
            Some(value) => Response::Value {
                key,
                value: value.clone(),
            },
            None => Response::Error {
                msg: format!("no key {key}"),
            },
        },
        Request::Set { key, value } => {
            let previous = db.insert(key.clone(), value.clone());
            Response::Set {
                key,
                value,
                previous,
            }
        }
    }
}

impl Request {
    fn parse(input: &str) -> Result<Request, String> {
        let mut parts = input.splitn(3, ' ');
        match parts.next() {
            Some("GET") => {
                let key = parts.next().ok_or("GET must be followed by a key")?;
                if parts.next().is_some() {
                    return Err("GET's key must not be followed by anything".into());
                }
                Ok(Request::Get {
                    key: key.to_string(),
                })
            }
            Some("SET") => {
                let key = match parts.next() {
                    Some(key) => key,
                    None => return Err("SET must be followed by a key".into()),
                };
                let value = match parts.next() {
                    Some(value) => value,
                    None => return Err("SET needs a value".into()),
                };
                Ok(Request::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            }
            Some(cmd) => Err(format!("unknown command: {cmd}")),
            None => Err("empty input".into()),
        }
    }
}

impl Response {
    fn serialize(&self) -> String {
        match *self {
            Response::Value { ref key, ref value } => format!("{key} = {value}"),
            Response::Set {
                ref key,
                ref value,
                ref previous,
            } => format!("set {key} = `{value}`, previous: {previous:?}"),
            Response::Error { ref msg } => format!("error: {msg}"),
        }
    }
}
```

运行bin程序，使用如下命令：

```bash
cargo run --bin server
```

出现：

```bash
 Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.05s
     Running `target/debug/server`
Listening on: 127.0.0.1:8080
```

即表示运行成功。

2. 对官方的示例程序进行修改：

在之前的kv.rs的测试中，经常出现这句代码：

```rust
let p= tempfile::tempdir()?.into_path().join("log");
let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
```

这实际上就在启动DB，我们在engine/mod.rs中：

```rust
pub mod kv;
```

供kv引擎被server调用。

此外，和示例不一样的是，我们是通过session来执行客户端传来的sql语句的。

**回顾：**

- 客户端请求 SQL：客户端将 SQL 语句发送给 Session。
- Session 解析 SQL：Session 通过 Parser 解析 SQL，获得解析后的语法树。
- 开启事务：Session 调用 Engine 的 begin 方法，开启一个新的事务。
- 构建执行计划：根据解析后的 SQL，Session 通过 Plan::build 构建执行计划。
- 执行事务：执行计划通过事务 (Transaction) 执行，Session 调用 Engine 提供的事务接口，执行 SQL 操作（如插入、更新、删除等）。
- 提交或回滚事务：如果 SQL 执行成功，Session 调用 transaction.commit() 提交事务；如果失败，则调用 transaction.rollback() 回滚事务。
- 返回结果：Session 返回执行结果给客户端。

所以还需要对server.rs实现抽象的session接口，故修改server如下：

```rust
#![warn(rust_2018_idioms)]

use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use my_sql_db::sql::engine::kv::KVEngine;
use my_sql_db::storage::disk::DiskEngine;
use my_sql_db::sql::engine;
use my_sql_db::error::Result;
use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use crate::Request::SQL;

const DB_STORAGE_PATH: &str = "./tmp/sqldb-test/log";  // 指定存储文件

enum Request{
    // 客户端的请求类型
    SQL(String),   // 普通SQL命令
    ListTables,    // show tables命令
    TableInfo(String),  // show table table_name 命令
}

pub struct ServerSession<E: engine::Engine> {
    session: engine::Session<E>,
}

impl<E: engine::Engine + 'static> ServerSession<E> {  // 由于engine是传进来的，可能生命周期不够长，这里强制为static
    pub fn new(engine: MutexGuard<E>) -> Result<Self>{
        Ok(Self{session: engine.session()?})
    }

    pub async fn handle_request(&mut self, socket: TcpStream) -> Result<()>{
        // 循环读取客户端的命令
        let mut lines = Framed::new(socket, LinesCodec::new());

        while let Some(result) = lines.next().await {
            match result {
                Ok(line) => {
                    // 解析line, 变成enum Request类型
                    let request = SQL(line);

                    // 执行request命令
                    match request {
                        SQL(sql) => {
                            let response = self.session.execute(&sql)?;
                            println!("execute sql result : {:?}", response);  // 返回给客户端，但是现在仅有server
                        }
                        Request::ListTables => todo!(),
                        Request::TableInfo(_) => todo!(),
                    }
                }
                Err(e) => {
                    println!("error on decoding from socket; error = {e:?}");
                }
            }
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());   // 启动TCP服务，监听8080端口

    let listener = TcpListener::bind(&addr).await?;
    println!("SQL DB starts, server is listening on: {addr}");

    // 初始化DB
    let p= PathBuf::from(DB_STORAGE_PATH);
    let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);

    // 多线程下的读写
    let shared_engine = Arc::new(Mutex::new(kvengine));

    loop {
        match listener.accept().await {
            Ok((socket, _)) => {
                // 拿到sql引擎的克隆实例
                let db = shared_engine.clone();
                // 通过session执行sql语句
                let mut server_session = ServerSession::new(db.lock()?)?;

                // 开启一个tokio任务
                tokio::spawn(async move {
                    match server_session.handle_request(socket).await{
                        Ok(_) => {}
                        Err(_) => {}
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {e:?}"),
        }
    }
}
```