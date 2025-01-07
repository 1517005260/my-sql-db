# 异步编程Tokio

在实现基础SQL数据库之后，构建交互式命令行。

官网：https://tokio.rs/tokio/tutorial

Tokio 是一个基于 Rust 的异步运行时库，用于构建高性能的异步应用，特别适用于网络服务和并发任务处理。它提供了线程池、异步 I/O、计时器和调度器等核心组件，并遵循 Rust 的异步编程模型，让开发者能够以非阻塞的方式处理大量任务。

## 什么是异步编程

异步编程是一种编程模式，允许程序在执行耗时任务时不会被阻塞，从而能够同时处理其他任务，显著提升程序的整体效率。

以在餐厅点餐为例，传统的同步方式就像你在点完餐后必须站在柜台前等待，无法做其他事情；而异步编程则类似于你点完餐后可以去找座位，服务员会在餐品准备好时通知你，这样你可以在等待过程中做其他事情.

同步与异步的关键区别在于，使用同步操作时，程序会完全阻塞，必须等待当前任务完成，例如，导致浏览器或应用界面卡死，无法进行其他操作。而使用异步操作时，程序仍保持响应，主任务在后台执行，界面可以正常交互，你可以自由切换标签页，或者进行其他操作。这样，JavaScript主线程得以空闲，程序运行流畅，用户体验也更佳.

异步编程广泛应用于多种场景，比如网络请求（如API调用）、文件上传或下载、数据库操作、定时任务和用户界面交互等。为了实现异步编程，通常可以使用回调函数、Promise，或更现代的async/await语法.

在最佳实践方面，要根据具体场景选择合适的异步处理方式。在需要等待结果的场景（如登录），可以通过显示加载状态、使用await等待结果、清晰展示操作结果并合理处理错误来提升用户体验。而在可以在后台处理的场景（如日志记录），则可以直接异步处理，不阻塞用户操作，且错误可以在后台静默处理.

## TinyDB示例

本项目的交互式命令行参考官网的tinydb示例，详见：https://github.com/tokio-rs/tokio/blob/master/examples/tinydb.rs

```rust
//! A "tiny database" and accompanying protocol
//!
//! This example shows the usage of shared state amongst all connected clients,
//! namely a database of key/value pairs. Each connected client can send a
//! series of GET/SET commands to query the current value of a key or set the
//! value of a key.
//!
//! This example has a simple protocol you can use to interact with the server.
//! To run, first run this in one terminal window:
//!
//!     cargo run --example tinydb
//!
//! and next in another windows run:
//!
//!     cargo run --example connect 127.0.0.1:8080
//!
//! In the `connect` window you can type in commands where when you hit enter
//! you'll get a response from the server for that command. An example session
//! is:
//!
//!
//!     $ cargo run --example connect 127.0.0.1:8080
//!     GET foo
//!     foo = bar
//!     GET FOOBAR
//!     error: no key FOOBAR
//!     SET FOOBAR my awesome string
//!     set FOOBAR = `my awesome string`, previous: None
//!     SET foo tokio
//!     set foo = `tokio`, previous: Some("bar")
//!     GET foo
//!     foo = tokio
//!
//! Namely you can issue two forms of commands:
//!
//! * `GET $key` - this will fetch the value of `$key` from the database and
//!   return it. The server's database is initially populated with the key `foo`
//!   set to the value `bar`
//! * `SET $key $value` - this will set the value of `$key` to `$value`,
//!   returning the previous value, if any.

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
    let db = Arc::new(Database {     // Arc: 多线程安全共享变量
        map: Mutex::new(initial_db), // Mutex： 即多线程的PV操作互斥
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
                tokio::spawn(async move {   // spawn 处理多线程
                    // Since our protocol is line-based we use `tokio_codecs`'s `LineCodec`
                    // to convert our stream of bytes, `socket`, into a `Stream` of lines
                    // as well as convert our line based responses into a stream of bytes.
                    let mut lines = Framed::new(socket, LinesCodec::new());  // Frame是Tokio的数据传输格式

                    // Here for every line we get back from the `Framed` decoder,
                    // we parse the request, and if it's valid we generate a response
                    // based on the values in the database.
                    while let Some(result) = lines.next().await {
                        match result {
                            Ok(line) => {
                                let response = handle_request(&line, &db);  // 我们sql数据库的handle_request即为从parser层开始逐步向下层的工作

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