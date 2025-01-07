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
use futures::SinkExt;
use crate::Request::SQL;

const DB_STORAGE_PATH: &str = "./tmp/sqldb-test/log";  // 指定存储文件
const RESPONSE_END : &str = "!!!THIS IS THE END!!!";   // 结束符，内容可以自定义一个不常见的字符串

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
                    let response = match request {
                        SQL(sql) => self.session.execute(&sql),
                        Request::ListTables => todo!(),
                        Request::TableInfo(_) => todo!(),
                    };

                    // 发送执行结果
                    let res = match response {
                        Ok(result_set) => result_set.to_string(),
                        Err(e) => e.to_string(),
                    };
                    if let Err(e) = lines.send(res.as_str()).await {
                        println!("error on sending response; error = {e:?}");
                    }
                    if let Err(e) = lines.send(RESPONSE_END).await {  // 发完结果后发个结束符
                        println!("error on sending response end; error = {e:?}");
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