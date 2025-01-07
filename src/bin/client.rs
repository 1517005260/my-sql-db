#![warn(rust_2018_idioms)]
use futures::{Sink, SinkExt, StreamExt, TryStreamExt};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

const RESPONSE_END : &str = "!!!THIS IS THE END!!!";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 指定服务器地址
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let addr = addr.parse::<SocketAddr>()?;
    let client = Client::new(addr);

    let mut editor = DefaultEditor::new()?;
    loop {
        let readline = editor.readline("sql-db>> ");
        match readline {
            Ok(cmd) => {  // 正常情况，拿到一条命令
                let cmd = cmd.trim();  // 去除空格
                if cmd.len() > 0 {
                    if cmd == "quit" {
                        break;
                    }
                    editor.add_history_entry(cmd)?;
                    client.exec_cmd(cmd).await?;
                }
            }
            Err(ReadlineError::Interrupted) => break,
            Err(ReadlineError::Eof) => break,
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}

pub struct Client{
    address: SocketAddr,
}

impl Client {
    pub fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    pub async fn exec_cmd(&self, cmd:&str) ->  Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(self.address).await?;
        let (r, w) = stream.split();
        let mut sink = FramedWrite::new(w, LinesCodec::new());
        let mut stream = FramedRead::new(r, LinesCodec::new());

        // 发送命令
        sink.send(cmd).await?;

        // 接收执行结果
        while let Some(val) = stream.try_next().await? {
            if val == RESPONSE_END {
                break;
            }
            // 打印执行结果
            println!("{}", val);
        }
        Ok(())
    }
}