# Client 实现

通过端口连接到TCP服务，并且发送请求即可

参考代码：https://github.com/tokio-rs/tokio/blob/master/examples/connect.rs

```rust
//! An example of hooking up stdin/stdout to either a TCP or UDP stream.
//!
//! This example will connect to a socket address specified in the argument list
//! and then forward all data read on stdin to the server, printing out all data
//! received on stdout. An optional `--udp` argument can be passed to specify
//! that the connection should be made over UDP instead of TCP, translating each
//! line entered on stdin to a UDP packet to be sent to the remote address.
//!
//! Note that this is not currently optimized for performance, especially
//! around buffer management. Rather it's intended to show an example of
//! working with a client.
//!
//! This example can be quite useful when interacting with the other examples in
//! this repository! Many of them recommend running this as a simple "hook up
//! stdin/stdout to a server" to get up and running.

#![warn(rust_2018_idioms)]

use futures::StreamExt;
use tokio::io;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

use std::env;
use std::error::Error;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Determine if we're going to run in TCP or UDP mode
    let mut args = env::args().skip(1).collect::<Vec<_>>();
    let tcp = match args.iter().position(|a| a == "--udp") {
        Some(i) => {
            args.remove(i);
            false
        }
        None => true,
    };

    // Parse what address we're going to connect to
    let addr = args
        .first()
        .ok_or("this program requires at least one argument")?;
    let addr = addr.parse::<SocketAddr>()?;

    let stdin = FramedRead::new(io::stdin(), BytesCodec::new());
    let stdin = stdin.map(|i| i.map(|bytes| bytes.freeze()));
    let stdout = FramedWrite::new(io::stdout(), BytesCodec::new());

    if tcp {
        tcp::connect(&addr, stdin, stdout).await?;
    } else {
        udp::connect(&addr, stdin, stdout).await?;
    }

    Ok(())
}

mod tcp {
    use bytes::Bytes;
    use futures::{future, Sink, SinkExt, Stream, StreamExt};
    use std::{error::Error, io, net::SocketAddr};
    use tokio::net::TcpStream;
    use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};

    pub async fn connect(
        addr: &SocketAddr,
        mut stdin: impl Stream<Item = Result<Bytes, io::Error>> + Unpin,
        mut stdout: impl Sink<Bytes, Error = io::Error> + Unpin,
    ) -> Result<(), Box<dyn Error>> {
        let mut stream = TcpStream::connect(addr).await?;
        let (r, w) = stream.split();
        let mut sink = FramedWrite::new(w, BytesCodec::new());
        // filter map Result<BytesMut, Error> stream into just a Bytes stream to match stdout Sink
        // on the event of an Error, log the error and end the stream
        let mut stream = FramedRead::new(r, BytesCodec::new())
            .filter_map(|i| match i {
                //BytesMut into Bytes
                Ok(i) => future::ready(Some(i.freeze())),
                Err(e) => {
                    println!("failed to read from socket; error={e}");
                    future::ready(None)
                }
            })
            .map(Ok);

        match future::join(sink.send_all(&mut stdin), stdout.send_all(&mut stream)).await {
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
            _ => Ok(()),
        }
    }
}

mod udp {
    use bytes::Bytes;
    use futures::{Sink, SinkExt, Stream, StreamExt};
    use std::error::Error;
    use std::io;
    use std::net::SocketAddr;
    use tokio::net::UdpSocket;

    pub async fn connect(
        addr: &SocketAddr,
        stdin: impl Stream<Item = Result<Bytes, io::Error>> + Unpin,
        stdout: impl Sink<Bytes, Error = io::Error> + Unpin,
    ) -> Result<(), Box<dyn Error>> {
        // We'll bind our UDP socket to a local IP/port, but for now we
        // basically let the OS pick both of those.
        let bind_addr = if addr.ip().is_ipv4() {
            "0.0.0.0:0"
        } else {
            "[::]:0"
        };

        let socket = UdpSocket::bind(&bind_addr).await?;
        socket.connect(addr).await?;

        tokio::try_join!(send(stdin, &socket), recv(stdout, &socket))?;

        Ok(())
    }

    async fn send(
        mut stdin: impl Stream<Item = Result<Bytes, io::Error>> + Unpin,
        writer: &UdpSocket,
    ) -> Result<(), io::Error> {
        while let Some(item) = stdin.next().await {
            let buf = item?;
            writer.send(&buf[..]).await?;
        }

        Ok(())
    }

    async fn recv(
        mut stdout: impl Sink<Bytes, Error = io::Error> + Unpin,
        reader: &UdpSocket,
    ) -> Result<(), io::Error> {
        loop {
            let mut buf = vec![0; 1024];
            let n = reader.recv(&mut buf[..]).await?;

            if n > 0 {
                stdout.send(Bytes::from(buf)).await?;
            }
        }
    }
}
```

## 代码实现

项目依赖：

```toml
bytes = "1.0.0"
```

1. 新建文件bin/client.rs

参考代码中实现了tcp和udp两种连接方式，这里我们仅保留tcp连接即可。

```rust
#![warn(rust_2018_idioms)]
use bytes::Bytes;
use futures::{future, Sink, SinkExt, Stream, StreamExt};
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};
use std::env;
use tokio::io;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 指定TCP的连接地址，与server保持一致
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());
    let addr = addr.parse::<SocketAddr>()?;

    let stdin = FramedRead::new(io::stdin(), BytesCodec::new());
    let stdin = stdin.map(|i| i.map(|bytes| bytes.freeze()));  // 终端的输入
    let stdout = FramedWrite::new(io::stdout(), BytesCodec::new());  // 打印到终端上

    connect(&addr, stdin, stdout).await?;

    Ok(())
}


pub async fn connect(addr: &SocketAddr, mut stdin: impl Stream<Item = Result<Bytes, io::Error>> + Unpin,
                     mut stdout: impl Sink<Bytes, Error = io::Error> + Unpin, ) -> Result<(), Box<dyn Error>> {
    // 进行tcp连接
    let mut stream = TcpStream::connect(addr).await?;
    let (r, w) = stream.split();
    let mut sink = FramedWrite::new(w, BytesCodec::new());

    let mut stream = FramedRead::new(r, BytesCodec::new())
        .filter_map(|i| match i {
            Ok(i) => future::ready(Some(i.freeze())),
            Err(e) => {
                println!("failed to read from socket; error={e}");
                future::ready(None)
            }
        })
        .map(Ok);

    match future::join(sink.send_all(&mut stdin), stdout.send_all(&mut stream)).await {
        (Err(e), _) | (_, Err(e)) => Err(e.into()),
        _ => Ok(()),
    }
}
```

与原来的代码相比，基本无需改变多少。

2. 在server.rs中增加发送结果回客户端的逻辑

```rust
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
            }
            Err(e) => {
                println!("error on decoding from socket; error = {e:?}");
            }
        }
    }

    Ok(())
}
```

3. 由于这里我们期望对返回的ResultSet进行打印输出，于是在executor/mod.rs中:

```rust
impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::CreateTable { table_name } => format!("CREATE TABLE {}", table_name),  // 创建成功提示
            ResultSet::Insert { count } => format!("INSERT {} rows", count),                  // 插入成功提示
            ResultSet::Scan { columns, rows } => {                          // 返回扫描结果
                let columns = columns.join(" | ");  // 每列用 | 分割
                let rows_len = rows.len();   // 一共多少行
                let rows = rows
                    .iter()
                    .map(|row| {
                        row.iter()  // 遍历一行的每个元素
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(" | ")   // 每列用 | 分割
                    })
                    .collect::<Vec<_>>()
                    .join("\n");       // 每行数据用 \n 分割
                format!("{}\n{}\n({} rows)", columns, rows, rows_len)
            }
            ResultSet::Update { count } => format!("UPDATE {} rows", count),               // 更新成功提示
            ResultSet::Delete { count } => format!("DELETE {} rows", count),               // 删除成功提示
        }
    }
}
```

4. 运行测试

服务端：

```bash
cargo run --bin server
```

成功则输出：

```bash
Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.78s
     Running `target/debug/server`
SQL DB starts, server is listening on: 127.0.0.1:8080
```

客户端：

```bash
cargo run --bin client
```

运行命令测试：

```bash
   Compiling my-sql-db v0.1.0 (/home/glk/project/my-sql-db)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.81s
     Running `target/debug/client`
create table t (a int primary key);
CREATE TABLE t
insert into t values (1),(2);
INSERT 2 rows
select * from t;
a
1
2
(2 rows)
```

测试成功，并且观察到 `./tmp/sqldb-test/log`中有二进制文件存放