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
