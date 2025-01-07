# 命令行完善

仅使用Tokio，只能构建一个非常简陋的命令行，而且不能识别小键盘上的“上下左右”键，现在我们对命令行的交互进行完善。

参考开源库：https://github.com/kkawakam/rustyline

## 代码实现

项目依赖：

```toml
rustyline = "15.0.0"
```

1. 修改client.rs，重写之前简陋的connect方法和main方法，使用rustyline代替

```rust
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
```

这里涉及到服务段的结束符处理：

```rust
// server.rs
const RESPONSE_END : &str = "!!!THIS IS THE END!!!";   // 结束符，内容可以自定义一个不常见的字符串

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
```

2. 运行测试：

```bash
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.18s
     Running `target/debug/client`
sql-db>>
sql-db>> create table t1 (a int primary key, b float);
CREATE TABLE t1
sql-db>> insert into t1 values(1,1.1), (2,2.2);
INSERT 2 rows
sql-db>> select * from t1;
a | b
1 | 1.1
2 | 2.2
(2 rows)
sql-db>>
```

可以看到效果还是不错的，我们再优化下表格的显示。

- 如果数据有短有长，那么列长应该以数据最长的那行为基准
- 列名和数据之间应该有分隔符显示

在executor/mod.rs中：

```rust
impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::CreateTable { table_name } => format!("CREATE TABLE {}", table_name),  // 创建成功提示
            ResultSet::Insert { count } => format!("INSERT {} rows", count),                  // 插入成功提示
            ResultSet::Scan { columns, rows } => { // 返回扫描结果
                let rows_len = rows.len();   // 一共多少行

                // 先找到列名的长度
                let mut max_len = columns.iter().map(|c| c.len()).collect::<Vec<usize>>();
                // 然后将列名和行数据进行比较，选出最长的那个
                for a_row in rows{
                    for(i, v) in a_row.iter().enumerate(){
                        if v.to_string().len() > max_len[i]{
                            max_len[i] = v.to_string().len();
                        }
                    }
                }

                // 展示列名
                let columns = columns.iter().zip(max_len.iter()) // 将两个迭代器 columns 和 max_len 配对在一起
                    .map(|(col, &len)| format!("{:width$}", col, width = len))
                    .collect::<Vec<_>>().join(" |");  // 每列用 | 分割

                // 展示列名和数据的分隔符
                let sep = max_len.iter().map(|v| format!("{}", "-".repeat(*v + 1)))  // 让“-”重复最大长度次
                    .collect::<Vec<_>>().join("+");  // 用 + 连接

                // 展示行
                let rows = rows.iter()
                    .map(|row| {
                        row.iter()
                            .zip(max_len.iter())
                            .map(|(v, &len)| format!("{:width$}", v.to_string(), width = len))
                            .collect::<Vec<_>>()
                            .join(" |")
                    })
                    .collect::<Vec<_>>()
                    .join("\n");       // 每行数据用 \n 分割

                format!("{}\n{}\n{}\n({} rows)", columns, sep, rows, rows_len)
            }
            ResultSet::Update { count } => format!("UPDATE {} rows", count),               // 更新成功提示
            ResultSet::Delete { count } => format!("DELETE {} rows", count),               // 删除成功提示
        }
    }
}
```

再进行测试：

```bash
sql-db>> select * from t1;
a |b
--+----
1 |1.1
2 |2.2
(2 rows)
sql-db>> insert into t1 values (10000000000000000, 100.00000000000);
INSERT 1 rows
sql-db>> select * from t1;
a                 |b
------------------+----
1                 |1.1
2                 |2.2
10000000000000000 |100
(3 rows)
sql-db>>
```