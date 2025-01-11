# AI 智能推荐

在命令行输入 `AI;` 后，调用大模型api，根据用户输入的历史记录，自动推荐下一条最可能的sql

## 代码实现

项目依赖：

```toml
dotenv = "0.15"
reqwest = { version = "0.11", features = ["json"] }
serde_json = "1.0"
```

1. 在项目的根目录新建.env文件，配置大模型的连接：

```env
API_URL=https://yunwu.ai/v1/chat/completions
API_KEY= your_api_key
MODEL=gpt-4o
```

记得放入gitignore文件，防止api_key泄露

2. 修改server.rs，增加识别`AI;`的逻辑：

```rust
#![warn(rust_2018_idioms)]

use tokio::net::{TcpListener, TcpStream};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use futures::SinkExt;
use my_sql_db::error::Result;
use my_sql_db::sql::engine;
use my_sql_db::sql::engine::kv::KVEngine;
use my_sql_db::storage::disk::DiskEngine;

use std::env;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use dotenv::dotenv;
use serde::{Deserialize, Serialize};

// AI API 请求与响应结构
#[derive(Serialize)]
struct AIRequest {
    model: String,
    messages: Vec<Message>,
    temperature: f32,
}

#[derive(Serialize, Deserialize)]
struct Message {
    role: String,
    content: String,
}

#[derive(Deserialize)]
struct AIResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: Message,
}

const DB_STORAGE_PATH: &str = "./tmp/sqldb-test/log"; // 指定存储文件
const RESPONSE_END: &str = "!!!THIS IS THE END!!!"; // 结束符，内容可以自定义一个不常见的字符串

// 定义请求类型
enum Request {
    SQL(String), // SQL命令
    AI,          // AI命令
}

pub struct ServerSession<E: engine::Engine> {
    session: engine::Session<E>,
    history: Vec<String>, // 维护历史 SQL 命令，供 AI 推荐使用
}

impl<E: engine::Engine + 'static> ServerSession<E> {
    pub fn new(engine: MutexGuard<'_, E>) -> Result<Self> {
        Ok(Self {
            session: engine.session()?,
            history: Vec::new(),
        })
    }

    pub async fn handle_request(&mut self, socket: TcpStream) -> Result<()> {
        let mut lines = Framed::new(socket, LinesCodec::new());

        while let Some(result) = lines.next().await {
            match result {
                Ok(line) => {
                    let trimmed = line.trim();
                    let request = if trimmed.eq_ignore_ascii_case("AI;") {
                        Request::AI
                    } else {
                        Request::SQL(line)
                    };

                    let response = match request {
                        // 用户输入AI; 命令
                        Request::AI => {
                            // 调用AI接口，返回推荐语句
                            if self.history.is_empty() {
                                Ok("SQL history is empty, AI recommend failed.".to_string())
                            } else {
                                self.get_ai_recommendation(&self.history).await
                            }
                        }
                        // 用户输入SQL
                        Request::SQL(sql) => {
                            if !sql.trim().is_empty() {
                                self.history.push(sql.clone());
                            }
                            // 执行SQL
                            self.session
                                .execute(&sql)
                                .map(|rs| rs.to_string())
                                .map_err(|e| e.into())
                        }
                    };

                    // 发送执行结果
                    let res = response.unwrap_or_else(|e| e.to_string());
                    if let Err(e) = lines.send(res.as_str()).await {
                        eprintln!("error on sending response; error = {e:?}");
                    }
                    // 发送结束符
                    if let Err(e) = lines.send(RESPONSE_END).await {
                        eprintln!("error on sending response end; error = {e:?}");
                    }
                }
                Err(e) => {
                    eprintln!("error on decoding from socket; error = {e:?}");
                }
            }
        }

        Ok(())
    }

    // 调用外部 AI API，获取推荐
    async fn get_ai_recommendation(&self, history: &[String]) -> Result<String> {
        // 从.env读取配置
        let api_url = env::var("API_URL").unwrap_or_default();
        let api_key = env::var("API_KEY").unwrap_or_default();
        let model = env::var("MODEL").unwrap_or_else(|_| "gpt-3.5-turbo".to_string());

        if api_url.is_empty() || api_key.is_empty() {
            return Ok("API_URL or API_KEY cannot be null, please recheck .env file.".to_string());
        }

        // 将历史SQL命令组装为上下文
        let mut messages = Vec::new();
        messages.push(Message {
            role: "system".into(),
            content:
                "你是一个SQL助手，根据用户的历史SQL，推荐下一条最可能的SQL语句。请仅返回sql语句。"
                    .into(),
        });

        for cmd in history {
            messages.push(Message {
                role: "user".into(),
                content: cmd.clone(),
            });
        }

        // 最后的提示
        messages.push(Message {
            role: "user".into(),
            content: "基于以上SQL历史，请推荐下一条最可能的SQL语句。".into(),
        });

        let req_body = AIRequest {
            model,
            messages,
            temperature: 0.7,
        };

        // 通过reqwest调用AI接口
        let client = reqwest::Client::new();
        let resp = client
            .post(&api_url)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", api_key))
            .json(&req_body)
            .send()
            .await;

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                return Ok(format!("AI recommend failed : {e}"));
            }
        };

        if !resp.status().is_success() {
            return Ok(format!(
                "AI recommend failed: {}",
                resp.status().as_u16()
            ));
        }

        let ai_response: AIResponse = match resp.json().await {
            Ok(json) => json,
            Err(e) => {
                return Ok(format!("AI recommend failed : {e}"));
            }
        };

        if let Some(choice) = ai_response.choices.first() {
            // 处理AI返回的内容，去除markdown格式
            let content = choice.message.content.replace("```sql", "").replace("```", "").trim().to_string();
            Ok(format!("SQL recommend by AI: \n     {}", content))
        } else {
            Ok("AI recommend failed".to_string())
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // 启动前先加载.env
    dotenv().ok();

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let listener = TcpListener::bind(&addr).await?;
    println!("SQL DB starts, server is listening on: {addr}");

    // 初始化DB
    let p = PathBuf::from(DB_STORAGE_PATH);
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

                // 开启一个tokio任务去处理当前socket的请求
                tokio::spawn(async move {
                    match server_session.handle_request(socket).await {
                        Ok(_) => {}
                        Err(e) => {
                            eprintln!("Internal server error {:?}", e);
                        }
                    }
                });
            }
            Err(e) => eprintln!("error accepting socket; error = {e:?}"),
        }
    }
}
```

3. 效果展示：

```bash
sql-db>> AI;
SQL history is empty, AI recommend failed.
[Execution time: 347.584µs]
sql-db>> select * from t;
a
---
1
10
11
(3 rows)
[Execution time: 887.047µs]
sql-db>> AI;
SQL recommend by AI: 
     SELECT COUNT(*) FROM t;
[Execution time: 1.274865928s]
sql-db>> SELECT COUNT(*) FROM t;
Parse Error: [Parser] Expected Ident, got token: *
[Execution time: 375.219µs]
sql-db>> SELECT COUNT(a) FROM t;
count
------
3
(1 rows)
[Execution time: 598.003µs]
```