#![warn(rust_2018_idioms)]
use futures::{SinkExt, TryStreamExt};
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::{Editor, Helper, Config, CompletionType, EditMode};
use rustyline::validate::{Validator, ValidationContext, ValidationResult};
use rustyline::validate::MatchingBracketValidator;
use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite, LinesCodec};

use my_sql_db::sql::parser::lexer::Keyword;
use strum::IntoEnumIterator;

const RESPONSE_END: &str = "!!!THIS IS THE END!!!";
const HISTORY_FILE: &str = ".history";

// 命令行历史文件存储路径为，本项目根目录下
fn get_history_path() -> PathBuf {
    PathBuf::from(HISTORY_FILE)
}

// 关键字补全器
struct SqlCompleter {
    keywords: Vec<String>,
}

impl SqlCompleter {
    fn new() -> Self {  // 获取所有关键字
        let keywords = Keyword::iter()
            .map(|kw| kw.to_str().to_string())
            .collect();
        Self { keywords }
    }
}

impl Completer for SqlCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        let start = line[..pos].rfind(|c: char| !c.is_alphanumeric() && c != '_').map_or(0, |i| i + 1);
        let prefix = &line[start..pos].to_uppercase();
        let candidates: Vec<Pair> = self.keywords.iter()
            .filter(|kw| kw.starts_with(prefix))
            .map(|kw| Pair {
                display: kw.to_string(),
                replacement: kw.to_string(),
            })
            .collect();
        Ok((start, candidates))
    }
}

// 实现 Helper 以支持补全和多行输入
struct SqlHelper {
    completer: SqlCompleter,
    highlighter: MatchingBracketValidator,
}

impl Helper for SqlHelper {}
impl Hinter for SqlHelper {
    type Hint = String;
}
impl Highlighter for SqlHelper {}
impl Completer for SqlHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &rustyline::Context<'_>,
    ) -> Result<(usize, Vec<Pair>), ReadlineError> {
        self.completer.complete(line, pos, ctx)
    }
}
impl Validator for SqlHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> Result<ValidationResult, ReadlineError> {
        self.highlighter.validate(ctx)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 指定服务器地址
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:8080".to_string());

    let addr = addr.parse::<SocketAddr>()?;
    let mut client = Client::new(addr).await?;

    // 配置 Rustyline
    let config = Config::builder()
        .history_ignore_dups(true)
        .expect("Failed to set history_ignore_dups")
        .completion_type(CompletionType::List)
        .edit_mode(EditMode::Vi)
        .build();
    let mut editor = Editor::with_config(config)?;
    let helper = SqlHelper {
        completer: SqlCompleter::new(),
        highlighter: MatchingBracketValidator::new(),
    };
    editor.set_helper(Some(helper));

    // 加载历史记录
    let history_path = get_history_path();
    if history_path.exists() {
        editor.load_history(&history_path)?;
    }

    // 多行命令输入变量
    let mut multiline = String::new();
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

        match readline {
            Ok(line) => {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    multiline.push_str(trimmed);
                    multiline.push(' '); // 保留空格
                    if trimmed.ends_with(';') {
                        // 完整的命令以分号结尾
                        let cmd = multiline.trim_end_matches(';').trim().to_string();
                        multiline.clear();
                        if cmd.eq_ignore_ascii_case("quit") {
                            break;
                        }
                        editor.add_history_entry(&cmd)?;
                        // 记录命令开始执行时间
                        let start_time = Instant::now();
                        if let Err(e) = client.exec_cmd(&cmd).await {
                            println!("Error executing command: {}", e);
                        }
                        // 记录结束时间并计算耗时
                        let duration = start_time.elapsed();
                        println!("[Execution time: {:?}]", duration);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => break, // Ctrl C
            Err(ReadlineError::Eof) => break,         // Ctrl D
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }

    // 保存历史记录
    editor.save_history(&get_history_path())?;

    Ok(())
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
        let (r, w) = self.stream.split();
        let mut sink = FramedWrite::new(w, LinesCodec::new());
        let mut stream = FramedRead::new(r, LinesCodec::new());

        // 发送命令
        sink.send(cmd).await?;

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

impl Drop for Client{
    fn drop(&mut self) {
        if self.transaction_version.is_some() {
            futures::executor::block_on(self.exec_cmd("ROLLBACK;")).expect("rollback failed");
        }
    }
}