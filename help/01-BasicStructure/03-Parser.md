# 解析器Parser的实现

在实现了[Lexer](./02-Lexer.md)之后，我们可以获取用户输入sql中的token，现在对于这些token，我们需要对其进行语法分析，并构建抽象语法树AST。

这里，我们还是先实现`create`,`insert`,`select`的解析

## 代码实现

1. 首先，我们在parser/mod.rs中，定义Parser：

```rust
use crate::sql::parser::lexer::Lexer;
use std::iter::Peekable;
// 定义Parser
pub struct Parser<'a>{
    lexer: Peekable<Lexer<'a>>  // parser的属性只有lexer，因为parser的数据来源仅是lexer
}

impl<'a> Parser<'a> {
    pub fn new(input: &'a str) -> Self {
        Parser{
            lexer: Lexer::new(input).peekable()  // 初始化
        }
    }
}
```

2. 接下来，先创建一个ast模块，以防后需用到：

在parser文件夹下新建：ast.rs

```rust
use crate::sql::types::DataType;
// 本模块是抽象语法树的定义


// 列定义
#[derive(Debug,PartialEq)]
pub struct Column{            // 列的各种属性
    pub name: String,         // 列名
    pub datatype: DataType,   // 列数据类型
    pub nullable: Option<bool>, // 列是否为空
    pub default: Option<Expression> // 列的默认值
}

#[derive(Debug,PartialEq)]
pub enum Expression{        // 目前表达式为了简单，仅支持常量，不支持：insert into Table_A value(11 * 11 + 2) 等
    Consts(Consts)
}

#[derive(Debug,PartialEq)]
pub enum Consts{
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

// 定义 Consts -> Expression 的类型转换
impl From<Consts> for Expression{
    fn from(c: Consts) -> Self{
        Self::Consts(c)
    }
}

// sql 语句的定义
#[derive(Debug,PartialEq)]
pub enum Sentence{
    CreateTable{
        name: String,               // 表名
        columns: Vec<Column>,       // 表的列
    },
    Insert{
        table_name: String,           // 目标表名
        columns: Option<Vec<Column>>,  // 目标列，可以为空
        values: Vec<Vec<Expression>>,   // 插入数据，是个二维数组
    }
}
```

抽象语法树的示例：

```
Statement::CreateTable
├── name: "users"
└── columns
    ├── Column { name: "id", datatype: "Integer" }
    └── Column { name: "name", datatype: "String" }
```

此外，这里用到了新的自定义类型DataType，由于这个类型可能被经常用到，所以我们把它放到sql/types/mod.rs里以供使用：

```rust
#[derive(Debug,PartialEq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}
```

最后别忘了在sql/mod.rs中添加：

```rust
pub mod types;
```

3. 继续实现Parser的解析`Create Table`方法：

```rust
pub mod ast;

use crate::sql::parser::lexer::{Keyword, Lexer, Token};
use crate::error::{Result, Error};
use crate::sql::parser::ast::Column;
use crate::sql::types::DataType;

// Parser的其他方法
impl<'a> Parser<'a> {
    // 解析获的sql
    pub fn parse(&mut self) -> Result<ast::Sentence>{
        let sentence = self.parse_sentence()?;   // 获取解析得的语句

        self.expect_next_token_is(Token::Semicolon)?;  // sql语句以分号结尾
        if let Some(token) = self.peek()? {
            // 后面如果还有token，说明语句不合法
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(sentence)
    }

    // 解析语句
    fn parse_sentence(&mut self) -> Result<ast::Sentence>{
        // 我们尝试查看第一个Token以进行分类
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(token) => Err(Error::Parse(format!("[Parser] Unexpected token {}",token))),  // 其他token
            None => Err(Error::Parse("[Parser] Unexpected EOF".to_string()))
        }
    }

    // 分类一：DDL语句 create、drop等
    fn parse_ddl(&mut self) -> Result<ast::Sentence>{
        match self.next()? {  // 这里要消耗token
            Token::Keyword(Keyword::Create) => match self.next()? {
                Token::Keyword(Keyword::Table) => self.parse_ddl_create_table(),  // CREATE TABLE
                token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),  // 语法错误
            },
            token => Err(Error::Parse(format!("[Parser] Unexpected token {}", token))),  // 其他如drop等暂未实现
        }
    }

    // 解析create table语句
    fn parse_ddl_create_table(&mut self) -> Result<ast::Sentence>{
        // 在进入本方法之前，已经由parse_ddl解析了CREATE TABLE，所以这里应该是表名和其他列约束条件
        let table_name = self.expect_next_is_indent()?;

        // 根据语法，create table table_name，后续接括号，里面是表的列定义
        self.expect_next_token_is(Token::OpenParen)?;

        let mut columns = Vec::new();
        loop{
            columns.push(self.parse_ddl_column()?);
            if self.next_if_is_token(Token::Comma).is_none(){  // 后面没有逗号，说明列解析完成
                break;
            }
        }

        self.expect_next_token_is(Token::CloseParen)?;
        Ok(ast::Sentence::CreateTable {
            name: table_name,
            columns
        })

    }

    // 解析column
    fn parse_ddl_column(&mut self) -> Result<ast::Column>{
        let mut column = Column{
            name: self.expect_next_is_indent()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int | Keyword::Integer) => DataType::Integer,
                Token::Keyword(Keyword::Float | Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::Bool | Keyword::Boolean) => DataType::Boolean,
                Token::Keyword(Keyword::String | Keyword::Text | Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parser] Unexpected token {}",token))),
            },
            nullable: None,
            default: None,
        };

        // 解析是否为空，是否有默认值
        while let Some(Token::Keyword(keyword)) = self.next_if_keyword() {
            match keyword {
                Keyword::Null => column.nullable = Some(true),
                Keyword::Not => {
                    self.expect_next_token_is(Token::Keyword(Keyword::Null))?;
                    column.nullable = Some(false);
                },
                Keyword::Default => column.default = Some(self.parse_expression()?),
                keyword => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}",keyword))),
            }
        }
        Ok(column)
    }

    // 解析表达式，目前仅有常量
    fn parse_expression(&mut self) -> Result<ast::Expression>{
        Ok(
            match self.next()? {
                Token::Number(n) =>{
                    // 分两种情况，如果这个token整个都是数字，则为整数
                    // 如果这个token段中包含小数点，则是浮点数
                    if n.chars().all(|c| c.is_ascii_digit()){
                        ast::Consts::Integer(n.parse()?).into()  // into() 将 Consts -> Expression
                    }else{
                        ast::Consts::Float(n.parse()?).into()
                    }
                },
                Token::String(s)=> ast::Consts::String(s).into(),
                Token::Keyword(Keyword::True) => ast::Consts::Boolean(true).into(),
                Token::Keyword(Keyword::False) => ast::Consts::Boolean(false).into(),
                Token::Keyword(Keyword::Null) => ast::Consts::Null.into(),
                token => return Err(Error::Parse(format!("[Parser] Unexpected expression token {}",token))),
            }
        )
    }



    // 一些小工具
    // 重写peek方法，因为原peek是迭代器，会返回Option，可能为None，但是我们不希望返回None
    fn peek(&mut self) -> Result<Option<Token>>{
        self.lexer.peek().cloned().transpose()   // Option<Result<T, E>> 调用 transpose() 后会变成 Result<Option<T>, E>，令我们能更方便地处理错误
    }

    // 重写next方法，因为我们希望next能一直返回token，如果不返回则报错
    fn next(&mut self) -> Result<Token>{
        self.lexer.next().unwrap_or_else(|| Err(Error::Parse("[Parser] Unexpected EOF".to_string())))   // unwrap_or_else：如果返回Some(Token)，返回Token；如果返回None，则执行闭包（报错）
    }

    // 下一个token必须是indent
    fn expect_next_is_indent(&mut self) -> Result<String>{
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!("[Parser] Expected Indent, got token: {}",token))),
        }
    }

    // 下一个token必须是指定的token
    fn expect_next_token_is(&mut self,expected_token:Token) -> Result<()>{
        let token = self.next()?;
        if token != expected_token {
            return Err(Error::Parse(format!("[Parser] Expected Token: {}, got token: {}",expected_token,token)));
        }
        Ok(())
    }

    // 如果下一个token满足条件，则跳转并返回
    fn next_if<F:Fn(&Token) -> bool>(&mut self, condition: F) -> Option<Token>{
        self.peek().unwrap_or(None).filter(|token| condition(token));
        self.next().ok()  // 因为peek被重写了，返回的是Result，需要用ok解包出option
    }

    // 如果下一个token是关键字，则跳转并返回
    fn next_if_keyword(&mut self) -> Option<Token>{
        self.next_if(|token| matches!(token, Token::Keyword(_)))
    }

    // 如果下一个token是指定的token，则跳转并返回
    fn next_if_is_token(&mut self,token:Token) -> Option<Token>{
        self.next_if(|t| t == &token)
    }
}
```

这里，对于token的输出（即format!("{}")）需要先对token实现Display接口的fmt方法，我们将token反转回原始字符即可，keyword同理：

```rust
impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Keyword(keyword) => keyword.to_str(),
            Token::Ident(ident) => ident,
            Token::String(s) => s,
            Token::Number(n) => n,
            Token::OpenParen => "(",
            Token::CloseParen => ")",
            Token::Comma => ",",
            Token::Semicolon => ";",
            Token::Asterisk => "*",
            Token::Plus => "+",
            Token::Minus => "-",
            Token::Slash => "/",
        })
    }
}


impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Create => "CREATE",
            Keyword::Table => "TABLE",
            Keyword::Int => "INT",
            Keyword::Integer => "INTEGER",
            Keyword::Boolean => "BOOLEAN",
            Keyword::Bool => "BOOL",
            Keyword::String => "STRING",
            Keyword::Text => "TEXT",
            Keyword::Varchar => "VARCHAR",
            Keyword::Float => "FLOAT",
            Keyword::Double => "DOUBLE",
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Insert => "INSERT",
            Keyword::Into => "INTO",
            Keyword::Values => "VALUES",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
            Keyword::Default => "DEFAULT",
            Keyword::Not => "NOT",
            Keyword::Null => "NULL",
            Keyword::Primary => "PRIMARY",
            Keyword::Key => "KEY",
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}
```

此外，在表达式expression的解析中，由于我们是自己实现了一个Result，但是代码里包含了系统自带的解析错误的处理代码，所以对error.rs做出如下新增：

```rust
use std::num::{ParseFloatError, ParseIntError};

// 兼容系统本身的解析数字报错
impl From<ParseIntError> for Error{
    fn from(value: ParseIntError) -> Self {
        Error::Parse(value.to_string())   // 直接将系统报错信息兼容进我们的报错系统即可
    }
}

impl From<ParseFloatError> for Error{
    fn from(value: ParseFloatError) -> Self {
        Error::Parse(value.to_string())
    }
}
```