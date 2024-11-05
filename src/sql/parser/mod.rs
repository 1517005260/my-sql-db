use std::iter::Peekable;
use crate::sql::parser::lexer::{Keyword, Lexer, Token};
use crate::error::{Result, Error};
use crate::sql::parser::ast::Column;
use crate::sql::types::DataType;

mod lexer;  // lexer模块仅parser文件内部可使用
pub mod ast;

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