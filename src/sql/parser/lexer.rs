use std::fmt::{Display, Formatter};
use std::iter::Peekable;
use std::str::Chars;
use crate::error::{Error, Result}; //自定义result
use crate::error::Error::Parse;

// 对token和Keyword的定义
// 派生注解解释：Debug允许你用{:?}打印调试信息，Clone允许用.clone()创建复制体，PartialEq允许对两个结构体的所有属性进行比较
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Keyword(Keyword),   // 关键字
    Ident(String),      // 表明、列名等特殊字符串
    String(String),     // 普通字符串
    Number(String),     // 数字（int、float等）
    OpenParen,          // (
    CloseParen,         // )
    Comma,              // ,
    Semicolon,          // ;
    Asterisk,           // *
    Plus,               // +
    Minus,              // -
    Slash,              // /
    Equal,              // =
}

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
            Token::Equal => "=",
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    Create,
    Table,
    Int,
    Integer,
    Boolean,
    Bool,
    String,
    Text,
    Varchar,
    Float,
    Double,
    Select,
    From,
    Insert,
    Into,
    Values,
    True,
    False,
    Default,
    Not,
    Null,
    Primary,
    Key,
    Update,
    Set,
    Where,
}

// word -> Keyword
impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {  // as_ref() 将值转换为引用
                "CREATE" => Keyword::Create,
                "TABLE" => Keyword::Table,
                "INT" => Keyword::Int,
                "INTEGER" => Keyword::Integer,
                "BOOLEAN" => Keyword::Boolean,
                "BOOL" => Keyword::Bool,
                "STRING" => Keyword::String,
                "TEXT" => Keyword::Text,
                "VARCHAR" => Keyword::Varchar,
                "FLOAT" => Keyword::Float,
                "DOUBLE" => Keyword::Double,
                "SELECT" => Keyword::Select,
                "FROM" => Keyword::From,
                "INSERT" => Keyword::Insert,
                "INTO" => Keyword::Into,
                "VALUES" => Keyword::Values,
                "TRUE" => Keyword::True,
                "FALSE" => Keyword::False,
                "DEFAULT" => Keyword::Default,
                "NOT" => Keyword::Not,
                "NULL" => Keyword::Null,
                "PRIMARY" => Keyword::Primary,
                "KEY" => Keyword::Key,
                "UPDATE" => Keyword::Update,
                "SET" => Keyword::Set,
                "WHERE" => Keyword::Where,
                _ => return None,
            }
        )
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
            Keyword::Update => "UPDATE",
            Keyword::Set => "SET",
            Keyword::Where => "WHERE",
        }
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.to_str())
    }
}

// 实现简单的词法分析Lexer
// lexer 结构体包含 iter 元素，实现了peekable接口（非消耗地提前查看下一个字符），指定接收泛型为chars，生命周期为a
pub struct Lexer<'a> {
    iter: Peekable<Chars<'a>>  // chars 包含对多个 token 的引用，所以需要生命周期
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        // 将传入的字符串 input 初始化为带 peekable 功能的字符迭代器 iter
        Self {
            iter: input.chars().peekable()
        }
    }

    // 隔离一些小方法，比如消除空格等
    // 消除空格，例如 select    *     from   t; 这也是有效的sql，我们的思路是利用迭代器一直查找下个字符，直到不为空格
    fn move_whitespace(&mut self){
        self.next_while(|c| c.is_whitespace());  // 注：这里的whitespace包括 空格,\n,\t等
        // 传参仅传condition闭包即可，&mut self 是隐式调用的
    }

    // 辅助方法
    // 判断当前字符a[i]是否满足条件，是则跳转到下一个字符a[i+1]，并返回该字符a[i]，否则返回None
    fn next_if<F: Fn(char) -> bool>                          // 泛型函数F：实现了接口Fn（像函数一样的闭包，可以被多次调用），指定了函数类型必须是 接收char返回bool
    (&mut self,condition: F) -> Option<char> {               // 接收参数condition：condition是F类型的函数或闭包
        self.iter.peek().filter(|&c| condition(*c))?; // 先探测 a[i] 是否满足条件（仅查看，不消耗）
        self.iter.next()                                     // 第一行代码执行成功，就执行这行代码。这里是iter不是peek，所以还会消耗该字符，返回a[i]
    }

    // 连续获取满足条件的字符，直到不满足为止
    fn next_while<F: Fn(char) -> bool>(&mut self, condition: F) -> Option<String> {
        let mut value = String::new();
        while let Some(c) = self.next_if(&condition) {
            value.push(c);
        }
        Some(value).filter(|s| !s.is_empty())  // 过滤空值
    }

    // 只有是token，才会返回当前token，并跳到下一个字符
    // 这里我们需要理解，如果遇到 +，那么 next_if 会返回 +，next_if_token会返回Token::Plus
    fn next_if_token<F:Fn(char) -> Option<Token>>(&mut self, condition: F) -> Option<Token>{
        let token = self.iter.peek().and_then(|c| condition(*c))?;
        // and_then 的效果是：如果 peek() 返回 Some(&char)，则对字符应用 condition，并尝试将其转换为 Option<Token>
        self.iter.next();
        Some(token)
    }

    // get next token
    fn scan(&mut self) -> Result<Option<Token>>{  // 扫描到的token可能为空，所以返回Option类型
        self.move_whitespace(); // 先消除多余空格，即变为 select * from t;

        // 由扫描到的第一个字符进行判断：
        match self.iter.peek(){
            Some('\'') => self.scan_string(),
            Some('"') => self.scan_string(),                   // 以单引号或者双引号打头的是字符串
            Some(c) if c.is_ascii_digit() => Ok(self.scan_number()),   // 数字
            Some(c) if c.is_alphabetic() => Ok(self.scan_word()),    // Ident、Keyword
            Some(_) => Ok(self.scan_symbol()),                                // 符号
            None => Ok(None),
        }
    }

    fn scan_string(&mut self) -> Result<Option<Token>> {
        // 不是单/双引号号开头
        if self.next_if(|c| c== '\'' || c=='"').is_none() {
            return Ok(None);
        }

        let mut value = String::new();
        loop {
            match self.iter.next() {
                Some('\'') => break,    // 匹配结束
                Some('"') => break,
                Some(c) => value.push(c),
                None => return Err(Error::Parse("[Lexer] Unexpected EOF of (String)".to_string()))
            }
        }
        Ok(Some(Token::String(value)))
    }

    fn scan_number(&mut self) -> Option<Token> {
        // 分部分扫描
        let mut num = self.next_while(|c| c.is_ascii_digit())?;  // ? 解包Option

        if let Some(sep) = self.next_if(|c| c=='.') {  // 小数点
            num.push(sep);
            // 小数点之后接着扫描
            while let Some(c) = self.next_if(|c| c.is_ascii_digit()) {
                num.push(c);
            }
        }
        Some(Token::Number(num))
    }

    fn scan_word(&mut self) -> Option<Token> {
        let mut val = self.next_if(|c| c.is_alphabetic())?.to_string();
        while let Some(c) = self.next_if(|c| c.is_alphanumeric() || c=='_') {  // alphanumeric是字母或数字
            val.push(c)
        }

        // 如果word是关键字，那么要转成关键字类型，否则为Ident类型
        Some(Keyword::transfer(&val).map_or(Token::Ident(val.to_lowercase()),   // map_or返回None
                                          Token::Keyword))                    // map_or返回Some
    }

    fn scan_symbol(&mut self) -> Option<Token> {
        self.next_if_token(|c| match c{
            '*' => Some(Token::Asterisk),
            '(' => Some(Token::OpenParen),
            ')' => Some(Token::CloseParen),
            ',' => Some(Token::Comma),
            ';' => Some(Token::Semicolon),
            '+' => Some(Token::Plus),
            '-' => Some(Token::Minus),
            '/' => Some(Token::Slash),
            '=' => Some(Token::Equal),
            _ => None,
        })
    }
}

// 标准迭代器接口
impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;   // 每次返回token/err

    fn next(&mut self) -> Option<Self::Item> {  // 要求实现的方法，返回每一步迭代的值（这里是token）
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),   // 成功解析到token
            Ok(None) => // 解析返回None，但是确实有字符，说明字符不合法
                self.iter.peek().map(|c| Err(Parse(format!("[Lexer] Unexpected character {}", c)))),
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests{
    use super::*;
    use std::vec;
    use crate::{
        error::Result,
        sql::parser::lexer::{Keyword, Token},
    };


    #[test]
    fn test_lexer_create_table() -> Result<()> {
        let tokens1 = Lexer::new(
            "CREATE table tbl
                (
                    id1 int primary key,
                    id2 integer
                );
                ",
        )
            .peekable()  // 由于实现了标准迭代器接口，故可以使用peekable()
            .collect::<Result<Vec<Token>>>()?;

        println!("tokens1: {:?}", tokens1);

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Create),
                Token::Keyword(Keyword::Table),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id1".to_string()),
                Token::Keyword(Keyword::Int),
                Token::Keyword(Keyword::Primary),
                Token::Keyword(Keyword::Key),
                Token::Comma,
                Token::Ident("id2".to_string()),
                Token::Keyword(Keyword::Integer),
                Token::CloseParen,
                Token::Semicolon
            ]
        );

        let tokens2 = Lexer::new(
            "CREATE table tbl
                        (
                            id1              int primary key,
                            id2 integer,
                            c1 bool null,
                            c2 boolean not null,
                            c3 float null,
                            c4 double,
                            c5 string,
                            c6 text,
                            c7 varchar default 'foo',
                            c8 int default 100,
                            c9 integer
                        );
                        ",
        )
            .peekable()
            .collect::<Result<Vec<Token>>>()?;

        println!("tokens2: {:?}", tokens2);

        assert!(tokens2.len() > 0);

        Ok(())
    }


    #[test]
    fn test_lexer_insert_into() -> Result<()> {
        let tokens1 = Lexer::new("insert into tbl values (1, '2', \"3\", true, false, 4.55);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("1".to_string()),
                Token::Comma,
                Token::String("2".to_string()),
                Token::Comma,
                Token::String("3".to_string()),
                Token::Comma,
                Token::Keyword(Keyword::True),
                Token::Comma,
                Token::Keyword(Keyword::False),
                Token::Comma,
                Token::Number("4.55".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );

        let tokens2 = Lexer::new("INSERT INTO       tbl (id, name, age) values (100, 'db', 10);")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens2,
            vec![
                Token::Keyword(Keyword::Insert),
                Token::Keyword(Keyword::Into),
                Token::Ident("tbl".to_string()),
                Token::OpenParen,
                Token::Ident("id".to_string()),
                Token::Comma,
                Token::Ident("name".to_string()),
                Token::Comma,
                Token::Ident("age".to_string()),
                Token::CloseParen,
                Token::Keyword(Keyword::Values),
                Token::OpenParen,
                Token::Number("100".to_string()),
                Token::Comma,
                Token::String("db".to_string()),
                Token::Comma,
                Token::Number("10".to_string()),
                Token::CloseParen,
                Token::Semicolon,
            ]
        );
        Ok(())
    }

    #[test]
    fn test_lexer_select() -> Result<()> {
        let tokens1 = Lexer::new("select * from tbl;")
            .peekable()
            .collect::<Result<Vec<_>>>()?;

        assert_eq!(
            tokens1,
            vec![
                Token::Keyword(Keyword::Select),
                Token::Asterisk,
                Token::Keyword(Keyword::From),
                Token::Ident("tbl".to_string()),
                Token::Semicolon,
            ]
        );
        Ok(())
    }
}