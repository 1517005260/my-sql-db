use std::collections::BTreeMap;
use std::iter::Peekable;
use crate::sql::parser::lexer::{Keyword, Lexer, Token};
use crate::error::{Result, Error};
use crate::sql::parser::ast::{Column, Expression, FromItem, JoinType, OrderBy, Sentence};
use crate::sql::parser::ast::FromItem::{Join, Table};
use crate::sql::parser::ast::JoinType::Cross;
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
    pub fn parse(&mut self) -> Result<Sentence>{
        let sentence = self.parse_sentence()?;   // 获取解析得的语句

        self.expect_next_token_is(Token::Semicolon)?;  // sql语句以分号结尾
        if let Some(token) = self.peek()? {
            // 后面如果还有token，说明语句不合法
            return Err(Error::Parse(format!("[Parser] Unexpected token {}", token)));
        }
        Ok(sentence)
    }

    // 解析语句
    fn parse_sentence(&mut self) -> Result<Sentence>{
        // 我们尝试查看第一个Token以进行分类
        match self.peek()? {
            Some(Token::Keyword(Keyword::Create)) => self.parse_ddl(),
            Some(Token::Keyword(Keyword::Select)) => self.parse_select(),
            Some(Token::Keyword(Keyword::Insert)) => self.parse_insert(),
            Some(Token::Keyword(Keyword::Update)) => self.parse_update(),
            Some(Token::Keyword(Keyword::Delete)) => self.parse_delete(),
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
    fn parse_ddl_create_table(&mut self) -> Result<Sentence>{
        // 在进入本方法之前，已经由parse_ddl解析了CREATE TABLE，所以这里应该是表名和其他列约束条件
        let table_name = self.expect_next_is_ident()?;

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
        Ok(Sentence::CreateTable {
            name: table_name,
            columns
        })

    }

    // 解析column
    fn parse_ddl_column(&mut self) -> Result<Column>{
        let mut column: Column = Column{
            name: self.expect_next_is_ident()?,
            datatype: match self.next()? {
                Token::Keyword(Keyword::Int) | Token::Keyword(Keyword::Integer) => DataType::Integer,
                Token::Keyword(Keyword::Float) | Token::Keyword(Keyword::Double) => DataType::Float,
                Token::Keyword(Keyword::Bool) | Token::Keyword(Keyword::Boolean) => DataType::Boolean,
                Token::Keyword(Keyword::String) | Token::Keyword(Keyword::Text) | Token::Keyword(Keyword::Varchar) => DataType::String,
                token => return Err(Error::Parse(format!("[Parser] Unexpected token {}",token))),
            },
            nullable: None,
            default: None,
            is_primary_key: false,
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
                Keyword::Primary => {
                    self.expect_next_token_is(Token::Keyword(Keyword::Key))?;  // 关键字为primary key
                    column.is_primary_key = true;
                },
                keyword => return Err(Error::Parse(format!("[Parser] Unexpected keyword {}",keyword))),
            }
        }
        Ok(column)
    }

    // 解析表达式，目前有常量和列名
    fn parse_expression(&mut self) -> Result<ast::Expression>{
        Ok(
            match self.next()? {
                Token::Ident(ident) =>{
                    // 列名
                    ast::Expression::Field(ident)
                },
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

    // 分类二：Select语句
    fn parse_select(&mut self) -> Result<Sentence>{
        Ok(Sentence::Select {
            select_condition: self.parse_select_condition()?,
            from_item: self.parse_from_condition()?,
            order_by: self.parse_order_by_condition()?,
            limit: {
                if self.next_if_is_token(Token::Keyword(Keyword::Limit)).is_some(){
                    Some(self.parse_expression()?)
                }else{
                    None
                }
            },
            offset:{
                if self.next_if_is_token(Token::Keyword(Keyword::Offset)).is_some(){
                    Some(self.parse_expression()?)
                }else{
                    None
                }
            },
        })
    }

    // 分类三：Insert语句
    fn parse_insert(&mut self) -> Result<Sentence>{
        self.expect_next_token_is(Token::Keyword(Keyword::Insert))?;
        self.expect_next_token_is(Token::Keyword(Keyword::Into))?;
        let table_name = self.expect_next_is_ident()?;

        // 接下来是可选项，我们需要做出判断：是否给出了指定列名
        let columns =
            if self.next_if_is_token(Token::OpenParen).is_some(){
                let mut cols = Vec::new();
                loop{
                    cols.push(self.expect_next_is_ident()?.to_string());
                    match self.next()? {
                        Token::CloseParen => break,
                        Token::Comma => continue,
                        token => return Err(Error::Parse(format!("[Parser] Unexpected token {}",token))),
                    }
                }
                Some(cols)
            }else { None };

        // 接下来是必选项，是value的信息：
        self.expect_next_token_is(Token::Keyword(Keyword::Values))?;
        // 插入多列：insert into table_a values (1,2,3),(4,5,6)
        let mut values = Vec::new();
        loop{
            self.expect_next_token_is(Token::OpenParen)?;
            let mut expressions = Vec::new();
            loop{
                expressions.push(self.parse_expression()?);
                match self.next()? {
                    Token::CloseParen => break,
                    Token::Comma => continue,
                    token => return Err(Error::Parse(format!("[Parser] Unexpected token {}",token))),
                }
            }
            values.push(expressions);
            if self.next_if_is_token(Token::Comma).is_none(){  // 每组数据应该以逗号连接
                break;
            }
        }
        Ok(Sentence::Insert {
            table_name,
            columns,
            values
        })
    }

    // 分类：Update语句
    fn parse_update(&mut self) -> Result<Sentence>{
        self.expect_next_token_is(Token::Keyword(Keyword::Update))?;
        let table_name = self.expect_next_is_ident()?;
        self.expect_next_token_is(Token::Keyword(Keyword::Set))?;

        // loop 更新 columns
        // 又由于Set时不能出现重复，即 set a=1, a=2，所以需要去重
        let mut columns = BTreeMap::new();
        loop{
            let col = self.expect_next_is_ident()?;
            self.expect_next_token_is(Token::Equal)?;
            let value = self.parse_expression()?;
            if columns.contains_key(&col){
                return Err(Error::Parse(format!("[Parser] Update column {} conflicted",col)));
            }
            columns.insert(col, value);
            // 如果后续没有逗号，说明解析完成，退出循环
            if self.next_if_is_token(Token::Comma).is_none(){
                break;
            }
        }
        Ok(Sentence::Update {
            table_name,
            columns,
            condition: self.parse_where_condition()?,
        })
    }

    // 分类：Delete语句
    fn parse_delete(&mut self) -> Result<Sentence>{
        self.expect_next_token_is(Token::Keyword(Keyword::Delete))?;
        self.expect_next_token_is(Token::Keyword(Keyword::From))?;
        let table_name = self.expect_next_is_ident()?;
        Ok(Sentence::Delete {
            table_name,
            condition: self.parse_where_condition()?,
        })
    }

    fn parse_select_condition(&mut self) -> Result<Vec<(Expression, Option<String>)>>{
        self.expect_next_token_is(Token::Keyword(Keyword::Select))?;

        let mut selects = Vec::new();
        // 如果是select *
        if self.next_if_is_token(Token::Asterisk).is_some(){
            return Ok(selects);
        }

        // 处理多个select的列
        loop{
            let col_name = self.parse_expression()?;
            // 查看是否有别名，比如 select user_name as a
            let nick_name= match self.next_if_is_token(Token::Keyword(Keyword::As)) {
                Some(_) => Some(self.expect_next_is_ident()?),
                None => None,
            };
            selects.push((col_name, nick_name));
            // 没有逗号，解析完毕
            if self.next_if_is_token(Token::Comma).is_none(){
                break;
            }
        }

        Ok(selects)
    }

    fn parse_from_condition(&mut self) -> Result<FromItem>{
        self.expect_next_token_is(Token::Keyword(Keyword::From))?;

        // 无论是否是join，肯定会有第一个表名
        let mut from_item = self.parse_table_name()?;

        // 看后面有无join关键字
        // 并且注意，可能会有多表连接，所以用while循环
        while let Some(join_type) = self.parse_join_type()?{
            let left = Box::new(from_item);  // 原来的第一个表名变成了左表
            let right = Box::new(self.parse_table_name()?);
            from_item = Join{join_type, left, right};
        }
        Ok(from_item)
    }

    fn parse_table_name(&mut self) -> Result<FromItem>{
        Ok(
            Table{
                name: self.expect_next_is_ident()?,
            }
        )
    }

    fn parse_join_type(&mut self) -> Result<Option<JoinType>>{
        if self.next_if_is_token(Token::Keyword(Keyword::Cross)).is_some(){
            // 有Cross这个关键字，那么后面一定要跟Join关键字
            self.expect_next_token_is(Token::Keyword(Keyword::Join))?;
            return Ok(Some(Cross));
        }
        Ok(None)
    }

    fn parse_where_condition(&mut self) -> Result<Option<(String, Expression)>>{
        if self.next_if_is_token(Token::Keyword(Keyword::Where)).is_none(){
            return Ok(None);  // 没有指定where条件
        }
        let col = self.expect_next_is_ident()?;
        self.expect_next_token_is(Token::Equal)?;
        let value = self.parse_expression()?;
        Ok(Some((col, value)))
    }

    fn parse_order_by_condition(&mut self) -> Result<Vec<(String, OrderBy)>>{
        let mut order_by_condition = Vec::new();
        if self.next_if_is_token(Token::Keyword(Keyword::Order)).is_none(){
            return Ok(order_by_condition); // 没有指定 Order By 条件
        }
        self.expect_next_token_is(Token::Keyword(Keyword::By))?;

        loop{ // 可能有多个排序条件
            let col = self.expect_next_is_ident()?;
            // 可以不指定asc或者desc，默认asc
            // matches! 是 Rust 中的一个宏，用于检查一个值是否与给定的模式匹配
            let order = match self.next_if(|token| matches!(token, Token::Keyword(Keyword::Asc) | Token::Keyword(Keyword::Desc))){
                Some(Token::Keyword(Keyword::Asc)) => OrderBy::Asc,
                Some(Token::Keyword(Keyword::Desc)) => OrderBy::Desc,
                _ => OrderBy::Asc,  // 默认asc
            };

            order_by_condition.push((col, order));
            if self.next_if_is_token(Token::Comma).is_none(){
                break;
            }
        }
        Ok(order_by_condition)
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

    // 下一个token必须是ident
    fn expect_next_is_ident(&mut self) -> Result<String>{
        match self.next()? {
            Token::Ident(ident) => Ok(ident),
            token => Err(Error::Parse(format!("[Parser] Expected Ident, got token: {}",token))),
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
        self.peek().unwrap_or(None).filter(|token| condition(token))?;
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

#[cfg(test)]
mod tests{
    use super::*;
    use crate::{error::Result};
    use crate::sql::parser::ast::Consts::Integer;
    use crate::sql::parser::ast::OrderBy::{Asc, Desc};

    #[test]
    fn test_parser_create_table() -> Result<()> {
        let sql1 =  "
            create table tbl1 (
                a int default 100,
                b float not null,
                c varchar null,
                d bool default true
            );
        ";
        let sentence1 = Parser::new(sql1).parse()?;
        println!("{:?}", sentence1);
        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let sentence2 = Parser::new(sql2).parse()?;
        assert_eq!(sentence1, sentence2);

        let sql3 = "
            create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        )
        ";

        let sentence3 = Parser::new(sql3).parse();
        assert!(sentence3.is_err());
        Ok(())
    }

    #[test]
    fn test_parser_insert() -> Result<()> {
        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let sentence1 = Parser::new(sql1).parse()?;
        assert_eq!(
            sentence1,
            ast::Sentence::Insert {
                table_name: "tbl1".to_string(),
                columns: None,
                values: vec![vec![
                    ast::Consts::Integer(1).into(),
                    ast::Consts::Integer(2).into(),
                    ast::Consts::Integer(3).into(),
                    ast::Consts::String("a".to_string()).into(),
                    ast::Consts::Boolean(true).into(),
                ]],
            }
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let sentence2 = Parser::new(sql2).parse()?;
        assert_eq!(
            sentence2,
            ast::Sentence::Insert {
                table_name: "tbl2".to_string(),
                columns: Some(vec!["c1".to_string(), "c2".to_string(), "c3".to_string()]),
                values: vec![
                    vec![
                        ast::Consts::Integer(3).into(),
                        ast::Consts::String("a".to_string()).into(),
                        ast::Consts::Boolean(true).into(),
                    ],
                    vec![
                        ast::Consts::Integer(4).into(),
                        ast::Consts::String("b".to_string()).into(),
                        ast::Consts::Boolean(false).into(),
                    ],
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_select() -> Result<()> {
        let sql = "select * from tbl1 limit 10 offset 20;";
        let sentence = Parser::new(sql).parse()?;
        assert_eq!(
            sentence,
            ast::Sentence::Select {
                select_condition:vec![],
                from_item: Table { name:"tbl1".into() },
                order_by: vec![],
                limit: Some(Expression::Consts(Integer(10))),
                offset: Some(Expression::Consts(Integer(20))),
            }
        );

        let sql = "select * from tbl1 order by a, b asc, c desc;";
        let sentence = Parser::new(sql).parse()?;
        assert_eq!(
            sentence,
            ast::Sentence::Select {
                select_condition:vec![],
                from_item: Table { name:"tbl1".into() },
                order_by: vec![
                    ("a".to_string(), Asc),
                    ("b".to_string(), Asc),
                    ("c".to_string(), Desc),
                ],
                limit: None,
                offset: None,
            }
        );

        let sql = "select a as col1, b as col2, c from tbl1 order by a, b asc, c desc;";
        let sentence = Parser::new(sql).parse()?;
        assert_eq!(
            sentence,
            ast::Sentence::Select {
                select_condition: vec![
                    (Expression::Field("a".into()), Some("col1".into())),
                    (Expression::Field("b".into()), Some("col2".into())),
                    (Expression::Field("c".into()), None),
                ],
                from_item: Table { name:"tbl1".into() },
                order_by: vec![
                    ("a".to_string(), Asc),
                    ("b".to_string(), Asc),
                    ("c".to_string(), Desc),
                ],
                limit: None,
                offset: None,
            }
        );

        let sql = "select * from tbl1 cross join tbl2 cross join tbl3;";
        let sentence = Parser::new(sql).parse()?;
        assert_eq!(
            sentence,
            ast::Sentence::Select {
                select_condition: vec![],
                from_item: ast::FromItem::Join {
                    left: Box::new(ast::FromItem::Join {
                        left: Box::new(ast::FromItem::Table {
                            name: "tbl1".into()
                        }),
                        right: Box::new(ast::FromItem::Table {
                            name: "tbl2".into()
                        }),
                        join_type: ast::JoinType::Cross
                    }),
                    right: Box::new(ast::FromItem::Table {
                        name: "tbl3".into()
                    }),
                    join_type: ast::JoinType::Cross
                },
                order_by: vec![],
                limit: None,
                offset: None,
            }
        );

        Ok(())
    }

    #[test]
    fn test_parser_update() -> Result<()> {
        let sql = "update tbl set a = 1, b = 2.0 where c = 'a';";
        let sentence = Parser::new(sql).parse()?;
        println!("{:?}",sentence);
        assert_eq!(
            sentence,
            Sentence::Update {
                table_name: "tbl".into(),
                columns: vec![
                    ("a".into(), ast::Consts::Integer(1).into()),
                    ("b".into(), ast::Consts::Float(2.0).into()),
                ]
                    .into_iter()
                    .collect(),
                condition: Some(("c".into(), ast::Consts::String("a".into()).into())),
            }
        );

        Ok(())
    }
}