pub mod kv;

use std::collections::HashSet;
use crate::error::{Error, Result};
use crate::error::Error::Internal;
use crate::sql::executor::ResultSet;
use crate::sql::parser::ast::Expression;
use crate::sql::parser::{ast, Parser};
use crate::sql::planner::Plan;
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};

// 定义sql引擎的抽象接口
pub trait Engine: Clone{               // 实现engine的结构体必须可以被clone
    type Transaction: Transaction;     // 实现engine的结构体也必须实现自定义接口Transaction

    fn begin(&self) -> Result<Self::Transaction>;   // 每个sql语句，我们都会将其封装在一个事务中运行，所以执行sql时需要先开启事务

    fn session(&self) -> Result<Session<Self>>{    // 客户端与sql服务端的连接靠session来维持
        Ok(Session{
            engine: self.clone(),     // 确保 Session 拥有当前引擎的一个副本
            transaction: None,        // 初始化为None，直到有显式事务
        })
    }
}

// 定义事务的抽象接口，可以接入底层的存储引擎
pub trait Transaction {
    fn commit(&self) -> Result<()>;

    fn rollback(&self) -> Result<()>;

    // 获取事务版本号
    fn get_version(&self) -> u64;

    // 创建行
    fn create_row(&mut self,table:String,row: Row)-> Result<()>;

    // 更新行
    fn update_row(&mut self,table:&Table, primary_key:&Value, row: Row)-> Result<()>;

    // 删除行
    fn delete_row(&mut self,table:&Table, primary_key:&Value)-> Result<()>;

    // 扫描表
    fn scan(&self,table_name: String, filter: Option<Expression>)-> Result<Vec<Row>>;

    // DDL
    fn create_table(&mut self, table:Table)-> Result<()>;
    fn drop_table(&mut self, name: String)-> Result<()>;

    // 获取表的信息
    fn get_table(&self, table_name:String)-> Result<Option<Table>>;

    // 获取所有表名
    fn get_all_table_names(&self)-> Result<Vec<String>>;

    // 必须获取表
    fn must_get_table(&self, table_name:String)-> Result<Table>{
        self.get_table(table_name.clone())?.  // ok_or : Option -> Result
            ok_or(Error::Internal(format!("[Get Table] Table \" {} \" does not exist",table_name)))
    }

    // 索引相关方法
    fn load_index(&self, table_name: &str, col_name: &str, col_value: &Value) -> Result<HashSet<Value>>;
    fn save_index(&mut self, table_name: &str, col_name: &str, col_value: &Value, index: HashSet<Value>) -> Result<()>;
    fn read_row_by_pk(&self, table_name: &str, pk: &Value) -> Result<Option<Row>>;
}

pub struct Session<E:Engine>{
    engine: E,  // 存储当前的 SQL 引擎实例
    transaction: Option<E::Transaction>,   // 显式事务命令
}

impl<E:Engine + 'static> Session<E> {
    // 执行客户端传来的sql语句
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet> {
        match Parser::new(sql).parse()? {    // 传进来的sql直接扔给parser解析
            ast::Sentence::Begin{} if self.transaction.is_some() =>{
                return Err(Internal("[Exec Transaction] Already in transaction".into()))
            },
            ast::Sentence::Commit{} | ast::Sentence::Rollback{}  if self.transaction.is_none()=> {
                return Err(Internal("[Exec Transaction] Not in transaction".into()))
            },
            ast::Sentence::Begin{} => {        // 处理事务命令
                let transaction = self.engine.begin()?;
                let version = transaction.get_version();
                self.transaction = Some(transaction);
                Ok(ResultSet::Begin { version })
            },
            ast::Sentence::Commit{} => {
                let transaction = self.transaction.take()  // take() 会将 Option 取出，同时将原来的 Option 设置为 None
                    .unwrap();
                // let transaction = self.transaction.as_ref().unwrap();
                let version = transaction.get_version();
                transaction.commit()?;
                Ok(ResultSet::Commit { version })
            },
            ast::Sentence::Rollback{} => {
                let transaction = self.transaction.take().unwrap();
                // let transaction = self.transaction.as_ref().unwrap();
                let version = transaction.get_version();
                transaction.rollback()?;
                Ok(ResultSet::Rollback { version })
            },
            sentence if self.transaction.is_some() =>{
                // 在事务内的sql
                Plan::build(sentence, self.transaction.as_mut().unwrap())?.execute(self.transaction.as_mut().unwrap())
            },
            sentence => {         //  获取到了一句无显式事务的sql
                let mut transaction = self.engine.begin()?;  // 开启事务

                // 开始构建plan
                match Plan::build(sentence, &mut transaction)?.    // 这里获得一个node
                    execute(&mut transaction) {
                    Ok(res) => {
                        transaction.commit()?;  // 成功，事务提交
                        Ok(res)
                    },
                    Err(e) => {
                        transaction.rollback()?;  // 失败，事务回滚
                        Err(e)
                    }
                }
            },
        }
    }
}