mod kv;

use crate::error::Result;
use crate::sql::executor::ResultSet;
use crate::sql::parser::Parser;
use crate::sql::planner::Plan;
use crate::sql::schema::Table;
use crate::sql::types::Row;

// 定义sql引擎的抽象接口
pub trait Engine: Clone{               // 实现engine的结构体必须可以被clone
    type Transaction: Transaction;     // 实现engine的结构体也必须实现自定义接口Transaction

    fn begin(&self) -> Result<Self::Transaction>;   // 每个sql语句，我们都会将其封装在一个事务中运行，所以执行sql时需要先开启事务

    fn session(&self) -> Result<Session<Self>>{    // 客户端与sql服务端的连接靠session来维持
        Ok(Session{
            engine: self.clone()     // 确保 Session 拥有当前引擎的一个副本
        })
    }
}

// 定义事务的抽象接口，可以接入底层的存储引擎
pub trait Transaction {
    fn commit(&self) -> Result<()>;

    fn rollback(&self) -> Result<()>;

    // 创建行
    fn create_row(&mut self,table:String,row: Row)-> Result<()>;

    // 扫描表
    fn scan(&self,table_name: String)-> Result<Vec<Row>>;

    // DDL
    fn create_table(&mut self, table:Table)-> Result<()>;

    // 获取表的信息
    fn get_table(&self, table_name:String)-> Result<Option<Table>>;
}

pub struct Session<E:Engine>{
    engine:E  // 存储当前的 SQL 引擎实例
}

impl<E:Engine> Session<E>{
    // 执行客户端传来的sql语句
    pub fn execute(&mut self, sql: &str) -> Result<ResultSet>{
        match Parser::new(sql).parse()?{    // 传进来的sql直接扔给parser解析
            sentence => {         //  获取到了一句sql
                  let mut transaction = self.engine.begin()?;  // 开启事务

                  // 开始构建plan
                  match Plan::build(sentence).    // 这里获得一个node
                      execute(&mut transaction){
                      Ok(res) => {
                          transaction.commit()?;  // 成功，事务提交
                          Ok(res)
                      },
                      Err(e) => {
                          transaction.rollback()?;  // 失败，事务回滚
                          Err(e)
                      }
                  }
            }
        }
    }
}