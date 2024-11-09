# SQL引擎定义

SQL引擎不同于[之前说的](./01-BasicStructure.md)常见数据库组件，而是作为了一个中间层，串联起了执行器以及底层存储引擎。它可以被视为用户和服务器交互的一个中间实例。它在整个 SQL 处理流程中扮演了一个“桥梁”或“中间层”的角色，负责将用户的 SQL 请求解析、执行，并将结果返回给用户。

这里，我们的存储引擎实现最简单的KV存储引擎。

## 代码实现

1. 新建sql/engine/mod.rs，存放sql引擎的相关代码

```rust
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
                  let mut transaction = self.engine.begin()?;  // 开启事务，获取 KVTransaction（E.Transaction）

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
```

这里，我们新增了planner/mod.rs中，planner与底层executor的交互方法：

```rust
// planner与executor交互，plan节点 -> 执行器结构体
    pub fn execute<T:Transaction>(self, transaction :&mut T) -> Result<ResultSet>{
        <dyn Executor<T>>::build(self.0).execute(transaction)  // self.0 == node 只有这一个元素
    }
```

然后要修改executor文件夹下的所有execute()方法参数：

```rust
pub trait Executor<T:Transaction>{
    fn execute(&self,transaction:&mut T) -> Result<ResultSet>;
}

impl<T:Transaction> dyn Executor<T>{
    pub fn build(node: Node) -> Box<dyn Executor<T>>{
        match node {
            Node::CreateTable {schema} => CreateTable::new(schema),
            Node::Insert {table_name,columns,values} => Insert::new(table_name, columns, values),
            Node::Scan {table_name} => Scan::new(table_name),
        }
    }
}
```

```rust
impl<T:Transaction> Executor<T> for Insert{
    fn execute(&self,transaction:&mut T) -> crate::error::Result<ResultSet> {
        todo!()
    }
}
```

```rust
impl<T:Transaction> Executor<T> for Scan{
    fn execute(&self,trasaction:&mut T) -> crate::error::Result<ResultSet> {
        todo!()
    }
}
```

```rust
impl<T:Transaction> Executor<T> for CreateTable{
    fn execute(&self,transaction:&mut T) -> crate::error::Result<ResultSet> {
        todo!()  // 具体逻辑等待存储引擎构建完成后再写
    }
}
```

2. SQLEngine实现的接口可以对接不同的底层存储模型，这里我们以KV为例：

新建engine/kv.rs  => KV是engine的一个具体实现

```rust
use crate::error::Result;
use crate::sql::engine::{Engine, Transaction};
use crate::sql::schema::Table;
use crate::sql::types::Row;
use crate::storage;

// KV engine 定义
pub struct KVEngine {
    pub kv : storage::Mvcc
}

impl Clone for KVEngine {
    fn clone(&self) -> Self {
        Self{kv: self.kv.clone()}
    }
}

impl Engine for KVEngine {
    type Transaction = KVTransaction;

    fn begin(&self) -> Result<Self::Transaction> {
        Ok(
            Self::Transaction::new(self.kv.begin()?)
        )
    }
}

// 封装存储引擎中的MvccTransaction
pub struct KVTransaction{
    transaction : storage::MvccTransaction
}

impl KVTransaction{
    pub fn new(transaction: storage::MvccTransaction) -> Self {
        Self{transaction}
    }
}

impl Transaction for KVTransaction {
    fn commit(&self) -> Result<()> {
        todo!()
    }

    fn rollback(&self) -> Result<()> {
        todo!()
    }

    fn create_row(&mut self, table: String, row: Row) -> Result<()> {
        todo!()
    }

    fn scan(&self, table_name: String) -> Result<Vec<Row>> {
        todo!()
    }

    fn create_table(&mut self, table: Table) -> Result<()> {
        todo!()
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        todo!()
    }
}
```

而KV的底层又是Storage部分，底层存储引擎封装了MVCC，我们需要新建src/storage（和sql是平级的）来先进行抽象定义：

新建mod.rs，记得顺便添加到lib.rs里:

```rust
use crate::error::Result;
pub struct Mvcc{
    // 多版本并发控制，Multi-Version Concurrency Control
}

impl Clone for Mvcc {  // 顶层支持多个所有者，所以需要实现clone方法
    fn clone(&self) -> Mvcc {
        Self{}
    }
}

impl Mvcc {
    pub fn new() -> Self { Self{} }

    pub fn begin(&self) -> Result<MvccTransaction>{   // 开启事务
        Ok(MvccTransaction::new())
    }
}

pub struct MvccTransaction{
    // 代表一个具体的事务
}  
impl MvccTransaction {
    pub fn new() -> Self { Self{} }
}
```

## 流程总结

客户端发送SQL语句 -> `Session`接收并调用`execute`方法 -> 使用`Parser`解析SQL -> 开启事务`transaction` -> 使用`Plan`构建执行计划 -> `Plan`调用`execute`并构建`Executor` -> `Executor`执行具体操作并与`Transaction`交互 -> 成功则`transaction.commit()`，失败则`transaction.rollback()` -> 返回`ResultSet`给客户端。

即：Engine是抽象接口，KV是其具体实现，实现过程调用了存储部分的Mvcc的实现，通过封装Mvcc实现了数据的存储和事务控制。

可以查看示例图：

![engine.png](img/engine.png)

以及全流程的总结：

![all.png](img/all.png)