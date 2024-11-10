# SQL的运行

之前已经简单搭建好了一个SQL数据库的所有基本组件，现在我们将所有组件串联起来，完成简单SQL的运行。

流程图还是和之前的一样：

![all](./img/all.png)

## 代码实现

1. 为了代码的结构清晰，这里我们将原本的storage/mod.rs中有关mvcc相关的内容全部移动到新文件mvcc.rs中。

现在mod.rs变成：

```rust
pub mod engine;
pub mod memory;
pub mod mvcc;
```

而mvcc我们现在先不实现，先简单定义，只是声明多线程下的读写方法，后续在事务中详细实现：

```rust
use std::sync::{Arc, Mutex};
use crate::error::Result;
use crate::storage::engine::Engine;

pub struct Mvcc<E:Engine>{     // 多版本并发控制，Multi-Version Concurrency Control
    // 这里是基于存储引擎的事务，所以我们既需要泛型，又需要线程安全
    engine: Arc<Mutex<E>>,   // arc是多线程读，mutex是多线程写
}

impl<E:Engine> Clone for Mvcc<E> {  // 顶层支持多个所有者，所以需要实现clone方法
    fn clone(&self) -> Self {
        Self{ engine: self.engine.clone() }
    }
}

impl<E:Engine> Mvcc<E> {
    pub fn new(engine:E) -> Self {
        Self{ engine:Arc::new(Mutex::new(engine)) }
    }

    pub fn begin(&self) -> Result<MvccTransaction<E>>{   // 开启事务
        Ok(MvccTransaction::begin(self.engine.clone()))  // 直接调用底层的事务实现
    }
}

pub struct MvccTransaction<E:Engine>{
    // 代表一个具体的事务
    engine: Arc<Mutex<E>>,
}
impl<E:Engine> MvccTransaction<E> {
    pub fn begin(engine: Arc<Mutex<E>>) -> Self {
        Self{ engine }
    }

    pub fn commit(&self) -> Result<()> {
        Ok(())
    }

    pub fn rollback(&self) -> Result<()> {
        Ok(())
    }

    pub fn set(&mut self, key:Vec<u8>, value:Vec<u8>) -> Result<()> {
        let mut engine = self.engine.lock()?;   // lock 获取 锁，进行独占
        engine.set(key, value)
    }

    pub fn get(&self, key:Vec<u8>) -> Result<Option<Vec<u8>>> {
        let mut engine = self.engine.lock()?;
        engine.get(key)
    }

    pub fn prefix_scan(&self, prefix:Vec<u8>) -> Result<Vec<ScanResult>>{
        let mut engine = self.engine.lock()?;
        let mut iter = engine.prefix_scan(prefix);
        let mut results = Vec::new();
        while let Some((key,value)) = iter.next(){
            results.push(ScanResult { key,value });
        }
        Ok(results)
    }
}

pub struct ScanResult{
    // prefix_scan() 的辅助结构体
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
```

这里需要补充一点：为什么需要前缀扫描方法？这是因为我们的逻辑表存在物理存储中，实际上是不能很好地被区分的。这里我们假如有A，B两个表，要扫描A表，即`select * from A`，怎么知道A的数据在哪里？我们将数据存储时，加上表名前缀就可以区分了。

此外，这里又涉及到了`lock()`可能的报错，这和之前的parse部分的error不一样，我们需要在自己的error.rs中新增兼容代码：

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Error{
    Parse(String), // 在解析器阶段报错，内容为String的错误
    Internal(String),   // 在数据库内部运行时的报错
}

// lock()相关报错处理
impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Error::Internal(value.to_string())
    }
}
```

2. 回到sql/engine下，我们刚刚修改了一些mvcc的代码，这里也要做出相应修改：

这里sql/engine和storage/engine可能会产生混淆，所以也需要修改

```rust
use crate::storage::{self,engine::Engine as storageEngine};  // self 即指 crate::storage

pub struct KVEngine<E:storageEngine> {
    pub kv : storage::mvcc::Mvcc<E>
}

impl<E:storageEngine> Clone for KVEngine<E> {...}

impl<E:storageEngine> Engine for KVEngine<E> {
    type Transaction = KVTransaction<E>;
    ...
}

pub struct KVTransaction<E:storageEngine>{
    transaction : storage::mvcc::MvccTransaction<E>
}

impl<E:storageEngine> KVTransaction<E>{
    pub fn new(transaction: storage::mvcc::MvccTransaction<E>) -> Self {
        Self{transaction}
    }
}

impl<E:storageEngine> Transaction for KVTransaction<E> {...}
```

3. 接着继续修改sql/executor，完善之前未完成的工作：

这里我们注意，由于 Rust 的所有权机制较为严格，而观察发现 SQL 语句在执行后不再被使用，因此可以直接转移所有权，而不需要借用。此外，这里我们采取了一种常见的工程实践：接口在编译期才能确定传入的数据类型，大小未知，因而不能直接分配到栈上。为了避免栈空间不足，使用 Box 将数据分配到堆上。

修改接口：

```rust
pub trait Executor<T:Transaction>{
    fn execute(self: Box<Self>,transaction:&mut T) -> Result<ResultSet>;
}
```

schema.rs:

```rust
impl<T:Transaction> Executor<T> for CreateTable{
    fn execute(self:Box<Self>,transaction:&mut T) -> crate::error::Result<ResultSet> {
        let table_name = self.schema.name.clone();
        transaction.create_table(self.schema)?;
        Ok(ResultSet{table_name})
    }
}
```

4. 在engine/kv.rs中进行具体`create_table`,`create_row`,`scan`的实现

```rust
use serde::{Deserialize, Serialize};

impl<E:storageEngine> Transaction for KVTransaction<E> {
    fn create_table(&mut self, table: Table) -> Result<()> {
        // 1. 判断表是否存在
        if self.get_table(table.name.clone())?.is_some() {
            return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" already exists", table.name.clone())))
        }

        // 2. 判断表的有效性
        if table.columns.is_empty() {
            return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" has no columns", table.name.clone())))
        }

        // 创建表成功，调用存储引擎存储
        // String -> 序列化 -> bincode
        let key = Key::Table(table.name.clone());
        let bin_code_key = bincode::serialize(&key)?;
        let value = bincode::serialize(&table)?;
        self.transaction.set(bin_code_key, value)?;

        Ok(())
    }

    fn get_table(&self, table_name: String) -> Result<Option<Table>> {
        let key = Key::Table(table_name);
        let bin_code_key = bincode::serialize(&key)?;
        let value = self.transaction.get(bin_code_key)?.map(
            |value| bincode::deserialize(&value)
        ).transpose()?;
        Ok(value)
    }
}

// 辅助方法：由于底层的存储的传入参数都是 u8, 用户给的字符串需要进行转换
#[derive(Debug,Serialize,Deserialize)]
enum Key{
    Table(String),
    Row(String,String),   // (table_name, primary_key)
}
```

这里，我们用到了第三方库，用于字符串向字节码的转换，便于底层存储，在`Cargo.toml`中新增：

```toml
[dependencies]
bincode = "1.3.3"
```

它的源码位于：https://crates.io/crates/bincode

`bincode`的使用还需要结合serde（用于序列化和反序列化），故继续新增：

```toml
serde = { version = "1.0", features = ["derive"] }
```

**序列化是指将对象或数据结构转换为一种特定格式的过程，这种格式通常是字符串、二进制数据或其他易于存储和传输的形式。**

这里也需要将序列化的错误与自定义的进行兼容，故再在error.rs中新增：

```rust
// 序列化相关报错处理
impl From<Box<ErrorKind>> for Error{
    fn from(value: Box<ErrorKind>) -> Self {
        Error::Internal(value.to_string())
    }
}
```

此外，所有相关的自定义数据结构也都要实现Serialize,Deserialize：

```rust
#[derive(Debug, PartialEq,Serialize,Deserialize)]
pub struct Table{}

#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub struct Column{}

#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub enum DataType {}

#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub enum Value {}
```