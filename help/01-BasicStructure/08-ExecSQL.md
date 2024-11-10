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
        let mut eng = self.engine.lock()?;
        let mut iter = eng.prefix_scan(prefix);
        let mut results = Vec::new();
        while let Some((key, value)) = iter.next().transpose()? {
            results.push(ScanResult { key, value });
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
        Ok(ResultSet::CreateTable{table_name})
    }
}
```

这里在engine/mod.rs中新增必须获取表的方法，方便后续不用做判断了：

```rust
pub trait Transaction {
    // 必须获取表
    fn must_get_table(&self, table_name:String)-> Result<Table>{
        self.get_table(table_name.clone())?.  // ok_or : Option -> Result
            ok_or(Error::Internal(format!("[Get Table] Table \" {} \" does not exist",table_name)))
    }
}
```

mutation.rs: 这里Insert的判断比较复杂，因为有很多可选条件

```rust
impl<T:Transaction> Executor<T> for Insert{
    fn execute(self:Box<Self>,transaction:&mut T) -> Result<ResultSet> {
        // 插入表之前，表必须是存在的
        let table = transaction.must_get_table(self.table_name.clone())?;

        // ResultSet成功结果返回插入行数
        let mut count = 0;

        // 现在手上表的数据类型是values:Vec<Vec<Expression>>,我们需要进行一些操作
        for exprs in self.values{
            // 1. 先将 Vec<Expression> 转换为 Row，即Vec<Value>
            let row = exprs.into_iter().map(|e|Value::from_expression_to_value(e))
                .collect::<Vec<Value>>();

            // 2. 可选项：是否指定了插入的列
            let insert_row = if self.columns.is_empty(){
                // 未指定插入列
                complete_row(&table,&row)?
            }else{
                // 指定插入列
                modify_row(&table,&self.columns,&row)?
            };
            transaction.create_row(self.table_name.clone(),insert_row)?;
            count += 1;
        }
        Ok(ResultSet::Insert {count})
    }
}

// 辅助判断方法
// 1. 补全列，即列对齐
fn complete_row(table: &Table, row: &Row) -> Result<Row>{
    let mut res = row.clone();
    for column in table.columns.iter().skip(row.len()){  // 跳过已经给定数据的列
        if let Some(default) = &column.default{
            // 有默认值
            res.push(default.clone());
        }else{
            // 建表时没有默认值但是insert时又没给数据
            return Err(Error::Internal(format!("[Insert Table] Column \" {} \" has no default value", column.name)));
        }
    }
    Ok(res)
}

// 2. 调整列信息并补全
fn modify_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row>{
    // 首先先判断给的列数和values的数量是否是一致的：
    if columns.len() != values.len(){
        return Err(Error::Internal("[Insert Table] Mismatch num of columns and values".to_string()));
    }

    // 有可能顺序是乱的，但是返回时顺序不能乱，这里考虑使用hash
    let mut inputs = HashMap::new();
    for (i,col_name) in columns.iter().enumerate(){  // enumerate()用于为迭代中的每个元素附加一个索引值
        inputs.insert(col_name,values[i].clone());
    }

    // 现在inputs就是顺序正常的插入行，之后和complete_row()思路差不多了
    let mut res = Vec::new();
    for col in table.columns.iter(){
        if let Some(value) = inputs.get(&col.name){
            res.push(value.clone());
        }else if let Some(default) = &col.default{
            res.push(default.clone());
        }else {
            return Err(Error::Internal(format!("[Insert Table] Column \" {} \" has no default value", col.name)));
        }
    }

    Ok(res)
}
```

这里，形象解释一下两个辅助方法：

```
1. 补全列

现在有表tbl，sql语句是： insert into tbl values(1,2,3);，即未指定D列的数据，也未指定插入什么列
    列名     A        B       C       D
行号  1      1        2       3     default

2. 调整列信息
现在有表tbl，sql语句是： insert into tbl(D,C) values(1,2);，即指定了，但没指定全列，顺序也不一定
    列名     A        B       C       D
行号  1    default  default   2       1
```

query.rs:

```rust
impl<T:Transaction> Executor<T> for Scan{
    fn execute(self:Box<Self>,trasaction:&mut T) -> crate::error::Result<ResultSet> {
        let table = trasaction.must_get_table(self.table_name.clone())?;
        let rows = trasaction.scan(self.table_name.clone())?;
        Ok(
            ResultSet::Scan{
                columns: table.columns.into_iter().map(|c| c.name.clone()).collect(),
                rows,
            }
        )
    }
}
```

4. 在engine/kv.rs中进行具体`create_table`,`create_row`,`scan`的实现

为了校验插入行的所有数据类型和列类型是否匹配，我们在types/mod.rs新增：

```rust
impl Value {
    pub fn get_datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }
}
```

```rust
use serde::{Deserialize, Serialize};

impl<E:storageEngine> Transaction for KVTransaction<E> {
    fn commit(&self) -> Result<()> {
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }

    fn create_row(&mut self, table: String, row: Row) -> Result<()> {
        let table = self.must_get_table(table)?;
        // 插入行数据的数据类型检查
        for (i,col) in table.columns.iter().enumerate() {
            match row[i].get_datatype() {
                None if col.nullable => continue,
                None => return Err(Error::Internal(format!("[Insert Table] Column \" {} \" cannot be null",col.name))),
                Some(datatype) if datatype != col.datatype => return Err(Error::Internal(format!("[Insert Table] Column \" {} \" mismatched data type",col.name))),
                _ => continue,
            }
        }
        // 存放数据，这里暂时以第一列为主键
        let key = Key::Row(table.name.clone(), row[0].clone());
        let bin_code_key = bincode::serialize(&key)?;
        let value = bincode::serialize(&row)?;
        self.transaction.set(bin_code_key, value)?;
        Ok(())
    }

    fn scan(&self, table_name: String) -> Result<Vec<Row>> {
        // 根据前缀扫描表
        let prefix = PrefixKey::Row(table_name.clone());
        let results = self.transaction.prefix_scan(bincode::serialize(&prefix)?)?;

        let mut rows = Vec::new();
        for res in results {
            let row: Row = bincode::deserialize(&res.value)?;
            rows.push(row);
        }
        Ok(rows)
    }
    
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
    Row(String,Value),   // (table_name, primary_key)
}

// 辅助枚举，用于前缀扫描
#[derive(Debug,Serialize,Deserialize)]
enum PrefixKey {
    Table, // 存的时候Table是第0个枚举，Row是第一个枚举，如果这里没有Table的话，扫描的时候是对不上的，所以要Table进行占位
    Row(String)
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

#[derive(Debug,PartialEq,Serialize,Deserialize,Clone)]
pub enum Value {}
```

5. 在kv.rs中写点测试：

```rust
// new方法定义
impl<E:storageEngine> KVEngine<E>{
    pub fn new(engine:E) -> Self {
        Self {
            kv: storage::mvcc::Mvcc::new(engine),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{error::Result, sql::engine::Engine, storage::memory::MemoryEngine};

    use super::KVEngine;

    #[test]
    fn test_create_table() -> Result<()> {
        let kvengine = KVEngine::new(MemoryEngine::new());
        let mut s = kvengine.session()?;

        s.execute("create table t1 (a int, b text default 'vv', c integer default 100);")?;
        s.execute("insert into t1 values(1, 'a', 1);")?;
        s.execute("insert into t1 values(2, 'b');")?;
        s.execute("insert into t1(c, a) values(200, 3);")?;

        s.execute("select * from t1;")?;

        Ok(())
    }
}
```