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