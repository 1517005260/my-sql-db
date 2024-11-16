use std::collections::HashSet;
use std::sync::{Arc, Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};
use crate::storage::engine::Engine;

pub type Version = u64;

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
        MvccTransaction::begin(self.engine.clone())  // 直接调用底层的事务实现
    }
}

pub struct MvccTransaction<E:Engine>{
    // 代表一个具体的事务
    engine: Arc<Mutex<E>>,
    state: TransactionState,
}

pub struct TransactionState{
    pub version: Version,  // 本事务版本号
    pub active_version: HashSet<Version>,  // 活跃事务对应的版本号
}

#[derive(Clone, Serialize, Deserialize)]
pub enum MvccKey{  // 和数据key类型区分
    NextVersion,   // 版本号
    ActiveTransactions(Version)  // 活跃事务，附有版本号
}

impl MvccKey{
    // 编码为二进制
    pub fn encode(&self) -> Vec<u8>{
        bincode::serialize(&self).unwrap()
    }

    // 解码二进制
    pub fn decode(data: Vec<u8>) -> Result<Self> {
        Ok(bincode::deserialize(&data)?)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum MvccKeyPrefix{  // MvccKey的前缀，用于扫描活跃事务
    NextVersion,   // 版本号前缀
    ActiveTransactions // 活跃事务前缀
}
impl MvccKeyPrefix {
    // 编码为二进制
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(&self).unwrap()
    }
}

impl<E:Engine> MvccTransaction<E> {

    // 开启事务
    pub fn begin(engine: Arc<Mutex<E>>) -> Result<Self> {
        // 1. 获取存储引擎
        let mut engine= engine.lock()?;
        // 2. 获取全局版本号，这里需要特判：第一个事务的版本号是空值
        let next_version = match engine.get(MvccKey::NextVersion.encode()) {
            None => 1,
            Some(version) => bincode::deserialize(&version)?,
        };
        // 3. 全局版本号++
        engine.set(MvccKey::NextVersion.encode(), bincode::serialize(&(next_version + 1))?)?;
        // 4. 获取活跃事务列表
        let active_version = Self::scan_active_transactions(&mut engine)?;
        // 5. 将本事务添加到活跃事务列表
        engine.set(MvccKey::ActiveTransactions(next_version).encode(), vec![])?;  // 事务活跃列表数据存在key里，value存空值即可

        Ok(Self{
            engine: engine.clone(),
            state: TransactionState{
            version: next_version,
            active_version,
            }
        })
    }

    // 获取活跃事务辅助方法
    fn scan_active_transactions(eng: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut res = HashSet::new();
        let mut iter = eng.prefix_scan(MvccKeyPrefix::ActiveTransactions.encode());
        while let Some((key, _)) = iter.next().transpose()? {  // key是二进制
            match MvccKey::decode(&key)? {
                MvccKey::ActiveTransactions(version) => {
                    res.insert(version);
                },
                _ => Err(Error::Internal(format!("[Scan Active Transactions] Unexpected key {:?}", String::from_utf8(&key)))?)
            }
        }
        Ok(res)
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