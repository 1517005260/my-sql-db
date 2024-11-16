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

impl TransactionState{
    fn is_visible(&self, version: Version) -> bool {
        if self.active_version.contains(&version) {
            false
        }else{
            version <= self.version
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub enum MvccKey{  // 和数据key类型区分
    NextVersion,   // 版本号
    ActiveTransactions(Version),  // 活跃事务版本号
    Write(Version,  #[serde(with = "serde_bytes")]Vec<u8>),     // 事务写入了哪些key
    Version( #[serde(with = "serde_bytes")]Vec<u8>, Version),  // (key, 所属version)
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
    ActiveTransactions, // 活跃事务前缀
    Write(Version),    // 事务写信息前缀
    Version(#[serde(with = "serde_bytes")] Vec<u8>),
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
        // 1. 获取存储引擎
        let mut engine = self.engine.lock()?;
        // 2. 获取事务写信息并删除
        let mut keys_to_be_deleted = Vec::new();
        let mut iter = engine.prefix_scan(MvccKeyPrefix::Write(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()?{
            keys_to_be_deleted.push(key);
        }
        drop(iter);  // 这里后续还要用到对engine的可变引用，而一次生命周期内仅能有一次引用，所以这里手动drop掉iter，停止对engine的可变引用
        for key in keys_to_be_deleted {
            engine.delete(key)?;
        }
        // 3. 从活跃列表删除本事务
        engine.delete(MvccKey::ActiveTransactions(self.state.version).encode())
    }

    pub fn rollback(&self) -> Result<()> {
        // 1. 获取存储引擎
        let mut engine = self.engine.lock()?;
        // 2. 获取事务写信息并删除
        let mut keys_to_be_deleted = Vec::new();
        let mut iter = engine.prefix_scan(MvccKeyPrefix::Write(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()?{
            // 这里比commit多一步删除写入log的真实数据
            match MvccKey::decode(&key)? {
                MvccKey::Write(_, raw_key) => {  // 这里找到的是不含版本信息的key
                    // 构造带版本信息的key
                    keys_to_be_deleted.push(MvccKey::Version(raw_key, self.state.version).encode());
                },
                _ => {
                    Err(Error::Internal(format!("[Transaction rollback] Unexpected key: {:?}", String::from_utf8(&key))))
                }
            }
            keys_to_be_deleted.push(key);
        }
        drop(iter);
        for key in keys_to_be_deleted {
            engine.delete(key)?;
        }
        // 3. 从活跃列表删除本事务
        engine.delete(MvccKey::ActiveTransactions(self.state.version).encode())
    }

    pub fn set(&mut self, key:Vec<u8>, value:Vec<u8>) -> Result<()> {
        self.update(key, Some(value))
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.update(key, None)
    }

    // set-delete 通用逻辑
    fn update(&self, key:Vec<u8>, value:Option<Vec<u8>>) -> Result<()> {  // 删除时value置空即可
        // 1. 获取存储引擎
        let mut engine= self.engine.lock()?;
        // 2. 检测是否冲突
        let from = MvccKey::Version(key.clone(), self.state.active_version.iter().min().copied().unwrap_or(self.state.version+1)).encode();
        // from 是最小的活跃版本，若活跃版本为空则置为 本事务版本+1
        let to = MvccKey::Version(key.clone(), u64::MAX).encode();
        // to 涵盖最大可能版本
        if let Some((key, _)) = engine.scan(from..=to).last().transpose()?{  // 取得key的最新版本
            match MvccKey::decode(&key)? {
                MvccKey::Version(_, version) => {
                    // 要修改的key的version是否对本事务可见
                    if !self.state.is_visible(version) {
                        Err(Error::WriteConflict)
                    }
                },
                _ => {
                    Err(Error::Internal(format!("[Transaction Update] Unexpected key: {:?}", String::from_utf8(&key))))
                }
            }
        }?;
        // 3. 不冲突，写入数据
        // 3.1 记录本version写入了哪些key，用于回滚数据
        engine.set(MvccKey::Write(self.state.version, key.clone()), vec![])?;
        // 3.2 写入实际的key-value数据
        engine.set(MvccKey::Version(key.clone(), self.state.version).encode(), bincode::serialize(&value)?)?;
        Ok(())
    }

    pub fn get(&self, key:Vec<u8>) -> Result<Option<Vec<u8>>> {
        // 1. 获取存储引擎
        let mut engine = self.engine.lock()?;
        // 2. 判断数据是否符合条件
        let from = MvccKey::Version(key.clone(), 0).encode();
        let to = MvccKey::Version(key.clone(), self.state.version).encode();
        let mut iter = engine.scan(from..=to).rev(); // rev 反转
        while let Some((key,value)) =  iter.next().transpose()?{
            match MvccKey::decode(&key)? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        Ok(bincode::deserialize(&value)?)
                    }
                },
                _ => {
                    Err(Error::Internal(format!("[Transaction get] Unexpected key: {:?}", String::from_utf8(&key))))
                }
            }
        }
        Ok(None)  // 未找到数据
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