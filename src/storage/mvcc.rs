use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use serde::{Deserialize, Serialize};
use crate::error::{Error, Result};
use crate::storage::engine::Engine;
use crate::storage::keyencode::{deserialize_key, serialize_key};

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

#[derive(Clone, Serialize, Deserialize,PartialEq,Debug)]
pub enum MvccKey{  // 和数据key类型区分
    NextVersion,   // 版本号
    ActiveTransactions(Version),  // 活跃事务版本号
    Write(Version,  #[serde(with = "serde_bytes")]Vec<u8>),     // 事务写入了哪些key
    Version( #[serde(with = "serde_bytes")]Vec<u8>, Version),  // (key, 所属version)
}

impl MvccKey{
    // 编码为二进制
    pub fn encode(&self) -> Result<Vec<u8>>{
        serialize_key(&self)
    }

    // 解码二进制
    pub fn decode(data: Vec<u8>) -> Result<Self> {
        deserialize_key(&data)
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
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }
}

impl<E:Engine> MvccTransaction<E> {
    // 开启事务
    pub fn begin(eng: Arc<Mutex<E>>) -> Result<Self> {
        // 1. 获取存储引擎
        let mut engine= eng.lock()?;
        // 2. 获取全局版本号，这里需要特判：第一个事务的版本号是空值
        let next_version = match engine.get(MvccKey::NextVersion.encode()?)? {
            Some(version) => bincode::deserialize(&version)?,
            None => 1,
        };
        // 3. 全局版本号++
        engine.set(MvccKey::NextVersion.encode()?, bincode::serialize(&(next_version + 1))?)?;
        // 4. 获取活跃事务列表
        let active_version = Self::scan_active_transactions(&mut engine)?;
        // 5. 将本事务添加到活跃事务列表
        engine.set(MvccKey::ActiveTransactions(next_version).encode()?, vec![])?;  // 事务活跃列表数据存在key里，value存空值即可

        Ok(Self{
            engine: eng.clone(),
            state: TransactionState{
            version: next_version,
            active_version,
            }
        })
    }

    // 获取事务版本号
    pub fn get_version(&self) -> u64{
        self.state.version
    }

    // 获取活跃事务辅助方法
    fn scan_active_transactions(eng: &mut MutexGuard<E>) -> Result<HashSet<Version>> {
        let mut res = HashSet::new();
        let mut iter = eng.prefix_scan(MvccKeyPrefix::ActiveTransactions.encode()?);
        while let Some((key, _)) = iter.next().transpose()? {  // key是二进制
            match MvccKey::decode(key.clone())? {
                MvccKey::ActiveTransactions(version) => {
                    res.insert(version);
                },
                _ => return Err(Error::Internal(format!("[Scan Active Transactions] Unexpected key {:?}", String::from_utf8(key))))
            }
        }
        Ok(res)
    }

    pub fn commit(&self) -> Result<()> {
        // 1. 获取存储引擎
        let mut engine = self.engine.lock()?;
        // 2. 获取事务写信息并删除
        let mut keys_to_be_deleted = Vec::new();
        let mut iter = engine.prefix_scan(MvccKeyPrefix::Write(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()?{
            keys_to_be_deleted.push(key);
        }
        drop(iter);  // 这里后续还要用到对engine的可变引用，而一次生命周期内仅能有一次引用，所以这里手动drop掉iter，停止对engine的可变引用
        for key in keys_to_be_deleted {
            engine.delete(key)?;
        }
        // 3. 从活跃列表删除本事务
        engine.delete(MvccKey::ActiveTransactions(self.state.version).encode()?)
    }

    pub fn rollback(&self) -> Result<()> {
        // 1. 获取存储引擎
        let mut engine = self.engine.lock()?;
        // 2. 获取事务写信息并删除
        let mut keys_to_be_deleted = Vec::new();
        let mut iter = engine.prefix_scan(MvccKeyPrefix::Write(self.state.version).encode()?);
        while let Some((key, _)) = iter.next().transpose()?{
            // 这里比commit多一步删除写入log的真实数据
            match MvccKey::decode(key.clone())? {
                MvccKey::Write(_, raw_key) => {  // 这里找到的是不含版本信息的key
                    // 构造带版本信息的key
                    keys_to_be_deleted.push(MvccKey::Version(raw_key, self.state.version).encode()?);
                },
                _ => {
                    return Err(Error::Internal(format!("[Transaction rollback] Unexpected key: {:?}", String::from_utf8(key))))
                }
            }
            keys_to_be_deleted.push(key);
        }
        drop(iter);
        for key in keys_to_be_deleted {
            engine.delete(key)?;
        }
        // 3. 从活跃列表删除本事务
        engine.delete(MvccKey::ActiveTransactions(self.state.version).encode()?)
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
        let from = MvccKey::Version(key.clone(), self.state.active_version.iter().min().copied().unwrap_or(self.state.version+1)).encode()?;
        // from 是最小的活跃版本，若活跃版本为空则置为 本事务版本+1
        let to = MvccKey::Version(key.clone(), u64::MAX).encode()?;
        // to 涵盖最大可能版本
        if let Some((key, _)) = engine.scan(from..=to).last().transpose()?{  // 取得key的最新版本
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    // 要修改的key的version是否对本事务可见
                    if !self.state.is_visible(version) {
                        return Err(Error::WriteConflict)
                    }
                },
                _ => {
                    return Err(Error::Internal(format!("[Transaction Update] Unexpected key: {:?}", String::from_utf8(key))))
                }
            }
        };
        // 3. 不冲突，写入数据
        // 3.1 记录本version写入了哪些key，用于回滚数据
        engine.set(MvccKey::Write(self.state.version, key.clone()).encode()?, vec![])?;
        // 3.2 写入实际的key-value数据
        engine.set(MvccKey::Version(key.clone(), self.state.version).encode()?, bincode::serialize(&value)?)?;
        Ok(())
    }

    pub fn get(&self, key:Vec<u8>) -> Result<Option<Vec<u8>>> {
        // 1. 获取存储引擎
        let mut engine = self.engine.lock()?;
        // 2. 判断数据是否符合条件
        let from = MvccKey::Version(key.clone(), 0).encode()?;
        let to = MvccKey::Version(key.clone(), self.state.version).encode()?;
        let mut iter = engine.scan(from..=to).rev(); // rev 反转
        while let Some((key,value)) =  iter.next().transpose()?{
            match MvccKey::decode(key.clone())? {
                MvccKey::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return Ok(bincode::deserialize(&value)?)
                    }
                },
                _ => {
                    return Err(Error::Internal(format!("[Transaction get] Unexpected key: {:?}", String::from_utf8(key))))
                }
            }
        }
        Ok(None)  // 未找到数据
    }

    pub fn prefix_scan(&self, prefix:Vec<u8>) -> Result<Vec<ScanResult>>{
        let mut eng = self.engine.lock()?;
        let mut encode_prefix = MvccKeyPrefix::Version(prefix).encode()?;
        // 截断最后两个0
        encode_prefix.truncate(encode_prefix.len() - 2);
        let mut iter = eng.prefix_scan(encode_prefix);
        let mut results = BTreeMap::new();
        while let Some((encode_key, encode_value)) = iter.next().transpose()? {
            // 这里拿到的是编码后的kv对，需要进行解码
            match MvccKey::decode(encode_key.clone())? {
                MvccKey::Version(key, version) => {
                    if self.state.is_visible(version) {
                        // value 也需要解码
                        match bincode::deserialize(&encode_value)?{
                            Some(value) => results.insert(key, value),
                            None => results.remove(&key)
                        };
                    }
                },
                _ => {
                    return Err(Error::Internal(format!("[Transaction Prefix_Scan] Unexpected key: {:?}", String::from_utf8(encode_key))))
                }
            }
        }
        Ok(
            results.into_iter().map(|(k,v)| ScanResult{key:k,value:v} ).collect()
        )
    }
}

#[derive(Debug, PartialEq)]
pub struct ScanResult{
    // prefix_scan() 的辅助结构体
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        storage::{disk::DiskEngine, engine::Engine, memory::MemoryEngine},
    };
    use super::*;

    // 1. Get
    fn get(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val3".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val4".to_vec())?;
        transaction.delete(b"key3".to_vec())?;
        transaction.commit()?;

        let transaction1 = mvcc.begin()?;
        assert_eq!(transaction1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(transaction1.get(b"key2".to_vec())?, Some(b"val3".to_vec()));
        assert_eq!(transaction1.get(b"key3".to_vec())?, None);

        Ok(())
    }

    #[test]
    fn test_get() -> Result<()> {
        get(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        get(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 2. Get Isolation
    fn get_isolation(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val3".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val4".to_vec())?;
        transaction.commit()?;

        let mut transaction1 = mvcc.begin()?;
        transaction1.set(b"key1".to_vec(), b"val2".to_vec())?;

        let mut transaction2 = mvcc.begin()?;

        let mut transaction3 = mvcc.begin()?;
        transaction3.set(b"key2".to_vec(), b"val4".to_vec())?;
        transaction3.delete(b"key3".to_vec())?;
        transaction3.commit()?;

        assert_eq!(transaction2.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(transaction2.get(b"key2".to_vec())?, Some(b"val3".to_vec()));
        assert_eq!(transaction2.get(b"key3".to_vec())?, Some(b"val4".to_vec()));

        Ok(())
    }
    #[test]
    fn test_get_isolation() -> Result<()> {
        get_isolation(MemoryEngine::new())?;

        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        get_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 3. scan prefix
    fn prefix_scan(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"aabb".to_vec(), b"val1".to_vec())?;
        transaction.set(b"abcc".to_vec(), b"val2".to_vec())?;
        transaction.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        transaction.set(b"acca".to_vec(), b"val4".to_vec())?;
        transaction.set(b"aaca".to_vec(), b"val5".to_vec())?;
        transaction.set(b"bcca".to_vec(), b"val6".to_vec())?;
        transaction.commit()?;

        let transaction1 = mvcc.begin()?;
        let iter1 = transaction1.prefix_scan(b"aa".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
            ]
        );

        let iter2 = transaction1.prefix_scan(b"a".to_vec())?;
        assert_eq!(
            iter2,
            vec![
                ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
                ScanResult {
                    key: b"abcc".to_vec(),
                    value: b"val2".to_vec()
                },
                ScanResult {
                    key: b"acca".to_vec(),
                    value: b"val4".to_vec()
                },
            ]
        );

        let iter3 = transaction1.prefix_scan(b"bcca".to_vec())?;
        assert_eq!(
            iter3,
            vec![ScanResult {
                key: b"bcca".to_vec(),
                value: b"val6".to_vec()
            },]
        );

        Ok(())
    }

    #[test]
    fn test_prefix_scan() -> Result<()> {
        prefix_scan(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        prefix_scan(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 4. scan isolation
    fn scan_isolation(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"aabb".to_vec(), b"val1".to_vec())?;
        transaction.set(b"abcc".to_vec(), b"val2".to_vec())?;
        transaction.set(b"bbaa".to_vec(), b"val3".to_vec())?;
        transaction.set(b"acca".to_vec(), b"val4".to_vec())?;
        transaction.set(b"aaca".to_vec(), b"val5".to_vec())?;
        transaction.set(b"bcca".to_vec(), b"val6".to_vec())?;
        transaction.commit()?;

        let mut transaction1 = mvcc.begin()?;
        let mut transaction2 = mvcc.begin()?;
        transaction2.set(b"acca".to_vec(), b"val4-1".to_vec())?;
        transaction2.set(b"aabb".to_vec(), b"val1-1".to_vec())?;

        let mut transaction3 = mvcc.begin()?;
        transaction3.set(b"bbaa".to_vec(), b"val3-1".to_vec())?;
        transaction3.delete(b"bcca".to_vec())?;
        transaction3.commit()?;

        let iter1 = transaction1.prefix_scan(b"aa".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
            ]
        );

        let iter2 = transaction1.prefix_scan(b"a".to_vec())?;
        assert_eq!(
            iter2,
            vec![
                ScanResult {
                    key: b"aabb".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"aaca".to_vec(),
                    value: b"val5".to_vec()
                },
                ScanResult {
                    key: b"abcc".to_vec(),
                    value: b"val2".to_vec()
                },
                ScanResult {
                    key: b"acca".to_vec(),
                    value: b"val4".to_vec()
                },
            ]
        );

        let iter3 = transaction1.prefix_scan(b"bcca".to_vec())?;
        assert_eq!(
            iter3,
            vec![ScanResult {
                key: b"bcca".to_vec(),
                value: b"val6".to_vec()
            },]
        );

        Ok(())
    }

    #[test]
    fn test_scan_isolation() -> Result<()> {
        scan_isolation(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        scan_isolation(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 5. set
    fn set(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val3".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val4".to_vec())?;
        transaction.set(b"key4".to_vec(), b"val5".to_vec())?;
        transaction.commit()?;

        let mut transaction1 = mvcc.begin()?;
        let mut transaction2 = mvcc.begin()?;

        transaction1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        transaction1.set(b"key2".to_vec(), b"val3-1".to_vec())?;
        transaction1.set(b"key2".to_vec(), b"val3-2".to_vec())?;

        transaction2.set(b"key3".to_vec(), b"val4-1".to_vec())?;
        transaction2.set(b"key4".to_vec(), b"val5-1".to_vec())?;

        transaction1.commit()?;
        transaction2.commit()?;

        let transaction = mvcc.begin()?;
        assert_eq!(transaction.get(b"key1".to_vec())?, Some(b"val1-1".to_vec()));
        assert_eq!(transaction.get(b"key2".to_vec())?, Some(b"val3-2".to_vec()));
        assert_eq!(transaction.get(b"key3".to_vec())?, Some(b"val4-1".to_vec()));
        assert_eq!(transaction.get(b"key4".to_vec())?, Some(b"val5-1".to_vec()));
        Ok(())
    }

    #[test]
    fn test_set() -> Result<()> {
        set(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        set(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 6. set conflict
    fn set_conflict(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val3".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val4".to_vec())?;
        transaction.set(b"key4".to_vec(), b"val5".to_vec())?;
        transaction.commit()?;

        let mut transaction1 = mvcc.begin()?;
        let mut transaction2 = mvcc.begin()?;

        transaction1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        transaction1.set(b"key1".to_vec(), b"val1-2".to_vec())?;

        assert_eq!(
            transaction2.set(b"key1".to_vec(), b"val1-3".to_vec()),
            Err(Error::WriteConflict)
        );

        let mut transaction3 = mvcc.begin()?;
        transaction3.set(b"key5".to_vec(), b"val6".to_vec())?;
        transaction3.commit()?;

        assert_eq!(
            transaction1.set(b"key5".to_vec(), b"val6-1".to_vec()),
            Err(Error::WriteConflict)
        );

        transaction1.commit()?;
        Ok(())
    }

    #[test]
    fn test_set_conflict() -> Result<()> {
        set_conflict(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        set_conflict(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 7. delete
    fn delete(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val3".to_vec())?;
        transaction.delete(b"key2".to_vec())?;
        transaction.delete(b"key3".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val3-1".to_vec())?;
        transaction.commit()?;

        let transaction1 = mvcc.begin()?;
        assert_eq!(transaction1.get(b"key2".to_vec())?, None);

        let iter = transaction1.prefix_scan(b"ke".to_vec())?;
        assert_eq!(
            iter,
            vec![
                ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3-1".to_vec()
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn test_delete() -> Result<()> {
        delete(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        delete(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 8. delete conflict
    fn delete_conflict(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.commit()?;

        let mut transaction1 = mvcc.begin()?;
        let mut transaction2 = mvcc.begin()?;
        transaction1.delete(b"key1".to_vec())?;
        transaction1.set(b"key2".to_vec(), b"val2-1".to_vec())?;

        assert_eq!(
            transaction2.delete(b"key1".to_vec()),
            Err(Error::WriteConflict)
        );
        assert_eq!(
            transaction2.delete(b"key2".to_vec()),
            Err(Error::WriteConflict)
        );

        Ok(())
    }

    #[test]
    fn test_delete_conflict() -> Result<()> {
        delete_conflict(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        delete_conflict(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 9. dirty read
    fn dirty_read(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val3".to_vec())?;
        transaction.commit()?;

        let mut transaction1 = mvcc.begin()?;
        let mut transaction2 = mvcc.begin()?;

        transaction2.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        assert_eq!(transaction1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_dirty_read() -> Result<()> {
        dirty_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        dirty_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 10. unrepeatable read
    fn unrepeatable_read(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val3".to_vec())?;
        transaction.commit()?;

        let mut transaction1 = mvcc.begin()?;
        let mut transaction2 = mvcc.begin()?;

        transaction2.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        assert_eq!(transaction1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        transaction2.commit()?;
        assert_eq!(transaction1.get(b"key1".to_vec())?, Some(b"val1".to_vec()));

        Ok(())
    }

    #[test]
    fn test_unrepeatable_read() -> Result<()> {
        unrepeatable_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        unrepeatable_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 11. phantom read
    fn phantom_read(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val3".to_vec())?;
        transaction.commit()?;

        let mut transaction1 = mvcc.begin()?;
        let mut transaction2 = mvcc.begin()?;

        let iter1 = transaction1.prefix_scan(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec()
                },
                ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec()
                },
            ]
        );

        transaction2.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        transaction2.set(b"key4".to_vec(), b"val4".to_vec())?;
        transaction2.commit()?;

        let iter1 = transaction1.prefix_scan(b"key".to_vec())?;
        assert_eq!(
            iter1,
            vec![
                ScanResult {
                    key: b"key1".to_vec(),
                    value: b"val1".to_vec()
                },
                ScanResult {
                    key: b"key2".to_vec(),
                    value: b"val2".to_vec()
                },
                ScanResult {
                    key: b"key3".to_vec(),
                    value: b"val3".to_vec()
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn test_phantom_read() -> Result<()> {
        phantom_read(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        phantom_read(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    // 12. rollback
    fn rollback(eng: impl Engine) -> Result<()> {
        let mvcc = Mvcc::new(eng);
        let mut transaction = mvcc.begin()?;
        transaction.set(b"key1".to_vec(), b"val1".to_vec())?;
        transaction.set(b"key2".to_vec(), b"val2".to_vec())?;
        transaction.set(b"key3".to_vec(), b"val3".to_vec())?;
        transaction.commit()?;

        let mut transaction1 = mvcc.begin()?;
        transaction1.set(b"key1".to_vec(), b"val1-1".to_vec())?;
        transaction1.set(b"key2".to_vec(), b"val2-1".to_vec())?;
        transaction1.set(b"key3".to_vec(), b"val3-1".to_vec())?;
        transaction1.rollback()?;

        let transaction2 = mvcc.begin()?;
        assert_eq!(transaction2.get(b"key1".to_vec())?, Some(b"val1".to_vec()));
        assert_eq!(transaction2.get(b"key2".to_vec())?, Some(b"val2".to_vec()));
        assert_eq!(transaction2.get(b"key3".to_vec())?, Some(b"val3".to_vec()));

        Ok(())
    }

    #[test]
    fn test_rollback() -> Result<()> {
        rollback(MemoryEngine::new())?;
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        rollback(DiskEngine::new(p.clone())?)?;
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
}