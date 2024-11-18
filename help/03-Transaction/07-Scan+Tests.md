# 数据库事务的数据扫描

与get方法类似：

```rust
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
```

当我们对事务Key进行编码后，存入数据的前缀就能够匹配起来了，我们将扫描的数据暂存如BTreeMap中，这样即能保证有序，又能保证在事务可见范围下，获取到的key是不重复且最新的。

## 代码实现

mvcc.rs:

之前我们直接：`let mut iter = eng.prefix_scan(prefix);`，这样是不对的，现在有了自定义编码后再来修改这个函数：


```rust
impl<E:Engine> MvccTransaction<E> {
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
```

这里需要注意，我们需要去掉最后的 [0, 0] 后缀，否则前缀编码后和原始值编码后不匹配：

```
原始值           编码后
97 98 99     -> 97 98 99 0 0
前缀原始值        前缀编码后
97 98        -> 97 98 0 0    -> 97 98
```

# 数据库事务测试

- Get: 普通情况下获取数据，如果数据有多个版本，应该能够拿到最新的版本
- Get Isolation: 多个事务运行，当前事务不能看见其他事务的修改，也不能看见 version 比它大的事务的修改
- Scan: 普通扫描
- Scan Isolation: 多个事务的扫描
- Set: 普通存放数据
- Set Conflict: 如果当前事务修改的 key 已经被其他事务修改过了，则写入冲突
- Delete: 删除数据
- Delete Conflict: 和 set conflict 类似
- Dirty read: 脏读，当前事务不能看到其他未提交事务的修改
- Unrepeatable read: 不可重复读，当前事务不能看到其他已提交事务的修改（并发事务或者未来的事务）
- Phantom read: 幻读，当前事务的读取区间内，其他事务插入了数据，对当前事务仍然不可见
- Rollback: 事务回滚，其修改被撤销，不能被任何其他的事务看到

## 代码实现

这里用到了第三方库：

```toml
tempfile = "3.12.0"
```

测试代码：

```rust
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
```