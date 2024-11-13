# 存储引擎的迭代器

首先来完善磁盘存储引擎的迭代器，这里的实现可以参考[内存存储的迭代器实现](../01-BasicStructure/07-MemStorage.md/#代码实现)。与内存的唯一不同就是磁盘需要向文件中先取出数据。

```rust
impl Engine for DiskEngine{
    type EngineIter<'a>= DiskEngineIter<'a>;
}

// 磁盘存储引擎的迭代器
pub struct DiskEngineIter<'a>{
    index:btree_map::Range<'a, Vec<u8>, (u64, u32)>,  // 范围迭代器, key | (offset, value-len)
    log: &'a mut Log,   // 需要从文件读取数据
}

impl<'a> DiskEngineIter<'a>{
    // self.index.next() 返回 Option<(&Vec<u8>, &(u64, u32))>
    fn iter_read_from_log(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (offset, value_len)) = item;
        let value = self.log.read_value(*offset, *value_len)?;
        Ok((key.clone(), value))
    }
}

impl<'a> EngineIter for DiskEngineIter<'a> {}

impl<'a> Iterator for DiskEngineIter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.index.next().map(|item| self.iter_read_from_log(item))
    }
}

impl<'a> DoubleEndedIterator for DiskEngineIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.index.next_back().map(|item| self.iter_read_from_log(item))
    }
}
```

有了迭代器之后，我们就可以完善扫描方法了：

```rust
impl Engine for DiskEngine{
    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIter<'_> {
        DiskEngineIter{
            index: self.key_dir.range(range),
            log: &mut self.log
        }
    }
}
```

调用 `scan` 方法的逻辑可以总结如下：

1. **调用 `scan` 方法**：当调用 `scan` 方法并传入一个 `range` 时，会生成一个新的 `DiskEngineIter` 迭代器，这个迭代器用于遍历 `self.key_dir` 中符合 `range` 范围的键值对。

2. **生成基于范围的迭代器**：
    - `self.key_dir.range(range)` 会生成一个 `BTreeMap` 的范围迭代器 `index`，该迭代器只会返回键在 `range` 内的所有键值对。

3. **`DiskEngineIter` 迭代器遍历**：
    - 生成的 `DiskEngineIter` 迭代器可以使用标准的 `Iterator` 方法，例如 `next` 和 `next_back`，来实现前向或后向遍历。
    - 每次调用 `next` 或 `next_back` 时，`DiskEngineIter` 会取到 `index` 中的下一个或上一个 `(key, (offset, value_len))`。

4. **基于自定义的 `map` 方法**：
    - `DiskEngineIter` 的 `next` 或 `next_back` 会调用 `map` 方法，该方法会解构 `(key, (offset, value_len))` 元组。
    - `map` 方法调用 `log.read_value(*offset, *value_len)` 来从磁盘文件中读取数据，返回 `(key.clone(), value)`。

5. **获取 `(key, value)`**：
    - 最终，每次迭代都会返回一个 `(key, value)`，其中 `key` 是当前项的键，而 `value` 是从 `log` 文件中读取的对应数据值。


# 存储引擎的测试

1. 在storage/engine.rs的测试模块中新增关于磁盘存储的通用测试：

```rust
#[test]
fn test_disk() -> Result<()> { 
   test_point_opt(DiskEngine::new(PathBuf::from("./tmp/sqldb1/db.log"))?)?;
    std::fs::remove_dir_all(PathBuf::from("./tmp/sqldb1"))?;  // 测试完成后删除

    test_scan(DiskEngine::new(PathBuf::from("./tmp/sqldb2/db.log"))?)?;
    std::fs::remove_dir_all(PathBuf::from("./tmp/sqldb2"))?;

    test_scan_prefix(DiskEngine::new(PathBuf::from("./tmp/sqldb3/db.log"))?)?;
    std::fs::remove_dir_all(PathBuf::from("./tmp/sqldb3"))?;
    Ok(())
}
```

2. 在disk.rs中新建单独的compact逻辑测试：

```rust
#[cfg(test)]
mod tests {
    use crate::{
        error::Result,
        storage::{disk::DiskEngine, engine::Engine},
    };
    use std::path::PathBuf;

    #[test]
    fn test_disk_engine_start() -> Result<()> {
        let eng = DiskEngine::new(PathBuf::from("./tmp/sqldb-log"))?;
        Ok(())
    }

    #[test]
    fn test_disk_engine_compact_1() -> Result<()> {
        let eng = DiskEngine::new_compact(PathBuf::from("./tmp/sqldb-log"))?;
        Ok(())
    }

    #[test]
    fn test_disk_engine_compact_2() -> Result<()> {
        let mut eng = DiskEngine::new(PathBuf::from("./tmp/sqldb/sqldb-log"))?;
        // 写一些数据
        eng.set(b"key1".to_vec(), b"value".to_vec())?;
        eng.set(b"key2".to_vec(), b"value".to_vec())?;
        eng.set(b"key3".to_vec(), b"value".to_vec())?;
        eng.delete(b"key1".to_vec())?;
        eng.delete(b"key2".to_vec())?;

        // 重写
        eng.set(b"aa".to_vec(), b"value1".to_vec())?;
        eng.set(b"aa".to_vec(), b"value2".to_vec())?;
        eng.set(b"aa".to_vec(), b"value3".to_vec())?;
        eng.set(b"bb".to_vec(), b"value4".to_vec())?;
        eng.set(b"bb".to_vec(), b"value5".to_vec())?;

        let iter = eng.scan(..);
        let v = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng);  // 结束eng的生命周期，释放排他锁

        let mut eng2 = DiskEngine::new_compact(PathBuf::from("./tmp/sqldb/sqldb-log"))?;
        let iter2 = eng2.scan(..);
        let v2 = iter2.collect::<Result<Vec<_>>>()?;
        assert_eq!(
            v2,
            vec![
                (b"aa".to_vec(), b"value3".to_vec()),
                (b"bb".to_vec(), b"value5".to_vec()),
                (b"key3".to_vec(), b"value".to_vec()),
            ]
        );
        drop(eng2);

        std::fs::remove_dir_all("./tmp/sqldb")?;

        Ok(())
    }
}
```