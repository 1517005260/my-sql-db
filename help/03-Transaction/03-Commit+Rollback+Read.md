# 事务的提交、回滚和读取

## Commit

事务的提交：提交完成后，效果对**后续开启**的所有事务均可见，当前运行的事务仍不可见。

由于我们的事务写入了[一些多余数据](./02-Begin+Write.md/#磁盘存储了什么)，我们这里只要保留**版本化键值对**即可。

1. 将本事务的版本号从活跃事务列表中清除，即删除：MvccKey::ActiveTransactions(Version)
2. 将本事务的写记录删除，即删除：MvccKey::Write(Version, Vec<u8>)
3. 保留MvccKey::Version(Vec<u8>, Version)，即保留{{key,version},value}的磁盘键值对，并让其正式对后来事务生效

### 代码实现

在mvcc.rs中新增：

```rust
#[derive(Clone, Serialize, Deserialize)]
pub enum MvccKeyPrefix{  // MvccKey的前缀，用于扫描活跃事务
    NextVersion,   // 版本号前缀
    ActiveTransactions, // 活跃事务前缀
    Write(Version),    // 事务写信息前缀
}

impl<E:Engine> MvccTransaction<E> {
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
}
```

## Rollback

思路同Commit，但是这里连版本化键值对都要删除

### 代码实现

在mvcc.rs中：

```rust
impl<E:Engine> MvccTransaction<E> {
    pub fn rollback(&self) -> Result<()> {
        // 1. 获取存储引擎
        let mut engine = self.engine.lock()?;
        // 2. 获取事务写信息并删除
        let mut keys_to_be_deleted = Vec::new();
        let mut iter = engine.prefix_scan(MvccKeyPrefix::Write(self.state.version).encode());
        while let Some((key, _)) = iter.next().transpose()?{
            // 这里比commit多一步删除写入log的真实数据
            match MvccKey::decode(key.clone())? {
                MvccKey::Write(_, raw_key) => {  // 这里找到的是不含版本信息的key
                    // 构造带版本信息的key
                    keys_to_be_deleted.push(MvccKey::Version(raw_key, self.state.version).encode());
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
        engine.delete(MvccKey::ActiveTransactions(self.state.version).encode())
    }
}
```

## 读取Get

在当前事务中，根据可见性条件判断，返回第一个符合条件的（版本号最大的）value。例如，当前的事务版本是9，则扫描的范围是0-9，后面发现数据有版本2，4，8，我们最终选择版本8。

### 代码实现

在mvcc.rs中：

```rust
impl<E:Engine> MvccTransaction<E> {
    pub fn get(&self, key:Vec<u8>) -> Result<Option<Vec<u8>>> {
        // 1. 获取存储引擎
        let mut engine = self.engine.lock()?;
        // 2. 判断数据是否符合条件
        let from = MvccKey::Version(key.clone(), 0).encode();
        let to = MvccKey::Version(key.clone(), self.state.version).encode();
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
}
```

## 验证ACID

- 原子性：事务一旦提交，后续内容一定可见、并且是持久化到磁盘中的。事务回滚，其所作的修改都会被删除，保证该事务的修改不会生效。
- 隔离性：基于快照隔离，事务的运行不会受其它事务干扰。
- 持久性：数据的修改直接存储在了磁盘文件中，就算数据库异常重启、崩溃等，修改仍然是有效的。
- 一致性：原子性、隔离性、持久性都满足。

常见并发问题：
- 脏读：其它正在运行中的事务修改之后，对当前事务不可见，所以没有脏读问题。
- 不可重复读：一个事务提交之后，其修改仍然对当前事务不可见，所以可重复读。
- 幻读：根据版本号判断机制，一个事务就算修改数据且提交，只要其版本号比当前事务大，那么其修改不可见，所以没有幻读问题。