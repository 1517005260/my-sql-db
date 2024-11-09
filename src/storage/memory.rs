use std::collections::{btree_map, BTreeMap};
use std::ops::RangeBounds;
use crate::error::Result;
use crate::storage::engine::{Engine, EngineIter};

// 内存存储引擎，即 ./engine.rs 的具体实现，使用BTreeMap
pub struct MemoryEngine{
    data: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MemoryEngine{
    pub fn new() -> Self{
        Self{
            data: BTreeMap::new(),
        }
    }
}

impl Engine for MemoryEngine{
    type EngineIter<'a> = MemoryEngineIter<'a>;

    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.data.insert(key, value);
        Ok(())
    }

    fn get(&mut self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let val = self.data.get(&key).cloned();
        Ok(val)
    }

    fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        self.data.remove(&key);
        Ok(())
    }

    fn scan(&mut self, range: impl RangeBounds<Vec<u8>>) -> Self::EngineIter<'_> {
        MemoryEngineIter{
            item: self.data.range(range),
        }
    }
}

// 内存存储引擎迭代器，可以直接使用B-Tree的内置方法
pub struct MemoryEngineIter<'a>{
    item: btree_map::Range<'a, Vec<u8>, Vec<u8>>,  // 引用了B树的键值对，至少是两个引用，所以需要生命周期
}

impl<'a> EngineIter for MemoryEngineIter<'a>{
    // 因为继承自DoubleEndedIterator接口，所以根据编译器提示需要实现next, next_back
}

impl<'a> MemoryEngineIter<'a>{
    // 手动将option转换为result
    fn map(item:(&Vec<u8>,&Vec<u8>)) -> <Self as Iterator>::Item {  // 这里的Item指的是 type Item = Result<(Vec<u8>, Vec<u8>)>;
        let (k,v) = item;
        Ok((k.clone(), v.clone()))
    }
}

// 支持向前遍历
impl<'a> Iterator for MemoryEngineIter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;  // 为了兼容后续磁盘，磁盘可能报err，所以这里迭代器用Result包裹

    fn next(&mut self) -> Option<Self::Item> {
        // 这里考虑直接用b树的迭代器，但是迭代器返回的是option，需要我们手动转换为result
        self.item.next().map(|tuple| Self::map(tuple))
    }
}

// 支持双向遍历
impl<'a> DoubleEndedIterator for MemoryEngineIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {  // 向前遍历
        self.item.next_back().map(|tuple| Self::map(tuple))
    }
}

