pub mod engine;
pub mod memory;

use crate::error::Result;
pub struct Mvcc{
    // 多版本并发控制，Multi-Version Concurrency Control
}

impl Clone for Mvcc {  // 顶层支持多个所有者，所以需要实现clone方法
    fn clone(&self) -> Mvcc {
        Self{}
    }
}

impl Mvcc {
    pub fn new() -> Self { Self{} }

    pub fn begin(&self) -> Result<MvccTransaction>{   // 开启事务
        Ok(MvccTransaction::new())
    }
}

pub struct MvccTransaction{
    // 代表一个具体的事务
}
impl MvccTransaction {
    pub fn new() -> Self { Self{} }
}