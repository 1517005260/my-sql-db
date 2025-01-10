use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use std::marker::PhantomData;

pub struct TableSchema<T: Transaction> {
    name: String,
    _marker: PhantomData<T>, // 通过添加 _marker: PhantomData<T>，我们告诉编译器该结构体实际上与 T 相关联，尽管它不直接使用 T
}

impl<T: Transaction> TableSchema<T> {
    pub fn new(name: &str) -> Box<Self> {
        Box::new(TableSchema {
            name: name.into(),
            _marker: PhantomData,
        })
    }
}

impl<T: Transaction> Executor<T> for TableSchema<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        let table = transaction.must_get_table(self.name.clone())?;
        let schema = table.to_string();

        Ok(ResultSet::TableSchema { schema })
    }
}

pub struct TableNames<T: Transaction> {
    _marker: PhantomData<T>,
}

impl<T: Transaction> TableNames<T> {
    pub fn new() -> Box<Self> {
        Box::new(TableNames {
            _marker: PhantomData,
        })
    }
}

impl<T: Transaction> Executor<T> for TableNames<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        let names = transaction.get_all_table_names()?;
        Ok(ResultSet::TableNames { names })
    }
}
