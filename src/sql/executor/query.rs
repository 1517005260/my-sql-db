use std::ops::RangeBounds;
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};

pub struct Scan{
    table_name: String,
}

impl Scan{
    pub fn new(table_name: String) -> Box<Self>{
        Box::new(Self{ table_name })
    }
}

impl<T:Transaction> Executor<T> for Scan{
    fn execute(&self,trasaction:&mut T) -> crate::error::Result<ResultSet> {
        todo!()
    }
}