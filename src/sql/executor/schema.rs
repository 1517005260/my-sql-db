use crate::sql::executor::{Executor, ResultSet};
use crate::sql::schema::Table;

pub struct CreateTable{
    schema: Table,
}

impl CreateTable{
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self {schema})
    }
}

impl Executor for CreateTable{
    fn execute(&self) -> crate::error::Result<ResultSet> {
        todo!()  // 具体逻辑等待存储引擎构建完成后再写
    }
}