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
    fn execute(self:Box<Self>,trasaction:&mut T) -> crate::error::Result<ResultSet> {
        let table = trasaction.must_get_table(self.table_name.clone())?;
        let rows = trasaction.scan(self.table_name.clone())?;
        Ok(
            ResultSet::Scan{
                columns: table.columns.into_iter().map(|c| c.name.clone()).collect(),
                rows,
            }
        )
    }
}