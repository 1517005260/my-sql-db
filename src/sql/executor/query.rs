use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;

pub struct Scan{
    table_name: String,
    filter: Option<(String, Expression)>
}

impl Scan{
    pub fn new(table_name: String, filter: Option<(String, Expression)>) -> Box<Self>{
        Box::new(Self{ table_name, filter })
    }
}

impl<T:Transaction> Executor<T> for Scan{
    fn execute(self:Box<Self>,trasaction:&mut T) -> crate::error::Result<ResultSet> {
        let table = trasaction.must_get_table(self.table_name.clone())?;
        let rows = trasaction.scan(self.table_name.clone(), self.filter)?;
        Ok(
            ResultSet::Scan{
                columns: table.columns.into_iter().map(|c| c.name.clone()).collect(),
                rows,
            }
        )
    }
}