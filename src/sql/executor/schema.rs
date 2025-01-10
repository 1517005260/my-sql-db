use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::schema::Table;

pub struct CreateTable {
    schema: Table,
}

impl CreateTable {
    pub fn new(schema: Table) -> Box<Self> {
        Box::new(Self { schema })
    }
}

impl<T: Transaction> Executor<T> for CreateTable {
    fn execute(self: Box<Self>, transaction: &mut T) -> crate::error::Result<ResultSet> {
        let table_name = self.schema.name.clone();
        transaction.create_table(self.schema)?;
        Ok(ResultSet::CreateTable { table_name })
    }
}

pub struct DropTable {
    name: String,
}

impl DropTable {
    pub fn new(name: String) -> Box<Self> {
        Box::new(Self { name })
    }
}

impl<T: Transaction> Executor<T> for DropTable {
    fn execute(self: Box<Self>, transaction: &mut T) -> crate::error::Result<ResultSet> {
        transaction.drop_table(self.name.clone())?;
        Ok(ResultSet::DropTable {
            table_name: self.name,
        })
    }
}
