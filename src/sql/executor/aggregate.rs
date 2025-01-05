use crate::error::Error::Internal;
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::sql::executor::calculate::Calculate;

pub struct Aggregate<T: Transaction> {
    source: Box<dyn Executor<T>>,
    expressions: Vec<(Expression, Option<String>)>,
}

impl<T: Transaction> Aggregate<T> {
    pub fn new( source: Box<dyn Executor<T>>, expressions: Vec<(Expression, Option<String>)>) -> Box<Self> {
        Box::new(Self { source, expressions })
    }
}

impl<T: Transaction> Executor<T> for Aggregate<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> crate::error::Result<ResultSet> {
        if let ResultSet::Scan {columns, rows} = self.source.execute(transaction)? {

            let mut new_rows = Vec::new();
            let mut new_cols = Vec::new();

            for(expr, nick_name) in self.expressions{
                if let Expression::Function(func_name, col_name) = expr {
                    let calculator = <dyn Calculate>::build(&func_name)?;
                    let value = calculator.calculate(&col_name, &columns, &rows)?;

                    new_cols.push(
                    if let Some(name) = nick_name {
                            name
                        } else { func_name }
                    );  // 没有别名，默认给agg函数名
                    new_rows.push(value);
                }
            }
            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: vec![new_rows],
            });
        }

        Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string()))
    }
}