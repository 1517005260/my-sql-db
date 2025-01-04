use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::error::{Result};
use crate::error::Error::Internal;

pub struct NestedLoopJoin<T:Transaction>{
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
}

impl<T:Transaction> NestedLoopJoin<T>{
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { left, right })
    }
}

impl<T:Transaction> Executor<T> for NestedLoopJoin<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        // 先扫描左表
        if let ResultSet::Scan {columns: left_cols, rows: left_rows} = self.left.execute(transaction)?{
            let mut new_rows = Vec::new();
            let mut new_columns = left_cols;
            // 再扫描右表
            if let ResultSet::Scan {columns: right_cols, rows: right_rows} = self.right.execute(transaction)? {
                // NestedLoopJoin 即遍历连接
                new_columns.extend(right_cols);

                for left_row in &left_rows{
                    for right_row in &right_rows{
                        let mut row = left_row.clone();
                        row.extend(right_row.clone());
                        new_rows.push(row);
                    }
                }
            }
            return Ok(ResultSet::Scan {columns: new_columns, rows: new_rows});
        }

        Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string()))
    }
}