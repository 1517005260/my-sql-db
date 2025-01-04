use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::error::{Result};
use crate::error::Error::Internal;
use crate::sql::parser::ast;
use crate::sql::parser::ast::Expression;
use crate::sql::types::Value;

pub struct NestedLoopJoin<T:Transaction>{
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    condition: Option<Expression>,
    outer: bool,
}

impl<T:Transaction> NestedLoopJoin<T>{
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>, condition: Option<Expression>, outer: bool) -> Box<Self> {
        Box::new(Self { left, right, condition, outer})
    }
}

impl<T:Transaction> Executor<T> for NestedLoopJoin<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        // 先扫描左表
        if let ResultSet::Scan {columns: left_cols, rows: left_rows} = self.left.execute(transaction)?{
            let mut new_rows = Vec::new();
            let mut new_columns = left_cols.clone();
            // 再扫描右表
            if let ResultSet::Scan {columns: right_cols, rows: right_rows} = self.right.execute(transaction)? {
                // NestedLoopJoin 即遍历连接
                new_columns.extend(right_cols.clone());

                for left_row in &left_rows{
                    let mut flag = false; // 表示左表的数据是否在右表匹配到
                    for right_row in &right_rows{
                        let mut row = left_row.clone();

                        // 如果有Join条件，需要查看是否满足条件，否则不予连接
                        if let Some(condition) = &self.condition{
                            match parse_expression(condition, &left_cols, left_row, &right_cols, right_row)? {
                                Value::Null => continue,  // 本次连接不匹配
                                Value::Boolean(false) => continue,
                                Value::Boolean(true) =>{
                                    // 可以连接
                                    flag = true;
                                    row.extend(right_row.clone());
                                    new_rows.push(row);
                                },
                                _ => return Err(Internal("[Executor] Unexpected expression".to_string()))
                            }
                        }else { // cross join
                            row.extend(right_row.clone());
                            new_rows.push(row);
                        }
                    }
                    // outer join 需要显示左表所有数据
                    if self.outer && flag==false {
                        let mut row = left_row.clone();
                        for _ in 0..right_rows[0].len() {
                            row.push(Value::Null);
                        }
                        new_rows.push(row);
                    }
                }
            }
            return Ok(ResultSet::Scan {columns: new_columns, rows: new_rows});
        }

        Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string()))
    }
}

// 解析表达式，看列是否相等，满足Join条件
fn parse_expression(expr: &Expression,
                    left_cols: &Vec<String>, left_row: &Vec<Value>,
                    right_cols: &Vec<String>, right_row: &Vec<Value>) -> Result<Value> {
    match expr {
        Expression::Field(col_name) => {
            // 根据列名，取对应行的数据
            let pos = match left_cols.iter().position(|col| *col == *col_name){
                Some(pos) => pos,
                None => return Err(Internal(format!("[Executor] Column {} does not exist", col_name))),
            };
            Ok(left_row[pos].clone())
        },
        Expression::Operation(operation) =>{
            match operation {
                ast::Operation::Equal(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;

                    // 取到两张表同名列的值，如果相等则可以连接
                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l == r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l == r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 == r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l == r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l == r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l == r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                }
            }
        },
        _ => return Err(Internal(format!("[Executor] Unexpected Expression {:?}", expr)))
    }
}