use std::collections::HashMap;
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::error::{Result};
use crate::error::Error::Internal;
use crate::sql::parser::ast::{parse_expression, Expression, Operation};
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
                        for _ in 0..right_cols.len() {
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

pub struct HashJoin<T:Transaction>{
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    condition: Option<Expression>,
    outer: bool,
}

impl<T:Transaction> HashJoin<T> {
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>, condition: Option<Expression>, outer: bool) -> Box<Self> {
        Box::new(Self { left, right, condition, outer})
    }
}

impl<T:Transaction> Executor<T> for HashJoin<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        // 先扫描左表
        if let ResultSet::Scan {columns: left_cols, rows: left_rows} = self.left.execute(transaction)?{
            let mut new_rows = Vec::new();
            let mut new_cols = left_cols.clone();
            // 再扫描右表
            if let ResultSet::Scan {columns: right_cols, rows: right_rows} = self.right.execute(transaction)? {

                new_cols.extend(right_cols.clone());

                // 解析HashJoin条件，即拿到左右两列的列名
                let (lcol, rcol) = match parse_join_condition(self.condition) {
                    Some(res) => res,
                    None => return Err(Internal("[Executor] Failed to parse join condition, please recheck column names".into())),
                };

                // 拿到连接列在表中的位置
                let left_pos = match left_cols.iter().position(|c| *c == lcol) {
                    Some(pos) => pos,
                    None => return Err(Internal(format!("[Executor] Column {} does not exist", lcol)))
                };

                let right_pos = match right_cols.iter().position(|c| *c == rcol) {
                    Some(pos) => pos,
                    None => return Err(Internal(format!("[Executor] Column {} does not exist", rcol)))
                };

                // 构建hash表（右），key 为 连接列的值， value为对应的一行数据
                // 可能一个key有不止一行数据，所以用列表存
                let mut map = HashMap::new();
                for row in &right_rows{
                    let rows = map.entry(row[right_pos].clone()).or_insert(Vec::new());
                    rows.push(row.clone());
                }

                // 扫描左表进行匹配
                for row in left_rows{
                    match map.get(&row[left_pos]) {  // 尝试与右表数据匹配
                        Some(rows) => {
                            for a_row in rows{
                                let mut row = row.clone();
                                row.extend(a_row.clone());
                                new_rows.push(row);
                            }
                        },
                        None => {
                            // 未匹配到，如果是外连接需要展示为null
                            if self.outer{
                                let mut row = row.clone();
                                for _ in 0..right_cols.len() {
                                    row.push(Value::Null);
                                }
                                new_rows.push(row);
                            }
                        },
                    }
                }
                return Ok(ResultSet::Scan {columns: new_cols, rows: new_rows});
            }
        }

        Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string()))
    }
}

// 解析join条件，获取左右两列
// 思路和index的条件判断一致
fn parse_join_condition(condition: Option<Expression>) -> Option<(String, String)>{
    match condition {
        Some(expr) => {
            match expr {
                // 解析列名
                Expression::Field(col) => Some((col, "".into())),
                Expression::Operation(operation) => {
                    match operation {
                        Operation::Equal(col1, col2) => {
                            // 递归调用进行解析
                            let left = parse_join_condition(Some(*col1));
                            let right = parse_join_condition(Some(*col2));

                            // 左右均为为(col, "")，现在进行组合
                            Some((left.unwrap().0, right.unwrap().0))
                        },
                        _ => None,
                    }
                },
                _ => None,
            }
        },
        None => None,
    }
}