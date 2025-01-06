use std::collections::HashMap;
use crate::error::Error::Internal;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::sql::executor::calculate::Calculate;
use crate::sql::types::{Row, Value};

pub struct Aggregate<T: Transaction> {
    source: Box<dyn Executor<T>>,
    expressions: Vec<(Expression, Option<String>)>,
    group_by: Option<Expression>,
}

impl<T: Transaction> Aggregate<T> {
    pub fn new( source: Box<dyn Executor<T>>, expressions: Vec<(Expression, Option<String>)>, group_by: Option<Expression>) -> Box<Self> {
        Box::new(Self { source, expressions, group_by})
    }
}

impl<T: Transaction> Executor<T> for Aggregate<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        if let ResultSet::Scan {columns, rows} = self.source.execute(transaction)? {

            let mut new_rows = Vec::new();
            let mut new_cols = Vec::new();

            // 为了方便，我们将之前计算聚集函数的过程写为一个闭包函数，供本execute方法内调用
            let mut calc = |col_value: Option<&Value>, rows: &Vec<Row>| -> Result<Row>{

                let mut new_row = Vec::new();

                for(expr, nick_name) in &self.expressions{
                    match expr {
                        Expression::Function(func_name, col_name) => {  // 聚集函数
                            let calculator = <dyn Calculate>::build(&func_name)?;
                            let value = calculator.calculate(&col_name, &columns, rows)?;

                            if new_cols.len() < self.expressions.len() {  // 这里需要限制输出的列以select表达式的长度为限
                                new_cols.push(
                                    if let Some(name) = nick_name { name.clone() } else { func_name.clone() }
                                );  // 没有别名，默认给agg函数名
                            }
                            new_row.push(value);
                        },
                        Expression::Field(col_name) => {  // group by的列名
                            // 需要判断，不可以 select c2 , min(c1) from t group by c3;
                            if let Some(Expression::Field(group_col)) = &self.group_by{
                                if *group_col != *col_name{
                                    return Err(Internal(format!("[Executor] Column {} must appear in GROUP BY or Aggregate function", col_name)))
                                }
                            }

                            if new_cols.len() < self.expressions.len() {
                                new_cols.push(
                                    if let Some(name) = nick_name { name.clone() } else { col_name.clone() }
                                );
                            }
                            new_row.push(col_value.unwrap().clone());

                        },
                        _ =>return Err(Internal("[Executor] Aggregate unexpected expression".into())),
                    }
                }
                Ok(new_row)
            };

            // 有无group by是两套不同的处理逻辑
            if let Some(Expression::Field(col_name)) = &self.group_by{
                // 有group by，则需要对数据进行分组，并进行每组的统计
                let pos = match columns.iter().position(|c| *c == *col_name) {
                    Some(pos) => pos,
                    None => return Err(Internal(format!("The group by column {} does not exist", col_name)))
                };

                // 创建hash map存储每个分组中不同的数据
                let mut groups = HashMap::new();
                for row in rows.iter(){
                    let key = &row[pos];
                    let value = groups.entry(key).or_insert(Vec::new());
                    value.push(row.clone());
                }

                // 进行计算
                for(key, row) in groups{
                    let row = calc(Some(key), &row)?;
                    new_rows.push(row);
                }
            }else {
                // 无group by，即直接计算agg，不需要分组
                let row = calc(None, &rows)?;
                new_rows.push(row);
            }

            return Ok(ResultSet::Scan {
                columns: new_cols,
                rows: new_rows,
            });
        }

        Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string()))
    }
}