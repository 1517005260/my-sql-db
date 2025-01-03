use std::cmp::Ordering::Equal;
use std::collections::HashMap;
use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::{Expression, OrderBy};
use crate::error::Error::Internal;
use crate::sql::parser::ast::OrderBy::Asc;

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

pub struct Projection<T: Transaction>{
    source: Box<dyn Executor<T>>,
    expressions: Vec<(Expression, Option<String>)>,
}

impl<T:Transaction> Projection<T>{
    pub fn new(source: Box<dyn Executor<T>>, expressions: Vec<(Expression, Option<String>)>) -> Box<Self>{
        Box::new(Self{ source, expressions })
    }
}

impl<T:Transaction> Executor<T> for Projection<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        match self.source.execute(transaction){
            Ok(ResultSet::Scan {columns, rows}) => {
                // 处理投影逻辑，我们需要根据expressions构建新的“表”
                let mut select_index = Vec::new(); // 选择的列的下标
                let mut new_columns = Vec::new();  // 选择的列

                for (expr, nick_name) in self.expressions{
                    if let Expression::Field(col_name) = expr{
                        // 找到col_name在原表中的下标
                        let position = match columns.iter().position(|c| *c == col_name){
                            Some(position) => position,
                            None => return Err(Internal(format!("[Executor] Projection column {} does not exist", col_name)))
                        };
                        select_index.push(position);
                        new_columns.push(if nick_name.is_some(){ nick_name.unwrap() } else { col_name});
                    };
                }

                // 根据选择的列，对每行内容进行过滤
                let mut new_rows = Vec::new();
                for row in rows{
                    let mut new_row = Vec::new();
                    for i in select_index.iter(){
                        new_row.push(row[*i].clone());
                    }
                    new_rows.push(new_row);
                }

                Ok(ResultSet::Scan { columns: new_columns, rows: new_rows })
            },
            _ => return Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string())),
        }
    }
}

pub struct Order<T: Transaction>{
    scan: Box<dyn Executor<T>>,
    order_by: Vec<(String, OrderBy)>
}

impl<T:Transaction> Order<T>{
    pub fn new(scan: Box<dyn Executor<T>>, order_by: Vec<(String, OrderBy)>) -> Box<Self>{
        Box::new(Self{ scan, order_by })
    }
}

impl<T:Transaction> Executor<T> for Order<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        // 首先和update一样，先需要拿到scan节点，否则报错
        match self.scan.execute(transaction){
            Ok(ResultSet::Scan {columns, mut rows}) => {
                // 处理排序逻辑
                // 首先我们要拿到排序列在整张表里的下标，比如有abcd四列，要对bd两列排序，下标就是b-1,d-3
                // 而在order by 的排序条件里，下标是 b-0,d-1 需要修改
                let mut order_col_index = HashMap::new();
                for (i, (col_name, _)) in self.order_by.iter().enumerate() {
                    // 这里需要判断，有可能用户指定的排序列不在表中，需要报错
                    match columns.iter().position(|c| *c == *col_name) {
                        Some(position) => order_col_index.insert(i, position),
                        None => return Err(Internal(format!("order by column {} is not in table", col_name))),
                    };
                }

                rows.sort_by(|row1, row2|{
                    for(i, (_, condition)) in self.order_by.iter().enumerate(){
                        let col_index = order_col_index.get(&i).unwrap();  // 拿到实际的表中列下标
                        let x = &row1[*col_index];  // row1_value
                        let y = &row2[*col_index];  // row2_value
                        match x.partial_cmp(y) {
                            Some(Equal) => continue,
                            Some(o) => return
                                if *condition == Asc {o}
                                else {o.reverse()},
                            None => continue,
                        }
                    }
                    Equal  // 其余情况认为相等
                });
                Ok(ResultSet::Scan { columns, rows })
            },
            _ => return Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string())),
        }
    }
}

pub struct Limit<T: Transaction>{
    source: Box<dyn Executor<T>>,
    limit: usize,
}

impl<T:Transaction> Limit<T> {
    pub fn new(source: Box<dyn Executor<T>>, limit: usize) -> Box<Self>{
        Box::new(Self{ source, limit })
    }
}

impl<T:Transaction> Executor<T> for Limit<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        match self.source.execute(transaction){
            Ok(ResultSet::Scan {columns, mut rows}) => {
                // 对输出的rows截断即可
                Ok(
                    ResultSet::Scan {
                        columns,
                        rows: rows.into_iter().take(self.limit).collect(),
                    }
                )
            },
            _ => return Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string())),
        }
    }
}

pub struct Offset<T: Transaction>{
    source: Box<dyn Executor<T>>,
    offset: usize,
}

impl<T:Transaction> Offset<T> {
    pub fn new(source: Box<dyn Executor<T>>, offset: usize) -> Box<Self>{
        Box::new(Self{ source, offset })
    }
}

impl<T:Transaction> Executor<T> for Offset<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        match self.source.execute(transaction){
            Ok(ResultSet::Scan {columns, mut rows}) => {
                // 对输出rows跳过即可
                Ok(
                    ResultSet::Scan {
                        columns,
                        rows: rows.into_iter().skip(self.offset).collect(),
                    }
                )
            },
            _ => return Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string())),
        }
    }
}