use crate::error::Error::Internal;
use crate::error::{Error, Result};
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::Expression;
use crate::sql::schema::Table;
use crate::sql::types::{Row, Value};
use std::collections::{BTreeMap, HashMap};

pub struct Insert {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Expression>>,
}

impl Insert {
    pub fn new(
        table_name: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            columns,
            values,
        })
    }
}

impl<T: Transaction> Executor<T> for Insert {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        // 插入表之前，表必须是存在的
        let table = transaction.must_get_table(self.table_name.clone())?;

        // ResultSet成功结果返回插入行数
        let mut count = 0;

        // 现在手上表的数据类型是values:Vec<Vec<Expression>>,我们需要进行一些操作
        for exprs in self.values {
            // 1. 先将 Vec<Expression> 转换为 Row，即Vec<Value>
            let row = exprs
                .into_iter()
                .map(|e| Value::from_expression_to_value(e))
                .collect::<Vec<Value>>();

            // 2. 可选项：是否指定了插入的列
            let insert_row = if self.columns.is_empty() {
                // 未指定插入列
                complete_row(&table, &row)?
            } else {
                // 指定插入列
                modify_row(&table, &self.columns, &row)?
            };
            transaction.create_row(self.table_name.clone(), insert_row)?;
            count += 1;
        }
        Ok(ResultSet::Insert { count })
    }
}

// 辅助判断方法
// 1. 补全列，即列对齐
fn complete_row(table: &Table, row: &Row) -> Result<Row> {
    let mut res = row.clone();
    for column in table.columns.iter().skip(row.len()) {
        // 跳过已经给定数据的列
        if let Some(default) = &column.default {
            // 有默认值
            res.push(default.clone());
        } else {
            // 建表时没有默认值但是insert时又没给数据
            return Err(Error::Internal(format!(
                "[Insert Table] Column \" {} \" has no default value",
                column.name
            )));
        }
    }
    Ok(res)
}

// 2. 调整列信息并补全
fn modify_row(table: &Table, columns: &Vec<String>, values: &Row) -> Result<Row> {
    // 首先先判断给的列数和values的数量是否是一致的：
    if columns.len() != values.len() {
        return Err(Error::Internal(
            "[Insert Table] Mismatch num of columns and values".to_string(),
        ));
    }

    // 有可能顺序是乱的，但是返回时顺序不能乱，这里考虑使用hash
    let mut inputs = HashMap::new();
    for (i, col_name) in columns.iter().enumerate() {
        // enumerate()用于为迭代中的每个元素附加一个索引值
        inputs.insert(col_name, values[i].clone());
    }

    // 现在inputs就是顺序正常的插入行，之后和complete_row()思路差不多了
    let mut res = Vec::new();
    for col in table.columns.iter() {
        if let Some(value) = inputs.get(&col.name) {
            res.push(value.clone());
        } else if let Some(default) = &col.default {
            res.push(default.clone());
        } else {
            return Err(Error::Internal(format!(
                "[Insert Table] Column \" {} \" has no default value",
                col.name
            )));
        }
    }

    Ok(res)
}

pub struct Update<T: Transaction> {
    table_name: String,
    scan: Box<dyn Executor<T>>, // scan 是一个执行节点，这里是递归的定义。执行节点又是Executor<T>接口的实现，在编译期不知道类型，需要Box包裹
    columns: BTreeMap<String, Expression>,
}

impl<T: Transaction> Update<T> {
    pub fn new(
        table_name: String,
        scan: Box<dyn Executor<T>>,
        columns: BTreeMap<String, Expression>,
    ) -> Box<Self> {
        Box::new(Self {
            table_name,
            scan,
            columns,
        })
    }
}

impl<T: Transaction> Executor<T> for Update<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        let mut count = 0;
        // 先获取到扫描的结果，这是我们需要更新的数据
        match self.scan.execute(transaction)? {
            ResultSet::Scan { columns, rows } => {
                // 处理更新流程
                let table = transaction.must_get_table(self.table_name.clone())?;
                // 遍历每行，更新列数据
                for row in rows {
                    let mut new_row = row.clone();
                    let primary_key = table.get_primary_key(&row)?;
                    for (i, col) in columns.iter().enumerate() {
                        if let Some(expression) = self.columns.get(col) {
                            // 如果本列需要修改
                            new_row[i] = Value::from_expression_to_value(expression.clone());
                        }
                    }
                    // 如果涉及了主键的更新，由于我们存储时用的是表名和主键一起作为key，所以这里需要删了重新建key
                    // 否则，key部分(table_name, primary_key) 不动，直接变value即可
                    transaction.update_row(&table, &primary_key, new_row)?;
                    count += 1;
                }
            }
            _ => {
                return Err(Internal(
                    "[Executor] Unexpected ResultSet, expected Scan Node".to_string(),
                ))
            }
        }

        Ok(ResultSet::Update { count })
    }
}

pub struct Delete<T: Transaction> {
    table_name: String,
    scan: Box<dyn Executor<T>>,
}

impl<T: Transaction> Delete<T> {
    pub fn new(table_name: String, scan: Box<dyn Executor<T>>) -> Box<Self> {
        Box::new(Self { table_name, scan })
    }
}

impl<T: Transaction> Executor<T> for Delete<T> {
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        let mut count = 0;
        match self.scan.execute(transaction)? {
            ResultSet::Scan { columns: _, rows } => {
                // columns 参数未用到
                let table = transaction.must_get_table(self.table_name)?;
                for row in rows {
                    // 删除行，而行定位的key为(table_name, primary_key)，所以还需要主键
                    let primary_key = table.get_primary_key(&row)?;
                    transaction.delete_row(&table, &primary_key)?;
                    count += 1;
                }
                Ok(ResultSet::Delete { count })
            }
            _ => Err(Internal(
                "[Executor] Unexpected ResultSet, expected Scan Node".to_string(),
            )),
        }
    }
}
