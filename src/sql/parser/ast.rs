use std::collections::BTreeMap;
use crate::error::Error::Internal;
use crate::sql::types::{DataType, Value};
// 本模块是抽象语法树的定义


// 列定义
#[derive(Debug,PartialEq)]
pub struct Column{            // 列的各种属性
    pub name: String,         // 列名
    pub datatype: DataType,   // 列数据类型
    pub nullable: Option<bool>, // 列是否为空
    pub default: Option<Expression>, // 列的默认值
    pub is_primary_key: bool,       // 本列是否为主键
    pub is_index: bool,             // 本列是否为索引
}

// 目前表达式为了简单，仅支持常量，不支持：insert into Table_A value(11 * 11 + 2) 等
// 更新：select的列名算作Expression
// 更新：join的条件——列相等算作Expression
// 更新：聚集函数算作表达式
#[derive(Debug,PartialEq,Clone)]
pub enum Expression{
    Consts(Consts),
    Field(String),
    Operation(Operation),
    Function(String, String),
}

// join的类型定义
#[derive(Debug,PartialEq,Clone)]
pub enum JoinType{
    Cross,
    Inner,
    Left,
    Right,
}

// from_item的定义，可以是表或者表的连接
#[derive(Debug,PartialEq,Clone)]
pub enum FromItem{
    Table{
        name: String,
    },
    Join{
        left: Box<FromItem>,  // 左表
        right: Box<FromItem>, // 右表
        join_type: JoinType,  // 连接类型
        condition: Option<Expression>, // 连接条件
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Consts{
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

// 排序抽象语法
#[derive(Debug, PartialEq, Clone)]
pub enum OrderBy{
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Operation{
    Equal(Box<Expression>, Box<Expression>),
    Greater(Box<Expression>, Box<Expression>),  // a > b，下同
    GreaterEqual(Box<Expression>, Box<Expression>),
    Less(Box<Expression>, Box<Expression>),
    LessEqual(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),
}

// 定义 Consts -> Expression 的类型转换
impl From<Consts> for Expression{
    fn from(c: Consts) -> Self{
        Self::Consts(c)
    }
}

// sql 语句的定义
#[derive(Debug,PartialEq)]
pub enum Sentence{
    CreateTable{
        name: String,               // 表名
        columns: Vec<Column>,       // 表的列
    },
    DropTable{
        name: String,
    },
    Insert{
        table_name: String,           // 目标表名
        columns: Option<Vec<String>>,  // 目标列，可以为空
        values: Vec<Vec<Expression>>,   // 插入数据，是个二维数组
    },
    Select{
        select_condition: Vec<(Expression, Option<String>)>,  // 列名，可选的别名
        from_item: FromItem,
        where_condition: Option<Expression>,
        group_by: Option<Expression>,
        having: Option<Expression>,
        order_by: Vec<(String, OrderBy)>, // 例如，order by col_a desc
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    Update{
        table_name: String,
        columns: BTreeMap<String, Expression>,
        condition: Option<Expression>
    },
    Delete{
        table_name: String,
        condition: Option<Expression>,
    },
    TableSchema{
        table_name: String,
    },
    TableNames{
        // 没有参数，因为是全体表
    },
    Begin{
        //  没有参数，因为事务号是底层mvcc自动增加的
    },
    Commit{
    },
    Rollback{
    },
}

// 解析表达式
pub fn parse_expression(expr: &Expression,
                    left_cols: &Vec<String>, left_row: &Vec<Value>,
                    right_cols: &Vec<String>, right_row: &Vec<Value>) -> crate::error::Result<Value> {
    match expr {
        Expression::Field(col_name) => {
            // 根据列名，取对应行的数据
            let pos = match left_cols.iter().position(|col| *col == *col_name){
                Some(pos) => pos,
                None => return Err(Internal(format!("[Executor] Column {} does not exist", col_name))),
            };
            Ok(left_row[pos].clone())
        },
        Expression::Consts(c) => {
            // 解析诸如 a = 3 中的常量
            let value = match c {
                Consts::Null => Value::Null,
                Consts::Boolean(v) => Value::Boolean(*v),
                Consts::Integer(v) => Value::Integer(*v),
                Consts::Float(v) => Value::Float(*v),
                Consts::String(v) => Value::String(v.clone()),
            };
            Ok(value)
        },
        Expression::Operation(operation) =>{
            match operation {
                Operation::Equal(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;

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
                },
                Operation::Greater(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;

                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l > r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l > r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 > r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l > r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l > r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l > r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
                Operation::GreaterEqual(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;

                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l >= r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l >= r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 >= r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l >= r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l >= r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l >= r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
                Operation::Less(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;

                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l < r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l < r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean((l as f64) < r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l < r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l < r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l < r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
                Operation::LessEqual(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;

                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l <= r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l <= r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 <= r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l <= r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l <= r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l <= r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
                Operation::NotEqual(left_expr, right_expr) =>{
                    let left_value = parse_expression(&left_expr, left_cols, left_row, right_cols, right_row)?;
                    let right_value = parse_expression(&right_expr, right_cols, right_row, left_cols, left_row)?;

                    Ok(match (left_value, right_value) {
                        (Value::Boolean(l), Value::Boolean(r)) => Value::Boolean(l != r),
                        (Value::Integer(l), Value::Integer(r)) => Value::Boolean(l != r),
                        (Value::Integer(l), Value::Float(r)) => Value::Boolean(l as f64 != r),
                        (Value::Float(l), Value::Integer(r)) => Value::Boolean(l != r as f64),
                        (Value::Float(l), Value::Float(r)) => Value::Boolean(l != r),
                        (Value::String(l), Value::String(r)) => Value::Boolean(l != r),
                        (Value::Null, _) => Value::Null,
                        (_, Value::Null) => Value::Null,
                        (l, r) => return Err(Internal(format!("[Executor] Can not compare expression {} and {}", l, r)))
                    })
                },
            }
        },
        _ => return Err(Internal(format!("[Executor] Unexpected Expression {:?}", expr)))
    }
}