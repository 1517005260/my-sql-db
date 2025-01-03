use std::collections::BTreeMap;
use crate::sql::types::DataType;
// 本模块是抽象语法树的定义


// 列定义
#[derive(Debug,PartialEq)]
pub struct Column{            // 列的各种属性
    pub name: String,         // 列名
    pub datatype: DataType,   // 列数据类型
    pub nullable: Option<bool>, // 列是否为空
    pub default: Option<Expression>, // 列的默认值
    pub is_primary_key: bool,       // 本列是否为主键
}

// 目前表达式为了简单，仅支持常量，不支持：insert into Table_A value(11 * 11 + 2) 等
// 更新：select的列名算作Expression
#[derive(Debug,PartialEq,Clone)]
pub enum Expression{
    Consts(Consts),
    Field(String),
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
    Insert{
        table_name: String,           // 目标表名
        columns: Option<Vec<String>>,  // 目标列，可以为空
        values: Vec<Vec<Expression>>,   // 插入数据，是个二维数组
    },
    Select{
        table_name: String,
        select_condition: Vec<(Expression, Option<String>)>,  // 列名，可选的别名
        order_by: Vec<(String, OrderBy)>, // 例如，order by col_a desc
        limit: Option<Expression>,
        offset: Option<Expression>,
    },
    Update{
        table_name: String,
        columns: BTreeMap<String, Expression>,
        condition: Option<(String, Expression)>
    },
    Delete{
        table_name: String,
        condition: Option<(String, Expression)>,
    },
}