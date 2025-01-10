use crate::sql::parser::ast::{Consts, Expression};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn from_expression_to_value(expression: Expression) -> Self {
        match expression {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(bool)) => Self::Boolean(bool),
            Expression::Consts(Consts::Integer(int)) => Self::Integer(int),
            Expression::Consts(Consts::Float(float)) => Self::Float(float),
            Expression::Consts(Consts::String(string)) => Self::String(string),
            _ => unreachable!(),
        }
    }

    pub fn get_datatype(&self) -> Option<DataType> {
        match self {
            Self::Null => None,
            Self::Boolean(_) => Some(DataType::Boolean),
            Self::Integer(_) => Some(DataType::Integer),
            Self::Float(_) => Some(DataType::Float),
            Self::String(_) => Some(DataType::String),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "{}", "NULL"),
            Value::Boolean(b) if *b => write!(f, "{}", "TRUE"),
            Value::Boolean(_) => write!(f, "{}", "FALSE"),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
        }
    }
}

impl PartialOrd for Value {
    // 参数：self-当前值；other-需要比较的值
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // null 是自定义类型，需要我们自己实现比较的逻辑
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            // 剩下这些系统自带类型已经实现好了partial_cmp，我们直接调就行
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (_, _) => None, // 其他情况统一认为不可比
        }
    }
}

// 使得Value类型可以作为HashMap的Key
impl Hash for Value {
    // 基础的数据类型其实都已经有hash的系统自带实现，这里我们简单调用即可
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Null => state.write_u8(0), // 唯一标识防止hash冲突
            // 即先写入一个唯一标识，再写入它hash后的值，防止不同类型的值产生相同的哈希值
            Value::Boolean(v) => {
                state.write_u8(1);
                v.hash(state);
            }
            Value::Integer(v) => {
                state.write_u8(2);
                v.hash(state);
            }
            Value::Float(v) => {
                state.write_u8(3);
                v.to_be_bytes().hash(state); // float本身没有实现hash，需要先转为二进制
            }
            Value::String(v) => {
                state.write_u8(4);
                v.hash(state);
            }
        }
    }
}

impl Eq for Value {}

pub type Row = Vec<Value>;
