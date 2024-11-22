use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use crate::sql::parser::ast::{Consts, Expression};

#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
}

#[derive(Debug,PartialEq,Serialize,Deserialize,Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl Value {
    pub fn from_expression_to_value(expression: Expression) -> Self{
        match expression {
            Expression::Consts(Consts::Null) => Self::Null,
            Expression::Consts(Consts::Boolean(bool)) => Self::Boolean(bool),
            Expression::Consts(Consts::Integer(int)) => Self::Integer(int),
            Expression::Consts(Consts::Float(float)) => Self::Float(float),
            Expression::Consts(Consts::String(string)) => Self::String(string),
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

pub type Row = Vec<Value>;