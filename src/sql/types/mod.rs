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

pub type Row = Vec<Value>;