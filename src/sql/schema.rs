use serde::{Deserialize, Serialize};
use crate::sql::types::{DataType, Row, Value};
use crate::error::*;

#[derive(Debug, PartialEq,Serialize,Deserialize)]
pub struct Table{
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table{
    // 判断表的有效性
    pub fn is_valid(&self) -> Result<()>{
        // 判断列是否为空
        if self.columns.is_empty() {
            return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" has no columns", self.name)));
        }

        // 判断主键信息
        match self.columns.iter().filter(|c| c.is_primary_key).count() {
            1 => {},
            0 => return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" has no primary key", self.name))),
            _ => return Err(Error::Internal(format!("[CreateTable] Failed, Table \" {} \" has multiple primary keys", self.name))),
        }

        Ok(())
    }

    // 获取主键
    pub fn get_primary_key(&self, row: &Row) -> Result<Value> {
        let index = self.columns.iter().position(|c| c.is_primary_key).unwrap();  // 由于建表时已经判断了主键信息，所以这里直接解包即可
        Ok(row[index].clone())
    }

    // 获取列索引
    pub fn get_col_index(&self, col_name: &str) -> Result<usize> {
        self.columns.iter().position(|c| c.name == col_name)
            .ok_or(Error::Internal(format!("[Get Column Index Failed] Column {} not found", col_name)))
    }
}

#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub struct Column{
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub is_primary_key: bool,
}