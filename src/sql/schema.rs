use std::fmt::{Display, Formatter};
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

        // 判断列是否有效
        for column in &self.columns {
            // 主键不能空
            if column.is_primary_key && column.nullable {
                return Err(Error::Internal(format!("[CreateTable] Failed, primary key \" {} \" cannot be nullable in table \" {} \"", column.name, self.name)));
            }

            // 列默认值需要和列数据类型匹配
            if let Some(default_value) = &column.default {
                match default_value.get_datatype() {
                    Some(datatype) => {
                        if datatype != column.datatype {
                            return Err(Error::Internal(format!("[CreateTable] Failed, default value type for column \" {} \" mismatch in table \" {} \"", column.name, self.name)))
                        }
                    },
                    None =>{}
                }
            }
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

impl Display for Table{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let column_description = self.columns.iter()
            .map(|c| format!("{}", c))
            .collect::<Vec<_>>().join(",\n");
        write!(f, "TABLE NAME: {} (\n{}\n)", self.name, column_description)
    }
}

#[derive(Debug,PartialEq,Serialize,Deserialize)]
pub struct Column{
    pub name: String,
    pub datatype: DataType,
    pub nullable: bool,
    pub default: Option<Value>,
    pub is_primary_key: bool,
    pub is_index: bool,
}

impl Display for Column{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut column_description = format!("  {} {:?} ", self.name, self.datatype);
        if self.is_primary_key {
            column_description += "PRIMARY KEY ";
        }
        if !self.nullable && !self.is_primary_key {
            column_description += "NOT NULL ";
        }
        if let Some(v) = &self.default {
            column_description += &format!("DEFAULT {}", v.to_string());
        }
        write!(f, "{}", column_description)
    }
}