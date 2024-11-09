mod schema;
mod mutation;
mod query;

use crate::error::Result;
use crate::sql::executor::mutation::Insert;
use crate::sql::executor::query::Scan;
use crate::sql::executor::schema::CreateTable;
use crate::sql::planner::Node;
use crate::sql::types::Row;

pub trait Executor{
    fn execute(&self) -> Result<ResultSet>;
}

// 执行结果集的定义
pub enum ResultSet{
    CreateTable{
        table_name: String,   // 创建表成功，则返回表名
    },
    Insert{
        count: usize,         // 插入表成功，则返回插入数
    },
    Scan{
        columns: Vec<String>,  // 扫描的列
        rows: Vec<Row>,        // 扫描的行
    }
}

impl dyn Executor{
    pub fn build(node: Node) -> Box<dyn Executor>{
        match node {
            Node::CreateTable {schema} => CreateTable::new(schema),
            Node::Insert {table_name,columns,values} => Insert::new(table_name, columns, values),
            Node::Scan {table_name} => Scan::new(table_name),
        }
    }
}