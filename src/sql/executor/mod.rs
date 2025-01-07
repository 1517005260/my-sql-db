mod schema;
mod mutation;
mod query;
mod join;
mod aggregate;
mod calculate;

use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::aggregate::Aggregate;
use crate::sql::executor::join::NestedLoopJoin;
use crate::sql::executor::mutation::{Delete, Insert, Update};
use crate::sql::executor::query::{Limit, Offset, Order, Scan, Projection, Having};
use crate::sql::executor::schema::CreateTable;
use crate::sql::planner::Node;
use crate::sql::types::Row;

pub trait Executor<T:Transaction>{
    fn execute(self: Box<Self>,transaction:&mut T) -> Result<ResultSet>;
}

// 执行结果集的定义
#[derive(Debug, PartialEq, Clone)]
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
    },
    Update{
        count: usize,   // 更新了多少条数据
    },
    Delete{
      count: usize,   // 删除了多少条数据
    },
}

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::CreateTable { table_name } => format!("CREATE TABLE {}", table_name),  // 创建成功提示
            ResultSet::Insert { count } => format!("INSERT {} rows", count),                  // 插入成功提示
            ResultSet::Scan { columns, rows } => {                          // 返回扫描结果
                let columns = columns.join(" | ");  // 每列用 | 分割
                let rows_len = rows.len();   // 一共多少行
                let rows = rows
                    .iter()
                    .map(|row| {
                        row.iter()  // 遍历一行的每个元素
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join(" | ")   // 每列用 | 分割
                    })
                    .collect::<Vec<_>>()
                    .join("\n");       // 每行数据用 \n 分割
                format!("{}\n{}\n({} rows)", columns, rows, rows_len)
            }
            ResultSet::Update { count } => format!("UPDATE {} rows", count),               // 更新成功提示
            ResultSet::Delete { count } => format!("DELETE {} rows", count),               // 删除成功提示
        }
    }
}

impl<T:Transaction + 'static> dyn Executor<T>{
    pub fn build(node: Node) -> Box<dyn Executor<T>>{
        match node {
            Node::CreateTable {schema} => CreateTable::new(schema),
            Node::Insert {table_name,columns,values} => Insert::new(table_name, columns, values),
            Node::Scan {table_name,filter} => Scan::new(table_name,filter),
            Node::Update {table_name, scan, columns} =>
                Update::new(table_name,
                            Self::build(*scan),
                            columns),
            Node::Delete {table_name, scan} => Delete::new(table_name, Self::build(*scan)),
            Node::OrderBy {scan, order_by} => Order::new(Self::build(*scan), order_by),
            Node::Limit {source, limit} => Limit::new(Self::build(*source), limit),
            Node::Offset {source, offset} => Offset::new(Self::build(*source), offset),
            Node::Projection {source, expressions} => Projection::new(Self::build(*source), expressions),
            Node::NestedLoopJoin { left, right, condition, outer} => NestedLoopJoin::new(Self::build(*left), Self::build(*right), condition, outer),
            Node::Aggregate { source, expression, group_by} => Aggregate::new(Self::build(*source), expression, group_by),
            Node::Having {source, condition} => Having::new(Self::build(*source), condition),
        }
    }
}