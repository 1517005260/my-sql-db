mod schema;
mod mutation;
mod query;
mod join;
mod aggregate;
mod calculate;
mod show;

use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::aggregate::Aggregate;
use crate::sql::executor::join::NestedLoopJoin;
use crate::sql::executor::mutation::{Delete, Insert, Update};
use crate::sql::executor::query::{Limit, Offset, Order, Scan, Projection, Having, ScanIndex, PkIndex};
use crate::sql::executor::schema::CreateTable;
use crate::sql::executor::show::{TableNames, TableSchema};
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
    TableSchema{
        schema: String,
    },
    TableNames{
        names: Vec<String>,
    },
    Begin{
        version: u64,
    },
    Commit{
        version: u64,
    },
    Rollback{
        version: u64,
    },
}

impl ResultSet {
    pub fn to_string(&self) -> String {
        match self {
            ResultSet::CreateTable { table_name } => format!("CREATE TABLE {}", table_name),  // 创建成功提示
            ResultSet::Insert { count } => format!("INSERT {} rows", count),                  // 插入成功提示
            ResultSet::Scan { columns, rows } => { // 返回扫描结果
                let rows_len = rows.len();   // 一共多少行

                // 先找到列名的长度
                let mut max_len = columns.iter().map(|c| c.len()).collect::<Vec<usize>>();
                // 然后将列名和行数据进行比较，选出最长的那个
                for a_row in rows {
                    for(i, v) in a_row.iter().enumerate() {
                        // 确保 i 在 max_len.len() 范围内
                        if i < max_len.len() {
                            if v.to_string().len() > max_len[i] {
                                max_len[i] = v.to_string().len();
                            }
                        } else {
                            // 如果发现列数不匹配，扩展 max_len
                            max_len.push(v.to_string().len());
                        }
                    }
                }

                // 展示列名
                let columns = columns.iter().zip(max_len.iter()) // 将两个迭代器 columns 和 max_len 配对在一起
                    .map(|(col, &len)| format!("{:width$}", col, width = len))
                    .collect::<Vec<_>>().join(" |");  // 每列用 | 分割

                // 展示列名和数据的分隔符
                let sep = max_len.iter().map(|v| format!("{}", "-".repeat(*v + 1)))  // 让“-”重复最大长度次
                    .collect::<Vec<_>>().join("+");  // 用 + 连接

                // 展示行
                let rows = rows.iter()
                    .map(|row| {
                        row.iter()
                            .zip(max_len.iter())
                            .map(|(v, &len)| format!("{:width$}", v.to_string(), width = len))
                            .collect::<Vec<_>>()
                            .join(" |")
                    })
                    .collect::<Vec<_>>()
                    .join("\n");       // 每行数据用 \n 分割

                format!("{}\n{}\n{}\n({} rows)", columns, sep, rows, rows_len)
            }
            ResultSet::Update { count } => format!("UPDATE {} rows", count),               // 更新成功提示
            ResultSet::Delete { count } => format!("DELETE {} rows", count),               // 删除成功提示
            ResultSet::TableSchema { schema } => format!("{}", schema),
            ResultSet::TableNames { names } => {
                if names.is_empty() {
                    "No tables found.".to_string()
                } else {
                    names.join("\n")
                }
            },
            ResultSet::Begin {version} => format!("TRANSACTION {} BEGIN", version),
            ResultSet::Commit {version} => format!("TRANSACTION {} COMMIT", version),
            ResultSet::Rollback {version} => format!("TRANSACTION {} ROLLBACK", version),
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
            Node::TableSchema {name} => TableSchema::new(&name),
            Node::TableNames { } => TableNames::new(),
            Node::ScanIndex { table_name, col_name, value} => ScanIndex::new(table_name, col_name, value),
            Node::PkIndex { table_name, value } => PkIndex::new(table_name, value),
        }
    }
}