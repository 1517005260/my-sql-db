use crate::error::Result;
use crate::sql::engine::Transaction;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::OrderBy::Asc;
use crate::sql::parser::ast::{Expression, OrderBy, Sentence};
use crate::sql::planner::planner::Planner;
use crate::sql::schema::Table;
use crate::sql::types::Value;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};

mod planner;

// 定义执行节点
#[derive(Debug,PartialEq)]
pub enum Node{
    CreateTable{
        schema: Table,
    },
    DropTable{
        name: String,
    },
    Insert{
        table_name: String,
        columns: Vec<String>,
        values:Vec<Vec<Expression>>  // 先暂时置为expression，后续再解析
    },
    Scan{
        // select
        table_name: String,
        // 过滤条件
        filter: Option<Expression>,
    },
    ScanIndex{
        table_name: String,
        col_name: String,
        value: Value,
    },
    PkIndex{
        table_name: String,
        value: Value,
    },
    Update{
        table_name: String,
        scan: Box<Node>,
        columns: BTreeMap<String, Expression>,
    },
    Delete{
        table_name: String,
        scan: Box<Node>,
    },
    OrderBy{
        scan: Box<Node>,
        order_by: Vec<(String, OrderBy)>,
    },
    Limit{
        source: Box<Node>,
        limit: usize,
    },
    Offset{
        source: Box<Node>,
        offset: usize,
    },
    Projection{
        source: Box<Node>,
        expressions: Vec<(Expression, Option<String>)>,
    },
    NestedLoopJoin{  // 嵌套循环节点，时间复杂度O(m * n)
        left: Box<Node>,
        right: Box<Node>,
        condition: Option<Expression>,
        outer: bool,
    },
    HashJoin{    // HashJoin节点，时间复杂度O(m+n)
        left: Box<Node>,
        right: Box<Node>,
        condition: Option<Expression>,
        outer: bool,
    },
    Aggregate{  // 聚集函数节点
        source: Box<Node>,
        expression: Vec<(Expression, Option<String>)>,  // Function, 别名
        group_by: Option<Expression>,
    },
    Having{
        source: Box<Node>,
        condition: Expression,
    },
    TableSchema{
        name: String,
    },
    TableNames{
    },
}

// Plan Node 的格式化输出方法
impl Display for Node{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.format(f, "", true)
    }
}

impl Node{
    fn format(&self, f: &mut Formatter<'_>,  // formatter进行输出
              prefix: &str,                  // 换行前缀
              is_first: bool,                // 是否是第一个节点
    ) -> std::fmt::Result {
        if is_first{
            writeln!(f, "           SQL PLAN           ")?;
            writeln!(f, "------------------------------")?;
        }else {
            writeln!(f)?;
        }

        let prefix =
            if prefix.is_empty() {
            " -> ".to_string()
        } else {
            write!(f, "{}", prefix)?;
            format!(" {}", prefix)  // 下一个prefix需要有层次感
        };

        match self {
            Node::CreateTable {schema} => {
                write!(f, "Create Table {}", schema.name)
            },
            Node::DropTable {name} => {
                write!(f, "Drop Table {}", name)
            },
            Node::Insert {table_name, columns:_, values:_} => {
                write!(f, "Insert Into Table {}", table_name)
            },
            Node::Scan {table_name, filter} => {
                write!(f, "Sequence Scan On Table {}", table_name)?;
                if let Some(filter) = filter {
                    write!(f, " ( Filter: {} )", filter)?;
                }
                Ok(())
            },
            Node::ScanIndex { table_name, col_name, value:_ } => {
                write!(f, "Index Scan On Table {}.{}", table_name, col_name)
            },
            Node::PkIndex { table_name, value } => {
                write!(f, "Primary Key Scan On Table {}({})", table_name, value)
            },
            Node::Update {table_name, scan, columns:_} => {
                write!(f, "Update On Table {}", table_name)?;
                (*scan).format(f, &prefix, false)
            },
            Node::Delete {table_name, scan} => {
                write!(f, "Delete On Table {}", table_name)?;
                (*scan).format(f, &prefix, false)
            },
            Node::OrderBy {scan, order_by} => {
                let condition = order_by.iter().
                    map(|c| {
                        format!("{} {}", c.0, if c.1 == Asc {"Asc"} else {"Desc"})
                    }).collect::<Vec<_>>().join(", ");
                write!(f, "Order By {}", condition)?;
                (*scan).format(f, &prefix, false)
            },
            Node::Limit {source, limit} => {
                write!(f, "Limit {}", limit)?;
                (*source).format(f, &prefix, false)
            }
            Node::Offset {source, offset} => {
                write!(f, "Offset {}", offset)?;
                (*source).format(f, &prefix, false)
            }
            Node::Projection {source, expressions} => {
                let selects = expressions.iter().map(|(col_name, nick_name)|{
                    format!("{} {}", col_name, if nick_name.is_some() {format!(" As {}", nick_name.clone().unwrap())} else {"".to_string()})
                }).collect::<Vec<_>>().join(", ");
                write!(f, "Projection {}", selects)?;
                (*source).format(f, &prefix, false)
            },
            Node::NestedLoopJoin {left, right, condition, outer:_} => {
                write!(f, "Nested Loop Join")?;
                if let Some(expr) = condition {
                    write!(f, "( {} )", expr)?;
                }
                (*left).format(f, &prefix, false)?;
                (*right).format(f, &prefix, false)
            },
            Node::HashJoin {left, right, condition, outer:_} => {
                write!(f, "Hash Join")?;
                if let Some(expr) = condition {
                    write!(f, "( {} )", expr)?;
                }
                (*left).format(f, &prefix, false)?;
                (*right).format(f, &prefix, false)
            },
            Node::Aggregate { source, expression, group_by} => {
                let agg = expression.iter().map(|(col_name, nick_name)|{
                    format!("{} {}", col_name, if nick_name.is_some() {format!(" As {}", nick_name.clone().unwrap())} else {"".to_string()})
                }).collect::<Vec<_>>().join(", ");
                write!(f, "Aggregate {} ", agg)?;
                if let Some(Expression::Field(col_name)) = group_by {
                    write!(f, "Group By {}", col_name)?;
                }
                (*source).format(f, &prefix, false)
            },
            Node::Having { source, condition} => {
                write!(f, "Filter: {}", condition)?;
                (*source).format(f, &prefix, false)
            },
            Node::TableSchema { name } => {
                write!(f, "Show Table Schema: {}", name)
            },
            Node::TableNames {} => {
                write!(f, "Show Table Names")
            },
        }
    }
}

// 定义执行计划，执行计划的底层是不同执行节点
// 多个Node节点组成了执行计划Plan树
#[derive(Debug,PartialEq)]
pub struct Plan(pub Node);  // 元素结构体，可以通过 let plan = Plan(node); 快速创建

// 实现构建Plan的方法
impl Plan{
    pub fn build<T: Transaction>(sentence: Sentence, transaction: &mut T) -> Result<Self>{
        Ok(Planner::new(transaction).build(sentence)?)
    }

    // planner与executor交互，plan节点 -> 执行器结构体
    pub fn execute<T:Transaction + 'static>(self, transaction :&mut T) -> Result<ResultSet>{
        <dyn Executor<T>>::build(self.0).execute(transaction)  // self.0 == node 只有这一个元素
    }
}

#[cfg(test)]
mod tests {
    use crate::sql::engine::kv::KVEngine;
    use crate::sql::engine::Engine;
    use crate::storage::disk::DiskEngine;
    use crate::{
        error::Result,
        sql::{
            parser::{
                ast::{self, Expression},
                Parser,
            },
            planner::{Node, Plan},
        },
    };

    #[test]
    fn test_plan_create_table() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut transaction = kvengine.begin()?;

        let sql1 = "
        create table tbl1 (
            a int default 100,
            b float not null,
            c varchar null,
            d bool default true
        );
        ";
        let sentence1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(sentence1, &mut transaction);
        println!("{:?}",p1);

        let sql2 = "
        create            table tbl1 (
            a int default     100,
            b float not null     ,
            c varchar      null,
            d       bool default        true
        );
        ";
        let sentence2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(sentence2, &mut transaction);
        assert_eq!(p1, p2);
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_plan_insert() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut transaction = kvengine.begin()?;

        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let sentence1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(sentence1,&mut transaction)?;
        assert_eq!(
            p1,
            Plan(Node::Insert {
                table_name: "tbl1".to_string(),
                columns: vec![],
                values: vec![vec![
                    Expression::Consts(ast::Consts::Integer(1)),
                    Expression::Consts(ast::Consts::Integer(2)),
                    Expression::Consts(ast::Consts::Integer(3)),
                    Expression::Consts(ast::Consts::String("a".to_string())),
                    Expression::Consts(ast::Consts::Boolean(true)),
                ]],
            })
        );

        let sql2 = "insert into tbl2 (c1, c2, c3) values (3, 'a', true),(4, 'b', false);";
        let sentence2 = Parser::new(sql2).parse()?;
        let p2 = Plan::build(sentence2, &mut transaction)?;
        assert_eq!(
            p2,
            Plan(Node::Insert {
                table_name: "tbl2".to_string(),
                columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
                values: vec![
                    vec![
                        Expression::Consts(ast::Consts::Integer(3)),
                        Expression::Consts(ast::Consts::String("a".to_string())),
                        Expression::Consts(ast::Consts::Boolean(true)),
                    ],
                    vec![
                        Expression::Consts(ast::Consts::Integer(4)),
                        Expression::Consts(ast::Consts::String("b".to_string())),
                        Expression::Consts(ast::Consts::Boolean(false)),
                    ],
                ],
            })
        );
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }

    #[test]
    fn test_plan_select() -> Result<()> {
        let p = tempfile::tempdir()?.into_path().join("sqldb-log");
        let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
        let mut transaction = kvengine.begin()?;

        let sql = "select * from tbl1;";
        let sentence = Parser::new(sql).parse()?;
        let plan = Plan::build(sentence, &mut transaction)?;
        assert_eq!(
            plan,
            Plan(Node::Scan {
                table_name: "tbl1".to_string(),
                filter: None,
            })
        );
        std::fs::remove_dir_all(p.parent().unwrap())?;
        Ok(())
    }
}