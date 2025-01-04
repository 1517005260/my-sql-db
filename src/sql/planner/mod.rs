use std::collections::BTreeMap;
use crate::sql::engine::Transaction;
use crate::error::Result;
use crate::sql::executor::{Executor, ResultSet};
use crate::sql::parser::ast::{Expression, OrderBy, Sentence};
use crate::sql::planner::planner::Planner;
use crate::sql::schema::Table;

mod planner;

// 定义执行节点
#[derive(Debug,PartialEq)]
pub enum Node{
    CreateTable{
        schema: Table,
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
        filter: Option<(String, Expression)>,
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
}

// 定义执行计划，执行计划的底层是不同执行节点
// 多个Node节点组成了执行计划Plan树
#[derive(Debug,PartialEq)]
pub struct Plan(pub Node);  // 元素结构体，可以通过 let plan = Plan(node); 快速创建

// 实现构建Plan的方法
impl Plan{
    pub fn build(sentence: Sentence) -> Result<Self>{
        Ok(Planner::new().build(sentence)?)
    }

    // planner与executor交互，plan节点 -> 执行器结构体
    pub fn execute<T:Transaction + 'static>(self, transaction :&mut T) -> Result<ResultSet>{
        <dyn Executor<T>>::build(self.0).execute(transaction)  // self.0 == node 只有这一个元素
    }
}

#[cfg(test)]
mod tests {
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
        let sql1 = "
        create table tbl1 (
            a int default 100,
            b float not null,
            c varchar null,
            d bool default true
        );
        ";
        let sentence1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(sentence1);
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
        let p2 = Plan::build(sentence2);
        assert_eq!(p1, p2);

        Ok(())
    }

    #[test]
    fn test_plan_insert() -> Result<()> {
        let sql1 = "insert into tbl1 values (1, 2, 3, 'a', true);";
        let sentence1 = Parser::new(sql1).parse()?;
        let p1 = Plan::build(sentence1)?;
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
        let p2 = Plan::build(sentence2)?;
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

        Ok(())
    }

    #[test]
    fn test_plan_select() -> Result<()> {
        let sql = "select * from tbl1;";
        let sentence = Parser::new(sql).parse()?;
        let p = Plan::build(sentence)?;
        assert_eq!(
            p,
            Plan(Node::Scan {
                table_name: "tbl1".to_string(),
                filter: None,
            })
        );

        Ok(())
    }
}