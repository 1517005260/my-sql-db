use crate::error::{Error, Result};
use crate::sql::engine::Transaction;
use crate::sql::parser::ast;
use crate::sql::parser::ast::JoinType::Cross;
use crate::sql::parser::ast::{Expression, FromItem, JoinType, Operation, Sentence};
use crate::sql::planner::{Node, Plan};
use crate::sql::schema;
use crate::sql::schema::Table;
use crate::sql::types::Value;

pub struct Planner<'a, T: Transaction> {
    // 辅助Plan的结构体
    transaction: &'a mut T,
}

impl<'a, T: Transaction> Planner<'a, T> {
    pub fn new(transaction: &'a mut T) -> Self {
        Self { transaction }
    }

    pub fn build(&mut self, sentence: Sentence) -> Result<Plan> {
        Ok(Plan(self.build_sentence(sentence)?))
    }

    // 将parser得到的sql-sentence转换为node节点
    fn build_sentence(&mut self, sentence: Sentence) -> Result<Node> {
        Ok(match sentence {
            Sentence::CreateTable { name, columns } => Node::CreateTable {
                schema: Table {
                    name,
                    columns: columns
                        .into_iter()
                        .map(|c| {
                            let nullable = c.nullable.unwrap_or(!c.is_primary_key); // 如果是主键，则!c.is_primary_key == false，不能为空
                            let default = match c.default {
                                Some(expression) => {
                                    Some(Value::from_expression_to_value(expression))
                                }
                                None if nullable => Some(Value::Null), // 如果没写default且可为null，则默认null
                                None => None,
                            };

                            schema::Column {
                                name: c.name,
                                datatype: c.datatype,
                                nullable,
                                default,
                                is_primary_key: c.is_primary_key,
                                is_index: c.is_index && !c.is_primary_key, // 主键不能建索引
                            }
                        })
                        .collect(),
                },
            },

            Sentence::DropTable { name } => Node::DropTable { name },

            Sentence::Insert {
                table_name,
                columns,
                values,
            } => Node::Insert {
                table_name,
                columns: columns.unwrap_or_default(), // columns 是 None 时，则使用 Vec::default()，即一个空的 Vec 列表，作为默认值返回。
                values,
            },

            Sentence::Select {
                select_condition,
                from_item,
                where_condition,
                group_by,
                having,
                order_by,
                limit,
                offset,
            } => {
                // from
                let mut node = self.build_from_item(from_item, &where_condition)?;

                // agg or group by
                let mut has_agg = false;
                if !select_condition.is_empty() {
                    for (expr, _) in select_condition.iter() {
                        // 判断expr是否是聚集函数
                        if let ast::Expression::Function(_, _) = expr {
                            has_agg = true;
                            break;
                        }
                    }

                    if group_by.is_some() {
                        has_agg = true;
                    }

                    if has_agg {
                        node = Node::Aggregate {
                            source: Box::new(node),
                            expression: select_condition.clone(),
                            group_by,
                        }
                    }
                }

                // having
                if let Some(expr) = having {
                    node = Node::Having {
                        source: Box::new(node),
                        condition: expr,
                    }
                }

                // 如果有order by，那么这里就返回OrderBy节点而不是Scan节点
                if !order_by.is_empty() {
                    node = Node::OrderBy {
                        scan: Box::new(node),
                        order_by,
                    }; // 更新 scan_node 为 order_by_node
                }

                // offset
                if let Some(expr) = offset {
                    node = Node::Offset {
                        source: Box::new(node),
                        offset: match Value::from_expression_to_value(expr) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::Internal("invalid offset".into())),
                        },
                    }
                }

                // limit
                if let Some(expr) = limit {
                    node = Node::Limit {
                        source: Box::new(node),
                        limit: match Value::from_expression_to_value(expr) {
                            Value::Integer(i) => i as usize,
                            _ => return Err(Error::Internal("invalid offset".into())),
                        },
                    }
                }

                // projection
                if !select_condition.is_empty() && has_agg == false {
                    node = Node::Projection {
                        source: Box::new(node),
                        expressions: select_condition,
                    }
                }

                node
            }

            Sentence::Update {
                table_name,
                columns,
                condition,
            } => Node::Update {
                table_name: table_name.clone(),
                scan: Box::new(self.build_scan_or_index(table_name, condition)?),
                columns,
            },

            Sentence::Delete {
                table_name,
                condition,
            } => Node::Delete {
                table_name: table_name.clone(),
                scan: Box::new(self.build_scan_or_index(table_name, condition)?),
            },

            Sentence::TableSchema { table_name } => Node::TableSchema { name: table_name },
            Sentence::TableNames {} => Node::TableNames {},
            Sentence::Begin {} | Sentence::Commit {} | Sentence::Rollback {} => {
                return Err(Error::Internal(
                    "[Planner] Unexpected transaction command".into(),
                ));
            }
            Sentence::Explain { sentence: _ } => {
                // 不使用字段sentence
                return Err(Error::Internal(
                    "[Planner] Unexpected explain command".into(),
                ));
            }
            Sentence::Flush {} => {
                return Err(Error::Internal("[Planner] Unexpected flush command".into()))
            }
        })
    }

    // 将from_item变成plan_node
    fn build_from_item(&mut self, item: FromItem, filter: &Option<Expression>) -> Result<Node> {
        let node = match item {
            FromItem::Table { name } => self.build_scan_or_index(name, filter.clone())?,
            FromItem::Join {
                left,
                right,
                join_type,
                condition,
            } => {
                // 优化： a right join b == b left join a， 这样一套逻辑就可以复用
                let (left, right) = match join_type {
                    JoinType::Right => (right, left),
                    _ => (left, right),
                };

                let outer = match join_type {
                    JoinType::Cross | JoinType::Inner => false,
                    _ => true,
                };

                if join_type == Cross {
                    Node::NestedLoopJoin {
                        left: Box::new(self.build_from_item(*left, filter)?),
                        right: Box::new(self.build_from_item(*right, filter)?),
                        condition,
                        outer,
                    }
                } else {
                    Node::HashJoin {
                        left: Box::new(self.build_from_item(*left, filter)?),
                        right: Box::new(self.build_from_item(*right, filter)?),
                        condition,
                        outer,
                    }
                }
            }
        };
        Ok(node)
    }

    // 根据filter条件判断是否可以走索引
    fn build_scan_or_index(&self, table_name: String, filter: Option<Expression>) -> Result<Node> {
        let node = match Self::parse_filter(filter.clone()) {
            Some((col, val)) => {
                // 即使条件是 b=2，但是若不是索引列，也不能走索引
                let table = self.transaction.must_get_table(table_name.clone())?;

                // 如果是主键，那走主键索引
                if table
                    .columns
                    .iter()
                    .position(|c| c.name == col && c.is_primary_key)
                    .is_some()
                {
                    return Ok(Node::PkIndex {
                        table_name,
                        value: val,
                    });
                }

                match table
                    .columns
                    .iter()
                    .position(|c| *c.name == col && c.is_index)
                {
                    Some(_) => {
                        // 本列有索引
                        Node::ScanIndex {
                            table_name,
                            col_name: col,
                            value: val,
                        }
                    }
                    None => Node::Scan { table_name, filter },
                }
            }
            None => Node::Scan { table_name, filter },
        };
        Ok(node)
    }

    // 解析上个函数的filter表达式
    // 实际上我们的hash索引仅支持 b=2 的条件，也即Expression::Operation::Equal
    fn parse_filter(filter: Option<Expression>) -> Option<(String, Value)> {
        match filter {
            Some(expr) => {
                match expr {
                    // 解析右边的常数
                    Expression::Consts(val) => Some((
                        "".into(),
                        Value::from_expression_to_value(Expression::Consts(val)),
                    )),
                    // 解析左边的列名
                    Expression::Field(col) => Some((col, Value::Null)),
                    Expression::Operation(operation) => {
                        match operation {
                            Operation::Equal(col, val) => {
                                // 递归调用进行解析
                                let left = Self::parse_filter(Some(*col));
                                let right = Self::parse_filter(Some(*val));

                                // 左边为(col, null)，右边为("", val)，现在进行组合
                                Some((left.unwrap().0, right.unwrap().1))
                            }
                            _ => None,
                        }
                    }
                    _ => None,
                }
            }
            None => None,
        }
    }
}
