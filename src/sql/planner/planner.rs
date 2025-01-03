use crate::sql::parser::ast::Sentence;
use crate::sql::planner::{Node, Plan};
use crate::sql::schema;
use crate::sql::schema::Table;
use crate::sql::types::Value;

pub struct Planner;  // 辅助Plan的结构体

impl Planner {
    pub fn new() -> Self {
        Self
    }

    pub fn build(&mut self, sentence: Sentence) -> Plan{
        Plan(self.build_sentence(sentence))
    }

    // 将parser得到的sql-sentence转换为node节点
    fn build_sentence(&mut self, sentence: Sentence) -> Node{
        match sentence {
            Sentence::CreateTable {name,columns} =>
                Node::CreateTable {
                    schema:Table{
                        name,
                        columns:
                            columns.into_iter().map(|c| {
                                let nullable = c.nullable.unwrap_or(!c.is_primary_key);  // 如果是主键，则!c.is_primary_key == false，不能为空
                                let default = match c.default {
                                    Some(expression) => Some(Value::from_expression_to_value(expression)),
                                    None if nullable => Some(Value::Null),  // 如果没写default且可为null，则默认null
                                    None => None,
                                };

                                schema::Column{
                                    name: c.name,
                                    datatype: c.datatype,
                                    nullable,
                                    default,
                                    is_primary_key: c.is_primary_key,
                                }
                            }).collect(),
                    }
                },

            Sentence::Insert { table_name, columns, values, } =>
                Node::Insert {
                    table_name,
                    columns:columns.unwrap_or_default(),  // columns 是 None 时，则使用 Vec::default()，即一个空的 Vec 列表，作为默认值返回。
                    values,
                },

            Sentence::Select {table_name, order_by} =>
                {
                    let scan_node = Node::Scan {table_name, filter:None};
                    // 如果有order by，那么这里就返回OrderBy节点而不是Scan节点
                    if !order_by.is_empty() {
                        let node = Node::OrderBy {
                            scan: Box::new(scan_node),
                            order_by,
                        };
                        node
                    }else {
                        scan_node
                    }
                },

            Sentence::Update {table_name, columns, condition} =>
            Node::Update {
                table_name: table_name.clone(),
                scan: Box::new(Node::Scan {table_name, filter: condition}),
                columns,
            },

            Sentence::Delete {table_name, condition} =>
                Node::Delete {
                    table_name:table_name.clone(),
                    scan: Box::new(Node::Scan {table_name, filter: condition})
                },

            }
        }
}