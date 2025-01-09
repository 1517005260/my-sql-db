# Hash Join 优化

之前实现的[简单Join](../04-SQLs/08-Join/08.1-Join.md)，原理如下：

```
表t1        表t2
 a           b
 --         --- 
 1           2
 2           3
 3           4
 4           5
```

如果连接条件是`a=b`，那么针对a列的每一条数据，都需要扫描一次t2全表，找到和a列数据相等的行，如果a列m行，b列n行，那么时间复杂度为O(m * n)

现在建立Hash表，如下所示：

```
           Hash-Key           Hash-Value 
 a           b
 --         --- 
 1           2       ->       完整行数据(2)
 2           3       ->       完整行数据(3)
 3           4       ->       完整行数据(4)
 4           5       ->       完整行数据(5)
```

建立hash表的时间复杂度为O(n)，即扫描一次b全体数据。匹配时，由于Hash表的平均查找效率是O(1)，所以匹配并扫描完一遍a的时间复杂度为O(m)，整体复杂度O(m+n)

## 代码实现

1. 修改Planner

在mod.rs中新增HashJoin节点

```rust
#[derive(Debug,PartialEq)]
pub enum Node{
    HashJoin{    // HashJoin节点，时间复杂度O(m+n)
        left: Box<Node>,
        right: Box<Node>,
        condition: Option<Expression>,
        outer: bool,
    },
}
```

在planner.rs中修改返回的节点。这里需要注意，目前本数据库实现的四种Join方法中，CrossJoin是不需要优化的，因为本来就需要组合扫描。而实际生产中，是否HashJoin是依据实际情况而定的（比如根据表的数据量进行修改等），但是这里学习起见，我们直接将剩下三种Join全部优化成HashJoin。

```rust
fn build_from_item(&mut self, item: FromItem, filter: &Option<Expression>) -> Result<Node>{
    let node = match item {
        FromItem::Table { name } => self.build_scan_or_index(name, filter.clone())?,
        FromItem::Join { left, right, join_type, condition } => {
            // 优化： a right join b == b left join a， 这样一套逻辑就可以复用
            let (left, right) = match join_type {
                JoinType::Right => (right, left),
                _ => (left, right),
            };

            let outer = match join_type  {
                JoinType::Cross | JoinType::Inner => false,
                _ => true,
            };

            if join_type == Cross{
                Node::NestedLoopJoin {
                    left: Box::new(self.build_from_item(*left, filter)?),
                    right: Box::new(self.build_from_item(*right, filter)?),
                    condition,
                    outer,
                }
            }else {
                Node::HashJoin{
                    left: Box::new(self.build_from_item(*left, filter)?),
                    right: Box::new(self.build_from_item(*right, filter)?),
                    condition,
                    outer,
                }
            }
            
        },
    };
    Ok(node)
}
```

2. 修改executor

join.rs中新增：

```rust
pub struct HashJoin<T:Transaction>{
    left: Box<dyn Executor<T>>,
    right: Box<dyn Executor<T>>,
    condition: Option<Expression>,
    outer: bool,
}

impl<T:Transaction> HashJoin<T> {
    pub fn new(left: Box<dyn Executor<T>>, right: Box<dyn Executor<T>>, condition: Option<Expression>, outer: bool) -> Box<Self> {
        Box::new(Self { left, right, condition, outer})
    }
}
```

mod.rs中新增：

```rust
impl<T:Transaction + 'static> dyn Executor<T>{
    pub fn build(node: Node) -> Box<dyn Executor<T>>{
        match node {
            Node::HashJoin { left, right, condition, outer } => HashJoin::new(Self::build(*left), Self::build(*right), condition, outer),
        }
    }
}
```

join.rs中继续修改：

这里也要先扫描两张全表，区别就是HashJoin就扫描这一次，普通Join还要扫描后续

```rust
impl<T:Transaction> Executor<T> for HashJoin<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        // 先扫描左表
        if let ResultSet::Scan {columns: left_cols, rows: left_rows} = self.left.execute(transaction)?{
            let mut new_rows = Vec::new();
            let mut new_cols = left_cols.clone();
            // 再扫描右表
            if let ResultSet::Scan {columns: right_cols, rows: right_rows} = self.right.execute(transaction)? {

                new_cols.extend(right_cols.clone());

                // 解析HashJoin条件，即拿到左右两列的列名
                let (lcol, rcol) = match parse_join_condition(self.condition) {
                    Some(res) => res,
                    None => return Err(Internal("[Executor] Failed to parse join condition, please recheck column names".into())),
                };

                // 拿到连接列在表中的位置
                let left_pos = match left_cols.iter().position(|c| *c == lcol) {
                    Some(pos) => pos,
                    None => return Err(Internal(format!("[Executor] Column {} does not exist", lcol)))
                };

                let right_pos = match right_cols.iter().position(|c| *c == rcol) {
                    Some(pos) => pos,
                    None => return Err(Internal(format!("[Executor] Column {} does not exist", rcol)))
                };

                // 构建hash表（右），key 为 连接列的值， value为对应的一行数据
                // 可能一个key有不止一行数据，所以用列表存
                let mut map = HashMap::new();
                for row in &right_rows{
                    let rows = map.entry(row[right_pos].clone()).or_insert(Vec::new());
                    rows.push(row.clone());
                }

                // 扫描左表进行匹配
                for row in left_rows{
                    match map.get(&row[left_pos]) {  // 尝试与右表数据匹配
                        Some(rows) => {
                            for a_row in rows{
                                let mut row = row.clone();
                                row.extend(a_row.clone());
                                new_rows.push(row);
                            }
                        },
                        None => {
                            // 未匹配到，如果是外连接需要展示为null
                            if self.outer{
                                let mut row = row.clone();
                                for _ in 0..right_rows[0].len() {
                                    row.push(Value::Null);
                                }
                                new_rows.push(row);
                            }
                        },
                    }
                }
                return Ok(ResultSet::Scan {columns: new_cols, rows: new_rows});
            }
        }

        Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string()))
    }
}

// 解析join条件，获取左右两列
// 思路和index的条件判断一致
fn parse_join_condition(condition: Option<Expression>) -> Option<(String, String)>{
    match condition {
        Some(expr) => {
            match expr {
                // 解析列名
                Expression::Field(col) => Some((col, "".into())),
                Expression::Operation(operation) => {
                    match operation {
                        Operation::Equal(col1, col2) => {
                            // 递归调用进行解析
                            let left = parse_join_condition(Some(*col1));
                            let right = parse_join_condition(Some(*col2));

                            // 左右均为为(col, "")，现在进行组合
                            Some((left.unwrap().0, right.unwrap().0))
                        },
                        _ => None,
                    }
                },
                _ => None,
            }
        },
        None => None,
    }
}
```

3. kv.rs中进行简单测试：

```rust
#[test]
fn test_hash_join() -> Result<()> {
    let p = tempfile::tempdir()?.into_path().join("sqldb-log");
    let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
    let mut s = kvengine.session()?;
    s.execute("create table t1 (a int primary key);")?;
    s.execute("create table t2 (b int primary key);")?;
    s.execute("create table t3 (c int primary key);")?;

    s.execute("insert into t1 values (1), (2), (3);")?;
    s.execute("insert into t2 values (2), (3), (4);")?;
    s.execute("insert into t3 values (3), (8), (9);")?;

    match s.execute("select * from t1 join t2 on a = b join t3 on a = c;")? {
        ResultSet::Scan { columns, rows } => {
            assert_eq!(columns.len(), 3);
            assert_eq!(rows.len(), 1);
        }
        _ => unreachable!(),
    }

    std::fs::remove_dir_all(p.parent().unwrap())?;
    Ok(())
}
```