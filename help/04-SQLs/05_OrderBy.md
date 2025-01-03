# Order By 语句实现

order by: 对select输出的结果进行排序

**语法定义**：

```sql
select * from table1 
order by a desc , b desc , c asc;
-- a列如果有重复，那重复的字段再看b列排序，以此类推
```

升序ASC，降序DESC，所以抽象出来就是这样：

```sql
SELECT * FROM TABLE_NAME
[ORDER BY COL_NAME [ASC | DESC] [,...]];
```

## Rust中用于排序的接口

在Rust中，`PartialOrd`和`Ord`是用于比较的两个不同的接口，主要区别在于它们如何处理比较操作。

`PartialOrd`：
- `PartialOrd`用于部分有序(Partial Order)的类型。部分有序意味着并不是所有值都可以相互比较。在数学中，这种情况发生在浮点数上，例如`NaN（非数字，比如缺失值、无穷值等）`。两个`NaN`值之间无法进行有效比较，这使得浮点数是部分有序的。
- `PartialOrd`提供的方法是`partial_cmp`，其返回值是`Option<Ordering>`，其中`Ordering`是`Less`、`Equal`或`Greater`之一。如果两个值无法比较，返回`None`。

```rust
pub trait PartialOrd<Rhs: ?Sized = Self>: PartialEq<Rhs> {
#[must_use]
#[stable(feature = "rust1", since = "1.0.0")]
#[rustc_diagnostic_item = "cmp_partialord_cmp"]
fn partial_cmp(&self, other: &Rhs) -> Option<Ordering>;
}
```

`Ord`:
- `Ord`用于完全有序(Total Order)的类型。完全有序意味着对于任何两个值，都能确定它们是小于、等于还是大于。`Ord`通常适用于整数、字符串等类型，它们总能比较出明确的大小关系。
- `Ord`提供的方法是`cmp`，其返回值是`Ordering`，不会有`None`的情况。

```rust
pub trait Ord: Eq + PartialOrd<Self> {
#[must_use]
#[stable(feature = "rustl", since = "1.0.0")]
#[rustc_diagnostic_item = "ord_cmp_method"]
fn cmp(&self, other: &Self) -> Ordering;
}
```

并且需要注意的是，Ord 扩展了 PartialOrd（即部分有序是完全有序的子集），这意味着任意实现了Ord接口的结构体，都必须同时实现 PartialOrd。

### 例子

```rust
let a = 1.0;
let b = f64::NAN;
assert_eq!(a.partial_cmp(&b), None); // 无法比较

let x = 1;
let y = 2;
assert_eq!(x.cmp(&y), std::cmp::Ordering::Less); // 可以比较
```

而目前，我们的数据库有如下的value类型：

```rust
#[derive(Debug,PartialEq,Serialize,Deserialize,Clone)]
pub enum Value {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}
```

由于包含了Null、Float等类型，显然我们需要使用`PartialOrd`

接下来我们看看两个字段的排序：

```rust
enum OrderBy {  // 定义升序、降序
    ASC,
    DESC,
}

// 模拟一张表，表有三列
let mut rows = vec![
    vec![1, 9, 11],
    vec![1, 3, 23],
    vec![4, 5, 41],
    vec![1, 2, 43],
    vec![2, 5, 25],
];

// 对第一列按升序排序，对第二列按降序排序
let columns = vec![
    OrderBy::ASC,
    OrderBy::DESC
];

// 自定义闭包函数
rows.sort_by(|row1, row2| { // 对于某两行
    let x = row1[0];  // 取出每行的第一列
    let y = row2[0];
    match x.partial_cmp(&y) {
        Some(Ordering::Equal) => {  // 如果第一列相等
            let x2 = row1[1];  // 取出每行的第二列  
            let y2 = row2[1];
            return x2.partial_cmp(&y2).unwrap().reverse();  // 按降序 reverse() 排序
        },
        Some(ord) => return ord,  // 如果第一列不相等，则按第一列升序即可
        _ => return Ordering::Equal,  // 其他情况全相等
    }
});

for r in rows {
    println!("{:?}", r);
}

/*
[1, 9, 11]
[1, 3, 23]
[1, 2, 43]
[2, 5, 25]
[4, 5, 41]
*/
```

当然以上写法仅能针对两个字段，我们现在给出多个字段的排序方法：

```rust
#[derive(PartialEq)]
enum OrderBy {
    ASC,
    DESC,
}

let mut rows = vec![
    vec![1, 9, 11],
    vec![1, 3, 23],
    vec![4, 5, 41],
    vec![1, 2, 43],
    vec![2, 5, 25],
];
let columns = vec![
    OrderBy::ASC,
    OrderBy::DESC,
];

rows.sort_by(|row1, row2| {
    for (i, ord) in columns.iter().enumerate() {
        // 第一次，i = 0, ord = OrderBy::ASC
        let a = row1[i];
        let b = row2[i];
        match a.partial_cmp(&b) {  // ord作为隐含条件传入a.partial_cmp(&b)
            Some(Ordering::Equal) => {}, // continue
            Some(o) => return if *ord == OrderBy::ASC { o } else { o.reverse() },
            None => {},
        }
    }
    Ordering::Equal  // 如果所有列都相等
});

for r in rows {
    println!("{:?}", r);
}
```

## 代码实现

流程还是按照从上至下的顺序修改

1. 在parser/lexer.rs中新增关键字：

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Keyword {
    // 新增
    Order,
    By,
    Asc,
    Desc,
}

impl Keyword {
    pub fn transfer(input: &str) -> Option<Self> {
        Some(
            match input.to_uppercase().as_ref() {
                // 新增
                "ORDER" => Keyword::Order,
                "BY" => Keyword::By,
                "ASC" => Keyword::Asc,
                "DESC" => Keyword::Desc,
        })
    }
}

impl Keyword {
    pub fn to_str(&self) -> &str {
        match self {
            Keyword::Order => "ORDER",
            Keyword::By => "BY",
            Keyword::Asc => "ASC",
            Keyword::Desc => "DESC",
        }
    }
}
```

在parser/ast.rs中，新增排序的抽象语法树：

```rust
// 排序抽象语法
#[derive(Debug, PartialEq, Clone)]
pub enum OrderBy{
    Asc,
    Desc,
}

#[derive(Debug,PartialEq)]
pub enum Sentence{
    // 其他不变
    Select{
        table_name: String,
        order_by: Vec<(String, OrderBy)>, // 例如，order by col_a desc
    },
}
```

在parser/mod.rs中，新增对select语句解析时，对order by的解析。和where一样，order by是个可选项，需要单独的一个函数来解析

```rust
// 分类二：Select语句
fn parse_select(&mut self) -> Result<Sentence>{
    // 先只实现select *
    self.expect_next_token_is(Token::Keyword(Keyword::Select))?;
    self.expect_next_token_is(Token::Asterisk)?;
    self.expect_next_token_is(Token::Keyword(Keyword::From))?;

    // 识别完关键字之后为表名
    let table_name = self.expect_next_is_ident()?;
    Ok(Sentence::Select {
        table_name,
        order_by: self.parse_order_by_condition()?,
    })
}

fn parse_order_by_condition(&mut self) -> Result<Vec<(String, OrderBy)>>{
        let mut order_by_condition = Vec::new();
        if self.next_if_is_token(Token::Keyword(Keyword::Order)).is_none(){
            return Ok(order_by_condition); // 没有指定 Order By 条件
        }
        self.expect_next_token_is(Token::Keyword(Keyword::By))?;

        loop{ // 可能有多个排序条件
            let col = self.expect_next_is_ident()?;
            // 可以不指定asc或者desc，默认asc
            // matches! 是 Rust 中的一个宏，用于检查一个值是否与给定的模式匹配
            let order = match self.next_if(|token| matches!(token, Token::Keyword(Keyword::Asc) | Token::Keyword(Keyword::Desc))){
                Some(Token::Keyword(Keyword::Asc)) => OrderBy::Asc,
                Some(Token::Keyword(Keyword::Desc)) => OrderBy::Desc,
                _ => OrderBy::Asc,  // 默认asc
            };

            order_by_condition.push((col, order));
            if self.next_if_is_token(Token::Comma).is_none(){
                break;
            }
        }
        Ok(order_by_condition)
    }

// 测试
#[test]
fn test_parser_select() -> Result<()> {
    let sql = "select * from tbl1;";
    let sentence = Parser::new(sql).parse()?;
    assert_eq!(
        sentence,
        ast::Sentence::Select {
            table_name: "tbl1".to_string(),
            order_by: vec![],  // 没有排序条件
        }
    );

    let sql = "select * from tbl1 order by a, b asc, c desc;";
    let sentence = Parser::new(sql).parse()?;
    assert_eq!(
        sentence,
        ast::Sentence::Select {
            table_name: "tbl1".to_string(),
            order_by: vec![
                ("a".to_string(), OrderBy::Asc),
                ("b".to_string(), OrderBy::Asc),
                ("c".to_string(), OrderBy::Desc),
            ],
        }
    );

    Ok(())
}
```

2. 之后修改planner

在mod.rs中先修改节点的定义

```rust
#[derive(Debug,PartialEq)]
pub enum Node{
    // 新增
    OrderBy{
        scan: Box<Node>,
        order_by: Vec<(String, OrderBy)>,
    },
}
```

然后修改原先planner中解析节点的方法：

```rust
fn build_sentence(&mut self, sentence: Sentence) -> Node{
    match sentence {
        // 修改
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
    }
}
```

3. 接着修改executor，我们需要处理具体的排序逻辑

首先我们需要在sql/types/mod.rs中实现排序方法：

```rust
impl PartialOrd for Value {
    // 参数：self-当前值；other-需要比较的值
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            // null 是自定义类型，需要我们自己实现比较的逻辑
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            // 剩下这些系统自带类型已经实现好了partial_cmp，我们直接调就行
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (_, _) => None,  // 其他情况统一认为不可比
        }
    }
}
```

排序仍然属于查询的范畴，所以我们在query.rs中修改：

```rust
pub struct Order<T: Transaction>{
    scan: Box<dyn Executor<T>>,
    order_by: Vec<(String, OrderBy)>
}

impl<T:Transaction> Order<T>{
    pub fn new(scan: Box<dyn Executor<T>>, order_by: Vec<(String, OrderBy)>) -> Box<Self>{
        Box::new(Self{ scan, order_by })
    }
}

impl<T:Transaction> Executor<T> for Order<T>{
    fn execute(self: Box<Self>, transaction: &mut T) -> Result<ResultSet> {
        // 首先和update一样，先需要拿到scan节点，否则报错
        match self.scan.execute(transaction){
            Ok(ResultSet::Scan {columns, mut rows}) => {
                // 处理排序逻辑
                // 首先我们要拿到排序列在整张表里的下标，比如有abcd四列，要对bd两列排序，下标就是b-1,d-3
                // 而在order by 的排序条件里，下标是 b-0,d-1 需要修改
                let mut order_col_index = HashMap::new();
                for (i, (col_name, _)) in self.order_by.iter().enumerate() {
                    // 这里需要判断，有可能用户指定的排序列不在表中，需要报错
                    match columns.iter().position(|c| *c == *col_name) {
                        Some(position) => order_col_index.insert(i, position),
                        None => return Err(Internal(format!("order by column {} is not in table", col_name))),
                    };
                }

                rows.sort_by(|row1, row2|{
                    for(i, (_, condition)) in self.order_by.iter().enumerate(){
                        let col_index = order_col_index.get(&i).unwrap();  // 拿到实际的表中列下标
                        let x = &row1[*col_index];  // row1_value
                        let y = &row2[*col_index];  // row2_value
                        match x.partial_cmp(y) {
                            Some(Equal) => continue,
                            Some(o) => return
                                if *condition == Asc {o}
                                else {o.reverse()},
                            None => continue,
                        }
                    }
                    Equal  // 其余情况认为相等
                });
                Ok(ResultSet::Scan { columns, rows })
            },
            _ => return Err(Internal("[Executor] Unexpected ResultSet, expected Scan Node".to_string())),
        }
    }
}
```

在mod.rs中修改：

```rust
impl<T:Transaction + 'static> dyn Executor<T>{
    pub fn build(node: Node) -> Box<dyn Executor<T>>{
        match node {
            // 新增
            Node::OrderBy {scan, order_by} => Order::new(Self::build(*scan), order_by),
        }
    }
}
```

4. 最后在engine/kv.rs中测试：

```rust
#[test]
fn test_sort() -> Result<()> {
    let p = tempfile::tempdir()?.into_path().join("sqldb-log");
    let kvengine = KVEngine::new(DiskEngine::new(p.clone())?);
    let mut s = kvengine.session()?;
    setup_table(&mut s)?;

    s.execute("insert into t3 values (1, 34, 22, 1.22);")?;
    s.execute("insert into t3 values (4, 23, 65, 4.23);")?;
    s.execute("insert into t3 values (3, 56, 22, 2.88);")?;
    s.execute("insert into t3 values (2, 87, 57, 6.78);")?;
    s.execute("insert into t3 values (5, 87, 14, 3.28);")?;
    s.execute("insert into t3 values (7, 87, 82, 9.52);")?;

    match s.execute("select * from t3 order by b, c desc;")? {
        ResultSet::Scan { columns, rows } => {
            for r in rows {
                println!("{:?}", r);
            }
        }
        _ => unreachable!(),
    }

    std::fs::remove_dir_all(p.parent().unwrap())?;
    Ok(())
}
```