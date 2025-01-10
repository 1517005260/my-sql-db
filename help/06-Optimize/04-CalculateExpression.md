# 表达式计算

需要支持加减乘除与乘方：

```sql
select * from t1 where c1 = 1+2;
```

## 基于运算符优先级的算法 Precedence Climbing

利用运算符的优先级进行爬升（Climbing），以决定表达式的结构和运算顺序

优先级：`括号 > 乘方 > 乘除 > 加减`

```
5 + 2 * 3 + 4

|-----------|   : 优先级 1
    |---|       : 优先级 2
```

上例的计算方式如下：

遍历表达式：
- 首先拿到数字5
- 其次拿到符号`+`
- 接着拿到数字2
  - 此时需要判断2后面的符号
  - 如果2后面的符号优先级更小，则可以计算`5+2`
  - 如果2后面的符号优先级更大，则需要计算后续的表达式

以此类推。

## 代码实现

主要修改parser：

在lexer中新增token。这里需要注意，之前`*`是代表选择的全体，现在还能代表乘法运算符

```rust
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Hat,                // ^
}

impl Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Token::Hat => "^",
        })
    }
}

impl Token {
    // 判断是否是数学运算符
    pub fn is_operator(&self) -> bool {
        match self {
            Token::Plus | Token::Minus | Token::Asterisk | Token::Slash | Token::Hat => true,
            _ => false,
        }
    }

    // 获取优先级
    pub fn get_priority(&self) -> i32{
        match self {
            Token::Plus | Token::Minus => 1,
            Token::Asterisk | Token::Slash => 2,
            Token::Hat => 3,
            _ => 0,
        }
    }

    pub fn calculate_expr(&self, left : Expression, right: Expression) -> Result<Expression> {
        let val = match (left, right){
            (Expression::Consts(c1), Expression::Consts(c2)) => match (c1, c2) {  // 只能计算常数的计算
                (Consts::Integer(l), Consts::Integer(r)) => {
                    self.calculate(l as f64, r as f64)?
                }
                (Consts::Integer(l), Consts::Float(r)) => {
                    self.calculate(l as f64, r)?
                }
                (Consts::Float(l), Consts::Integer(r)) => {
                    self.calculate(l, r as f64)?
                }
                (Consts::Float(l), Consts::Float(r)) => {
                    self.calculate(l, r)?
                }
                _ => return Err(Parse("[Lexer] Cannot calculate the expression".into())),
            },
            _ => return Err(Parse("[Lexer] Cannot calculate the expression".into())),
        };

        Ok(Expression::Consts(Consts::Float(val)))
    }

    fn calculate(&self, left: f64, right: f64) -> Result<f64> {
        Ok(match self {
            Token::Asterisk => left * right,
            Token::Plus => left + right,
            Token::Minus => left - right,
            Token::Slash => left / right,
            Token::Hat => left.powf(right),  // powf无论如何都返回浮点数
            _ => return Err(Parse("[Lexer] Cannot calculate the expression".into())),
        })
    }
}

fn scan_symbol(&mut self) -> Option<Token> {
    match self.iter.peek()? {
        _ => self.next_if_token(|c|
            match c {
                '^' => Some(Token::Hat),
            })        
    }
}
```

在mod.rs中增加表达式解析方法：

```rust
fn parse_expression(&mut self) -> Result<Expression>{
    let expr =match self.next()? {
        Token::OpenParen => {
            // 括号里面单独看为一个新表达式计算
            let expr = self.calculate_expression(1)?;
            self.expect_next_token_is(Token::CloseParen)?;
            expr
        },
    };
    Ok(expr)
}

// 计算数学表达式
// 这里是不处理括号的，括号在parse_expression()里面处理
/** 例如计算 5+2+1：
    初始 prev_priority=1， left = 5 ，token = + ，是运算符，可以继续处理
    并且此时 (+.priority = 1) == (prev_priority = 1)，所以不会跳出循环
    结束时置 next_priority = +.priority + 1 => 2

    递归调用下 prev_priority=2，left=2, token = + ，是运算符，可以继续处理
    但此时 (+.priority = 1) < (prev_priority = 2)，会跳出循环
    所以right=2

    接着计算left与right的计算结果即可
**/
fn calculate_expression(&mut self, prev_priority: i32) -> Result<Expression>{
    let mut left = self.parse_expression()?;  // 第一个数字
    loop{
        // 第一个数字后面的计算符
        let token = match self.peek()? {
            Some(t) => t,
            None => break,   // 第一个数字后面没有计算符了
        };

        if !token.is_operator()  // 不是运算符，比如右括号，说明计算结束
            || token.get_priority() < prev_priority  //  前面的优先级是大于left后面的符号优先级的，说明要先计算前面
        {
            break;
        }

        let next_priority = token.get_priority() + 1;  // 爬升法
        self.next()?; // 跳到下个token

        // 递归计算右边的表达式
        let right = self.calculate_expression(next_priority)?;

        // 计算左右两边的计算结果
        left = token.calculate_expr(left, right)?;
    }
    Ok(left)
}
```

理解算法中的关键设计： 为什么需要为右边表达式设置更高优先级？

**可以理解为人为建立一个更高门槛，而本算法只能向下走不能向上走。**

核心原因是为了处理**相同优先级运算符的左结合性**。我们用例子来说明：

考虑表达式 `5 - 2 - 1`，它应该按照从左到右的顺序计算：

```
(5 - 2) - 1 = 2
```

而不是：

```
5 - (2 - 1) = 4  // 这是错误的结果
```

让我们看看代码是如何实现这一点的：
```rust
let next_priority = token.get_priority() + 1;  // 关键行
```

例如处理 `5 - 2 - 1` 时：
1. 第一次循环：
    - 当前处理 `5`，遇到第一个 `-`（优先级1）
    - 为右边设置 `next_priority = 1 + 1 = 2`
    - 递归处理 `2 - 1` 时，因为 `-` 的优先级(1) < prev_priority(2)
    - 所以递归调用直接返回 `2`，不会继续处理后面的减法

2. 这样就保证了：
    - 先计算 `5 - 2`
    - 再计算结果 `3 - 1`

如果不增加优先级：
```rust
let next_priority = token.get_priority();  // 假设这样写
```
那么处理 `5 - 2 - 1` 时：
1. 递归处理右边表达式时，因为 `-` 的优先级等于 prev_priority
2. 会继续计算 `2 - 1`
3. 导致错误的运算顺序：`5 - (2 - 1)`

这个设计还能正确处理不同优先级的情况。比如 `5 + 2 * 3`：
1. 遇到 `+`，为右边设置 `next_prec = 1 + 1 = 2`
2. 处理 `2 * 3` 时，因为 `*` 的优先级(2) >= prev_priority(2)
3. 所以会先计算 `2 * 3`
4. 最后再计算 `5 + 6`

修改完成后可以进行替换：

```rust
// 解析表达式当中的Operation类型
fn parse_operation(&mut self) -> Result<Expression>{
    let left = self.parse_expression()?;
    let token = self.next()?;
    let res = match token{
        Token::Equal => Expression::Operation(Operation::Equal(
            Box::new(left),
            Box::new(self.calculate_expression(1)?),
        )),
        Token::Greater => Expression::Operation(Operation::Greater(
            Box::new(left),
            Box::new(self.calculate_expression(1)?),
        )),
        Token::GreaterEqual => Expression::Operation(Operation::GreaterEqual(
            Box::new(left),
            Box::new(self.calculate_expression(1)?),
        )),
        Token::Less=> Expression::Operation(Operation::Less(
            Box::new(left),
            Box::new(self.calculate_expression(1)?),
        )),
        Token::LessEqual=> Expression::Operation(Operation::LessEqual(
            Box::new(left),
            Box::new(self.calculate_expression(1)?),
        )),
        Token::NotEqual => Expression::Operation(Operation::NotEqual(
            Box::new(left),
            Box::new(self.calculate_expression(1)?),
        )),
        _ => return Err(Error::Internal(format!("[Parser] Unexpected token {}",token))),
    };
    Ok(res)
}
```

另外发现不能识别`where 主键=表达式`的格式，问题出现在executor/query.rs。因为

```rust
trasaction.read_row_by_pk(&self.table_name, &self.value)?
```

需要对值Value进行编码，而Value的编码会出现问题：

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

即使最后的条件里，是`where a=1+2`，肉眼上观察`3=3.0`，但是编码时由于枚举位置不一样，所以发生了错误，遂进行修改：

```rust
// engine/query.rs

impl<T:Transaction> Executor<T> for PkIndex{
    fn execute(self:Box<Self>,trasaction: &mut T) -> Result<ResultSet> {
        let table = trasaction.must_get_table(self.table_name.clone())?;
        let mut rows = Vec::new();
        let mut pk_value = self.value.clone();
        if let Value::Float(f) = self.value{
            // 我们查看小数部分是否为0，如果为0说明是整数，需要进行转换
            if f.fract() == 0.0{
                pk_value = Value::Integer(f as i64);
            }
        }
        if let Some(row) = trasaction.read_row_by_pk(&self.table_name,  &pk_value)?{
            rows.push(row);
        }

        // println!("pk index");

        Ok(ResultSet::Scan {
            columns: table.columns.into_iter().map(|c| c.name.clone()).collect(),
            rows,
        })
    }
}
```