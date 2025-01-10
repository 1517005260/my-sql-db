use crate::error::*;
use crate::sql::types::{Row, Value};

// 通用计算接口，供聚集函数使用
pub trait Calculate {
    fn new(&self) -> Box<dyn Calculate>;
    fn calculate(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Row>) -> Result<Value>;
}

impl dyn Calculate {
    // 根据函数名字找agg函数
    pub fn build(func_name: &String) -> Result<Box<dyn Calculate>> {
        Ok(match func_name.to_uppercase().as_ref() {
            "COUNT" => Count::new(&Count),
            "SUM" => Sum::new(&Sum),
            "MIN" => Min::new(&Min),
            "MAX" => Max::new(&Max),
            "AVG" => Avg::new(&Avg),
            _ => {
                return Err(Error::Internal(
                    "[Executor] Unknown aggregate function".into(),
                ))
            }
        })
    }
}

// 接下来是agg常见函数定义
// count
pub struct Count;

impl Calculate for Count {
    fn new(&self) -> Box<dyn Calculate> {
        Box::new(Count)
    }

    fn calculate(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Row>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => {
                return Err(Error::Internal(format!(
                    "[Executor] Column {} does not exist",
                    col_name
                )))
            }
        };

        // 找到row[pos]，进行计数，如果是null则不予统计
        let mut cnt = 0;
        for row in rows.iter() {
            if row[pos] != Value::Null {
                cnt += 1;
            }
        }

        Ok(Value::Integer(cnt))
    }
}

// min
pub struct Min;

impl Calculate for Min {
    fn new(&self) -> Box<dyn Calculate> {
        Box::new(Min)
    }

    fn calculate(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Row>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => {
                return Err(Error::Internal(format!(
                    "[Executor] Column {} does not exist",
                    col_name
                )))
            }
        };

        // 如果是null则跳过，如果全部是null则无最小值，返回null
        let mut min = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap()); // 和之前的order by排序逻辑一致
            min = values[0].clone();
        }

        Ok(min)
    }
}

// max
pub struct Max;

impl Calculate for Max {
    fn new(&self) -> Box<dyn Calculate> {
        Box::new(Max)
    }

    fn calculate(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Row>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => {
                return Err(Error::Internal(format!(
                    "[Executor] Column {} does not exist",
                    col_name
                )))
            }
        };

        // 如果是null则跳过，如果全部是null则无最小值，返回null
        let mut max = Value::Null;
        let mut values = Vec::new();
        for row in rows.iter() {
            if row[pos] != Value::Null {
                values.push(&row[pos]);
            }
        }
        if !values.is_empty() {
            values.sort_by(|a, b| a.partial_cmp(b).unwrap());
            max = values[values.len() - 1].clone();
        }

        Ok(max)
    }
}

// sum
pub struct Sum;

impl Calculate for Sum {
    fn new(&self) -> Box<dyn Calculate> {
        Box::new(Sum)
    }

    fn calculate(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Row>) -> Result<Value> {
        let pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => {
                return Err(Error::Internal(format!(
                    "[Executor] Column {} does not exist",
                    col_name
                )))
            }
        };

        let mut sum = None;
        for row in rows.iter() {
            // 如果是整数或浮点数，统一按浮点数求和。其他类型不可求和
            match row[pos] {
                Value::Null => continue,
                Value::Integer(v) => {
                    if sum == None {
                        sum = Some(0.0)
                    }
                    sum = Some(sum.unwrap() + v as f64)
                }
                Value::Float(v) => {
                    if sum == None {
                        sum = Some(0.0)
                    }
                    sum = Some(sum.unwrap() + v)
                }
                _ => {
                    return Err(Error::Internal(format!(
                        "[Executor] Can not calculate sum of column {}",
                        col_name
                    )))
                }
            }
        }

        Ok(match sum {
            Some(sum) => Value::Float(sum),
            None => Value::Null,
        })
    }
}

// average
pub struct Avg;

impl Calculate for Avg {
    fn new(&self) -> Box<dyn Calculate> {
        Box::new(Avg)
    }

    fn calculate(&self, col_name: &String, cols: &Vec<String>, rows: &Vec<Row>) -> Result<Value> {
        let _pos = match cols.iter().position(|c| *c == *col_name) {
            Some(pos) => pos,
            None => {
                return Err(Error::Internal(format!(
                    "[Executor] Column {} does not exist",
                    col_name
                )))
            }
        };

        // avg = sum / count
        let sum = Sum::new(&Sum).calculate(col_name, cols, rows)?;
        let count = Count::new(&Count).calculate(col_name, cols, rows)?;
        let avg = match (sum, count) {
            (Value::Float(s), Value::Integer(c)) => Value::Float(s / c as f64),
            _ => Value::Null,
        };
        Ok(avg)
    }
}
