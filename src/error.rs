// 自定义Result返回的错误类型
// 对标准的Result进行重写即可

use std::num::{ParseFloatError, ParseIntError};

pub type Result<T> = std::result::Result<T,Error>;

// 自定义错误类型
#[derive(Debug, Clone, PartialEq)]
pub enum Error{
    Parse(String), // 在解析器阶段报错，内容为String的错误
}

// 兼容系统本身的解析数字报错
impl From<ParseIntError> for Error{
    fn from(value: ParseIntError) -> Self {
        Error::Parse(value.to_string())   // 直接将系统报错信息兼容进我们的报错系统即可
    }
}

impl From<ParseFloatError> for Error{
    fn from(value: ParseFloatError) -> Self {
        Error::Parse(value.to_string())
    }
}