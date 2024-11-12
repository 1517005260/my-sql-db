// 自定义Result返回的错误类型
// 对标准的Result进行重写即可

use std::num::{ParseFloatError, ParseIntError};
use std::sync::PoisonError;
use bincode::ErrorKind;

pub type Result<T> = std::result::Result<T,Error>;

// 自定义错误类型
#[derive(Debug, Clone, PartialEq)]
pub enum Error{
    Parse(String), // 在解析器阶段报错，内容为String的错误
    Internal(String),   // 在数据库内部运行时的报错
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

// lock()相关报错处理
impl<T> From<PoisonError<T>> for Error {
    fn from(value: PoisonError<T>) -> Self {
        Error::Internal(value.to_string())
    }
}

// 序列化相关报错处理
impl From<Box<ErrorKind>> for Error{
    fn from(value: Box<ErrorKind>) -> Self {
        Error::Internal(value.to_string())
    }
}

// 文件相关错误
impl From<std::io::Error> for Error{
    fn from(value: std::io::Error) -> Self {
        Error::Internal(value.to_string())
    }
}