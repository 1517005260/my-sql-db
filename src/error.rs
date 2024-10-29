// 自定义Result返回的错误类型
// 对标准的Result进行重写即可

pub type Result<T> = std::result::Result<T,Error>;

// 自定义错误类型
#[derive(Debug, Clone, PartialEq)]
pub enum Error{
    Parse(String), // 在解析器阶段报错，内容为String的错误
}