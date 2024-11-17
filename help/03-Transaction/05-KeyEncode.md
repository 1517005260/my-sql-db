# 事务key自定义编码代码实现

## Serializer

1. 新建文件storage/keyencode.rs，参考：https://serde.rs/impl-serializer.html 实现自定义的编码逻辑

先参照官网逻辑，实现Serializer的结构体和接口方法

```rust
use serde::{ser, Serialize};

pub struct Serializer {
    output: Vec<u8>,    // 最终序列化的目标
}

impl<'a> ser::Serializer for &'a mut Serializer {
    type Ok = ();
    type Error = ();
    type SerializeSeq = ();
    type SerializeTuple = ();
    type SerializeTupleStruct = ();
    type SerializeTupleVariant = ();
    type SerializeMap = ();
    type SerializeStruct = ();
    type SerializeStructVariant = ();

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_variant(self, name: &'static str, variant_index: u32, variant: &'static str) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    fn serialize_newtype_variant<T>(self, name: &'static str, variant_index: u32, variant: &'static str, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        todo!()
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_tuple_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_tuple_variant(self, name: &'static str, variant_index: u32, variant: &'static str, len: usize) -> Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        todo!()
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct, Self::Error> {
        todo!()
    }

    fn serialize_struct_variant(self, name: &'static str, variant_index: u32, variant: &'static str, len: usize) -> Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }
}
```

这里的待实现方法非常多，我们不用全部实现，根据之前的分析，只要实现：

```
u64 - version
bytes - 字节数组（转换为二进制的key、value等）
unit_variant - 无附加的枚举类型，如NextVersion
newtype_variant - ActiveTransactions(Version) 这种有附加定义 自定义数据类型 的枚举类型
tuple_variant - Write(Version, Vec<u8>) 这种附加元组的枚举类型
seq - Vec<u8>
tuple - (Version, Vec<u8>)
```

而且，简单类型如u64等就是结构体里的方法，无需递归地实现复杂接口，于是真正要实现的就变为：Seq, Tuple 和 TupleVariant

2. 根据[官网说明](https://serde.rs/error-handling.html)，在error.rs中，实现错误兼容逻辑：

```rust
// 事务key编码相关错误
impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Internal(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Internal(msg.to_string())
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Parse(err) => write!(f, "Parse Error: {}", err),
            Error::Internal(err) => write!(f, "Internal Error: {}", err),
            Error::WriteConflict => write!(f, "Write conflicted in transaction, please try again"),
        }
    }
}

impl std::error::Error for Error {}
```

3. 根据官网提供的示例，我们来对照实现，注意这里的Result需要换成自定义的Result（全局替换）：

```rust
use crate::error::{Error, Result};

impl<'a> ser::Serializer for &'a mut Serializer {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Self;   // Self(Serializer) 必须实现 SerializeSeq

    type SerializeTuple = Self;

    type SerializeTupleStruct = ser::Impossible<Self::Ok, Self::Error>;  // 不用实现

    type SerializeTupleVariant = Self;

    type SerializeMap = ser::Impossible<Self::Ok, Self::Error>;  // 不用实现

    type SerializeStruct = ser::Impossible<Self::Ok, Self::Error>;  // 不用实现

    type SerializeStructVariant = ser::Impossible<Self::Ok, Self::Error>;  // 不用实现

    fn serialize_bool(self, v: bool) -> Result<()> {
        todo!()
    }

    fn serialize_i8(self, v: i8) -> Result<()> {
        todo!()
    }

    fn serialize_i16(self, v: i16) -> Result<()> {
        todo!()
    }

    fn serialize_i32(self, v: i32) -> Result<()> {
        todo!()
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        todo!()
    }

    fn serialize_u8(self, v: u8) -> Result<()> {
        todo!()
    }

    fn serialize_u16(self, v: u16) -> Result<()> {
        todo!()
    }

    fn serialize_u32(self, v: u32) -> Result<()> {
        todo!()
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        todo!()
    }

    fn serialize_f32(self, v: f32) -> Result<()> {
        todo!()
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        todo!()
    }

    fn serialize_char(self, v: char) -> Result<()> {
        todo!()
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        todo!()
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        todo!()
    }

    fn serialize_none(self) -> Result<()> {
        todo!()
    }

    fn serialize_some<T>(self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<()> {
        todo!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<()> {
        todo!()
    }

    fn serialize_unit_variant(self, name: &'static str, variant_index: u32, variant: &'static str) -> Result<()> {
        todo!()
    }

    fn serialize_newtype_struct<T>(self, name: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    fn serialize_newtype_variant<T>(self, name: &'static str, variant_index: u32, variant: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        todo!()
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        todo!()
    }

    fn serialize_tuple_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeTupleStruct> {
        todo!()
    }

    fn serialize_tuple_variant(self, name: &'static str, variant_index: u32, variant: &'static str, len: usize) -> Result<Self::SerializeTupleVariant> {
        todo!()
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap> {
        todo!()
    }

    fn serialize_struct(self, name: &'static str, len: usize) -> Result<Self::SerializeStruct> {
        todo!()
    }

    fn serialize_struct_variant(self, name: &'static str, variant_index: u32, variant: &'static str, len: usize) -> Result<Self::SerializeStructVariant> {
        todo!()
    }
}
```

4. 实现简单类型的自定义序列化

```rust
impl<'a> ser::Serializer for &'a mut Serializer {
    fn serialize_u64(self, v: u64) -> Result<()> {
        // 定长数字，直接放到output里，并转换成字节数组即可
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<()> {
        // Vec<u8>，可以进行优化
        // 详细逻辑见 04-Intro-KeyEncode.md 中的 0 0 结尾
        let mut res = Vec::new();
        for item in v.into_iter() {
            match item {
                0 => res.extend([0, 255]),  // 0， 变为 0 255
                b => res.push(*b),  // 非0，直接编码
            }
        }
        res.extend([0, 0]);  //  0 0 结尾

        self.output.extend(res);
        Ok(())
    }

    // MvccKey::NextVersion 单独的枚举
    // 直接编为 0 1 2 3即可
    // 例子
    /**
    pub enum MvccKey{
        NextVersion,          // 0
        ActiveTransactions,   // 1
        Write,                // 2
        Version,              // 3
        }
    **/
    fn serialize_unit_variant(self, name: &'static str, variant_index: u32, variant: &'static str) -> Result<()> {
        self.output.extend(u8::try_from(variant_index));   // 直接将index转为u8即可
        Ok(())
    }

    // MvccKey::ActiveTransactions(Version), 比MvccKey::NextVersion就多了个 Version(u64)
    fn serialize_newtype_variant<T>(self, name: &'static str, variant_index: u32, variant: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self)  // 根据value的类型自动进行序列化，这里已知Version就是u64，所以就是在调之前写的序列化u64方法
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)  // 复杂类型的序列化，以递归地自定义地trait为准
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    // MvccKey::Write(Version, Vec<u8>), 即带元组的枚举类型
    fn serialize_tuple_variant(self, name: &'static str, variant_index: u32, variant: &'static str, len: usize) -> Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }
}
```

5. 实现复杂类型的自定义序列化，可以参考官网的json实现，我们稍作修改即可

```rust
// 单独处理复杂类型
impl<'a> ser::SerializeSeq for &'a mut Serializer {
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize
    {
        value.serialize(&mut **self)  // 对seq里的值单独编码即可
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();

    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)  // tuple和seq数组实际上是类似的
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<()>
    where
        T: ?Sized + ser::Serialize,
    {
        value.serialize(&mut **self)  // 和tuple一样
    }

    fn end(self) -> Result<()> {
        Ok(())
    }
}
```

6. 添加一个向外暴露的公有方法，便于直接调用序列化的方法：

```rust
// 传入的实际上是 #[derive(Serialize)] pub enum MvccKey
pub fn serialize_key<T: serde::Serialize>(key: &T) -> Result<Vec<u8>> {
    let mut ser = Serializer { output: Vec::new() };
    key.serialize(&mut ser)?;
    Ok(ser.output)
}
```

7. 序列化方法的测试：

```rust
#[cfg(test)]
mod tests {
    use crate::storage::mvcc::{MvccKey, MvccKeyPrefix};
    use super::*;

    #[test]
    fn test_encode() {
        let ser_cmp = |k: MvccKey, v: Vec<u8>| {
            let res = serialize_key(&k).unwrap();
            assert_eq!(res, v);
        };

        ser_cmp(MvccKey::NextVersion, vec![0]);
        ser_cmp(MvccKey::ActiveTransactions(1), vec![1, 0, 0, 0, 0, 0, 0, 0, 1]);
        ser_cmp(
            MvccKey::Write(1, vec![1, 2, 3]),
            vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0, 0],
        );
        ser_cmp(
            MvccKey::Version(b"abc".to_vec(), 11),
            vec![3, 97, 98, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11],
        );
    }

    #[test]
    fn test_encode_prefix() {
        let ser_cmp = |k: MvccKeyPrefix, v: Vec<u8>| {
            let res = serialize_key(&k).unwrap();
            assert_eq!(res, v);
        };

        ser_cmp(MvccKeyPrefix::NextVersion, vec![0]);
        ser_cmp(MvccKeyPrefix::ActiveTransactions, vec![1]);
        ser_cmp(MvccKeyPrefix::Write(1), vec![2, 0, 0, 0, 0, 0, 0, 0, 1]);
        ser_cmp(
            MvccKeyPrefix::Version(b"ab".to_vec()),
            vec![3, 97, 98, 0, 0],
        );
    }
}
```