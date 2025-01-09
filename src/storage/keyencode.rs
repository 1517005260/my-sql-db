use serde::{de, ser, Serialize};
use serde::de::{EnumAccess, IntoDeserializer, SeqAccess, VariantAccess, Visitor};
use crate::error::{Error, Result};
use String;

pub struct Serializer {
    output: Vec<u8>,    // 最终序列化的目标
}

// 传入的实际上是 #[derive(Serialize)] pub enum MvccKey
pub fn serialize_key<T: serde::Serialize>(key: &T) -> Result<Vec<u8>> {
    let mut ser = Serializer { output: Vec::new() };
    key.serialize(&mut ser)?;
    Ok(ser.output)
}

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
        self.output.push(v as u8);
        Ok(())
    }

    fn serialize_i8(self, _v: i8) -> Result<()> {
        todo!()
    }

    fn serialize_i16(self, _v: i16) -> Result<()> {
        todo!()
    }

    fn serialize_i32(self, _v: i32) -> Result<()> {
        todo!()
    }

    fn serialize_i64(self, v: i64) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_u8(self, _v: u8) -> Result<()> {
        todo!()
    }

    fn serialize_u16(self, _v: u16) -> Result<()> {
        todo!()
    }

    fn serialize_u32(self, _v: u32) -> Result<()> {
        todo!()
    }

    fn serialize_u64(self, v: u64) -> Result<()> {
        // 定长数字，直接放到output里，并转换成字节数组即可
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_f32(self, _v: f32) -> Result<()> {
        todo!()
    }

    fn serialize_f64(self, v: f64) -> Result<()> {
        self.output.extend(v.to_be_bytes());
        Ok(())
    }

    fn serialize_char(self, _v: char) -> Result<()> {
        todo!()
    }

    fn serialize_str(self, v: &str) -> Result<()> {
        self.serialize_bytes(v.as_bytes())
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

    fn serialize_none(self) -> Result<()> {
        todo!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<()> {
        todo!()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<()> {
        todo!()
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
    fn serialize_unit_variant(self, _name: &'static str, variant_index: u32, _variant: &'static str) -> Result<()> {
        self.output.extend(u8::try_from(variant_index));   // 直接将index转为u8即可
        Ok(())
    }

    fn serialize_newtype_struct<T>(self, _name: &'static str, _value: &T) -> Result<()>
    where
        T: ?Sized + Serialize
    {
        todo!()
    }

    // MvccKey::ActiveTransactions(Version), 比MvccKey::NextVersion就多了个 Version(u64)
    fn serialize_newtype_variant<T>(self, name: &'static str, variant_index: u32, variant: &'static str, value: &T) -> Result<()>
    where
        T: ?Sized + Serialize
    {
        self.serialize_unit_variant(name, variant_index, variant)?;
        value.serialize(self)  // 根据value的类型自动进行序列化，这里已知Version就是u64，所以就是在调之前写的序列化u64方法
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq> {
        Ok(self)  // 复杂类型的序列化，以递归地自定义地trait为准
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple> {
        Ok(self)
    }

    fn serialize_tuple_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeTupleStruct> {
        todo!()
    }

    // MvccKey::Write(Version, Vec<u8>), 即带元组的枚举类型
    fn serialize_tuple_variant(self, name: &'static str, variant_index: u32, variant: &'static str, _len: usize) -> Result<Self::SerializeTupleVariant> {
        self.serialize_unit_variant(name, variant_index, variant)?;
        Ok(self)
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap> {
        todo!()
    }

    fn serialize_struct(self, _name: &'static str, _len: usize) -> Result<Self::SerializeStruct> {
        todo!()
    }

    fn serialize_struct_variant(self, _name: &'static str, _variant_index: u32, _variant: &'static str, _len: usize) -> Result<Self::SerializeStructVariant> {
        todo!()
    }
}

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

pub struct Deserializer<'de> {   // 'de 更具语义性，表明生命周期与反序列化相关
    input: &'de [u8],  // 传入的是Vec<u8>
}

pub fn deserialize_key<'a, T: serde::Deserialize<'a>>(input: &'a [u8]) -> Result<T> {
    let mut de = Deserializer { input };
    T::deserialize(&mut de)
}

// 辅助方法
impl<'de> Deserializer<'de> {
    // 取出[0,len) 部分，留下[len,)部分
    fn take_bytes(&mut self, len: usize) -> &[u8] {
        let bytes = &self.input[..len];
        self.input = &self.input[len..];
        bytes
    }

    // 反解析字节码： 0 0 = 结束   0 255 = 0
    fn next_bytes(&mut self) -> Result<Vec<u8>> {
        let mut res = Vec::new();
        let mut iter = self.input.iter().enumerate();
        let index = loop{
            match iter.next(){
                Some((_,0)) => {       // index: _  value: 0
                    match iter.next(){
                        Some((i,0)) => break i+1,   // index: i  value: 0
                        Some((_,255)) => res.push(0),
                        _ => return Err(Error::Internal("[Deserializer] Unexpected Input".to_string()))
                    }
                },
                Some((_,val)) => res.push(*val),
                _ => return Err(Error::Internal("[Deserializer] Unexpected Input".to_string()))
            }
        };
        self.input = &self.input[index..];  // 处理完毕后截断
        Ok(res)
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        let v = self.take_bytes(1)[0];
        // v == 0 false
        // v == 1 true
        visitor.visit_bool(v != 0)  // v=0 则 v!=0 == false，反之 v!=0 == true
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        let bytes = self.take_bytes(8);
        let v = i64::from_be_bytes(bytes.try_into()?);
        visitor.visit_i64(v)
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        // u64 固定占64bit,8个字节
        let bytes = self.take_bytes(8);
        let v = u64::from_be_bytes(bytes.try_into()?);  // bytes 是&[u8],from_be_bytes需要Vec<u8>，这里需要try_into()做一个转换
        // 以上代码从字节数组中取出了数字
        visitor.visit_u64(v)  // 把u64传给visitor进行处理，这个visitor可以是反序列化的类
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        let bytes = self.take_bytes(8);
        let v = f64::from_be_bytes(bytes.try_into()?);
        visitor.visit_f64(v)
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        let bytes = self.next_bytes()?;
        visitor.visit_str(&String::from_utf8(bytes)?)
    }

    fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_bytes(&self.next_bytes()?)
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_bytes(&self.next_bytes()?)
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple_struct<V>(self, _name: &'static str, _len: usize, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_map<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_struct<V>(self, _name: &'static str, _fields: &'static [&'static str], _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_enum<V>(self, _name: &'static str, _variants: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }
}

impl<'de, 'a> SeqAccess<'de> for Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self).map(Some)
    }
}

impl<'de, 'a> EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        let index = self.take_bytes(1)[0] as u32;
        let variant_index: Result<_> = seed.deserialize(index.into_deserializer());
        Ok((variant_index?, self))
    }
}

impl<'de, 'a> VariantAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    fn unit_variant(self) -> Result<()> {
        Ok(())
    }

    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self)
    }

    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn struct_variant<V>(self, _fields: &'static [&'static str], _visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}

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

    #[test]
    fn test_decode() {
        let der_cmp = |k: MvccKey, v: Vec<u8>| {
            let res: MvccKey = deserialize_key(&v).unwrap();
            assert_eq!(res, k);
        };

        der_cmp(MvccKey::NextVersion, vec![0]);
        der_cmp(MvccKey::ActiveTransactions(1), vec![1, 0, 0, 0, 0, 0, 0, 0, 1]);
        der_cmp(
            MvccKey::Write(1, vec![1, 2, 3]),
            vec![2, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0, 0],
        );
        der_cmp(
            MvccKey::Version(b"abc".to_vec(), 11),
            vec![3, 97, 98, 99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11],
        );
    }
}