# 事务key自定义编码代码实现

## Deserializer

1. 思路基本上同Serializer，只不过是反向的操作，可以参考[官网实现](https://serde.rs/impl-deserializer.html)，继续在keyencode.rs中：

```rust
pub struct Deserializer<'de> {   // 'de 更具语义性，表明生命周期与反序列化相关
    input: &'de [u8],  // 传入的是Vec<u8>
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(self, name: &'static str, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(self, name: &'static str, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_struct<V>(self, name: &'static str, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_enum<V>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        todo!()
    }
}
```

2. 将序列化时实现的方法全部在反序列化中实现

```rust
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

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(self)
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_seq(self)
    }

    fn deserialize_enum<V>(self, name: &'static str, variants: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>
    {
        visitor.visit_enum(self)
    }
}
```

这里又引入了新的错误，我们需要在error.rs中再次进行兼容：

```rust
// &[u8] -> Vec<u8> 相关错误
impl From<TryFromSliceError> for Error {
    fn from(value: TryFromSliceError) -> Self {
        Error::Internal(value.to_string())
    }
}
```

3. 递归地实现接口：

```rust
impl<'de, 'a> SeqAccess<'de> for Deserializer<'de> {
    type Error = Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>>
    where
        T: de::DeserializeSeed<'de>,
    {
        seed.deserialize(self).map(Some)  // map(Some) 将解析结果包装为 Option<T::Value>
    }
}

impl<'de, 'a> EnumAccess<'de> for &mut Deserializer<'de> {
    type Error = Error;

    type Variant = Self;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant)>
    where
        V: de::DeserializeSeed<'de>,
    {
        // 将枚举的字节码转换成枚举成员本身
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

    fn tuple_variant<V>(self, len: usize, visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        visitor.visit_seq(self)
    }

    fn struct_variant<V>(self, fields: &'static [&'static str], visitor: V) -> Result<V::Value>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}
```

4. 对外暴露通用接口函数：

```rust
pub fn deserialize_key<'a, T: serde::Deserialize<'a>>(input: &'a [u8]) -> Result<T> {
    let mut de = Deserializer { input };
    T::deserialize(&mut de)
}
```

5. 测试

```rust
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
```

这里涉及到了对MvccKey的比较，所以需要手动实现对应注解：

```rust
#[derive(Clone, Serialize, Deserialize,PartialEq,Debug)]
pub enum MvccKey{  // 和数据key类型区分
    NextVersion,   // 版本号
    ActiveTransactions(Version),  // 活跃事务版本号
    Write(Version,  #[serde(with = "serde_bytes")]Vec<u8>),     // 事务写入了哪些key
    Version( #[serde(with = "serde_bytes")]Vec<u8>, Version),  // (key, 所属version)
}
```

## 修改上层Mvcc的接口

由于这里我们实现了更高级的编码方法，遂对原来的mvcc.rs中的编码方法进行修改：

```rust
impl MvccKey{
    // 编码为二进制
    pub fn encode(&self) -> Result<Vec<u8>>{
        serialize_key(&self)
    }

    // 解码二进制
    pub fn decode(data: Vec<u8>) -> Result<Self> {
        deserialize_key(&data)
    }
}

impl MvccKeyPrefix {
    // 编码为二进制
    pub fn encode(&self) -> Result<Vec<u8>> {
        serialize_key(&self)
    }
}


// 之后全局替换 .encode() 为 .encode()? 解包 Result 即可
```