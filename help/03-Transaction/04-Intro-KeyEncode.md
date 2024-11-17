# 事务Key自定义编码

对于我们自定义的事务key：

```rust
#[derive(Clone, Serialize, Deserialize)]
pub enum MvccKey{
    NextVersion,
    ActiveTransactions(Version), 
    Write(Version, Vec<u8>),
    Version(Vec<u8>, Version),
}
```

如果传入的key是可以在栈内存放的，比如i32，那么使用我们之前的代码是可以完成编码的：

```rust
let k1 = MvccKey::TxnActive(101);
println!("{:?}", bincode::serialize(&k1)?); // [1, 0, 0, 0, 101, 0, 0, 0, 0, 0, 0, 0]
let k2 = MvccKeyPrefix::TxnActive;
println!("{:?}", bincode::serialize(&k2)?); // [1, 0, 0, 0]
```

但是如果key是字符串等需要涉及堆存储的数据结构，那么目前的代码就会出问题：

```rust
let k1 = MvccKey::Version(b"key111".to_vec(), 101);
println!("{:?}", k1.encode());
// [3, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 101, 121, 49, 49, 49, 101, 121, 0, 0, 0, 0, 0]

let k2 = MvccKeyPrefix::Version(b"key".to_vec());
println!("{:?}", k2.encode());
// [3, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 101, 121]

// 这里我们期望应该是：
// [3, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 101, 121]
//              x
```

可以发现目前的代码不能很好地处理key是字符串等地情况，所以我们需要自定义编码来解决这个问题。

## 使用Serde

Serde是Rust的知名序列化和反序列化第三方库，例如，serde_json就可以将数据序列化为json：

```rust
#[derive(Serialize, Deserialize)]
struct Address {
    street: String,
    city: String,
}

fn print_an_address() -> Result<()> {
    // 一些数据
    let address = Address {
        street: "10 Downing Street".to_owned(),
        city: "London".to_owned(),
    };

    // 将其序列化为 JSON 字符串
    let j = serde_json::to_string(&address)?;

    // 打印、写入文件或发送到 HTTP 服务器
    println!("{}", j);

    Ok(())
}
```

除了json之外，我们还能根据自己的需求，自定义序列化逻辑，完成事务的自定义key编码。

serde的类型系统，这是与rust映射的：

- **14种原始类型**
    - `bool`
    - `i8`, `i16`, `i32`, `i64`, `i128`
    - `u8`, `u16`, `u32`, `u64`, `u128`
    - `f32`, `f64`
    - `char`

- **字符串 (string)**
    - UTF-8 字节，带有长度，无空终止符。可能包含 0 字节。
    - 在序列化时，所有字符串处理方式相同。在反序列化时，有三种风格：临时字符串、拥有的字符串和借用的字符串。这种区分在 [理解反序列化生命周期](https://docs.rs/serde/latest/serde/) 中有所解释，并且是 Serde 高效零拷贝反序列化的关键。

- **字节数组 (byte array)** - `[u8]`
    - 类似于字符串，在反序列化过程中，字节数组可以是临时的、拥有的或借用的。

- **选项 (option)**
    - 要么没有值，要么有某个值。

- **单位类型 (unit)**
    - Rust 中 `()` 的类型。它表示一个匿名值，且不包含任何数据。

- **单位结构体 (unit_struct)**
    - 示例：`struct Unit` 或 `PhantomData<T>`。它表示一个命名值，但不包含任何数据。

- **单位变体 (unit_variant)**
    - 示例：`E::A` 和 `E::B` 在 `enum E { A, B }` 中。

- **新类型结构体 (newtype_struct)**
    - 示例：`struct Millimeters(u8)`。

- **新类型变体 (newtype_variant)**
    - 示例：`E::N` 在 `enum E { N(u8) }` 中。

- **序列 (seq)**
    - 一种可变大小的异构值序列，例如 `Vec<T>` 或 `HashSet<T>`。
    - 序列化时，长度可能在迭代数据之前未知。
    - 反序列化时，通过查看序列化数据来确定长度。注意，同质 Rust 集合（如 `vec![Value::Bool(true), Value::Char('c')]`）可能会序列化为异构的 Serde 序列，在这种情况下，它包含一个 Serde 布尔值后跟一个 Serde 字符。

- **元组 (tuple)**
    - 静态大小的异构值序列，其长度在序列化时已知。例如 `(u8,)` 或 `(String, u64, Vec<T>)` 或 `[u64; 10]`。

- **元组结构体 (tuple_struct)**
    - 一个具名元组，例如 `struct Rgb(u8, u8, u8)`。

- **元组变体 (tuple_variant)**
    - 示例：`E::T` 在 `enum E { T(u8, u8) }` 中。

- **映射 (map)**
    - 可变大小的异构键值对，例如 `BTreeMap<K, V>`。
    - 序列化时，长度可能在迭代所有条目之前未知。
    - 反序列化时，通过查看序列化数据来确定长度。

- **结构体 (struct)**
    - 一个静态大小的异构键值对，其中键是编译时常量字符串，并且无需查看序列化数据即可在反序列化时知道其长度。例如：`struct S { r: u8, g: u8, b: u8 }`。

- **结构体变体 (struct_variant)**
    - 示例：`E::S` 在 `enum E { S { r: u8, g: u8, b: u8 } }` 中。

[官方自定义demo](https://serde.rs/impl-serializer.html)

### 自定义序列化

我们需要序列化的事务key：

```rust
pub enum MvccKey{
    NextVersion,
    ActiveTransactions(Version), 
    Write(Version, Vec<u8>),
    Version(Vec<u8>, Version),
}

pub enum MvccKeyPrefix{  
    NextVersion,
    ActiveTransactions, 
    Write(Version),
}
```

对应一下Serde中的数据类型，我们需要实现：

```
u64 - version
bytes - 字节数组（转换为二进制的key、value等）
unit_variant - 无附加的枚举类型，如NextVersion
newtype_variant - ActiveTransactions(Version) 这种有附加定义 自定义数据类型 的枚举类型
tuple_variant - Write(Version, Vec<u8>) 这种附加元组的枚举类型
seq - Vec<u8>
tuple - (Version, Vec<u8>)
```

而对于Vec<u8>，相比于seq的处理，[serde_bytes](https://crates.io/crates/serde_bytes)提供了一种更高效的方法，即将Vec<u8>映射为serde_bytes::ByteBuf，这样serde处理时可以更加高效。

所以我们新引入该cargo包：

```toml
[dependencies]
serde_bytes = "0.11.15"
```

然后根据包要求，修改mvcc.rs中一些enum的定义：

```rust
#[derive(Clone, Serialize, Deserialize)]
pub enum MvccKey{  // 和数据key类型区分
    NextVersion,   // 版本号
    ActiveTransactions(Version),  // 活跃事务版本号
    Write(Version,  #[serde(with = "serde_bytes")]Vec<u8>),     // 事务写入了哪些key
    Version( #[serde(with = "serde_bytes")]Vec<u8>, Version),  // (key, 所属version)
}

#[derive(Clone, Serialize, Deserialize)]
pub enum MvccKeyPrefix{  // MvccKey的前缀，用于扫描活跃事务
  NextVersion,   // 版本号前缀
  ActiveTransactions, // 活跃事务前缀
  Write(Version),    // 事务写信息前缀
  Version(#[serde(with = "serde_bytes")] Vec<u8>),
}
```

### `Vec<u8>`的处理

写了这么多，核心的问题其实就是，我们希望MvccKeyPrefix编码后的值，是MvccKey的前缀。但是如下的编码方式，会在解码时遇到问题：

```rust
let k1 = MvccKey::Version(b"abc".to_vec(), 101);
// 3, 97, 98, 99, 101  Version编码为3

let k2 = MvccKeyPrefix::Version(b"a".to_vec());
// 3, 97
```

这里，我们存储了(abc, 101)的(key,version)键，肉眼可以看出97, 98, 99对应abc，但是解码的时候，我们并不知道abc字符串的长度，所以无法判断什么时候终止解析。

一个容易想到的办法是：加一个定长数组len来记录长度：

```rust
let k1 = MvccKey::Version(b"abc".to_vec(), 101);
// 3, 0, 0, 0, 3, 97, 98, 99, 101
// Version     3  abc         101
```

但是本方法对于前缀则行不通：

```rust
let k2 = MvccKeyPrefix::Version(b"a".to_vec());
// 3, 0, 0, 0, 1, 97
// Version     1  a
```

对前缀使用相同方式编码，则编码后的值不匹配。

#### 一种两全其美的方法

对于u8类型的Vec，我们采用结尾标识符来表示结束，由于u8∈[0,255]，所以我们采用如下方式来编码Vec：

```
结尾： 0 0 
原本的0 ：0 255
```

按照这个规则，我们可以举点例子：

```
原始值           编码后
97 98 99     -> 97 98 99 0 0
97 98 0 99   -> 97 98 0 255 99 0 0
97 98 0 0 99 -> 97 98 0 255 0 255 99 0 0
```

它们对应的前缀（去除额外添加的结尾标识）：

```
原始
97 98 99 -> 97 98 99 0 0
前缀
97 -> 97 0 0 -> 97

原始
97 0 98 99 -> 97 0 255 98 99 0 0
前缀
97 0 -> 97 0 255 0 0 -> 97 0 255
```