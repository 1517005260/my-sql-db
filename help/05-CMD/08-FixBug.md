# 数据库bug修复

1. 问题描述：

```bash
thread 'tokio-runtime-worker' panicked at src/sql/executor/mod.rs:73:57:
index out of bounds: the len is 1 but the index is 1
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
```

复现：

```sql
create table t (a text primary key); 
insert into table t values("aa"); 
select * from t;
-- 但是插入'aa'就是正常的
```

越界错误处理：

```rust
// executor/mod.rs

for a_row in rows {
    for(i, v) in a_row.iter().enumerate() {
        // 确保 i 在 max_len.len() 范围内
        if i < max_len.len() {
            if v.to_string().len() > max_len[i] {
                max_len[i] = v.to_string().len();
            }
        } else {
            // 如果发现列数不匹配，扩展 max_len
            max_len.push(v.to_string().len());
        }
    }
}
```

2. 相似表名混淆

问题复现：

```bash
sql-db>> show tables;
No tables found.
[Execution time: 745.538µs]
sql-db>> create table t (a int PRIMARY KEY);
CREATE TABLE t
[Execution time: 801.433µs]
sql-db>> create table t1 (b int PRIMARY KEY);
CREATE TABLE t1
[Execution time: 500.215µs]
sql-db>> insert into t1 values(1);
INSERT 1 rows
[Execution time: 820.223µs]
sql-db>> select * from t;
a
--
1
(1 rows)
[Execution time: 614.381µs]
sql-db>> select * from t1;
b
--
1
(1 rows)
[Execution time: 1.372424ms]
```

发现混淆了相似表名，我们在keycode中，serialize_str 的处理不正确]()

修改如下：

```rust
// keyencode.rs
fn serialize_str(self, v: &str) -> Result<()> {
    self.serialize_bytes(v.as_bytes())
}
```

#### 为什么会出现

原始的 `serialize_str` 函数直接将字符串字节追加到输出中：

```rust
fn serialize_str(self, v: &str) -> Result<()> {
    self.output.extend(v.as_bytes());
    Ok(())
}
```
- 问题：
    - 缺乏边界信息：没有明确的分隔符或长度前缀，导致反序列化时无法区分相似的字符串。
    - 混淆风险：例如，"t" 和 "t1" 被序列化为 `[0x74]` 和 `[0x74, 0x31]`，可能无法正确区分。

**改进方案：使用 `serialize_bytes`**

通过调用 `serialize_bytes` 来处理字节序列的序列化，`serialize_bytes` 添加了长度前缀等边界信息：

```rust
fn serialize_str(self, v: &str) -> Result<()> {
    self.serialize_bytes(v.as_bytes())
}
```

- 解决方案：
    - 明确边界：`serialize_bytes` 确保每个字节序列有清晰的边界信息，如长度前缀。
    - 防止混淆：即使表名相似，反序列化时也能准确区分不同的字符串。

##### 例子

- "t" 和 "t1" 原始序列化：
  - 序列化 "t"：`[0x74]`
  - 序列化 "t1"：`[0x74, 0x31]`
  - 反序列化时，如果没有边界信息，可能会将 `[0x74, 0x31]`误解析为 "t" 后跟一个额外的字符，导致混淆。

- "t" 和 "t1" 改进序列化：
    - 序列化 "t"：
        - 长度前缀：`0x01, 0x00, 0x00, 0x00`（长度 1，小端格式）
        - 字节内容：`0x74`
    - 序列化 "t1"：
        - 长度前缀：`0x02, 0x00, 0x00, 0x00`（长度 2）
        - 字节内容：`0x74, 0x31`
    - 反序列化时，系统通过读取长度前缀，能准确区分 "t" 和 "t1"，避免了混淆。


3. 顺手修复keyencode中的warning问题，即未使用的变量需要加`_`前缀

此外还可以根据编译器提示去移除不必要的mut

4. Client端因为ctrl c或者ctrl d退出时，应该回滚已经开始的事务

在client.rs中：

```rust
impl Drop for Client{
    fn drop(&mut self) {
        if self.transaction_version.is_some() {
            futures::executor::block_on(self.exec_cmd("ROLLBACK;")).expect("rollback failed");
        }
    }
}
```