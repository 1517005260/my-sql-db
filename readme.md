# 基于Rust实现关系型数据库

开发笔记详见[help](./help)

## 使用示例

### 运行

环境检查：

```bash
(base) glk@ggg:~/project/my-sql-db$ rustc --version
rustc 1.81.0 (eeb90cda1 2024-09-04)
(base) glk@ggg:~/project/my-sql-db$ cargo --version
cargo 1.81.0 (2dbb1af80 2024-08-20)
```

填写.env文件：

```env
API_URL=https://yunwu.ai/v1/chat/completions
API_KEY=sk-***
MODEL=gpt-4o
```

服务端启动：

```bash
(base) glk@ggg:~/project/my-sql-db$ cargo run --bin server
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.19s
     Running `target/debug/server`
SQL DB starts, server is listening on: 127.0.0.1:8080
```

客户端启动：

```bash
(base) glk@ggg:~/project/my-sql-db$ cargo run --bin client
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.08s
     Running `target/debug/client`
sql-db>>
```

### SQL 示例

1. 创建表：

```bash
sql-db>> create table t1 ( a int primary key, b text default 'vv', c integer default 100 );
CREATE TABLE t1
[Execution time: 657.595µs]
sql-db>> create table t2 ( a int primary key, b integer default 100, c float default 1.1, d bool default false, e boolean default true, f text default 'v1', g string default 'v2', h varchar default 'v3' );
CREATE TABLE t2
[Execution time: 603.554µs]
sql-db>> create table t3 ( a int primary key, b int default 12 null, c integer default NULL, d float not NULL );
CREATE TABLE t3
[Execution time: 558.908µs]
sql-db>> create table t4 ( a bool primary key, b int default 12, d boolean default true );
CREATE TABLE t4
[Execution time: 546.284µs]
```

2. 插入数据：

```bash
sql-db>> insert into t1 (a) values (1);
INSERT 1 rows
[Execution time: 1.041524ms]
sql-db>> insert into t1 values (2, 'a', 2);
INSERT 1 rows
[Execution time: 615.527µs]
sql-db>> insert into t1(b,a) values ('b', 3);
INSERT 1 rows
[Execution time: 678.207µs]
sql-db>> select * from t1;
a |b  |c
--+---+----
1 |vv |100
2 |a  |2
3 |b  |100
(3 rows)
[Execution time: 677.234µs]
sql-db>> insert into t2 (a) values (1);
INSERT 1 rows
[Execution time: 636.796µs]
sql-db>> select * from t2;
a |b   |c   |d     |e    |f  |g  |h
--+----+----+------+-----+---+---+---
1 |100 |1.1 |FALSE |TRUE |v1 |v2 |v3
(1 rows)
[Execution time: 697.497µs]
sql-db>> insert into t3 (a, d) values (1, 1.1);
INSERT 1 rows
[Execution time: 710.597µs]
sql-db>> select * from t3;
a |b  |c    |d
--+---+-----+----
1 |12 |NULL |1.1
(1 rows)
[Execution time: 639.331µs]
sql-db>> insert into t4 (a) values (true);
INSERT 1 rows
[Execution time: 702.903µs]
sql-db>> select * from t4;
a    |b  |d
-----+---+-----
TRUE |12 |TRUE
(1 rows)
[Execution time: 591.925µs]
```

3. Update / Delete 测试：

```bash
sql-db>> update t4 set a = false where b=12;
UPDATE 1 rows
[Execution time: 808.294µs]
sql-db>> select * from t4;
a     |b  |d
------+---+-----
FALSE |12 |TRUE
(1 rows)
[Execution time: 759.832µs]
sql-db>> delete from t4 where d=true;
DELETE 1 rows
[Execution time: 746.273µs]
sql-db>> select * from t4;
a |b |d
--+--+--

(0 rows)
[Execution time: 666.484µs]
```

4. Drop Table / Show Tables / Show Table 语句测试：

```bash
sql-db>> show tables;
t1
t2
t3
t4
[Execution time: 782.595µs]
sql-db>> show table t2;
TABLE NAME: t2 (
  a Integer PRIMARY KEY ,
  b Integer DEFAULT 100,
  c Float DEFAULT 1.1,
  d Boolean DEFAULT FALSE,
  e Boolean DEFAULT TRUE,
  f String DEFAULT v1,
  g String DEFAULT v2,
  h String DEFAULT v3
)
[Execution time: 758.028µs]
sql-db>> drop table t4;
DROP TABLE t4
[Execution time: 656.555µs]
sql-db>> show table t4;
Internal Error: [Get Table] Table " t4 " does not exist
[Execution time: 471.01µs]
sql-db>> show tables;
t1
t2
t3
[Execution time: 603.002µs]
```

5. 表达式计算

```bash
sql-db>> select * from t1;
a |b  |c
--+---+----
1 |vv |100
2 |a  |2
3 |b  |100
(3 rows)
[Execution time: 804.686µs]
sql-db>> select * from t1 where c = 2^3 * (8 + 1) + 28;
a |b  |c
--+---+----
1 |vv |100
3 |b  |100
(2 rows)
[Execution time: 721.406µs]
```

6. 复杂Select示例：

```bash
sql-db>> show tables;
t1
t2
t3
[Execution time: 582.353µs]
sql-db>> select * from t1;
a |b  |c  
--+---+----
1 |vv |100
2 |a  |2
3 |b  |100
(3 rows)
[Execution time: 627.836µs]
sql-db>> select * from t2;
a |b   |c   |d     |e    |f  |g  |h
--+----+----+------+-----+---+---+---
1 |100 |1.1 |FALSE |TRUE |v1 |v2 |v3
(1 rows)
[Execution time: 625.684µs]
sql-db>> select * from t3;
a |b  |c    |d
--+---+-----+----
1 |12 |NULL |1.1
(1 rows)
[Execution time: 592.023µs]
sql-db>> select * from t1 cross join t2;
a |b  |c   |a |b   |c   |d     |e    |f  |g  |h
--+---+----+--+----+----+------+-----+---+---+---
1 |vv |100 |1 |100 |1.1 |FALSE |TRUE |v1 |v2 |v3
2 |a  |2   |1 |100 |1.1 |FALSE |TRUE |v1 |v2 |v3
3 |b  |100 |1 |100 |1.1 |FALSE |TRUE |v1 |v2 |v3
(3 rows)
[Execution time: 797.002µs]
sql-db>> select a,b from t1 cross join t2;
a |b
--+---
1 |vv
2 |a
3 |b
(3 rows)
[Execution time: 1.254869ms]
sql-db>> select a,b from t1 cross join t2 order by c desc;
a |b
--+---
1 |vv
3 |b
2 |a
(3 rows)
[Execution time: 1.241003ms]
sql-db>> select * from t1 join t2 on a=a;
a |b  |c   |a |b   |c   |d     |e    |f  |g  |h
--+---+----+--+----+----+------+-----+---+---+---
1 |vv |100 |1 |100 |1.1 |FALSE |TRUE |v1 |v2 |v3
(1 rows)
[Execution time: 1.010861ms]
sql-db>> select * from t1 left join t2 on a=a;
a |b  |c   |a    |b    |c    |d     |e    |f    |g    |h
--+---+----+-----+-----+-----+------+-----+-----+-----+-----
1 |vv |100 |1    |100  |1.1  |FALSE |TRUE |v1   |v2   |v3
2 |a  |2   |NULL |NULL |NULL |NULL  |NULL |NULL |NULL |NULL
3 |b  |100 |NULL |NULL |NULL |NULL  |NULL |NULL |NULL |NULL
(3 rows)
[Execution time: 886.063µs]
```

聚合函数、Limit/Offset 测试：

```bash
sql-db>> create table t (a int primary key, b text, c float);
CREATE TABLE t
[Execution time: 714.964µs]
sql-db>> insert into t values (1, 'aa', 3.1), (2, 'bb', 5.3), (3, null, null), (4, null, 4.6), (5, 'bb', 5.8), (6, 'dd', 1.4);
INSERT 6 rows
[Execution time: 915.017µs]
sql-db>> select * from t;
a |b    |c
--+-----+-----
1 |aa   |3.1
2 |bb   |5.3
3 |NULL |NULL
4 |NULL |4.6
5 |bb   |5.8
6 |dd   |1.4
(6 rows)
[Execution time: 785.571µs]
sql-db>> select b, min(c), max(a), avg(c) from t group by b order by avg;
b    |min |max |avg
-----+----+----+-----
dd   |1.4 |6   |1.4
aa   |3.1 |1   |3.1
NULL |4.6 |4   |4.6
bb   |5.3 |5   |5.55
(4 rows)
[Execution time: 2.982239ms]
sql-db>> select b, min(c), max(a), avg(c) from t group by b order by avg limit 1 offset 1;
b  |min |max |avg
---+----+----+----
aa |3.1 |1   |3.1
(1 rows)
[Execution time: 880.367µs]
```

7. Explain 执行计划

```bash
sql-db>> explain select b, min(c), max(a), avg(c) from t group by b order by avg limit 1 offset 1;
           SQL PLAN
------------------------------
Limit 1
 -> Offset 1
  -> Order By avg Asc
   -> Aggregate b , min(c) , max(a) , avg(c)  Group By b
    -> Sequence Scan On Table t
[Execution time: 795.291µs]
```

8. 事务

```bash
sql-db>> begin;
TRANSACTION 55 BEGIN
[Execution time: 956.798µs]
transaction#55>> commit;
TRANSACTION 55 COMMIT
[Execution time: 462.293µs]
sql-db>> begin;
TRANSACTION 56 BEGIN
[Execution time: 789.678µs]
transaction#56>> insert into t values(0, 'zz', 3.1);
INSERT 1 rows
[Execution time: 732.62µs]
transaction#56>> select * from t;
a |b    |c
--+-----+-----
0 |zz   |3.1
1 |aa   |3.1
2 |bb   |5.3
3 |NULL |NULL
4 |NULL |4.6
5 |bb   |5.8
6 |dd   |1.4
(7 rows)
[Execution time: 795.147µs]
transaction#56>> rollback;
TRANSACTION 56 ROLLBACK
[Execution time: 457.474µs]
sql-db>> select * from t;
a |b    |c
--+-----+-----
1 |aa   |3.1
2 |bb   |5.3
3 |NULL |NULL
4 |NULL |4.6
5 |bb   |5.8
6 |dd   |1.4
(6 rows)
[Execution time: 992.965µs]
```

9. AI推荐：

基于每次启动客户端的sql历史数据推荐下一条最可能的sql。

现在删除存储文件（`$PROJECT$/tmp/sqldb-test/log`），重新开始：

```bash
sql-db>> create table t (a int primary key, b text, c float);
CREATE TABLE t
[Execution time: 1.949747ms]
sql-db>> insert into t values (1, 'aa', 3.1), (2, 'bb', 5.3), (3, null, null), (4, null, 4.6), (5, 'bb', 5.8), (6, 'dd', 1.4);
INSERT 6 rows
[Execution time: 1.375558ms]
sql-db>> show table t;
TABLE NAME: t (
  a Integer PRIMARY KEY ,
  b String DEFAULT NULL,
  c Float DEFAULT NULL
)
[Execution time: 1.355694ms]
sql-db>> AI;
SQL recommend by AI: 
     select * from t;
[Execution time: 1.219715004s]
sql-db>> select * from t;
a |b    |c   
--+-----+-----
1 |aa   |3.1
2 |bb   |5.3
3 |NULL |NULL
4 |NULL |4.6
5 |bb   |5.8
6 |dd   |1.4
(6 rows)
[Execution time: 774.569µs]
sql-db>> AI;
SQL recommend by AI: 
     select * from t where b is not null;
[Execution time: 1.19555074s]
```

但是由于自己写的数据库，语法跟主流数据库不是很对的齐，所以推荐成功的概率没有那么高。