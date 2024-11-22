# 运行所有测试

加上`RUSTFLAGS="-Awarnings"`以忽略所有警告：

```bash
(base) glk@ggg:~/project/my-sql-db$ RUSTFLAGS="-Awarnings" cargo test
   Compiling proc-macro2 v1.0.89
   Compiling unicode-ident v1.0.13
   Compiling serde v1.0.214
   Compiling rustix v0.38.40
   Compiling linux-raw-sys v0.4.14
   Compiling bitflags v2.6.0
   Compiling fastrand v2.2.0
   Compiling cfg-if v1.0.0
   Compiling once_cell v1.20.2
   Compiling quote v1.0.37
   Compiling syn v2.0.87
   Compiling fs4 v0.8.4
   Compiling tempfile v3.14.0
   Compiling serde_derive v1.0.214
   Compiling bincode v1.3.3
   Compiling serde_bytes v0.11.15
   Compiling my-sql-db v0.1.0 (/home/glk/project/my-sql-db)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 7.66s
     Running unittests src/lib.rs (target/debug/deps/my_sql_db-3521196fd52dc68f)

running 30 tests
test sql::parser::lexer::tests::test_lexer_create_table ... ok
test sql::parser::lexer::tests::test_lexer_select ... ok
test sql::parser::lexer::tests::test_lexer_insert_into ... ok
test sql::parser::tests::test_parser_create_table ... ok
test sql::parser::tests::test_parser_insert ... ok
test sql::parser::tests::test_parser_select ... ok
test sql::engine::kv::tests::test_create_table ... ok
test sql::planner::tests::test_plan_insert ... ok
test sql::planner::tests::test_plan_create_table ... ok
test sql::planner::tests::test_plan_select ... ok
test storage::disk::tests::test_disk_engine_compact_1 ... ok
test storage::disk::tests::test_disk_engine_start ... ok
test storage::keyencode::tests::test_encode ... ok
test storage::engine::tests::test_memory ... ok
test storage::keyencode::tests::test_decode ... ok
test storage::keyencode::tests::test_encode_prefix ... ok
test storage::disk::tests::test_disk_engine_compact_2 ... ok
test storage::engine::tests::test_disk ... ok
test storage::mvcc::tests::test_dirty_read ... ok
test storage::mvcc::tests::test_delete ... ok
test storage::mvcc::tests::test_delete_conflict ... ok
test storage::mvcc::tests::test_get ... ok
test storage::mvcc::tests::test_phantom_read ... ok
test storage::mvcc::tests::test_get_isolation ... ok
test storage::mvcc::tests::test_prefix_scan ... ok
test storage::mvcc::tests::test_rollback ... ok
test storage::mvcc::tests::test_scan_isolation ... ok
test storage::mvcc::tests::test_set ... ok
test storage::mvcc::tests::test_unrepeatable_read ... ok
test storage::mvcc::tests::test_set_conflict ... ok

test result: ok. 30 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests my_sql_db

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```