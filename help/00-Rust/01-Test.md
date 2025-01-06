# 运行所有测试

加上`RUSTFLAGS="-Awarnings"`以忽略所有警告：

```bash
(base) glk@ggg:~/project/my-sql-db$ RUSTFLAGS="-Awarnings" cargo test
    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.01s
     Running unittests src/lib.rs (target/debug/deps/my_sql_db-3521196fd52dc68f)

running 39 tests
test sql::engine::kv::tests::test_create_table ... ok
test sql::parser::lexer::tests::test_lexer_create_table ... ok
test sql::parser::lexer::tests::test_lexer_insert_into ... ok
test sql::parser::lexer::tests::test_lexer_select ... ok
test sql::engine::kv::tests::test_join ... ok
test sql::engine::kv::tests::test_cross_join ... ok
test sql::parser::tests::test_parser_create_table ... ok
test sql::engine::kv::tests::test_agg ... ok
test sql::parser::tests::test_parser_insert ... ok
test sql::engine::kv::tests::test_group_by ... ok
test sql::planner::tests::test_plan_select ... ok
test sql::parser::tests::test_parser_select ... ok
test sql::planner::tests::test_plan_create_table ... ok
test sql::parser::tests::test_parser_update ... ok
test storage::disk::tests::test_disk_engine_start ... ok
test storage::engine::tests::test_memory ... ok
test sql::engine::kv::tests::test_sort ... ok
test storage::keyencode::tests::test_encode ... ok
test storage::disk::tests::test_disk_engine_compact_1 ... ok
test storage::keyencode::tests::test_encode_prefix ... ok
test sql::planner::tests::test_plan_insert ... ok
test storage::keyencode::tests::test_decode ... ok
test sql::engine::kv::tests::test_insert ... ok
test sql::engine::kv::tests::test_update ... ok
test storage::mvcc::tests::test_dirty_read ... ok
test storage::engine::tests::test_disk ... ok
test sql::engine::kv::tests::test_delete ... ok
test storage::mvcc::tests::test_delete_conflict ... ok
test storage::mvcc::tests::test_get ... ok
test storage::mvcc::tests::test_get_isolation ... ok
test storage::mvcc::tests::test_delete ... ok
test storage::mvcc::tests::test_phantom_read ... ok
test storage::mvcc::tests::test_prefix_scan ... ok
test storage::mvcc::tests::test_unrepeatable_read ... ok
test storage::mvcc::tests::test_rollback ... ok
test storage::mvcc::tests::test_set_conflict ... ok
test storage::mvcc::tests::test_set ... ok
test storage::mvcc::tests::test_scan_isolation ... ok
test storage::disk::tests::test_disk_engine_compact_2 ... ok

test result: ok. 39 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

   Doc-tests my_sql_db

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```