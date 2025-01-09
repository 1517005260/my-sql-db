# 运行所有测试

加上`RUSTFLAGS="-Awarnings"`以忽略所有警告：

```bash
(base) glk@ggg:~/my-sql-db$ RUSTFLAGS="-Awarnings" cargo test
   Compiling proc-macro2 v1.0.92
   Compiling unicode-ident v1.0.14
   Compiling libc v0.2.169
   Compiling autocfg v1.4.0
   Compiling cfg-if v1.0.0
   Compiling smallvec v1.13.2
   Compiling rustix v0.38.43
   Compiling pin-project-lite v0.2.16
   Compiling futures-core v0.3.31
   Compiling bitflags v2.6.0
   Compiling parking_lot_core v0.9.10
   Compiling futures-sink v0.3.31
   Compiling scopeguard v1.2.0
   Compiling linux-raw-sys v0.4.15
   Compiling memchr v2.7.4
   Compiling pin-utils v0.1.0
   Compiling cfg_aliases v0.2.1
   Compiling serde v1.0.217
   Compiling futures-io v0.3.31
   Compiling futures-task v0.3.31
   Compiling syn v1.0.109
   Compiling bytes v1.9.0
   Compiling futures-channel v0.3.31
   Compiling nix v0.29.0
   Compiling rustversion v1.0.19
   Compiling nibble_vec v0.1.0
   Compiling endian-type v0.1.2
   Compiling home v0.5.11
   Compiling once_cell v1.20.2
   Compiling log v0.4.22
   Compiling radix_trie v0.2.1
   Compiling lazy_static v1.5.0
   Compiling utf8parse v0.2.2
   Compiling heck v0.4.1
   Compiling fastrand v2.3.0
   Compiling unicode-width v0.2.0
   Compiling unicode-segmentation v1.12.0
   Compiling slab v0.4.9
   Compiling lock_api v0.4.12
   Compiling colored v2.2.0
   Compiling strum v0.24.1
   Compiling quote v1.0.38
   Compiling syn v2.0.95
   Compiling socket2 v0.5.8
   Compiling signal-hook-registry v1.4.2
   Compiling mio v1.0.3
   Compiling dirs-sys v0.3.7
   Compiling getrandom v0.2.15
   Compiling dirs v4.0.0
   Compiling parking_lot v0.12.3
   Compiling fd-lock v4.0.2
   Compiling tempfile v3.15.0
   Compiling fs4 v0.8.4
   Compiling strum_macros v0.24.3
   Compiling rustyline v15.0.0
   Compiling futures-macro v0.3.31
   Compiling tokio-macros v2.5.0
   Compiling serde_derive v1.0.217
   Compiling tokio v1.43.0
   Compiling futures-util v0.3.31
   Compiling futures-executor v0.3.31
   Compiling futures v0.3.31
   Compiling tokio-util v0.7.13
   Compiling tokio-stream v0.1.17
   Compiling bincode v1.3.3
   Compiling serde_bytes v0.11.15
   Compiling my-sql-db v0.1.0 (/home/glk/my-sql-db)
    Finished `test` profile [unoptimized + debuginfo] target(s) in 13.14s
     Running unittests src/lib.rs (target/debug/deps/my_sql_db-b6b8e78f9f7c446f)

running 43 tests
test sql::engine::kv::tests::test_create_table ... ok
test sql::engine::kv::tests::test_cross_join ... ok
test sql::engine::kv::tests::test_agg ... ok
test sql::parser::lexer::tests::test_lexer_create_table ... ok
test sql::engine::kv::tests::test_group_by ... ok
test sql::parser::lexer::tests::test_lexer_insert_into ... ok
test sql::parser::tests::test_parser_create_table ... ok
test sql::engine::kv::tests::test_hash_join ... ok
test sql::parser::lexer::tests::test_lexer_select ... ok
test sql::engine::kv::tests::test_filter ... ok
test sql::parser::tests::test_parser_select ... ok
test storage::disk::tests::test_disk_engine_compact_1 ... ok
test sql::planner::tests::test_plan_create_table ... ok
test sql::planner::tests::test_plan_insert ... ok
test sql::parser::tests::test_parser_insert ... ok
test sql::engine::kv::tests::test_primary_key_scan ... ok
test storage::keyencode::tests::test_decode ... ok
test sql::planner::tests::test_plan_select ... ok
test storage::disk::tests::test_disk_engine_start ... ok
test sql::engine::kv::tests::test_index ... ok
test sql::engine::kv::tests::test_insert ... ok
test storage::engine::tests::test_memory ... ok
test sql::engine::kv::tests::test_sort ... ok
test storage::disk::tests::test_disk_engine_compact_2 ... ok
test sql::parser::tests::test_parser_update ... ok
test sql::engine::kv::tests::test_join ... ok
test storage::engine::tests::test_disk ... ok
test storage::keyencode::tests::test_encode ... ok
test storage::keyencode::tests::test_encode_prefix ... ok
test storage::mvcc::tests::test_delete_conflict ... ok
test sql::engine::kv::tests::test_delete ... ok
test sql::engine::kv::tests::test_update ... ok
test storage::mvcc::tests::test_dirty_read ... ok
test storage::mvcc::tests::test_delete ... ok
test storage::mvcc::tests::test_get ... ok
test storage::mvcc::tests::test_get_isolation ... ok
test storage::mvcc::tests::test_phantom_read ... ok
test storage::mvcc::tests::test_rollback ... ok
test storage::mvcc::tests::test_prefix_scan ... ok
test storage::mvcc::tests::test_unrepeatable_read ... ok
test storage::mvcc::tests::test_scan_isolation ... ok
test storage::mvcc::tests::test_set ... ok
test storage::mvcc::tests::test_set_conflict ... ok

test result: ok. 43 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.01s

     Running unittests src/bin/client.rs (target/debug/deps/client-0df4e3ae35ff15a7)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running unittests src/bin/server.rs (target/debug/deps/server-ad2441328b165340)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

   Doc-tests my_sql_db

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```