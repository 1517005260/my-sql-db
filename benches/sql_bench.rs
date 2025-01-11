use criterion::{criterion_group, criterion_main, Criterion};
use my_sql_db::sql::engine::kv::KVEngine;
use my_sql_db::sql::engine::Engine;
use my_sql_db::storage::disk::DiskEngine;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::TempDir;

static COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn benchmark_operations(c: &mut Criterion) {
    println!("=== Starting SQL Benchmarks ===");

    COUNTER.store(0, Ordering::SeqCst);

    let mut group = c.benchmark_group("SQL Operations");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(1));
    group.warm_up_time(Duration::from_millis(500));

    // 数据库设置
    let setup_db = || {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        println!("Created temp directory at: {:?}", temp_dir.path());

        let db_file = temp_dir.path().join("test.db");
        println!("Database file will be at: {:?}", db_file);

        let kv_engine =
            KVEngine::new(DiskEngine::new(db_file).expect("Failed to create DiskEngine"));

        let mut session = kv_engine.session().expect("Failed to create session");

        println!("Creating test table...");
        session
            .execute("CREATE TABLE test (id INT PRIMARY KEY, value TEXT);")
            .expect("Failed to create table");

        println!("Inserting initial data...");
        for _ in 0..10 {
            let id = COUNTER.fetch_add(1, Ordering::SeqCst);
            session
                .execute(&format!(
                    "INSERT INTO test (id, value) VALUES ({}, 'value_{}');",
                    id, id
                ))
                .expect("Failed to insert initial data");
        }

        (temp_dir, session)
    };

    // INSERT 基准测试
    {
        let (_temp_dir, mut session) = setup_db();
        println!("Benchmarking INSERT...");
        group.bench_function("insert", |b| {
            b.iter(|| {
                let id = COUNTER.fetch_add(1, Ordering::SeqCst);
                session
                    .execute(&format!(
                        "INSERT INTO test (id, value) VALUES ({}, 'bench_{}');",
                        id, id
                    ))
                    .expect("Insert failed")
            })
        });
    }

    // SELECT 基准测试
    {
        let (_temp_dir, mut session) = setup_db();
        println!("Benchmarking SELECT...");
        group.bench_function("select", |b| {
            b.iter(|| {
                session
                    .execute("SELECT * FROM test WHERE id = 5;")
                    .expect("Select failed")
            })
        });
    }

    // UPDATE 基准测试
    {
        let (_temp_dir, mut session) = setup_db();
        println!("Benchmarking UPDATE...");
        group.bench_function("update", |b| {
            b.iter(|| {
                session
                    .execute("UPDATE test SET value = 'updated' WHERE id = 5;")
                    .expect("Update failed")
            })
        });
    }

    // DELETE 基准测试
    {
        let (_temp_dir, mut session) = setup_db();
        println!("Benchmarking DELETE...");
        group.bench_function("delete", |b| {
            let mut current_id = 0;
            b.iter(|| {
                // 先插入一条新记录
                current_id += 1;
                session
                    .execute(&format!(
                        "INSERT INTO test (id, value) VALUES ({}, 'to_delete');",
                        current_id + 1000 // 使用较大的ID避免冲突
                    ))
                    .expect("Setup insert failed");

                // 然后删除它
                session
                    .execute(&format!(
                        "DELETE FROM test WHERE id = {};",
                        current_id + 1000
                    ))
                    .expect("Delete failed")
            })
        });
    }

    group.finish();
    println!("=== SQL Benchmarks Completed ===");
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_secs(1))
        .warm_up_time(Duration::from_millis(500));
    targets = benchmark_operations
}
criterion_main!(benches);
