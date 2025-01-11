# 基准测试

前面在各个模块下的测试都是单元测试，现在引入 `cargo bench` 来进行基准测试

## 代码实现

参考：https://github.com/bheisler/criterion.rs

项目依赖：

```toml
[[bench]]
name = "sql_bench"
harness = false

[dependencies]
criterion = "0.5"
```

根据依赖，在项目根目录下（与src平级）新建benches/sql_bence.rs

这是官网的示例测试文件，我们在engine/kv.rs的单元测试上，进行改写：

```rust
use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};

fn fibonacci(n: u64) -> u64 {
    match n {
        0 => 1,
        1 => 1,
        n => fibonacci(n-1) + fibonacci(n-2),
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("fib 20", |b| b.iter(|| fibonacci(black_box(20))));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
```

我们的benchmark:

```rust
use criterion::{criterion_group, criterion_main, Criterion};
use my_sql_db::sql::engine::kv::KVEngine;
use my_sql_db::sql::engine::Engine;
use my_sql_db::storage::disk::DiskEngine;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::TempDir;

static COUNTER: AtomicUsize = AtomicUsize::new(0);  // 防止重复id

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
```

测试结果：

```bash
Running benches/sql_bench.rs (target/release/deps/sql_bench-9e662591716e112b)
Gnuplot not found, using plotters backend
=== Starting SQL Benchmarks ===
Created temp directory at: "/tmp/.tmpYpkNOQ"
Database file will be at: "/tmp/.tmpYpkNOQ/test.db"
Creating test table...
Inserting initial data...
Benchmarking INSERT...
SQL Operations/insert   time:   [18.931 µs 19.704 µs 20.960 µs]
                        change: [-3.7315% +4.7227% +14.719%] (p = 0.38 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild
Created temp directory at: "/tmp/.tmpsNPrYt"
Database file will be at: "/tmp/.tmpsNPrYt/test.db"
Creating test table...
Inserting initial data...
Benchmarking SELECT...
SQL Operations/select   time:   [11.320 µs 11.550 µs 11.767 µs]
                        change: [-8.0752% -2.6085% +1.5045%] (p = 0.42 > 0.05)
                        No change in performance detected.
Created temp directory at: "/tmp/.tmpYUbzPD"
Database file will be at: "/tmp/.tmpYUbzPD/test.db"
Creating test table...
Inserting initial data...
Benchmarking UPDATE...
SQL Operations/update   time:   [12.745 µs 12.933 µs 13.433 µs]
                        change: [-1.1438% +3.9095% +15.950%] (p = 0.56 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Created temp directory at: "/tmp/.tmprklFY8"
Database file will be at: "/tmp/.tmprklFY8/test.db"
Creating test table...
Inserting initial data...
Benchmarking DELETE...
SQL Operations/delete   time:   [40.531 µs 41.296 µs 42.197 µs]
                        change: [-17.942% -9.7935% -2.0730%] (p = 0.04 < 0.05)
                        Performance has improved.

=== SQL Benchmarks Completed ===
```

在target/criterion/report/index.html里可以看见测试的可视化报告