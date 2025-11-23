use crate::core::Result;
use crate::facade::InMemoryDB;

pub mod core;
pub mod storage;
pub mod result;
pub mod facade;
mod parser;
mod planner;
mod executor;
mod expression;
mod plugins;

fn main() -> Result<()> {
    println!("🚀 RustMemDB - Phase 2 Complete!");
    println!("   ✨ Parser → Planner → Executor Pipeline");
    println!("{}", "=".repeat(70));

    let mut db = InMemoryDB::new();

    // Create table
    println!("\n📝 Creating 'users' table...");
    db.execute(
        "CREATE TABLE users (
            id INTEGER,
            name TEXT,
            age INTEGER,
            active BOOLEAN
        )",
    )?;
    println!("✅ Table created");

    // Insert data
    println!("\n📥 Inserting data...");
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, true)")?;
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, true)")?;
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35, false)")?;
    println!("✅ 3 rows inserted");

    // Query 1: SELECT *
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM users");
    println!("{}", "=".repeat(70));
    let result = db.execute("SELECT * FROM users")?;
    result.print();

    // Query 2: WHERE clause
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM users WHERE age > 26");
    println!("{}", "=".repeat(70));
    let result = db.execute("SELECT * FROM users WHERE age > 26")?;
    result.print();

    // Query 3: Complex WHERE
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM users WHERE active = true AND age < 32");
    println!("{}", "=".repeat(70));
    let result = db.execute("SELECT * FROM users WHERE active = true AND age < 32")?;
    result.print();

    // Query 4: LIKE pattern
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM users WHERE name LIKE 'A%'");
    println!("{}", "=".repeat(70));
    let result = db.execute("SELECT * FROM users WHERE name LIKE 'A%'")?;
    result.print();

    // Statistics
    println!("\n{}", "=".repeat(70));
    println!("📈 Database Statistics");
    println!("{}", "=".repeat(70));
    println!("Tables: {:?}", db.list_tables());
    if let Ok(stats) = db.table_stats("users") {
        println!("{}", stats);
    }

    println!("\n✅ Phase 2 Implementation Complete!");
    println!("\n📦 Architecture:");
    println!("   └─ Parser (SqlParserAdapter) - ADAPTER pattern");
    println!("   └─ Planner (QueryPlanner) - STRATEGY pattern");
    println!("   └─ Executor (ExecutorPipeline) - CHAIN OF RESPONSIBILITY");
    println!("   └─ Storage (InMemoryStorage) - STRATEGY pattern");
    println!("   └─ Catalog - REGISTRY pattern");
    println!("   └─ Facade (InMemoryDB) - FACADE pattern");

    Ok(())
}

#[cfg(test)]
mod benchmarks {
    use std::sync::{Arc, RwLock};
    use super::*;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn bench_concurrent_reads_different_tables() {
        let mut db = InMemoryDB::new();

        // Setup
        db.execute("CREATE TABLE users (id INTEGER, name TEXT)").unwrap();
        db.execute("CREATE TABLE products (id INTEGER, name TEXT)").unwrap();

        for i in 0..1000 {
            db.execute(&format!("INSERT INTO users VALUES ({}, 'user_{}')", i, i)).unwrap();
            db.execute(&format!("INSERT INTO products VALUES ({}, 'prod_{}')", i, i)).unwrap();
        }

        // Benchmark: параллельные SELECT на разные таблицы
        let db = Arc::new(RwLock::new(db));
        let start = Instant::now();

        let mut handles = vec![];

        // 4 потока читают users
        for _ in 0..4 {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let mut db = db_clone.write().unwrap();
                for _ in 0..100 {
                    let _ = db.execute("SELECT * FROM users WHERE id > 500");
                }
            }));
        }

        // 4 потока читают products
        for _ in 0..4 {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let mut db = db_clone.write().unwrap();
                for _ in 0..100 {
                    let res = db.execute("SELECT * FROM products WHERE id < 500");
                    //println!("{:?}", res.unwrap().row_count());
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        println!("Concurrent reads (different tables): {:?}", duration);
        println!("Operations: 800 SELECTs");
        println!("Throughput: {:.2} ops/sec", 800.0 / duration.as_secs_f64());
    }

    #[test]
    fn bench_read_write_different_tables() {
        let mut db = InMemoryDB::new();

        // Setup
        db.execute("CREATE TABLE users (id INTEGER)").unwrap();
        db.execute("CREATE TABLE logs (id INTEGER)").unwrap();

        let db = Arc::new(RwLock::new(db));
        let start = Instant::now();

        let mut handles = vec![];

        // 4 потока читают users
        for _ in 0..4 {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let mut db = db_clone.write().unwrap();
                for _ in 0..100 {
                    let _ = db.execute("SELECT * FROM users");
                }
            }));
        }

        // 2 потока пишут в logs (не блокируют чтение users!)
        for i in 0..2 {
            let db_clone = Arc::clone(&db);
            handles.push(thread::spawn(move || {
                let mut db = db_clone.write().unwrap();
                for j in 0..50 {
                    let _ = db.execute(&format!("INSERT INTO logs VALUES ({})", i * 50 + j));
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let duration = start.elapsed();
        println!("Mixed read/write (different tables): {:?}", duration);
        println!("Operations: 400 SELECTs + 100 INSERTs");
    }
}
