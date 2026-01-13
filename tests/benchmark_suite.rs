use rustmemodb::InMemoryDB;
use std::time::{Duration, Instant};

#[tokio::test]
async fn comprehensive_performance_benchmark() {
    println!("\n========================================================================");
    println!("🚀 RUSTMEMODB COMPREHENSIVE PERFORMANCE BENCHMARK");
    println!("========================================================================");

    let mut db = InMemoryDB::new();
    let row_count = 50_000; 
    let join_row_count = 10_000;

    // ========================================================================
    // 1. INSERT Performance (Testing O(1) Uniqueness Checks)
    // ========================================================================
    println!("\n[1/5] Testing INSERT performance with PRIMARY KEY (Checking O(1) Constraint)...");
    
    // Create table with Primary Key (automatically indexed)
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT UNIQUE)").await.unwrap();

    let start_insert = Instant::now();
    for i in 0..row_count {
        // Using formatted string which adds some overhead, but realistic
        let sql = format!("INSERT INTO users VALUES ({}, 'User {}', 'user{}@example.com')", i, i, i);
        db.execute(&sql).await.unwrap();
    }
    let duration_insert = start_insert.elapsed();
    let ops_per_sec = row_count as f64 / duration_insert.as_secs_f64();
    
    println!("✅ Inserted {} rows in {:?}", row_count, duration_insert);
    println!("⚡ Throughput: {:.2} inserts/sec", ops_per_sec);
    
    // Expectation: If this was O(N), 50,000^2 would take minutes/hours. 
    // It should finish in < 2 seconds.
    assert!(duration_insert.as_secs() < 10, "Insert performance is too slow! O(N) check might be active.");

    // ========================================================================
    // 2. SELECT / SCAN Performance (Testing Zero-Copy Scan)
    // ========================================================================
    println!("\n[2/5] Testing Full Table Scan (Read Performance)...");
    
    let start_scan = Instant::now();
    let result = db.execute("SELECT * FROM users").await.unwrap();
    let duration_scan = start_scan.elapsed();
    
    assert_eq!(result.row_count(), row_count as usize);
    println!("✅ Scanned {} rows in {:?}", row_count, duration_scan);
    
    // ========================================================================
    // 3. INDEX LOOKUP Performance
    // ========================================================================
    println!("\n[3/5] Testing Point Lookup via Index...");
    
    let target_id = row_count / 2;
    let start_lookup = Instant::now();
    let result = db.execute(&format!("SELECT * FROM users WHERE id = {}", target_id)).await.unwrap();
    let duration_lookup = start_lookup.elapsed();
    
    assert_eq!(result.row_count(), 1);
    println!("✅ Point lookup in {:?}", duration_lookup);
    assert!(duration_lookup < Duration::from_millis(5), "Index lookup too slow");

    // ========================================================================
    // 4. JOIN Performance (Testing Hash Join)
    // ========================================================================
    println!("\n[4/5] Testing JOIN Performance (Hash Join)...");
    
    // Create second table
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount FLOAT)").await.unwrap();
    
    println!("   -> Generating {} orders...", join_row_count);
    for i in 0..join_row_count {
        db.execute(&format!("INSERT INTO orders VALUES ({}, {}, 99.99)", i, i)).await.unwrap();
    }

    let start_join = Instant::now();
    let result = db.execute("SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id").await.unwrap();
    let duration_join = start_join.elapsed();
    
    assert_eq!(result.row_count(), join_row_count as usize);
    println!("✅ Joined {}x{} tables (Result: {} rows) in {:?}", row_count, join_row_count, join_row_count, duration_join);
    
    // Expectation: Hash Join should be very fast (< 1s). Nested Loop would be 50k * 10k = 500M ops (slow).
    assert!(duration_join.as_secs() < 2, "Join is too slow! Hash Join might be broken.");

    // ========================================================================
    // 5. UPDATE & VACUUM Performance (Testing MVCC & GC)
    // ========================================================================
    println!("\n[5/5] Testing MVCC Overhead and Vacuum...");
    
    // Churn a single row 2000 times
    let updates = 2000;
    println!("   -> Updating a row {} times...", updates);
    let start_update = Instant::now();
    for i in 0..updates {
        db.execute(&format!("UPDATE users SET name = 'Updated {}' WHERE id = 0", i)).await.unwrap();
    }
    println!("   -> Updates finished in {:?}", start_update.elapsed());

    // Vacuum
    let start_vacuum = Instant::now();
    let freed = db.vacuum().await.unwrap();
    let duration_vacuum = start_vacuum.elapsed();
    
    println!("✅ Vacuumed {} versions in {:?}", freed, duration_vacuum);
    assert_eq!(freed, updates, "Vacuum should free exactly the number of overwritten versions");

    println!("\n========================================================================");
    println!("🎉 BENCHMARK COMPLETE: ALL SYSTEMS NOMINAL");
    println!("========================================================================");
}
