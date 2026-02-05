use rustmemodb::InMemoryDB;
use std::time::Instant;

mod perf_utils;
mod perf_wal;
mod perf_select_filter;

#[tokio::test]
async fn test_index_performance() {
    let mut db = InMemoryDB::new();
    let row_count = 10_000; // Reduced for tests to run faster, but still enough to show index benefit
    let target_value = 5_000;

    println!("Generating {} rows...", row_count);
    
    // 1. Setup: Create table
    db.execute("CREATE TABLE large_table (id INTEGER, val INTEGER)").await.unwrap();

    // 2. Insert data
    for i in 0..row_count {
        db.execute(&format!("INSERT INTO large_table VALUES ({}, {})", i, i)).await.unwrap();
    }
    
    // Warmup query
    db.execute("SELECT * FROM large_table LIMIT 1").await.unwrap();

    // 3. Measure Full Table Scan (No Index)
    println!("Running Full Table Scan (No Index)...");
    let start_no_index = Instant::now();
    let result_no_index = db.execute(&format!("SELECT * FROM large_table WHERE val = {}", target_value)).await.unwrap();
    let duration_no_index = start_no_index.elapsed();
    
    assert_eq!(result_no_index.row_count(), 1);
    println!("Full Table Scan took: {:?}", duration_no_index);

    // 4. Create Index
    println!("Creating Index on 'val' column...");
    let start_create_index = Instant::now();
    db.create_index("large_table", "val").await.unwrap();
    println!("Index creation took: {:?}", start_create_index.elapsed());

    // 5. Measure Index Scan
    println!("Running Index Scan...");
    let start_with_index = Instant::now();
    let result_with_index = db.execute(&format!("SELECT * FROM large_table WHERE val = {}", target_value)).await.unwrap();
    let duration_with_index = start_with_index.elapsed();
    
    assert_eq!(result_with_index.row_count(), 1);
    println!("Index Scan took: {:?}", duration_with_index);

    // 6. Compare results
    let improvement = duration_no_index.as_secs_f64() / duration_with_index.as_secs_f64();
    println!("Speedup factor: {:.2}x", improvement);

    assert!(duration_with_index < duration_no_index, "Index scan should be faster");
}

#[tokio::test]
#[ignore]
async fn perf_index_scan_after_updates() {
    use std::time::Instant;

    let mut db = InMemoryDB::new();
    let row_count = 50_000;
    let update_stride = 10;
    let target_value = 9_999;

    db.execute("CREATE TABLE large_table (id INTEGER, val INTEGER)").await.unwrap();

    for i in 0..row_count {
        let val = i % 1_000;
        db.execute(&format!("INSERT INTO large_table VALUES ({}, {})", i, val)).await.unwrap();
    }

    let start_no_index = Instant::now();
    let result_no_index = db.execute(&format!("SELECT * FROM large_table WHERE val = {}", target_value)).await.unwrap();
    let duration_no_index = start_no_index.elapsed();
    assert_eq!(result_no_index.row_count(), 0);

    db.create_index("large_table", "val").await.unwrap();

    for i in (0..row_count).step_by(update_stride) {
        db.execute(&format!("UPDATE large_table SET val = {} WHERE id = {}", target_value, i)).await.unwrap();
    }

    let start_with_index = Instant::now();
    let result_with_index = db.execute(&format!("SELECT * FROM large_table WHERE val = {}", target_value)).await.unwrap();
    let duration_with_index = start_with_index.elapsed();

    let expected_count = (row_count + update_stride - 1) / update_stride;
    assert_eq!(result_with_index.row_count(), expected_count);

    let cfg = perf_utils::start_run().unwrap();
    perf_utils::record_metric(&cfg, "full_scan_no_index", duration_no_index).unwrap();
    perf_utils::record_metric(&cfg, "index_scan_after_updates", duration_with_index).unwrap();
    perf_utils::finalize_run(&cfg).unwrap();

    assert!(duration_with_index < duration_no_index, "Index scan should be faster after updates");
}
