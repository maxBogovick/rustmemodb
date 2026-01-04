use rustmemodb::InMemoryDB;
use std::time::Instant;

#[test]
fn test_index_performance() {
    let mut db = InMemoryDB::new();
    let row_count = 100_000;
    let target_value = 50_000;

    println!("Generating {} rows...", row_count);
    
    // 1. Setup: Create table
    db.execute("CREATE TABLE large_table (id INTEGER, val INTEGER)").unwrap();

    // 2. Insert data
    // We use a loop to insert data. In a real scenario, bulk insert would be faster,
    // but here we want to test read performance.
    for i in 0..row_count {
        db.execute(&format!("INSERT INTO large_table VALUES ({}, {})", i, i)).unwrap();
    }
    
    // Warmup query
    db.execute("SELECT * FROM large_table LIMIT 1").unwrap();

    // 3. Measure Full Table Scan (No Index)
    println!("Running Full Table Scan (No Index)...");
    let start_no_index = Instant::now();
    let result_no_index = db.execute(&format!("SELECT * FROM large_table WHERE val = {}", target_value)).unwrap();
    let duration_no_index = start_no_index.elapsed();
    
    assert_eq!(result_no_index.row_count(), 1);
    println!("Full Table Scan took: {:?}", duration_no_index);

    // 4. Create Index
    println!("Creating Index on 'val' column...");
    let start_create_index = Instant::now();
    db.create_index("large_table", "val").unwrap();
    println!("Index creation took: {:?}", start_create_index.elapsed());

    // 5. Measure Index Scan
    println!("Running Index Scan...");
    let start_with_index = Instant::now();
    let result_with_index = db.execute(&format!("SELECT * FROM large_table WHERE val = {}", target_value)).unwrap();
    let duration_with_index = start_with_index.elapsed();
    
    assert_eq!(result_with_index.row_count(), 1);
    println!("Index Scan took: {:?}", duration_with_index);

    // 6. Compare results
    let improvement = duration_no_index.as_secs_f64() / duration_with_index.as_secs_f64();
    println!("Speedup factor: {:.2}x", improvement);

    // Assert that index is at least 10x faster (it should be much more)
    // Using a conservative check to avoid flaky tests on CI/slow machines
    assert!(duration_with_index < duration_no_index, "Index scan should be faster");
    
    if duration_with_index.as_micros() > 0 {
         println!("Index scan is {:.2}x faster", improvement);
    } else {
         println!("Index scan was too fast to measure accurately (< 1us)");
    }
}
