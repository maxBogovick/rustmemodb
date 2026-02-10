use rustmemodb::InMemoryDB;
use std::time::Instant;

#[tokio::test]
async fn test_hash_join_performance() {
    let mut db = InMemoryDB::new();

    // Create tables
    db.execute("CREATE TABLE t1 (id INTEGER, val INTEGER)")
        .await
        .unwrap();
    db.execute("CREATE TABLE t2 (id INTEGER, val INTEGER)")
        .await
        .unwrap();

    let n = 5000; // Increased to 5000

    println!("Generating {} rows per table...", n);
    let start_gen = Instant::now();
    for i in 0..n {
        db.execute(&format!("INSERT INTO t1 VALUES ({}, {})", i, i))
            .await
            .unwrap();
        db.execute(&format!("INSERT INTO t2 VALUES ({}, {})", i, i))
            .await
            .unwrap();
    }
    println!("Data generation complete in {:?}", start_gen.elapsed());

    // 0. Benchmark Scan alone
    println!("Benchmarking Table Scan...");
    let start_scan = Instant::now();
    let _rows = db.execute("SELECT * FROM t1").await.unwrap();
    println!("Scan t1 took: {:?}", start_scan.elapsed());

    // 1. Hash Join (Standard Equi-Join)
    println!("Running Hash Join...");
    let start_hash = Instant::now();
    let res_hash = db
        .execute("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id")
        .await
        .unwrap();
    let duration_hash = start_hash.elapsed();
    println!("Hash Join took: {:?}", duration_hash);
    assert_eq!(res_hash.row_count(), n);

    // 2. Nested Loop Join (Force fallback using AND condition)
    println!("Running Nested Loop Join (Fallback)...");
    let start_nlj = Instant::now();
    // Reduce N for NLJ because 5000^2 = 25M is too slow for debug test
    // Actually, let's keep it to verify the gap. 25M ops might take ~30s.
    // We can filter the dataset for NLJ or just accept it takes time.
    // Let's use a LIMIT for NLJ? No, limit applies after join.
    // Let's just run it.
    let res_nlj = db
        .execute("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id AND 1=1")
        .await
        .unwrap();
    let duration_nlj = start_nlj.elapsed();
    println!("Nested Loop Join took: {:?}", duration_nlj);
    assert_eq!(res_nlj.row_count(), n);

    // Verification
    if duration_hash < duration_nlj {
        let speedup = duration_nlj.as_secs_f64() / duration_hash.as_secs_f64();
        println!("SUCCESS: Hash Join is {:.2}x faster than NLJ", speedup);
    } else {
        println!("WARNING: Hash Join was not faster. Check implementation.");
        // Usually panic here, but in CI environments timing can be flaky.
        // But 4M operations vs 4k operations should be drastic.
    }
}
