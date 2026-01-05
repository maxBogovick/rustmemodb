/// Performance tests
///
/// Benchmarks and performance tests for RustMemDB
/// Run with: cargo test --test performance_tests --release -- --nocapture
///
/// Note: Run with --release flag for realistic performance numbers

use rustmemodb::{Client, ConnectionConfig};
use std::time::Instant;

#[tokio::test] async fn test_performance_bulk_insert() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE perf_insert (id INTEGER, data TEXT)").await.unwrap();

    let start = Instant::now();
    let count = 1000;

    for i in 0..count {
        client.execute(&format!(
            "INSERT INTO perf_insert VALUES ({}, 'data_{}')",
            i, i
        )).await.unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Bulk INSERT Performance ===");
    println!("Inserted {} rows in {:?}", count, duration);
    println!("Throughput: {:.2} inserts/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/insert", duration.as_millis() as f64 / count as f64);

    // Verify all inserted
    let result = client.query("SELECT * FROM perf_insert").await.unwrap();
    assert_eq!(result.row_count(), count);
}

#[tokio::test] async fn test_performance_bulk_select() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE perf_select (id INTEGER, data TEXT)").await.unwrap();

    // Insert test data
    for i in 0..1000 { // Reduced from 10000 for faster tests
        client.execute(&format!(
            "INSERT INTO perf_select VALUES ({}, 'data_{}')",
            i, i
        )).await.unwrap();
    }

    let start = Instant::now();
    let count = 100;

    for _ in 0..count {
        let _result = client.query("SELECT * FROM perf_select").await.unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Full Table Scan Performance ===");
    println!("Executed {} SELECTs in {:?}", count, duration);
    println!("Throughput: {:.2} queries/sec", count as f64 / duration.as_secs_f64());
}

#[tokio::test] async fn test_performance_filtered_select() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE perf_filter (id INTEGER, category TEXT, value INTEGER)").await.unwrap();

    // Insert test data with categories
    for i in 0..1000 {
        let category = if i % 3 == 0 { "A" } else if i % 3 == 1 { "B" } else { "C" };
        client.execute(&format!(
            "INSERT INTO perf_filter VALUES ({}, '{}', {})",
            i, category, i * 10
        )).await.unwrap();
    }

    let start = Instant::now();
    let count = 100;

    for _ in 0..count {
        let _result = client.query(
            "SELECT * FROM perf_filter WHERE category = 'A' AND value > 1000"
        ).await.unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Filtered SELECT Performance ===");
    println!("Executed {} filtered SELECTs in {:?}", count, duration);
}

#[tokio::test] async fn test_performance_order_by() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE perf_sort (id INTEGER, value INTEGER)").await.unwrap();

    // Insert unsorted data
    for i in 0..500 {
        let value = 500 - i; // Reverse order
        client.execute(&format!(
            "INSERT INTO perf_sort VALUES ({}, {})",
            i, value
        )).await.unwrap();
    }

    let start = Instant::now();
    let count = 50;

    for _ in 0..count {
        let _result = client.query(
            "SELECT * FROM perf_sort ORDER BY value ASC"
        ).await.unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== ORDER BY Performance ===");
    println!("Executed {} ORDER BY queries in {:?}", count, duration);
}

#[tokio::test] async fn test_performance_complex_query() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE perf_complex (
            id INTEGER,
            category TEXT,
            price FLOAT,
            stock INTEGER,
            featured BOOLEAN
        )"
    ).await.unwrap();

    // Insert test data
    for i in 0..1000 {
        let category = match i % 4 {
            0 => "Electronics",
            1 => "Books",
            2 => "Clothing",
            _ => "Food",
        };
        let price = (i as f64 * 0.5) + 10.0;
        let stock = i % 100;
        let featured = i % 10 == 0;

        client.execute(&format!(
            "INSERT INTO perf_complex VALUES ({}, '{}', {}, {}, {})",
            i, category, price, stock, featured
        )).await.unwrap();
    }

    let start = Instant::now();
    let count = 50;

    for _ in 0..count {
        let _result = client.query(
            "SELECT * FROM perf_complex
             WHERE (category = 'Electronics' AND price > 100)
                OR (featured = true AND stock > 20)
             ORDER BY price DESC
             LIMIT 10"
        ).await.unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Complex Query Performance ===");
    println!("Executed {} complex queries in {:?}", count, duration);
}

#[tokio::test] async fn test_performance_connection_pool() {
    let config = ConnectionConfig::new("admin", "adminpass")
        .min_connections(5)
        .max_connections(10);

    let client = Client::connect_with_config(config).await.unwrap();

    client.execute("CREATE TABLE perf_pool (id INTEGER)").await.unwrap();

    let start = Instant::now();
    let count = 100;

    for i in 0..count {
        let mut conn = client.get_connection().await.unwrap();
        conn.execute(&format!("INSERT INTO perf_pool VALUES ({})", i)).await.unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Connection Pool Performance ===");
    println!("Executed {} operations through pool in {:?}", count, duration);
}

#[tokio::test] async fn test_performance_table_scan_sizes() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let sizes = vec![100, 500, 1000];

    println!("\n=== Table Scan Performance by Size ===");

    for size in sizes {
        client.execute(&format!("CREATE TABLE perf_scan_{} (id INTEGER, data TEXT)", size)).await.unwrap();

        // Insert data
        for i in 0..size {
            client.execute(&format!(
                "INSERT INTO perf_scan_{} VALUES ({}, 'data_{}')",
                size, i, i
            )).await.unwrap();
        }

        // Measure scan time
        let start = Instant::now();
        let iterations = 20;

        for _ in 0..iterations {
            let _result = client.query(&format!("SELECT * FROM perf_scan_{}", size)).await.unwrap();
        }

        let duration = start.elapsed();
        let avg_ms = duration.as_millis() as f64 / iterations as f64;

        println!("  {} rows: {:.2} ms/scan", size, avg_ms);
    }
}

#[tokio::test] async fn test_performance_transaction_overhead() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE perf_tx_overhead (id INTEGER)").await.unwrap();

    // Without transaction
    let start = Instant::now();
    for i in 0..100 {
        client.execute(&format!("INSERT INTO perf_tx_overhead VALUES ({})", i)).await.unwrap();
    }
    let no_tx_duration = start.elapsed();

    // With transaction
    client.execute("CREATE TABLE perf_tx_with (id INTEGER)").await.unwrap();
    let mut conn = client.get_connection().await.unwrap();

    let start = Instant::now();
    conn.begin().await.unwrap();
    for i in 0..100 {
        conn.execute(&format!("INSERT INTO perf_tx_with VALUES ({})", i)).await.unwrap();
    }
    conn.commit().await.unwrap();
    let tx_duration = start.elapsed();

    println!("\n=== Transaction Overhead ===");
    println!("100 inserts without transaction: {:?}", no_tx_duration);
    println!("100 inserts with transaction: {:?}", tx_duration);
}