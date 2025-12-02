/// Performance tests
///
/// Benchmarks and performance tests for RustMemDB
/// Run with: cargo test --test performance_tests --release -- --nocapture
///
/// Note: Run with --release flag for realistic performance numbers

use rustmemodb::Client;
use std::time::Instant;

#[test]
fn test_performance_bulk_insert() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE perf_insert (id INTEGER, data TEXT)").unwrap();

    let start = Instant::now();
    let count = 1000;

    for i in 0..count {
        client.execute(&format!(
            "INSERT INTO perf_insert VALUES ({}, 'data_{}')",
            i, i
        )).unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Bulk INSERT Performance ===");
    println!("Inserted {} rows in {:?}", count, duration);
    println!("Throughput: {:.2} inserts/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/insert", duration.as_millis() as f64 / count as f64);

    // Verify all inserted
    let result = client.query("SELECT * FROM perf_insert").unwrap();
    assert_eq!(result.row_count(), count);
}

#[test]
fn test_performance_bulk_select() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE perf_select (id INTEGER, data TEXT)").unwrap();

    // Insert test data
    for i in 0..10000 {
        client.execute(&format!(
            "INSERT INTO perf_select VALUES ({}, 'data_{}')",
            i, i
        )).unwrap();
    }

    let start = Instant::now();
    let count = 100;

    for _ in 0..count {
        let _result = client.query("SELECT * FROM perf_select").unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Full Table Scan Performance ===");
    println!("Executed {} SELECTs in {:?}", count, duration);
    println!("Throughput: {:.2} queries/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/query", duration.as_millis() as f64 / count as f64);
}

#[test]
fn test_performance_filtered_select() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE perf_filter (id INTEGER, category TEXT, value INTEGER)").unwrap();

    // Insert test data with categories
    for i in 0..5000 {
        let category = if i % 3 == 0 { "A" } else if i % 3 == 1 { "B" } else { "C" };
        client.execute(&format!(
            "INSERT INTO perf_filter VALUES ({}, '{}', {})",
            i, category, i * 10
        )).unwrap();
    }

    let start = Instant::now();
    let count = 100;

    for _ in 0..count {
        let _result = client.query(
            "SELECT * FROM perf_filter WHERE category = 'A' AND value > 1000"
        ).unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Filtered SELECT Performance ===");
    println!("Executed {} filtered SELECTs in {:?}", count, duration);
    println!("Throughput: {:.2} queries/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/query", duration.as_millis() as f64 / count as f64);
}

#[test]
fn test_performance_order_by() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE perf_sort (id INTEGER, value INTEGER)").unwrap();

    // Insert unsorted data
    for i in 0..1000 {
        let value = 1000 - i; // Reverse order
        client.execute(&format!(
            "INSERT INTO perf_sort VALUES ({}, {})",
            i, value
        )).unwrap();
    }

    let start = Instant::now();
    let count = 50;

    for _ in 0..count {
        let _result = client.query(
            "SELECT * FROM perf_sort ORDER BY value ASC"
        ).unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== ORDER BY Performance ===");
    println!("Executed {} ORDER BY queries in {:?}", count, duration);
    println!("Throughput: {:.2} queries/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/query", duration.as_millis() as f64 / count as f64);
}

#[test]
fn test_performance_complex_query() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute(
        "CREATE TABLE perf_complex (
            id INTEGER,
            category TEXT,
            price FLOAT,
            stock INTEGER,
            featured BOOLEAN
        )"
    ).unwrap();

    // Insert test data
    for i in 0..2000 {
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
        )).unwrap();
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
        ).unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Complex Query Performance ===");
    println!("Executed {} complex queries in {:?}", count, duration);
    println!("Throughput: {:.2} queries/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/query", duration.as_millis() as f64 / count as f64);
}

#[test]
fn test_performance_connection_pool() {
    use rustmemodb::ConnectionConfig;
    use std::time::Duration;

    let config = ConnectionConfig::new("admin", "adminpass")
        .min_connections(5)
        .max_connections(10);

    let client = Client::connect_with_config(config).unwrap();

    client.execute("CREATE TABLE perf_pool (id INTEGER)").unwrap();

    let start = Instant::now();
    let count = 100;

    for i in 0..count {
        let mut conn = client.get_connection().unwrap();
        conn.execute(&format!("INSERT INTO perf_pool VALUES ({})", i)).unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Connection Pool Performance ===");
    println!("Executed {} operations through pool in {:?}", count, duration);
    println!("Throughput: {:.2} ops/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/op", duration.as_millis() as f64 / count as f64);
}

#[test]
fn test_performance_table_scan_sizes() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let sizes = vec![100, 500, 1000, 5000];

    println!("\n=== Table Scan Performance by Size ===");

    for size in sizes {
        client.execute(&format!("CREATE TABLE perf_scan_{} (id INTEGER, data TEXT)", size)).unwrap();

        // Insert data
        for i in 0..size {
            client.execute(&format!(
                "INSERT INTO perf_scan_{} VALUES ({}, 'data_{}')",
                size, i, i
            )).unwrap();
        }

        // Measure scan time
        let start = Instant::now();
        let iterations = 20;

        for _ in 0..iterations {
            let _result = client.query(&format!("SELECT * FROM perf_scan_{}", size)).unwrap();
        }

        let duration = start.elapsed();
        let avg_ms = duration.as_millis() as f64 / iterations as f64;

        println!("  {} rows: {:.2} ms/scan ({:.0} rows/ms)",
            size, avg_ms, size as f64 / avg_ms);
    }
}

#[test]
fn test_performance_like_operator() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE perf_like (id INTEGER, email TEXT)").unwrap();

    // Insert test data
    for i in 0..1000 {
        let domain = if i % 2 == 0 { "example.com" } else { "test.org" };
        client.execute(&format!(
            "INSERT INTO perf_like VALUES ({}, 'user{}@{}')",
            i, i, domain
        )).unwrap();
    }

    let start = Instant::now();
    let count = 50;

    for _ in 0..count {
        let _result = client.query(
            "SELECT * FROM perf_like WHERE email LIKE '%@example.com'"
        ).unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== LIKE Operator Performance ===");
    println!("Executed {} LIKE queries in {:?}", count, duration);
    println!("Throughput: {:.2} queries/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/query", duration.as_millis() as f64 / count as f64);
}

#[test]
fn test_performance_between_operator() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE perf_between (id INTEGER, value INTEGER)").unwrap();

    // Insert test data
    for i in 0..2000 {
        client.execute(&format!(
            "INSERT INTO perf_between VALUES ({}, {})",
            i, i * 5
        )).unwrap();
    }

    let start = Instant::now();
    let count = 50;

    for _ in 0..count {
        let _result = client.query(
            "SELECT * FROM perf_between WHERE value BETWEEN 1000 AND 5000"
        ).unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== BETWEEN Operator Performance ===");
    println!("Executed {} BETWEEN queries in {:?}", count, duration);
    println!("Throughput: {:.2} queries/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/query", duration.as_millis() as f64 / count as f64);
}

#[test]
fn test_performance_multi_column_sort() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute(
        "CREATE TABLE perf_multi_sort (category TEXT, subcategory TEXT, value INTEGER)"
    ).unwrap();

    // Insert test data
    for i in 0..1000 {
        let category = match i % 3 {
            0 => "A",
            1 => "B",
            _ => "C",
        };
        let subcategory = match i % 5 {
            0 => "X",
            1 => "Y",
            2 => "Z",
            3 => "W",
            _ => "V",
        };

        client.execute(&format!(
            "INSERT INTO perf_multi_sort VALUES ('{}', '{}', {})",
            category, subcategory, i
        )).unwrap();
    }

    let start = Instant::now();
    let count = 30;

    for _ in 0..count {
        let _result = client.query(
            "SELECT * FROM perf_multi_sort ORDER BY category ASC, subcategory DESC, value ASC"
        ).unwrap();
    }

    let duration = start.elapsed();

    println!("\n=== Multi-Column Sort Performance ===");
    println!("Executed {} multi-column sorts in {:?}", count, duration);
    println!("Throughput: {:.2} queries/sec", count as f64 / duration.as_secs_f64());
    println!("Average: {:.2} ms/query", duration.as_millis() as f64 / count as f64);
}

#[test]
fn test_performance_memory_usage_estimation() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute(
        "CREATE TABLE perf_memory (
            id INTEGER,
            name TEXT,
            email TEXT,
            age INTEGER,
            balance FLOAT
        )"
    ).unwrap();

    let rows = 10000;

    let start = Instant::now();

    for i in 0..rows {
        client.execute(&format!(
            "INSERT INTO perf_memory VALUES ({}, 'User{}', 'user{}@example.com', {}, {})",
            i, i, i, 20 + (i % 50), (i as f64) * 100.5
        )).unwrap();
    }

    let insert_duration = start.elapsed();

    // Query to measure scan time
    let scan_start = Instant::now();
    let result = client.query("SELECT * FROM perf_memory").unwrap();
    let scan_duration = scan_start.elapsed();

    println!("\n=== Memory & Performance Summary ===");
    println!("Total rows: {}", rows);
    println!("Insert time: {:?} ({:.2} ms/row)",
        insert_duration,
        insert_duration.as_millis() as f64 / rows as f64);
    println!("Scan time: {:?}", scan_duration);
    println!("Rows retrieved: {}", result.row_count());
    println!("Estimated throughput: {:.2} rows/sec",
        rows as f64 / scan_duration.as_secs_f64());
}

#[test]
fn test_performance_transaction_overhead() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE perf_tx_overhead (id INTEGER)").unwrap();

    // Without transaction
    let start = Instant::now();
    for i in 0..100 {
        client.execute(&format!("INSERT INTO perf_tx_overhead VALUES ({})", i)).unwrap();
    }
    let no_tx_duration = start.elapsed();

    // With transaction
    client.execute("CREATE TABLE perf_tx_with (id INTEGER)").unwrap();
    let mut conn = client.get_connection().unwrap();

    let start = Instant::now();
    conn.begin().unwrap();
    for i in 0..100 {
        conn.execute(&format!("INSERT INTO perf_tx_with VALUES ({})", i)).unwrap();
    }
    conn.commit().unwrap();
    let tx_duration = start.elapsed();

    println!("\n=== Transaction Overhead ===");
    println!("100 inserts without transaction: {:?}", no_tx_duration);
    println!("100 inserts with transaction: {:?}", tx_duration);
    println!("Overhead: {:.2}%",
        ((tx_duration.as_micros() as f64 / no_tx_duration.as_micros() as f64) - 1.0) * 100.0);
}
