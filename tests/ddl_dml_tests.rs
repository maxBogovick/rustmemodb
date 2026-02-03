use std::sync::{Arc};
use tokio::sync::Mutex;
use std::time::Instant;
use rustmemodb::Client;

#[tokio::test]
async fn test_drop_table() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create table
    client.execute("CREATE TABLE test_drop (id INTEGER, name TEXT)").await.unwrap();

    // Insert data
    client.execute("INSERT INTO test_drop VALUES (1, 'test')").await.unwrap();

    // Drop table
    let result = client.execute("DROP TABLE test_drop").await;
    assert!(result.is_ok());

    // Table should not exist
    let result = client.query("SELECT * FROM test_drop").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_drop_table_if_exists() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Drop non-existent table with IF EXISTS should not fail
    let result = client.execute("DROP TABLE IF EXISTS non_existent").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_create_table_if_not_exists() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_create_if (id INTEGER)").await.unwrap();
    let result = client.execute("CREATE TABLE IF NOT EXISTS test_create_if (id INTEGER)").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_drop_table_non_existent() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Drop non-existent table without IF EXISTS should fail
    let result = client.execute("DROP TABLE non_existent").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_delete_all_rows() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create and populate table
    client.execute("CREATE TABLE test_delete (id INTEGER, name TEXT)").await.unwrap();
    client.execute("INSERT INTO test_delete VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").await.unwrap();

    // Delete all rows
    let result = client.execute("DELETE FROM test_delete").await.unwrap();
    assert_eq!(result.affected_rows(), Some(3));

    // Verify all rows deleted
    let result = client.query("SELECT * FROM test_delete").await.unwrap();
    assert_eq!(result.row_count(), 0);
}

#[tokio::test]
async fn test_delete_with_where() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create and populate table
    client.execute("CREATE TABLE test_delete_where (id INTEGER, name TEXT)").await.unwrap();
    client.execute("INSERT INTO test_delete_where VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").await.unwrap();

    // Delete specific row
    let result = client.execute("DELETE FROM test_delete_where WHERE id = 2").await.unwrap();
    assert_eq!(result.affected_rows(), Some(1));

    // Verify correct row deleted
    let result = client.query("SELECT * FROM test_delete_where ORDER BY id").await.unwrap();
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_delete_with_complex_where() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create and populate table
    client.execute("CREATE TABLE test_delete_complex (id INTEGER, age INTEGER)").await.unwrap();
    client.execute("INSERT INTO test_delete_complex VALUES (1, 20), (2, 30), (3, 25), (4, 35)").await.unwrap();

    // Delete rows with complex condition
    let result = client.execute("DELETE FROM test_delete_complex WHERE age > 25").await.unwrap();
    assert_eq!(result.affected_rows(), Some(2));

    // Verify correct rows deleted
    let result = client.query("SELECT * FROM test_delete_complex ORDER BY id").await.unwrap();
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_update_all_rows() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create and populate table
    client.execute("CREATE TABLE test_update (id INTEGER, name TEXT)").await.unwrap();
    client.execute("INSERT INTO test_update VALUES (1, 'Alice'), (2, 'Bob')").await.unwrap();

    // Update all rows
    let result = client.execute("UPDATE test_update SET name = 'Updated'").await.unwrap();
    assert_eq!(result.affected_rows(), Some(2));

    // Verify all rows updated
    let result = client.query("SELECT name FROM test_update").await.unwrap();
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_update_with_where() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create and populate table
    client.execute("CREATE TABLE test_update_where (id INTEGER, name TEXT, age INTEGER)").await.unwrap();
    client.execute("INSERT INTO test_update_where VALUES (1, 'Alice', 25), (2, 'Bob', 30)").await.unwrap();

    // Update specific row
    let result = client.execute("UPDATE test_update_where SET age = 26 WHERE id = 1").await.unwrap();
    assert_eq!(result.affected_rows(), Some(1));

    // Verify correct row updated
    let result = client.query("SELECT age FROM test_update_where WHERE id = 1").await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_update_multiple_columns() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create and populate table
    client.execute("CREATE TABLE test_update_multi (id INTEGER, name TEXT, age INTEGER)").await.unwrap();
    client.execute("INSERT INTO test_update_multi VALUES (1, 'Alice', 25)").await.unwrap();

    // Update multiple columns
    let result = client.execute("UPDATE test_update_multi SET name = 'Alicia', age = 26 WHERE id = 1").await.unwrap();
    assert_eq!(result.affected_rows(), Some(1));
}

#[tokio::test]
async fn test_update_with_expression() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create and populate table
    client.execute("CREATE TABLE test_update_expr (id INTEGER, age INTEGER)").await.unwrap();
    client.execute("INSERT INTO test_update_expr VALUES (1, 25), (2, 30)").await.unwrap();

    // Update with expression
    let result = client.execute("UPDATE test_update_expr SET age = age + 1").await.unwrap();
    assert_eq!(result.affected_rows(), Some(2));
}

#[tokio::test]
async fn test_delete_and_select() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create and populate table
    client.execute("CREATE TABLE test_del_sel (id INTEGER, name TEXT)").await.unwrap();
    client.execute("INSERT INTO test_del_sel VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')").await.unwrap();

    // Delete one row
    client.execute("DELETE FROM test_del_sel WHERE id = 2").await.unwrap();

    // Query remaining rows
    let result = client.query("SELECT * FROM test_del_sel ORDER BY id").await.unwrap();
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_update_and_select() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create and populate table
    client.execute("CREATE TABLE test_upd_sel (id INTEGER, status TEXT)").await.unwrap();
    client.execute("INSERT INTO test_upd_sel VALUES (1, 'pending'), (2, 'pending')").await.unwrap();

    // Update rows
    client.execute("UPDATE test_upd_sel SET status = 'completed' WHERE id = 1").await.unwrap();

    // Query updated rows
    let result = client.query("SELECT status FROM test_upd_sel WHERE id = 1").await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_update_load_sequential() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Setup: Create table and insert test data
    client.execute("CREATE TABLE load_test_update (id INTEGER, value INTEGER, status TEXT)").await.unwrap();

    let insert_count = 1000;
    for i in 0..insert_count {
        client.execute(&format!(
            "INSERT INTO load_test_update VALUES ({}, {}, 'initial')",
            i, i * 10
        )).await.unwrap();
    }

    // Load test: Sequential updates
    let start = Instant::now();
    let mut updated_count = 0;

    for i in 0..insert_count {
        let result = client.execute(&format!(
            "UPDATE load_test_update SET value = {}, status = 'updated' WHERE id = {}",
            i * 20, i
        )).await.unwrap();
        updated_count += result.affected_rows().unwrap_or(0);
    }

    let duration = start.elapsed();

    println!("Sequential UPDATE Load Test:");
    println!("  Total updates: {}", insert_count);
    println!("  Rows updated: {}", updated_count);
    println!("  Duration: {:?}", duration);
    println!("  Updates/sec: {:.2}", insert_count as f64 / duration.as_secs_f64());

    assert_eq!(updated_count, insert_count);
}

#[tokio::test]
async fn test_update_load_batch() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Setup
    client.execute("CREATE TABLE load_test_batch (id INTEGER, counter INTEGER)").await.unwrap();

    let insert_count = 1000;
    for i in 0..insert_count {
        client.execute(&format!(
            "INSERT INTO load_test_batch VALUES ({}, 0)", i
        )).await.unwrap();
    }

    // Load test: Batch updates with WHERE conditions
    let start = Instant::now();
    let batch_size = 100;
    let mut total_updated = 0;

    for batch in 0..(insert_count / batch_size) {
        let start_id = batch * batch_size;
        let end_id = start_id + batch_size;

        let result = client.execute(&format!(
            "UPDATE load_test_batch SET counter = counter + 1 WHERE id >= {} AND id < {}",
            start_id, end_id
        )).await.unwrap();

        total_updated += result.affected_rows().unwrap_or(0);
    }

    let duration = start.elapsed();

    println!("Batch UPDATE Load Test:");
    println!("  Batch size: {}", batch_size);
    println!("  Total batches: {}", insert_count / batch_size);
    println!("  Rows updated: {}", total_updated);
    println!("  Duration: {:?}", duration);
    println!("  Updates/sec: {:.2}", total_updated as f64 / duration.as_secs_f64());

    assert_eq!(total_updated, insert_count);
}

#[tokio::test]
async fn test_update_load_all_rows() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Setup
    client.execute("CREATE TABLE load_test_all (id INTEGER, flag BOOLEAN, timestamp INTEGER)").await.unwrap();

    let row_count = 5000;
    for i in 0..row_count {
        client.execute(&format!(
            "INSERT INTO load_test_all VALUES ({}, FALSE, 0)", i
        )).await.unwrap();
    }

    // Load test: Multiple full table updates
    let iterations = 10;
    let start = Instant::now();

    for iter in 0..iterations {
        let result = client.execute(&format!(
            "UPDATE load_test_all SET timestamp = {}", iter
        )).await.unwrap();

        assert_eq!(result.affected_rows(), Some(row_count));
    }

    let duration = start.elapsed();
    let total_updates = row_count * iterations;

    println!("Full Table UPDATE Load Test:");
    println!("  Rows per update: {}", row_count);
    println!("  Iterations: {}", iterations);
    println!("  Total row updates: {}", total_updates);
    println!("  Duration: {:?}", duration);
    println!("  Updates/sec: {:.2}", total_updates as f64 / duration.as_secs_f64());
}

#[tokio::test]
async fn test_update_load_complex_where() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Setup
    client.execute("CREATE TABLE load_test_complex (id INTEGER, score INTEGER, category TEXT)").await.unwrap();

    let row_count = 2000;
    for i in 0..row_count {
        let category = if i % 3 == 0 { "A" } else if i % 3 == 1 { "B" } else { "C" };
        client.execute(&format!(
            "INSERT INTO load_test_complex VALUES ({}, {}, '{}')",
            i, i % 100, category
        )).await.unwrap();
    }

    // Load test: Updates with complex WHERE clauses
    let start = Instant::now();
    let mut total_updated = 0;

    // Update pattern 1: score range
    for threshold in (0..100).step_by(10) {
        let result = client.execute(&format!(
            "UPDATE load_test_complex SET score = score + 10 WHERE score >= {} AND score < {}",
            threshold, threshold + 10
        )).await.unwrap();
        total_updated += result.affected_rows().unwrap_or(0);
    }

    // Update pattern 2: category-based
    for category in ["A", "B", "C"] {
        let result = client.execute(&format!(
            "UPDATE load_test_complex SET score = 0 WHERE category = '{}'",
            category
        )).await.unwrap();
        total_updated += result.affected_rows().unwrap_or(0);
    }

    let duration = start.elapsed();

    println!("Complex WHERE UPDATE Load Test:");
    println!("  Total updates executed: 13");
    println!("  Total rows updated: {}", total_updated);
    println!("  Duration: {:?}", duration);
    println!("  Avg updates/sec: {:.2}", total_updated as f64 / duration.as_secs_f64());
}

#[tokio::test]
async fn test_update_load_concurrent() {
    let client = Arc::new(Mutex::new(Client::connect("admin", "adminpass").await.unwrap()));

    // Setup
    {
        let client = client.lock().await;
        client.execute("CREATE TABLE load_test_concurrent (id INTEGER, value INTEGER)").await.unwrap();

        for i in 0..1000 {
            client.execute(&format!(
                "INSERT INTO load_test_concurrent VALUES ({}, {})", i, 0
            )).await.unwrap();
        }
    }

    // Load test: Concurrent updates (simulated)
    let start = Instant::now();
    let thread_count = 4;
    let updates_per_thread = 250;

    let mut handles = vec![];

    for thread_id in 0..thread_count {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            let mut local_updates = 0;
            let start_id = thread_id * updates_per_thread;

            for i in 0..updates_per_thread {
                let id = start_id + i;
                let client = client_clone.lock().await;

                let result = client.execute(&format!(
                    "UPDATE load_test_concurrent SET value = value + 1 WHERE id = {}", id
                )).await.unwrap();

                local_updates += result.affected_rows().unwrap_or(0);
            }

            local_updates
        });

        handles.push(handle);
    }

    let mut total_updated = 0;
    for handle in handles {
        total_updated += handle.await.unwrap();
    }

    let duration = start.elapsed();

    println!("Concurrent UPDATE Load Test:");
    println!("  Threads: {}", thread_count);
    println!("  Updates per thread: {}", updates_per_thread);
    println!("  Total rows updated: {}", total_updated);
    println!("  Duration: {:?}", duration);
    println!("  Updates/sec: {:.2}", total_updated as f64 / duration.as_secs_f64());

    assert_eq!(total_updated, 1000);
}

#[tokio::test]
async fn test_update_load_mixed_operations() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Setup
    client.execute("CREATE TABLE load_test_mixed (id INTEGER, data TEXT, version INTEGER)").await.unwrap();

    for i in 0..500 {
        client.execute(&format!(
            "INSERT INTO load_test_mixed VALUES ({}, 'data_{}', 0)", i, i
        )).await.unwrap();
    }

    // Load test: Mixed UPDATE and SELECT operations
    let start = Instant::now();
    let iterations = 100;

    for i in 0..iterations {
        // Update
        client.execute(&format!(
            "UPDATE load_test_mixed SET version = version + 1 WHERE id < {}",
            (i % 500) + 50
        )).await.unwrap();

        // Read to verify
        let result = client.query(&format!(
            "SELECT COUNT(*) FROM load_test_mixed WHERE version > {}", i
        )).await.unwrap();
        assert!(result.row_count() > 0);
    }

    let duration = start.elapsed();

    println!("Mixed UPDATE/SELECT Load Test:");
    println!("  Iterations: {}", iterations);
    println!("  Duration: {:?}", duration);
    println!("  Operations/sec: {:.2}", (iterations * 2) as f64 / duration.as_secs_f64());
}
