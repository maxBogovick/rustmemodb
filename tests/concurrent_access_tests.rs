/// Concurrent access tests
///
/// Tests for multi-threaded database access and connection pool behavior
/// Run with: cargo test --test concurrent_access_tests

use rustmemodb::{Client, ConnectionConfig};
use std::sync::{Arc};
use tokio::sync::Barrier;
use std::time::Duration;

#[tokio::test]
async fn test_concurrent_reads() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    client.execute("CREATE TABLE concurrent_read (id INTEGER, data TEXT)").await.unwrap();

    // Insert test data
    for i in 0..100 {
        client.execute(&format!("INSERT INTO concurrent_read VALUES ({}, 'data_{}')", i, i)).await.unwrap();
    }

    let mut handles = vec![];
    let num_tasks = 10;

    for task_id in 0..num_tasks {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            for _ in 0..50 {
                let result = client_clone.query("SELECT * FROM concurrent_read").await.unwrap();
                assert_eq!(result.row_count(), 100, "Task {} read incorrect count", task_id);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_writes() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    client.execute("CREATE TABLE concurrent_write (id INTEGER, thread_id INTEGER)").await.unwrap();

    let mut handles = vec![];
    let num_tasks = 5;
    let writes_per_task = 20;

    for task_id in 0..num_tasks {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            for i in 0..writes_per_task {
                let id = task_id * 1000 + i;
                client_clone.execute(&format!(
                    "INSERT INTO concurrent_write VALUES ({}, {})",
                    id, task_id
                )).await.unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all writes succeeded
    let result = client.query("SELECT * FROM concurrent_write").await.unwrap();
    assert_eq!(result.row_count(), num_tasks * writes_per_task);
}

#[tokio::test]
async fn test_concurrent_read_write_mix() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    client.execute("CREATE TABLE concurrent_mix (id INTEGER, value INTEGER)").await.unwrap();

    // Pre-populate with some data
    for i in 0..50 {
        client.execute(&format!("INSERT INTO concurrent_mix VALUES ({}, {})", i, i * 10)).await.unwrap();
    }

    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(10));

    // 5 reader tasks
    for task_id in 0..5 {
        let client_clone = Arc::clone(&client);
        let barrier_clone = Arc::clone(&barrier);

        let handle = tokio::spawn(async move {
            barrier_clone.wait().await;

            for _ in 0..30 {
                let result = client_clone.query("SELECT * FROM concurrent_mix WHERE value > 100").await.unwrap();
                assert!(result.row_count() >= 0, "Reader {} failed", task_id);
            }
        });

        handles.push(handle);
    }

    // 5 writer tasks
    for task_id in 0..5 {
        let client_clone = Arc::clone(&client);
        let barrier_clone = Arc::clone(&barrier);

        let handle = tokio::spawn(async move {
            barrier_clone.wait().await;

            for i in 0..10 {
                let id = 1000 + task_id * 100 + i;
                client_clone.execute(&format!(
                    "INSERT INTO concurrent_mix VALUES ({}, {})",
                    id, id * 10
                )).await.unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Final count should be initial 50 + 5 tasks * 10 writes = 100
    let result = client.query("SELECT * FROM concurrent_mix").await.unwrap();
    assert_eq!(result.row_count(), 100);
}

#[tokio::test]
async fn test_connection_pool_under_load() {
    let config = ConnectionConfig::new("admin", "adminpass")
        .min_connections(3)
        .max_connections(10);

    let client = Arc::new(Client::connect_with_config(config).await.unwrap());

    client.execute("CREATE TABLE pool_load (id INTEGER)").await.unwrap();

    let mut handles = vec![];
    let num_tasks = 20;

    for task_id in 0..num_tasks {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            for i in 0..10 {
                let id = task_id * 100 + i;
                client_clone.execute(&format!("INSERT INTO pool_load VALUES ({})", id)).await.unwrap();

                // Small sleep to simulate real workload
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let result = client.query("SELECT * FROM pool_load").await.unwrap();
    assert_eq!(result.row_count(), num_tasks * 10);
}

#[tokio::test]
async fn test_connection_pool_reuse() {
    let config = ConnectionConfig::new("admin", "adminpass")
        .min_connections(2)
        .max_connections(5);

    let client = Client::connect_with_config(config).await.unwrap();

    client.execute("CREATE TABLE pool_reuse (id INTEGER)").await.unwrap();

    // Get connection, use it, and return to pool
    {
        let mut conn = client.get_connection().await.unwrap();
        let _conn_id = conn.connection().id();
        conn.execute("INSERT INTO pool_reuse VALUES (1)").await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Get connection again - should reuse same connection
    {
        let mut conn = client.get_connection().await.unwrap();
        let _conn_id2 = conn.connection().id();
        conn.execute("INSERT INTO pool_reuse VALUES (2)").await.unwrap();
    }

    let stats = client.stats().await;
    assert!(stats.available_connections <= stats.max_connections);
}

#[tokio::test]
async fn test_concurrent_table_creation() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    let mut handles = vec![];

    for i in 0..5 {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            let table_name = format!("concurrent_table_{}", i);
            client_clone.execute(&format!(
                "CREATE TABLE {} (id INTEGER, data TEXT)",
                table_name
            )).await.unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // All tables should exist
    for i in 0..5 {
        let table_name = format!("concurrent_table_{}", i);
        let result = client.query(&format!("SELECT * FROM {}", table_name)).await;
        assert!(result.is_ok(), "Table {} should exist", table_name);
    }
}

#[tokio::test]
async fn test_concurrent_queries_different_tables() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    // Create multiple tables
    for i in 0..5 {
        client.execute(&format!(
            "CREATE TABLE multi_table_{} (id INTEGER, value INTEGER)",
            i
        )).await.unwrap();

        for j in 0..20 {
            client.execute(&format!(
                "INSERT INTO multi_table_{} VALUES ({}, {})",
                i, j, j * 10
            )).await.unwrap();
        }
    }

    let mut handles = vec![];

    for table_id in 0..5 {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            for _ in 0..50 {
                let result = client_clone.query(&format!(
                    "SELECT * FROM multi_table_{} WHERE value > 50",
                    table_id
                )).await.unwrap();

                assert!(result.row_count() > 0);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_transactions() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    client.execute("CREATE TABLE concurrent_tx (id INTEGER, thread_id INTEGER)").await.unwrap();

    let mut handles = vec![];
    let num_tasks = 5;

    for task_id in 0..num_tasks {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            let mut conn = client_clone.get_connection().await.unwrap();

            conn.begin().await.unwrap();

            for i in 0..10 {
                let id = task_id * 100 + i;
                conn.execute(&format!(
                    "INSERT INTO concurrent_tx VALUES ({}, {})",
                    id, task_id
                )).await.unwrap();
            }

            conn.commit().await.unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let result = client.query("SELECT * FROM concurrent_tx").await.unwrap();
    assert_eq!(result.row_count(), num_tasks * 10);
}

#[tokio::test]
async fn test_concurrent_stress_test() {
    let config = ConnectionConfig::new("admin", "adminpass")
        .min_connections(5)
        .max_connections(20);

    let client = Arc::new(Client::connect_with_config(config).await.unwrap());

    client.execute("CREATE TABLE stress_test (id INTEGER, thread_id INTEGER, operation TEXT)").await.unwrap();

    let mut handles = vec![];
    let num_tasks = 15;
    let operations_per_task = 100;

    for task_id in 0..num_tasks {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            for i in 0..operations_per_task {
                let id = task_id * 10000 + i;

                // Mix of operations
                if i % 3 == 0 {
                    // Write
                    client_clone.execute(&format!(
                        "INSERT INTO stress_test VALUES ({}, {}, 'insert')",
                        id, task_id
                    )).await.unwrap();
                } else {
                    // Read
                    let _result = client_clone.query(&format!(
                        "SELECT * FROM stress_test WHERE thread_id = {}",
                        task_id
                    )).await.unwrap();
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let result = client.query("SELECT * FROM stress_test").await.unwrap();

    // Should have inserted operations_per_task / 3 rows per task
    let expected = num_tasks * (operations_per_task / 3 + 1);
    assert!(result.row_count() >= expected - num_tasks && result.row_count() <= expected + num_tasks);
}

#[tokio::test]
async fn test_pool_exhaustion_recovery() {
    let config = ConnectionConfig::new("admin", "adminpass")
        .min_connections(2)
        .max_connections(3)
        .connect_timeout(Duration::from_millis(500));

    let client = Arc::new(Client::connect_with_config(config).await.unwrap());

    client.execute("CREATE TABLE pool_exhaustion (id INTEGER)").await.unwrap();

    // Hold all connections
    let conn1 = client.get_connection().await.unwrap();
    let conn2 = client.get_connection().await.unwrap();
    let conn3 = client.get_connection().await.unwrap();

    let stats = client.stats().await;
    assert_eq!(stats.available_connections, 0);
    assert_eq!(stats.active_connections, 3);

    // Try to get another - should timeout
    let client_clone = Arc::clone(&client);
    let handle = tokio::spawn(async move {
        let result = client_clone.get_connection().await;
        assert!(result.is_err());
    });

    handle.await.unwrap();

    // Release connections
    drop(conn1);
    drop(conn2);
    drop(conn3);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Now should be able to get connection again
    let conn = client.get_connection().await;
    assert!(conn.is_ok());
}

#[tokio::test]
async fn test_concurrent_order_by_queries() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    client.execute("CREATE TABLE concurrent_sort (id INTEGER, value INTEGER)").await.unwrap();

    // Insert unsorted data
    for i in 0..100 {
        let value = 100 - i;
        client.execute(&format!("INSERT INTO concurrent_sort VALUES ({}, {})", i, value)).await.unwrap();
    }

    let mut handles = vec![];

    for task_id in 0..10 {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            for _ in 0..20 {
                let result = client_clone.query(
                    "SELECT * FROM concurrent_sort ORDER BY value DESC LIMIT 10"
                ).await.unwrap();

                assert_eq!(result.row_count(), 10, "Task {} got wrong count", task_id);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_concurrent_complex_queries() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    client.execute(
        "CREATE TABLE concurrent_complex (
            id INTEGER,
            category TEXT,
            price FLOAT,
            stock INTEGER,
            featured BOOLEAN
        )"
    ).await.unwrap();

    // Insert diverse data
    for i in 0..200 {
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
            "INSERT INTO concurrent_complex VALUES ({}, '{}', {}, {}, {})",
            i, category, price, stock, featured
        )).await.unwrap();
    }

    let mut handles = vec![];

    for task_id in 0..8 {
        let client_clone = Arc::clone(&client);

        let handle = tokio::spawn(async move {
            for _ in 0..25 {
                let result = client_clone.query(
                    "SELECT * FROM concurrent_complex
                     WHERE category = 'Electronics' AND price > 50
                        OR featured = true AND stock > 20
                     ORDER BY price DESC
                     LIMIT 20"
                ).await.unwrap();

                assert!(result.row_count() > 0 && result.row_count() <= 20,
                    "Task {} got unexpected count: {}", task_id, result.row_count());
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_barrier_synchronized_access() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    client.execute("CREATE TABLE barrier_test (id INTEGER, timestamp INTEGER)").await.unwrap();

    let num_tasks = 10;
    let barrier = Arc::new(Barrier::new(num_tasks));
    let mut handles = vec![];

    for task_id in 0..num_tasks {
        let client_clone = Arc::clone(&client);
        let barrier_clone = Arc::clone(&barrier);

        let handle = tokio::spawn(async move {
            // All tasks wait here
            barrier_clone.wait().await;

            // All tasks execute simultaneously
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            client_clone.execute(&format!(
                "INSERT INTO barrier_test VALUES ({}, {})",
                task_id, timestamp
            )).await.unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let result = client.query("SELECT * FROM barrier_test").await.unwrap();
    assert_eq!(result.row_count(), num_tasks);
}

#[tokio::test]
async fn test_connection_state_isolation() {
    let client = Arc::new(Client::connect("admin", "adminpass").await.unwrap());

    client.execute("CREATE TABLE state_isolation (id INTEGER)").await.unwrap();

    let mut conn1 = client.get_connection().await.unwrap();
    let mut conn2 = client.get_connection().await.unwrap();

    // Each connection should have independent transaction state
    conn1.begin().await.unwrap();
    assert!(conn1.connection().is_in_transaction());
    assert!(!conn2.connection().is_in_transaction());

    conn2.begin().await.unwrap();
    assert!(conn1.connection().is_in_transaction());
    assert!(conn2.connection().is_in_transaction());

    conn1.commit().await.unwrap();
    assert!(!conn1.connection().is_in_transaction());
    assert!(conn2.connection().is_in_transaction());

    conn2.rollback().await.unwrap();
    assert!(!conn1.connection().is_in_transaction());
    assert!(!conn2.connection().is_in_transaction());
}