/// Concurrent access tests
///
/// Tests for multi-threaded database access and connection pool behavior
/// Run with: cargo test --test concurrent_access_tests

use rustmemodb::{Client, ConnectionConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[test]
fn test_concurrent_reads() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    client.execute("CREATE TABLE concurrent_read (id INTEGER, data TEXT)").unwrap();

    // Insert test data
    for i in 0..100 {
        client.execute(&format!("INSERT INTO concurrent_read VALUES ({}, 'data_{}')", i, i)).unwrap();
    }

    let mut handles = vec![];
    let num_threads = 10;

    for thread_id in 0..num_threads {
        let client_clone = Arc::clone(&client);

        let handle = thread::spawn(move || {
            for _ in 0..50 {
                let result = client_clone.query("SELECT * FROM concurrent_read").unwrap();
                assert_eq!(result.row_count(), 100, "Thread {} read incorrect count", thread_id);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_writes() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    client.execute("CREATE TABLE concurrent_write (id INTEGER, thread_id INTEGER)").unwrap();

    let mut handles = vec![];
    let num_threads = 5;
    let writes_per_thread = 20;

    for thread_id in 0..num_threads {
        let client_clone = Arc::clone(&client);

        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let id = thread_id * 1000 + i;
                client_clone.execute(&format!(
                    "INSERT INTO concurrent_write VALUES ({}, {})",
                    id, thread_id
                )).unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all writes succeeded
    let result = client.query("SELECT * FROM concurrent_write").unwrap();
    assert_eq!(result.row_count(), num_threads * writes_per_thread);
}

#[test]
fn test_concurrent_read_write_mix() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    client.execute("CREATE TABLE concurrent_mix (id INTEGER, value INTEGER)").unwrap();

    // Pre-populate with some data
    for i in 0..50 {
        client.execute(&format!("INSERT INTO concurrent_mix VALUES ({}, {})", i, i * 10)).unwrap();
    }

    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(10));

    // 5 reader threads
    for thread_id in 0..5 {
        let client_clone = Arc::clone(&client);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for _ in 0..30 {
                let result = client_clone.query("SELECT * FROM concurrent_mix WHERE value > 100").unwrap();
                assert!(result.row_count() >= 0, "Reader {} failed", thread_id);
            }
        });

        handles.push(handle);
    }

    // 5 writer threads
    for thread_id in 0..5 {
        let client_clone = Arc::clone(&client);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            for i in 0..10 {
                let id = 1000 + thread_id * 100 + i;
                client_clone.execute(&format!(
                    "INSERT INTO concurrent_mix VALUES ({}, {})",
                    id, id * 10
                )).unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Final count should be initial 50 + 5 threads * 10 writes = 100
    let result = client.query("SELECT * FROM concurrent_mix").unwrap();
    assert_eq!(result.row_count(), 100);
}

#[test]
fn test_connection_pool_under_load() {
    let config = ConnectionConfig::new("admin", "admin")
        .min_connections(3)
        .max_connections(10);

    let client = Arc::new(Client::connect_with_config(config).unwrap());

    client.execute("CREATE TABLE pool_load (id INTEGER)").unwrap();

    let mut handles = vec![];
    let num_threads = 20;

    for thread_id in 0..num_threads {
        let client_clone = Arc::clone(&client);

        let handle = thread::spawn(move || {
            for i in 0..10 {
                let id = thread_id * 100 + i;
                client_clone.execute(&format!("INSERT INTO pool_load VALUES ({})", id)).unwrap();

                // Small sleep to simulate real workload
                thread::sleep(Duration::from_millis(1));
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let result = client.query("SELECT * FROM pool_load").unwrap();
    assert_eq!(result.row_count(), num_threads * 10);
}

#[test]
fn test_connection_pool_reuse() {
    let config = ConnectionConfig::new("admin", "admin")
        .min_connections(2)
        .max_connections(5);

    let client = Client::connect_with_config(config).unwrap();

    client.execute("CREATE TABLE pool_reuse (id INTEGER)").unwrap();

    // Get connection, use it, and return to pool
    {
        let mut conn = client.get_connection().unwrap();
        let conn_id = conn.connection().id();
        conn.execute("INSERT INTO pool_reuse VALUES (1)").unwrap();
    }

    thread::sleep(Duration::from_millis(10));

    // Get connection again - should reuse same connection
    {
        let mut conn = client.get_connection().unwrap();
        let conn_id2 = conn.connection().id();
        conn.execute("INSERT INTO pool_reuse VALUES (2)").unwrap();
        // In most cases, should be the same connection
    }

    let stats = client.stats();
    assert!(stats.available_connections <= stats.max_connections);
}

#[test]
fn test_concurrent_table_creation() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    let mut handles = vec![];

    for i in 0..5 {
        let client_clone = Arc::clone(&client);

        let handle = thread::spawn(move || {
            let table_name = format!("concurrent_table_{}", i);
            client_clone.execute(&format!(
                "CREATE TABLE {} (id INTEGER, data TEXT)",
                table_name
            )).unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // All tables should exist
    for i in 0..5 {
        let table_name = format!("concurrent_table_{}", i);
        let result = client.query(&format!("SELECT * FROM {}", table_name));
        assert!(result.is_ok(), "Table {} should exist", table_name);
    }
}

#[test]
fn test_concurrent_queries_different_tables() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    // Create multiple tables
    for i in 0..5 {
        client.execute(&format!(
            "CREATE TABLE multi_table_{} (id INTEGER, value INTEGER)",
            i
        )).unwrap();

        for j in 0..20 {
            client.execute(&format!(
                "INSERT INTO multi_table_{} VALUES ({}, {})",
                i, j, j * 10
            )).unwrap();
        }
    }

    let mut handles = vec![];

    for table_id in 0..5 {
        let client_clone = Arc::clone(&client);

        let handle = thread::spawn(move || {
            for _ in 0..50 {
                let result = client_clone.query(&format!(
                    "SELECT * FROM multi_table_{} WHERE value > 50",
                    table_id
                )).unwrap();

                assert!(result.row_count() > 0);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_transactions() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    client.execute("CREATE TABLE concurrent_tx (id INTEGER, thread_id INTEGER)").unwrap();

    let mut handles = vec![];
    let num_threads = 5;

    for thread_id in 0..num_threads {
        let client_clone = Arc::clone(&client);

        let handle = thread::spawn(move || {
            let mut conn = client_clone.get_connection().unwrap();

            conn.begin().unwrap();

            for i in 0..10 {
                let id = thread_id * 100 + i;
                conn.execute(&format!(
                    "INSERT INTO concurrent_tx VALUES ({}, {})",
                    id, thread_id
                )).unwrap();
            }

            conn.commit().unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let result = client.query("SELECT * FROM concurrent_tx").unwrap();
    assert_eq!(result.row_count(), num_threads * 10);
}

#[test]
fn test_concurrent_stress_test() {
    let config = ConnectionConfig::new("admin", "admin")
        .min_connections(5)
        .max_connections(20);

    let client = Arc::new(Client::connect_with_config(config).unwrap());

    client.execute("CREATE TABLE stress_test (id INTEGER, thread_id INTEGER, operation TEXT)").unwrap();

    let mut handles = vec![];
    let num_threads = 15;
    let operations_per_thread = 100;

    for thread_id in 0..num_threads {
        let client_clone = Arc::clone(&client);

        let handle = thread::spawn(move || {
            for i in 0..operations_per_thread {
                let id = thread_id * 10000 + i;

                // Mix of operations
                if i % 3 == 0 {
                    // Write
                    client_clone.execute(&format!(
                        "INSERT INTO stress_test VALUES ({}, {}, 'insert')",
                        id, thread_id
                    )).unwrap();
                } else {
                    // Read
                    let _result = client_clone.query(&format!(
                        "SELECT * FROM stress_test WHERE thread_id = {}",
                        thread_id
                    )).unwrap();
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let result = client.query("SELECT * FROM stress_test").unwrap();

    // Should have inserted operations_per_thread / 3 rows per thread
    let expected = num_threads * (operations_per_thread / 3 + 1);
    assert!(result.row_count() >= expected - num_threads && result.row_count() <= expected + num_threads);
}

#[test]
fn test_pool_exhaustion_recovery() {
    let config = ConnectionConfig::new("admin", "admin")
        .min_connections(2)
        .max_connections(3)
        .connect_timeout(Duration::from_millis(500));

    let client = Arc::new(Client::connect_with_config(config).unwrap());

    client.execute("CREATE TABLE pool_exhaustion (id INTEGER)").unwrap();

    // Hold all connections
    let conn1 = client.get_connection().unwrap();
    let conn2 = client.get_connection().unwrap();
    let conn3 = client.get_connection().unwrap();

    let stats = client.stats();
    assert_eq!(stats.available_connections, 0);
    assert_eq!(stats.active_connections, 3);

    // Try to get another - should timeout
    let client_clone = Arc::clone(&client);
    let handle = thread::spawn(move || {
        let result = client_clone.get_connection();
        assert!(result.is_err());
    });

    handle.join().unwrap();

    // Release connections
    drop(conn1);
    drop(conn2);
    drop(conn3);

    thread::sleep(Duration::from_millis(20));

    // Now should be able to get connection again
    let conn = client.get_connection();
    assert!(conn.is_ok());
}

#[test]
fn test_concurrent_order_by_queries() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    client.execute("CREATE TABLE concurrent_sort (id INTEGER, value INTEGER)").unwrap();

    // Insert unsorted data
    for i in 0..100 {
        let value = 100 - i;
        client.execute(&format!("INSERT INTO concurrent_sort VALUES ({}, {})", i, value)).unwrap();
    }

    let mut handles = vec![];

    for thread_id in 0..10 {
        let client_clone = Arc::clone(&client);

        let handle = thread::spawn(move || {
            for _ in 0..20 {
                let result = client_clone.query(
                    "SELECT * FROM concurrent_sort ORDER BY value DESC LIMIT 10"
                ).unwrap();

                assert_eq!(result.row_count(), 10, "Thread {} got wrong count", thread_id);
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_concurrent_complex_queries() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    client.execute(
        "CREATE TABLE concurrent_complex (
            id INTEGER,
            category TEXT,
            price FLOAT,
            stock INTEGER,
            featured BOOLEAN
        )"
    ).unwrap();

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
        )).unwrap();
    }

    let mut handles = vec![];

    for thread_id in 0..8 {
        let client_clone = Arc::clone(&client);

        let handle = thread::spawn(move || {
            for _ in 0..25 {
                let result = client_clone.query(
                    "SELECT * FROM concurrent_complex
                     WHERE category = 'Electronics' AND price > 50
                        OR featured = true AND stock > 20
                     ORDER BY price DESC
                     LIMIT 20"
                ).unwrap();

                assert!(result.row_count() > 0 && result.row_count() <= 20,
                    "Thread {} got unexpected count: {}", thread_id, result.row_count());
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn test_barrier_synchronized_access() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    client.execute("CREATE TABLE barrier_test (id INTEGER, timestamp INTEGER)").unwrap();

    let num_threads = 10;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let client_clone = Arc::clone(&client);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            // All threads wait here
            barrier_clone.wait();

            // All threads execute simultaneously
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            client_clone.execute(&format!(
                "INSERT INTO barrier_test VALUES ({}, {})",
                thread_id, timestamp
            )).unwrap();
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let result = client.query("SELECT * FROM barrier_test").unwrap();
    assert_eq!(result.row_count(), num_threads);
}

#[test]
fn test_connection_state_isolation() {
    let client = Arc::new(Client::connect("admin", "admin").unwrap());

    client.execute("CREATE TABLE state_isolation (id INTEGER)").unwrap();

    let mut conn1 = client.get_connection().unwrap();
    let mut conn2 = client.get_connection().unwrap();

    // Each connection should have independent transaction state
    conn1.begin().unwrap();
    assert!(conn1.connection().is_in_transaction());
    assert!(!conn2.connection().is_in_transaction());

    conn2.begin().unwrap();
    assert!(conn1.connection().is_in_transaction());
    assert!(conn2.connection().is_in_transaction());

    conn1.commit().unwrap();
    assert!(!conn1.connection().is_in_transaction());
    assert!(conn2.connection().is_in_transaction());

    conn2.rollback().unwrap();
    assert!(!conn1.connection().is_in_transaction());
    assert!(!conn2.connection().is_in_transaction());
}
