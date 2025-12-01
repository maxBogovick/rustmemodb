/// Client API tests
///
/// Tests for the high-level Client API (PostgreSQL/MySQL-like interface)
/// Run with: cargo test --test client_api_tests

use rustmemodb::{Client, ConnectionConfig};
use std::time::Duration;

#[test]
fn test_client_simple_connect() {
    let client = Client::connect("admin", "admin").unwrap();

    let stats = client.stats();
    assert!(stats.total_connections >= 1);
}

#[test]
fn test_client_with_config() {
    let config = ConnectionConfig::new("admin", "admin")
        .max_connections(5)
        .min_connections(2);

    let client = Client::connect_with_config(config).unwrap();

    let stats = client.stats();
    assert_eq!(stats.total_connections, 2); // min_connections
}

#[test]
fn test_client_from_url() {
    let client = Client::connect_url(
        "rustmemodb://admin:admin@localhost:5432/testdb"
    ).unwrap();

    assert!(client.stats().total_connections > 0);
}

#[test]
fn test_client_execute_create_table() {
    let client = Client::connect("admin", "admin").unwrap();

    let result = client.execute(
        "CREATE TABLE test_users (id INTEGER, name TEXT)"
    );

    assert!(result.is_ok());
}

#[test]
fn test_client_execute_insert() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_products (id INTEGER, name TEXT)").unwrap();

    let result = client.execute(
        "INSERT INTO test_products VALUES (1, 'Laptop')"
    );

    assert!(result.is_ok());
}

#[test]
fn test_client_query() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_items (id INTEGER, data TEXT)").unwrap();
    client.execute("INSERT INTO test_items VALUES (1, 'data1')").unwrap();
    client.execute("INSERT INTO test_items VALUES (2, 'data2')").unwrap();

    let result = client.query("SELECT * FROM test_items").unwrap();

    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_client_get_connection() {
    let client = Client::connect("admin", "admin").unwrap();

    let mut conn = client.get_connection().unwrap();

    assert!(conn.connection().is_active());
    assert_eq!(conn.connection().username(), "admin");
}

#[test]
fn test_client_connection_id() {
    let client = Client::connect("admin", "admin").unwrap();

    let mut conn1 = client.get_connection().unwrap();
    let id1 = conn1.connection().id();

    drop(conn1);
    std::thread::sleep(Duration::from_millis(10));

    let mut conn2 = client.get_connection().unwrap();
    let id2 = conn2.connection().id();

    // Should reuse same connection
    assert_eq!(id1, id2);
}

#[test]
fn test_client_multiple_queries() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_multi (id INTEGER)").unwrap();

    for i in 1..=10 {
        client.execute(&format!("INSERT INTO test_multi VALUES ({})", i)).unwrap();
    }

    let result = client.query("SELECT * FROM test_multi").unwrap();
    assert_eq!(result.row_count(), 10);
}

#[test]
fn test_client_pool_stats() {
    let config = ConnectionConfig::new("admin", "admin")
        .min_connections(3)
        .max_connections(10);

    let client = Client::connect_with_config(config).unwrap();

    let stats = client.stats();

    assert_eq!(stats.total_connections, 3);
    assert_eq!(stats.available_connections, 3);
    assert_eq!(stats.active_connections, 0);
    assert_eq!(stats.max_connections, 10);
}

#[test]
fn test_client_connection_reuse() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_reuse (id INTEGER)").unwrap();

    // Use connection and return to pool
    {
        let mut conn = client.get_connection().unwrap();
        conn.execute("INSERT INTO test_reuse VALUES (1)").unwrap();
    }

    std::thread::sleep(Duration::from_millis(10));

    let stats = client.stats();
    assert_eq!(stats.active_connections, 0);
    assert!(stats.available_connections > 0);
}

#[test]
fn test_client_invalid_credentials() {
    let result = Client::connect("invalid_user", "wrong_password");

    assert!(result.is_err());
}

#[test]
fn test_client_timeout_on_pool_exhaustion() {
    let config = ConnectionConfig::new("admin", "admin")
        .max_connections(2)
        .connect_timeout(Duration::from_millis(100));

    let client = Client::connect_with_config(config).unwrap();

    let _conn1 = client.get_connection().unwrap();
    let _conn2 = client.get_connection().unwrap();

    // Third connection should timeout
    let result = client.get_connection();
    assert!(result.is_err());
}

#[test]
fn test_client_auth_manager_access() {
    let client = Client::connect("admin", "admin").unwrap();

    let auth = client.auth_manager();
    let users = auth.list_users().unwrap();

    assert!(users.contains(&"admin".to_string()));
}

#[test]
fn test_client_execute_with_where() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_where (id INTEGER, value INTEGER)").unwrap();
    client.execute("INSERT INTO test_where VALUES (1, 10)").unwrap();
    client.execute("INSERT INTO test_where VALUES (2, 20)").unwrap();
    client.execute("INSERT INTO test_where VALUES (3, 30)").unwrap();

    let result = client.query("SELECT * FROM test_where WHERE value > 15").unwrap();

    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_client_execute_with_order_by() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_order (id INTEGER, name TEXT)").unwrap();
    client.execute("INSERT INTO test_order VALUES (3, 'Charlie')").unwrap();
    client.execute("INSERT INTO test_order VALUES (1, 'Alice')").unwrap();
    client.execute("INSERT INTO test_order VALUES (2, 'Bob')").unwrap();

    let result = client.query("SELECT * FROM test_order ORDER BY id ASC").unwrap();

    assert_eq!(result.row_count(), 3);
}

#[test]
fn test_client_execute_with_limit() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute("CREATE TABLE test_limit (id INTEGER)").unwrap();
    for i in 1..=10 {
        client.execute(&format!("INSERT INTO test_limit VALUES ({})", i)).unwrap();
    }

    let result = client.query("SELECT * FROM test_limit LIMIT 5").unwrap();

    assert_eq!(result.row_count(), 5);
}

#[test]
fn test_client_complex_query() {
    let client = Client::connect("admin", "admin").unwrap();

    client.execute(
        "CREATE TABLE test_complex (id INTEGER, category TEXT, value INTEGER)"
    ).unwrap();

    client.execute("INSERT INTO test_complex VALUES (1, 'A', 100)").unwrap();
    client.execute("INSERT INTO test_complex VALUES (2, 'B', 200)").unwrap();
    client.execute("INSERT INTO test_complex VALUES (3, 'A', 150)").unwrap();
    client.execute("INSERT INTO test_complex VALUES (4, 'B', 50)").unwrap();

    let result = client.query(
        "SELECT * FROM test_complex
         WHERE category = 'A' AND value > 120
         ORDER BY value DESC
         LIMIT 1"
    ).unwrap();

    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_client_connection_state() {
    let client = Client::connect("admin", "admin").unwrap();

    let mut conn = client.get_connection().unwrap();

    assert!(conn.connection().is_active());
    assert!(!conn.connection().is_in_transaction());

    conn.connection().close().unwrap();
    assert!(!conn.connection().is_active());
}

#[test]
#[ignore]
//TODO need fix it
fn test_client_url_parsing() {
    let urls = vec![
        "rustmemodb://user:pass@localhost/db",
        "rustmemodb://admin:admin@localhost:5432/production",
        "rustmemodb://alice:secret@example.com:3306/staging",
    ];

    for url in urls {
        let result = Client::connect_url(url);
        assert!(result.is_ok(), "Failed to parse URL: {}", url);
    }
}

#[test]
fn test_client_invalid_url() {
    let invalid_urls = vec![
        "invalid://url",
        "rustmemodb://nopassword",
        "rustmemodb://user@nohost",
    ];

    for url in invalid_urls {
        let result = Client::connect_url(url);
        assert!(result.is_err(), "Should fail for invalid URL: {}", url);
    }
}
