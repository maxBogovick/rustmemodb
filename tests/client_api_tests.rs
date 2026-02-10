use rustmemodb::core::Value;
/// Client API tests
///
/// Tests for the high-level Client API (PostgreSQL/MySQL-like interface)
/// Run with: cargo test --test client_api_tests
use rustmemodb::{Client, ConnectionConfig};
use std::time::Duration;

#[tokio::test]
async fn test_client_simple_connect() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let stats = client.stats().await;
    assert!(stats.total_connections >= 1);
}

#[tokio::test]
async fn test_client_with_config() {
    let config = ConnectionConfig::new("admin", "adminpass")
        .max_connections(5)
        .min_connections(2);

    let client = Client::connect_with_config(config).await.unwrap();

    let stats = client.stats().await;
    assert_eq!(stats.total_connections, 2); // min_connections
}

#[tokio::test]
async fn test_client_from_url() {
    let client = Client::connect_url("rustmemodb://admin:adminpass@localhost:5432/testdb")
        .await
        .unwrap();

    assert!(client.stats().await.total_connections > 0);
}

#[tokio::test]
async fn test_client_execute_create_table() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let result = client
        .execute("CREATE TABLE test_users (id INTEGER, name TEXT)")
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_client_execute_insert() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client
        .execute("CREATE TABLE test_products (id INTEGER, name TEXT)")
        .await
        .unwrap();

    let result = client
        .execute("INSERT INTO test_products VALUES (1, 'Laptop')")
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_client_query() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client
        .execute("CREATE TABLE test_items (id INTEGER, data TEXT)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_items VALUES (1, 'data1')")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_items VALUES (2, 'data2')")
        .await
        .unwrap();

    let result = client.query("SELECT * FROM test_items").await.unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_client_get_connection() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    assert!(conn.connection().is_active());
    assert_eq!(conn.connection().username(), "admin");
}

#[tokio::test]
async fn test_client_connection_id() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn1 = client.get_connection().await.unwrap();
    let id1 = conn1.connection().id();

    drop(conn1);
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut conn2 = client.get_connection().await.unwrap();
    let id2 = conn2.connection().id();

    // Should reuse same connection
    assert_eq!(id1, id2);
}

#[tokio::test]
async fn test_client_multiple_queries() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client
        .execute("CREATE TABLE test_multi (id INTEGER)")
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .execute(&format!("INSERT INTO test_multi VALUES ({})", i))
            .await
            .unwrap();
    }

    let result = client.query("SELECT * FROM test_multi").await.unwrap();
    assert_eq!(result.row_count(), 10);
}

#[tokio::test]
async fn test_client_pool_stats() {
    let config = ConnectionConfig::new("admin", "adminpass")
        .min_connections(3)
        .max_connections(10);

    let client = Client::connect_with_config(config).await.unwrap();

    let stats = client.stats().await;

    assert_eq!(stats.total_connections, 3);
    assert_eq!(stats.available_connections, 3);
    assert_eq!(stats.active_connections, 0);
    assert_eq!(stats.max_connections, 10);
}

#[tokio::test]
async fn test_client_connection_reuse() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client
        .execute("CREATE TABLE test_reuse (id INTEGER)")
        .await
        .unwrap();

    // Use connection and return to pool
    {
        let mut conn = client.get_connection().await.unwrap();
        conn.execute("INSERT INTO test_reuse VALUES (1)")
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(50)).await;

    let stats = client.stats().await;
    assert_eq!(stats.active_connections, 0);
    assert!(stats.available_connections > 0);
}

#[tokio::test]
async fn test_client_invalid_credentials() {
    let result = Client::connect("invalid_user", "wrong_password").await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_client_timeout_on_pool_exhaustion() {
    let config = ConnectionConfig::new("admin", "adminpass")
        .max_connections(2)
        .connect_timeout(Duration::from_millis(100));

    let client = Client::connect_with_config(config).await.unwrap();

    let _conn1 = client.get_connection().await.unwrap();
    let _conn2 = client.get_connection().await.unwrap();

    // Third connection should timeout
    let result = client.get_connection().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_client_auth_manager_access() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let auth = client.auth_manager();
    let users = auth.list_users().await.unwrap();

    assert!(users.contains(&"admin".to_string()));
}

#[tokio::test]
async fn test_client_execute_with_where() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client
        .execute("CREATE TABLE test_where (id INTEGER, value INTEGER)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_where VALUES (1, 10)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_where VALUES (2, 20)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_where VALUES (3, 30)")
        .await
        .unwrap();

    let result = client
        .query("SELECT * FROM test_where WHERE value > 15")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_client_execute_with_order_by() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client
        .execute("CREATE TABLE test_order (id INTEGER, name TEXT)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_order VALUES (3, 'Charlie')")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_order VALUES (1, 'Alice')")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_order VALUES (2, 'Bob')")
        .await
        .unwrap();

    let result = client
        .query("SELECT * FROM test_order ORDER BY id ASC")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 3);
}

#[tokio::test]
async fn test_prepared_statement_params() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    client
        .execute("CREATE TABLE test_ps (id INTEGER, name TEXT)")
        .await
        .unwrap();

    let mut conn = client.get_connection().await.unwrap();
    let stmt = conn
        .connection()
        .prepare("INSERT INTO test_ps VALUES ($1, $2)")
        .unwrap();
    stmt.execute(&[&1, &"Alice"]).await.unwrap();

    let result = conn
        .execute("SELECT * FROM test_ps WHERE id = 1")
        .await
        .unwrap();
    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows()[0][0], Value::Integer(1));
    assert_eq!(result.rows()[0][1], Value::Text("Alice".to_string()));
}

#[tokio::test]
async fn test_client_execute_with_limit() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client
        .execute("CREATE TABLE test_limit (id INTEGER)")
        .await
        .unwrap();
    for i in 1..=10 {
        client
            .execute(&format!("INSERT INTO test_limit VALUES ({})", i))
            .await
            .unwrap();
    }

    let result = client
        .query("SELECT * FROM test_limit LIMIT 5")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 5);
}

#[tokio::test]
async fn test_client_complex_query() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client
        .execute("CREATE TABLE test_complex (id INTEGER, category TEXT, value INTEGER)")
        .await
        .unwrap();

    client
        .execute("INSERT INTO test_complex VALUES (1, 'A', 100)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_complex VALUES (2, 'B', 200)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_complex VALUES (3, 'A', 150)")
        .await
        .unwrap();
    client
        .execute("INSERT INTO test_complex VALUES (4, 'B', 50)")
        .await
        .unwrap();

    let result = client
        .query(
            "SELECT * FROM test_complex
         WHERE category = 'A' AND value > 120
         ORDER BY value DESC
         LIMIT 1",
        )
        .await
        .unwrap();

    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_client_connection_state() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    assert!(conn.connection().is_active());
    assert!(!conn.connection().is_in_transaction());

    conn.connection().close().await.unwrap();
    assert!(!conn.connection().is_active());
}

#[tokio::test]
#[ignore]
//TODO need fix it
async fn test_client_url_parsing() {
    let urls = vec![
        "rustmemodb://user:pass@localhost/db",
        "rustmemodb://admin:adminpass@localhost:5432/production",
        "rustmemodb://alice:secret@example.com:3306/staging",
    ];

    for url in urls {
        let result = Client::connect_url(url).await;
        assert!(result.is_ok(), "Failed to parse URL: {}", url);
    }
}

#[tokio::test]
async fn test_client_invalid_url() {
    let invalid_urls = vec![
        "invalid://url",
        "rustmemodb://nopassword",
        "rustmemodb://user@nohost",
    ];

    for url in invalid_urls {
        let result = Client::connect_url(url).await;
        assert!(result.is_err(), "Should fail for invalid URL: {}", url);
    }
}
