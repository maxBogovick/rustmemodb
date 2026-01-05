/// Transaction tests
///
/// Tests for transaction support (BEGIN, COMMIT, ROLLBACK)
/// Run with: cargo test --test transaction_tests

use rustmemodb::Client;

#[tokio::test]
async fn test_transaction_begin_commit() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_tx (id INTEGER, data TEXT)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    // Begin transaction
    assert!(conn.begin().await.is_ok());
    assert!(conn.connection().is_in_transaction());

    // Execute operations
    conn.execute("INSERT INTO test_tx VALUES (1, 'data1')").await.unwrap();
    conn.execute("INSERT INTO test_tx VALUES (2, 'data2')").await.unwrap();

    // Commit
    assert!(conn.commit().await.is_ok());
    assert!(!conn.connection().is_in_transaction());

    // Verify data was committed
    let result = client.query("SELECT * FROM test_tx").await.unwrap();
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_transaction_begin_rollback() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_rollback (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("INSERT INTO test_rollback VALUES (1)").await.unwrap();

    // Rollback instead of commit
    assert!(conn.rollback().await.is_ok());
    assert!(!conn.connection().is_in_transaction());

    // Verify data was NOT committed
    let result = client.query("SELECT * FROM test_rollback").await.unwrap();
    assert_eq!(result.row_count(), 0);
}

#[tokio::test]
async fn test_transaction_auto_rollback_on_drop() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_auto_rollback (id INTEGER)").await.unwrap();

    {
        let mut conn = client.get_connection().await.unwrap();
        conn.begin().await.unwrap();
        conn.execute("INSERT INTO test_auto_rollback VALUES (1)").await.unwrap();

        // Connection dropped here without commit - should auto-rollback
    }

    // Verify data was rolled back
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let result = client.query("SELECT * FROM test_auto_rollback").await.unwrap();
    assert_eq!(result.row_count(), 0);
}

#[tokio::test]
async fn test_transaction_multiple_operations() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE accounts (id INTEGER, balance FLOAT)").await.unwrap();
    client.execute("INSERT INTO accounts VALUES (1, 1000.0)").await.unwrap();
    client.execute("INSERT INTO accounts VALUES (2, 500.0)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();

    // Perform multiple operations in transaction
    conn.execute("INSERT INTO accounts VALUES (3, 750.0)").await.unwrap();
    conn.execute("INSERT INTO accounts VALUES (4, 250.0)").await.unwrap();

    conn.commit().await.unwrap();

    let result = client.query("SELECT * FROM accounts").await.unwrap();
    assert_eq!(result.row_count(), 4);
}

#[tokio::test]
async fn test_transaction_error_no_transaction() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    // Try to commit without begin - should fail
    let result = conn.commit().await;
    assert!(result.is_err());

    // Try to rollback without begin - SQL standard: no-op, not error
    let result = conn.rollback().await;
    assert!(result.is_ok(), "ROLLBACK without transaction should be no-op");
}

#[tokio::test]
async fn test_transaction_error_double_begin() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();

    // Try to begin again
    let result = conn.begin().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_transaction_nested_not_supported() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();

    // Nested transaction not supported
    let result = conn.begin().await;
    assert!(result.is_err());

    conn.rollback().await.unwrap();
}

#[tokio::test]
#[ignore] // Isolation not yet fully implemented in storage layer
async fn test_transaction_isolation_between_connections() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_isolation (id INTEGER)").await.unwrap();

    // Connection 1 starts transaction
    let mut conn1 = client.get_connection().await.unwrap();
    conn1.begin().await.unwrap();
    conn1.execute("INSERT INTO test_isolation VALUES (1)").await.unwrap();

    // Connection 2 can still query (reads committed data)
    let mut conn2 = client.get_connection().await.unwrap();
    let result = conn2.execute("SELECT * FROM test_isolation").await.unwrap();

    // Should NOT see uncommitted data from conn1
    assert_eq!(result.row_count(), 0);

    conn1.commit().await.unwrap();
    
    // Now it should be visible
    let result = conn2.execute("SELECT * FROM test_isolation").await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_transaction_rollback_preserves_previous_state() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_preserve (id INTEGER, value INTEGER)").await.unwrap();
    client.execute("INSERT INTO test_preserve VALUES (1, 100)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    // Start transaction and modify data
    conn.begin().await.unwrap();
    conn.execute("INSERT INTO test_preserve VALUES (2, 200)").await.unwrap();
    conn.rollback().await.unwrap();

    // Original data should be preserved
    let result = client.query("SELECT * FROM test_preserve WHERE id = 1").await.unwrap();
    assert_eq!(result.row_count(), 1);
    
    let result = client.query("SELECT * FROM test_preserve WHERE id = 2").await.unwrap();
    assert_eq!(result.row_count(), 0);
}

#[tokio::test]
async fn test_transaction_commit_after_rollback_fails() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.rollback().await.unwrap();

    // Transaction is no longer active
    let result = conn.commit().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_transaction_multiple_sequential() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_sequential (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    // First transaction
    conn.begin().await.unwrap();
    conn.execute("INSERT INTO test_sequential VALUES (1)").await.unwrap();
    conn.commit().await.unwrap();

    // Second transaction
    conn.begin().await.unwrap();
    conn.execute("INSERT INTO test_sequential VALUES (2)").await.unwrap();
    conn.commit().await.unwrap();

    let result = client.query("SELECT * FROM test_sequential").await.unwrap();
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_transaction_state_tracking() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    // Initially not in transaction
    assert!(!conn.connection().is_in_transaction());

    // After begin
    conn.begin().await.unwrap();
    assert!(conn.connection().is_in_transaction());

    // After commit
    conn.commit().await.unwrap();
    assert!(!conn.connection().is_in_transaction());

    // After begin again
    conn.begin().await.unwrap();
    assert!(conn.connection().is_in_transaction());

    // After rollback
    conn.rollback().await.unwrap();
    assert!(!conn.connection().is_in_transaction());
}

#[tokio::test]
async fn test_transaction_with_query_in_middle() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_query_tx (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();

    conn.execute("INSERT INTO test_query_tx VALUES (1)").await.unwrap();

    // Query within transaction
    let result = conn.execute("SELECT * FROM test_query_tx").await.unwrap();
    assert!(result.row_count() >= 1);

    conn.execute("INSERT INTO test_query_tx VALUES (2)").await.unwrap();

    conn.commit().await.unwrap();
}

#[tokio::test]
async fn test_transaction_error_handling() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_error_tx (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();

    conn.execute("INSERT INTO test_error_tx VALUES (1)").await.unwrap();

    // Attempt invalid operation
    let invalid_result = conn.execute("INSERT INTO nonexistent VALUES (1)").await;
    assert!(invalid_result.is_err());

    // Transaction should still be active
    assert!(conn.connection().is_in_transaction());

    // Can still rollback
    assert!(conn.rollback().await.is_ok());
}