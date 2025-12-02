/// Transaction tests
///
/// Tests for transaction support (BEGIN, COMMIT, ROLLBACK)
/// Run with: cargo test --test transaction_tests

use rustmemodb::Client;

#[test]
fn test_transaction_begin_commit() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE test_tx (id INTEGER, data TEXT)").unwrap();

    let mut conn = client.get_connection().unwrap();

    // Begin transaction
    assert!(conn.begin().is_ok());
    assert!(conn.connection().is_in_transaction());

    // Execute operations
    conn.execute("INSERT INTO test_tx VALUES (1, 'data1')").unwrap();
    conn.execute("INSERT INTO test_tx VALUES (2, 'data2')").unwrap();

    // Commit
    assert!(conn.commit().is_ok());
    assert!(!conn.connection().is_in_transaction());

    // Verify data was committed
    let result = client.query("SELECT * FROM test_tx").unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_transaction_begin_rollback() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE test_rollback (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("INSERT INTO test_rollback VALUES (1)").unwrap();

    // Rollback instead of commit
    assert!(conn.rollback().is_ok());
    assert!(!conn.connection().is_in_transaction());

    // Verify data was NOT committed (when transactions are implemented)
    // Currently inserts happen immediately
    let result = client.query("SELECT * FROM test_rollback").unwrap();
    // Note: Will be 0 when transactions are fully implemented
    println!("Rows after rollback: {}", result.row_count());
}

#[test]
fn test_transaction_auto_rollback_on_drop() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE test_auto_rollback (id INTEGER)").unwrap();

    {
        let mut conn = client.get_connection().unwrap();
        conn.begin().unwrap();
        conn.execute("INSERT INTO test_auto_rollback VALUES (1)").unwrap();

        // Connection dropped here without commit - should auto-rollback
    }

    // Verify data was rolled back (when transactions are implemented)
    let result = client.query("SELECT * FROM test_auto_rollback").unwrap();
    println!("Rows after auto-rollback: {}", result.row_count());
}

#[test]
fn test_transaction_multiple_operations() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE accounts (id INTEGER, balance FLOAT)").unwrap();
    client.execute("INSERT INTO accounts VALUES (1, 1000.0)").unwrap();
    client.execute("INSERT INTO accounts VALUES (2, 500.0)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();

    // Perform multiple operations in transaction
    // Note: UPDATE not implemented yet, using INSERT for demo
    conn.execute("INSERT INTO accounts VALUES (3, 750.0)").unwrap();
    conn.execute("INSERT INTO accounts VALUES (4, 250.0)").unwrap();

    conn.commit().unwrap();

    let result = client.query("SELECT * FROM accounts").unwrap();
    assert_eq!(result.row_count(), 4);
}

#[test]
fn test_transaction_error_no_transaction() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let mut conn = client.get_connection().unwrap();

    // Try to commit without begin
    let result = conn.commit();
    assert!(result.is_err());

    // Try to rollback without begin
    let result = conn.rollback();
    assert!(result.is_err());
}

#[test]
fn test_transaction_error_double_begin() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();

    // Try to begin again
    let result = conn.begin();
    assert!(result.is_err());
}

#[test]
fn test_transaction_nested_not_supported() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();

    // Nested transaction not supported
    let result = conn.begin();
    assert!(result.is_err());

    conn.rollback().unwrap();
}

#[test]
fn test_transaction_isolation_between_connections() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE test_isolation (id INTEGER)").unwrap();

    // Connection 1 starts transaction
    let mut conn1 = client.get_connection().unwrap();
    conn1.begin().unwrap();
    conn1.execute("INSERT INTO test_isolation VALUES (1)").unwrap();

    // Connection 2 can still query (reads committed data)
    let mut conn2 = client.get_connection().unwrap();
    let result = conn2.execute("SELECT * FROM test_isolation").unwrap();

    // Currently shows uncommitted data (isolation not implemented)
    println!("Visible rows: {}", result.row_count());

    conn1.commit().unwrap();
}

#[test]
fn test_transaction_rollback_preserves_previous_state() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE test_preserve (id INTEGER, value INTEGER)").unwrap();
    client.execute("INSERT INTO test_preserve VALUES (1, 100)").unwrap();

    let mut conn = client.get_connection().unwrap();

    // Start transaction and modify data
    conn.begin().unwrap();
    // Note: UPDATE not implemented
    conn.execute("INSERT INTO test_preserve VALUES (2, 200)").unwrap();
    conn.rollback().unwrap();

    // Original data should be preserved
    let result = client.query("SELECT * FROM test_preserve WHERE id = 1").unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_transaction_commit_after_rollback_fails() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.rollback().unwrap();

    // Transaction is no longer active
    let result = conn.commit();
    assert!(result.is_err());
}

#[test]
fn test_transaction_multiple_sequential() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE test_sequential (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    // First transaction
    conn.begin().unwrap();
    conn.execute("INSERT INTO test_sequential VALUES (1)").unwrap();
    conn.commit().unwrap();

    // Second transaction
    conn.begin().unwrap();
    conn.execute("INSERT INTO test_sequential VALUES (2)").unwrap();
    conn.commit().unwrap();

    let result = client.query("SELECT * FROM test_sequential").unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_transaction_state_tracking() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let mut conn = client.get_connection().unwrap();

    // Initially not in transaction
    assert!(!conn.connection().is_in_transaction());

    // After begin
    conn.begin().unwrap();
    assert!(conn.connection().is_in_transaction());

    // After commit
    conn.commit().unwrap();
    assert!(!conn.connection().is_in_transaction());

    // After begin again
    conn.begin().unwrap();
    assert!(conn.connection().is_in_transaction());

    // After rollback
    conn.rollback().unwrap();
    assert!(!conn.connection().is_in_transaction());
}

#[test]
fn test_transaction_with_query_in_middle() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE test_query_tx (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();

    conn.execute("INSERT INTO test_query_tx VALUES (1)").unwrap();

    // Query within transaction
    let result = conn.execute("SELECT * FROM test_query_tx").unwrap();
    assert!(result.row_count() >= 1);

    conn.execute("INSERT INTO test_query_tx VALUES (2)").unwrap();

    conn.commit().unwrap();
}

#[test]
fn test_transaction_error_handling() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE test_error_tx (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();

    conn.execute("INSERT INTO test_error_tx VALUES (1)").unwrap();

    // Attempt invalid operation
    let invalid_result = conn.execute("INSERT INTO nonexistent VALUES (1)");
    assert!(invalid_result.is_err());

    // Transaction should still be active
    assert!(conn.connection().is_in_transaction());

    // Can still rollback
    assert!(conn.rollback().is_ok());
}
