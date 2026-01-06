/// Comprehensive Transaction Tests
///
/// Tests for full MVCC transaction support with snapshot isolation
/// Run with: cargo test --test transaction_comprehensive_tests

use rustmemodb::{Client};

#[tokio::test]
async fn test_transaction_insert_commit() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_insert (id INTEGER, name TEXT)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_insert VALUES (1, 'Alice')").await.unwrap();
    conn.execute("INSERT INTO txn_insert VALUES (2, 'Bob')").await.unwrap();
    conn.commit().await.unwrap();

    let result = client.query("SELECT * FROM txn_insert").await.unwrap();
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_transaction_insert_rollback() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_rollback_ins (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_rollback_ins VALUES (1)").await.unwrap();
    conn.execute("INSERT INTO txn_rollback_ins VALUES (2)").await.unwrap();
    conn.rollback().await.unwrap();

    // After rollback, no rows should exist
    let result = client.query("SELECT * FROM txn_rollback_ins").await.unwrap();
    assert_eq!(result.row_count(), 0, "Rollback should discard all inserts");
}

#[tokio::test]
async fn test_transaction_update_commit() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_update (id INTEGER, value INTEGER)").await.unwrap();
    client.execute("INSERT INTO txn_update VALUES (1, 100)").await.unwrap();
    client.execute("INSERT INTO txn_update VALUES (2, 200)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("UPDATE txn_update SET value = 150 WHERE id = 1").await.unwrap();
    conn.commit().await.unwrap();

    let result = client.query("SELECT value FROM txn_update WHERE id = 1").await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_transaction_update_rollback() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_rollback_upd (id INTEGER, value INTEGER)").await.unwrap();
    client.execute("INSERT INTO txn_rollback_upd VALUES (1, 100)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("UPDATE txn_rollback_upd SET value = 999 WHERE id = 1").await.unwrap();
    conn.rollback().await.unwrap();

    // After rollback, value should still be 100
    let result = client.query("SELECT * FROM txn_rollback_upd WHERE value = 100").await.unwrap();
    assert_eq!(result.row_count(), 1, "Rollback should restore original value");

    let result = client.query("SELECT * FROM txn_rollback_upd WHERE value = 999").await.unwrap();
    assert_eq!(result.row_count(), 0, "Updated value should not exist after rollback");
}

#[tokio::test]
async fn test_transaction_delete_commit() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_delete (id INTEGER)").await.unwrap();
    client.execute("INSERT INTO txn_delete VALUES (1)").await.unwrap();
    client.execute("INSERT INTO txn_delete VALUES (2)").await.unwrap();
    client.execute("INSERT INTO txn_delete VALUES (3)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("DELETE FROM txn_delete WHERE id = 2").await.unwrap();
    conn.commit().await.unwrap();

    let result = client.query("SELECT * FROM txn_delete").await.unwrap();
    assert_eq!(result.row_count(), 2, "One row should be deleted");

    let result = client.query("SELECT * FROM txn_delete WHERE id = 2").await.unwrap();
    assert_eq!(result.row_count(), 0, "Deleted row should not exist");
}

#[tokio::test]
async fn test_transaction_delete_rollback() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_rollback_del (id INTEGER)").await.unwrap();
    client.execute("INSERT INTO txn_rollback_del VALUES (1)").await.unwrap();
    client.execute("INSERT INTO txn_rollback_del VALUES (2)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("DELETE FROM txn_rollback_del WHERE id = 1").await.unwrap();
    conn.rollback().await.unwrap();

    // After rollback, both rows should exist
    let result = client.query("SELECT * FROM txn_rollback_del").await.unwrap();
    assert_eq!(result.row_count(), 2, "Rollback should restore deleted rows");
}

#[tokio::test]
async fn test_transaction_mixed_operations_commit() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_mixed (id INTEGER, value INTEGER)").await.unwrap();
    client.execute("INSERT INTO txn_mixed VALUES (1, 100)").await.unwrap();
    client.execute("INSERT INTO txn_mixed VALUES (2, 200)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_mixed VALUES (3, 300)").await.unwrap();
    conn.execute("UPDATE txn_mixed SET value = 150 WHERE id = 1").await.unwrap();
    conn.execute("DELETE FROM txn_mixed WHERE id = 2").await.unwrap();
    conn.commit().await.unwrap();

    // Should have rows 1 (updated) and 3 (inserted), but not 2 (deleted)
    let result = client.query("SELECT * FROM txn_mixed").await.unwrap();
    assert_eq!(result.row_count(), 2);

    let result = client.query("SELECT * FROM txn_mixed WHERE id = 2").await.unwrap();
    assert_eq!(result.row_count(), 0);
}

#[tokio::test]
async fn test_transaction_mixed_operations_rollback() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_mixed_rb (id INTEGER, value INTEGER)").await.unwrap();
    client.execute("INSERT INTO txn_mixed_rb VALUES (1, 100)").await.unwrap();
    client.execute("INSERT INTO txn_mixed_rb VALUES (2, 200)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_mixed_rb VALUES (3, 300)").await.unwrap();
    conn.execute("UPDATE txn_mixed_rb SET value = 999 WHERE id = 1").await.unwrap();
    conn.execute("DELETE FROM txn_mixed_rb WHERE id = 2").await.unwrap();
    conn.rollback().await.unwrap();

    // After rollback, should have original 2 rows with original values
    let result = client.query("SELECT * FROM txn_mixed_rb").await.unwrap();
    assert_eq!(result.row_count(), 2, "Rollback should restore all original rows");

    let result = client.query("SELECT * FROM txn_mixed_rb WHERE id = 3").await.unwrap();
    assert_eq!(result.row_count(), 0, "Inserted row should not exist");

    let result = client.query("SELECT * FROM txn_mixed_rb WHERE value = 999").await.unwrap();
    assert_eq!(result.row_count(), 0, "Updated value should not exist");
}

#[tokio::test]
async fn test_transaction_multiple_updates_same_row() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_multi_upd (id INTEGER, value INTEGER)").await.unwrap();
    client.execute("INSERT INTO txn_multi_upd VALUES (1, 100)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("UPDATE txn_multi_upd SET value = 200 WHERE id = 1").await.unwrap();
    conn.execute("UPDATE txn_multi_upd SET value = 300 WHERE id = 1").await.unwrap();
    conn.execute("UPDATE txn_multi_upd SET value = 400 WHERE id = 1").await.unwrap();
    conn.commit().await.unwrap();

    // Final value should be 400
    let result = client.query("SELECT * FROM txn_multi_upd WHERE value = 400").await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_transaction_insert_then_update() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_ins_upd (id INTEGER, value INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_ins_upd VALUES (1, 100)").await.unwrap();
    conn.execute("UPDATE txn_ins_upd SET value = 200 WHERE id = 1").await.unwrap();
    conn.commit().await.unwrap();

    let result = client.query("SELECT * FROM txn_ins_upd WHERE value = 200").await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_transaction_insert_then_delete() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_ins_del (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_ins_del VALUES (1)").await.unwrap();
    conn.execute("DELETE FROM txn_ins_del WHERE id = 1").await.unwrap();
    conn.commit().await.unwrap();

    // Net effect: no rows
    let result = client.query("SELECT * FROM txn_ins_del").await.unwrap();
    assert_eq!(result.row_count(), 0);
}

#[tokio::test]
async fn test_transaction_auto_commit_mode() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_auto (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    // Without BEGIN, operations should auto-commit
    conn.execute("INSERT INTO txn_auto VALUES (1)").await.unwrap();

    // Should be immediately visible
    let result = client.query("SELECT * FROM txn_auto").await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_transaction_sequential_on_same_connection() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_seq (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    // First transaction
    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_seq VALUES (1)").await.unwrap();
    conn.commit().await.unwrap();

    // Second transaction
    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_seq VALUES (2)").await.unwrap();
    conn.commit().await.unwrap();

    // Third transaction with rollback
    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_seq VALUES (3)").await.unwrap();
    conn.rollback().await.unwrap();

    let result = client.query("SELECT * FROM txn_seq").await.unwrap();
    assert_eq!(result.row_count(), 2, "Should have rows from first two transactions only");
}

#[tokio::test]
async fn test_transaction_error_nested_transaction() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();

    // Attempting nested BEGIN should fail
    let result = conn.begin().await;
    assert!(result.is_err());

    conn.rollback().await.unwrap();
}

#[tokio::test]
async fn test_transaction_error_commit_without_begin() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    let result = conn.commit().await;
    assert!(result.is_err(), "COMMIT without BEGIN should fail");
}

#[tokio::test]
async fn test_transaction_rollback_without_begin_is_noop() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    // SQL standard: ROLLBACK without transaction is no-op
    let result = conn.rollback().await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_transaction_with_select_inside() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_select (id INTEGER, value INTEGER)").await.unwrap();
    client.execute("INSERT INTO txn_select VALUES (1, 100)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();

    // SELECT within transaction
    let result = conn.execute("SELECT * FROM txn_select").await.unwrap();
    assert_eq!(result.row_count(), 1);

    conn.execute("INSERT INTO txn_select VALUES (2, 200)").await.unwrap();

    // SELECT should see uncommitted changes within transaction
    let result = conn.execute("SELECT * FROM txn_select").await.unwrap();
    assert_eq!(result.row_count(), 2);

    conn.commit().await.unwrap();
}

#[tokio::test]
async fn test_transaction_bulk_insert_rollback() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_bulk (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    for i in 1..=100 {
        conn.execute(&format!("INSERT INTO txn_bulk VALUES ({})", i)).await.unwrap();
    }
    conn.rollback().await.unwrap();

    // All 100 inserts should be rolled back
    let result = client.query("SELECT * FROM txn_bulk").await.unwrap();
    assert_eq!(result.row_count(), 0);
}

#[tokio::test]
async fn test_transaction_bulk_insert_commit() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_bulk_commit (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    for i in 1..=50 {
        conn.execute(&format!("INSERT INTO txn_bulk_commit VALUES ({})", i)).await.unwrap();
    }
    conn.commit().await.unwrap();

    let result = client.query("SELECT * FROM txn_bulk_commit").await.unwrap();
    assert_eq!(result.row_count(), 50);
}

#[tokio::test]
async fn test_transaction_error_recovery() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_error (id INTEGER)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("INSERT INTO txn_error VALUES (1)").await.unwrap();

    // Attempt invalid operation
    let _ = conn.execute("INSERT INTO nonexistent_table VALUES (1)").await;

    // Transaction should still be active, can be rolled back
    assert!(conn.connection().is_in_transaction());
    conn.rollback().await.unwrap();

    let result = client.query("SELECT * FROM txn_error").await.unwrap();
    assert_eq!(result.row_count(), 0);
}

#[tokio::test]
async fn test_transaction_state_tracking_complete() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    assert!(!conn.connection().is_in_transaction());

    conn.begin().await.unwrap();
    assert!(conn.connection().is_in_transaction());

    conn.commit().await.unwrap();
    assert!(!conn.connection().is_in_transaction());

    conn.begin().await.unwrap();
    assert!(conn.connection().is_in_transaction());

    conn.rollback().await.unwrap();
    assert!(!conn.connection().is_in_transaction());
}

#[tokio::test]
async fn test_transaction_update_multiple_rows() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_upd_multi (id INTEGER, category TEXT, value INTEGER)").await.unwrap();
    client.execute("INSERT INTO txn_upd_multi VALUES (1, 'A', 10)").await.unwrap();
    client.execute("INSERT INTO txn_upd_multi VALUES (2, 'A', 20)").await.unwrap();
    client.execute("INSERT INTO txn_upd_multi VALUES (3, 'B', 30)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("UPDATE txn_upd_multi SET value = 99 WHERE category = 'A'").await.unwrap();
    conn.commit().await.unwrap();

    let result = client.query("SELECT * FROM txn_upd_multi WHERE value = 99").await.unwrap();
    assert_eq!(result.row_count(), 2, "Both 'A' category rows should be updated");

    let result = client.query("SELECT * FROM txn_upd_multi WHERE value = 30").await.unwrap();
    assert_eq!(result.row_count(), 1, "'B' category row should be unchanged");
}

#[tokio::test]
async fn test_transaction_delete_multiple_rows() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_del_multi (id INTEGER, status TEXT)").await.unwrap();
    client.execute("INSERT INTO txn_del_multi VALUES (1, 'active')").await.unwrap();
    client.execute("INSERT INTO txn_del_multi VALUES (2, 'inactive')").await.unwrap();
    client.execute("INSERT INTO txn_del_multi VALUES (3, 'inactive')").await.unwrap();
    client.execute("INSERT INTO txn_del_multi VALUES (4, 'active')").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();

    conn.begin().await.unwrap();
    conn.execute("DELETE FROM txn_del_multi WHERE status = 'inactive'").await.unwrap();
    conn.commit().await.unwrap();

    let result = client.query("SELECT * FROM txn_del_multi").await.unwrap();
    assert_eq!(result.row_count(), 2, "Only 'active' rows should remain");
}

#[tokio::test]
async fn test_transaction_connection_drop_auto_rollback() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE txn_drop (id INTEGER)").await.unwrap();

    {
        let mut conn = client.get_connection().await.unwrap();
        conn.begin().await.unwrap();
        conn.execute("INSERT INTO txn_drop VALUES (1)").await.unwrap();
        conn.execute("INSERT INTO txn_drop VALUES (2)").await.unwrap();
        // Explicitly close connection to trigger rollback (async drop is not supported)
        conn.close().await.unwrap();
    }

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    let result = client.query("SELECT * FROM txn_drop").await.unwrap();
    assert_eq!(result.row_count(), 0, "Explicit close should rollback transaction");
}