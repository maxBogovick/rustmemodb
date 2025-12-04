/// Comprehensive Transaction Tests
///
/// Tests for full MVCC transaction support with snapshot isolation
/// Run with: cargo test --test transaction_comprehensive_tests

use rustmemodb::{Client, Value};

#[test]
fn test_transaction_insert_commit() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_insert (id INTEGER, name TEXT)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_insert VALUES (1, 'Alice')").unwrap();
    conn.execute("INSERT INTO txn_insert VALUES (2, 'Bob')").unwrap();
    conn.commit().unwrap();

    let result = client.query("SELECT * FROM txn_insert").unwrap();
    assert_eq!(result.row_count(), 2);
}

#[test]
fn test_transaction_insert_rollback() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_rollback_ins (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_rollback_ins VALUES (1)").unwrap();
    conn.execute("INSERT INTO txn_rollback_ins VALUES (2)").unwrap();
    conn.rollback().unwrap();

    // After rollback, no rows should exist
    let result = client.query("SELECT * FROM txn_rollback_ins").unwrap();
    assert_eq!(result.row_count(), 0, "Rollback should discard all inserts");
}

#[test]
fn test_transaction_update_commit() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_update (id INTEGER, value INTEGER)").unwrap();
    client.execute("INSERT INTO txn_update VALUES (1, 100)").unwrap();
    client.execute("INSERT INTO txn_update VALUES (2, 200)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("UPDATE txn_update SET value = 150 WHERE id = 1").unwrap();
    conn.commit().unwrap();

    let result = client.query("SELECT value FROM txn_update WHERE id = 1").unwrap();
    assert_eq!(result.row_count(), 1);
    // TODO: Verify value is 150 when we have value extraction API
}

#[test]
fn test_transaction_update_rollback() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_rollback_upd (id INTEGER, value INTEGER)").unwrap();
    client.execute("INSERT INTO txn_rollback_upd VALUES (1, 100)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("UPDATE txn_rollback_upd SET value = 999 WHERE id = 1").unwrap();
    conn.rollback().unwrap();

    // After rollback, value should still be 100
    let result = client.query("SELECT * FROM txn_rollback_upd WHERE value = 100").unwrap();
    assert_eq!(result.row_count(), 1, "Rollback should restore original value");

    let result = client.query("SELECT * FROM txn_rollback_upd WHERE value = 999").unwrap();
    assert_eq!(result.row_count(), 0, "Updated value should not exist after rollback");
}

#[test]
fn test_transaction_delete_commit() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_delete (id INTEGER)").unwrap();
    client.execute("INSERT INTO txn_delete VALUES (1)").unwrap();
    client.execute("INSERT INTO txn_delete VALUES (2)").unwrap();
    client.execute("INSERT INTO txn_delete VALUES (3)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("DELETE FROM txn_delete WHERE id = 2").unwrap();
    conn.commit().unwrap();

    let result = client.query("SELECT * FROM txn_delete").unwrap();
    assert_eq!(result.row_count(), 2, "One row should be deleted");

    let result = client.query("SELECT * FROM txn_delete WHERE id = 2").unwrap();
    assert_eq!(result.row_count(), 0, "Deleted row should not exist");
}

#[test]
fn test_transaction_delete_rollback() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_rollback_del (id INTEGER)").unwrap();
    client.execute("INSERT INTO txn_rollback_del VALUES (1)").unwrap();
    client.execute("INSERT INTO txn_rollback_del VALUES (2)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("DELETE FROM txn_rollback_del WHERE id = 1").unwrap();
    conn.rollback().unwrap();

    // After rollback, both rows should exist
    let result = client.query("SELECT * FROM txn_rollback_del").unwrap();
    assert_eq!(result.row_count(), 2, "Rollback should restore deleted rows");
}

#[test]
fn test_transaction_mixed_operations_commit() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_mixed (id INTEGER, value INTEGER)").unwrap();
    client.execute("INSERT INTO txn_mixed VALUES (1, 100)").unwrap();
    client.execute("INSERT INTO txn_mixed VALUES (2, 200)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_mixed VALUES (3, 300)").unwrap();
    conn.execute("UPDATE txn_mixed SET value = 150 WHERE id = 1").unwrap();
    conn.execute("DELETE FROM txn_mixed WHERE id = 2").unwrap();
    conn.commit().unwrap();

    // Should have rows 1 (updated) and 3 (inserted), but not 2 (deleted)
    let result = client.query("SELECT * FROM txn_mixed").unwrap();
    assert_eq!(result.row_count(), 2);

    let result = client.query("SELECT * FROM txn_mixed WHERE id = 2").unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_transaction_mixed_operations_rollback() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_mixed_rb (id INTEGER, value INTEGER)").unwrap();
    client.execute("INSERT INTO txn_mixed_rb VALUES (1, 100)").unwrap();
    client.execute("INSERT INTO txn_mixed_rb VALUES (2, 200)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_mixed_rb VALUES (3, 300)").unwrap();
    conn.execute("UPDATE txn_mixed_rb SET value = 999 WHERE id = 1").unwrap();
    conn.execute("DELETE FROM txn_mixed_rb WHERE id = 2").unwrap();
    conn.rollback().unwrap();

    // After rollback, should have original 2 rows with original values
    let result = client.query("SELECT * FROM txn_mixed_rb").unwrap();
    assert_eq!(result.row_count(), 2, "Rollback should restore all original rows");

    let result = client.query("SELECT * FROM txn_mixed_rb WHERE id = 3").unwrap();
    assert_eq!(result.row_count(), 0, "Inserted row should not exist");

    let result = client.query("SELECT * FROM txn_mixed_rb WHERE value = 999").unwrap();
    assert_eq!(result.row_count(), 0, "Updated value should not exist");
}

#[test]
fn test_transaction_multiple_updates_same_row() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_multi_upd (id INTEGER, value INTEGER)").unwrap();
    client.execute("INSERT INTO txn_multi_upd VALUES (1, 100)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("UPDATE txn_multi_upd SET value = 200 WHERE id = 1").unwrap();
    conn.execute("UPDATE txn_multi_upd SET value = 300 WHERE id = 1").unwrap();
    conn.execute("UPDATE txn_multi_upd SET value = 400 WHERE id = 1").unwrap();
    conn.commit().unwrap();

    // Final value should be 400
    let result = client.query("SELECT * FROM txn_multi_upd WHERE value = 400").unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_transaction_insert_then_update() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_ins_upd (id INTEGER, value INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_ins_upd VALUES (1, 100)").unwrap();
    conn.execute("UPDATE txn_ins_upd SET value = 200 WHERE id = 1").unwrap();
    conn.commit().unwrap();

    let result = client.query("SELECT * FROM txn_ins_upd WHERE value = 200").unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_transaction_insert_then_delete() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_ins_del (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_ins_del VALUES (1)").unwrap();
    conn.execute("DELETE FROM txn_ins_del WHERE id = 1").unwrap();
    conn.commit().unwrap();

    // Net effect: no rows
    let result = client.query("SELECT * FROM txn_ins_del").unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_transaction_auto_commit_mode() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_auto (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    // Without BEGIN, operations should auto-commit
    conn.execute("INSERT INTO txn_auto VALUES (1)").unwrap();

    // Should be immediately visible
    let result = client.query("SELECT * FROM txn_auto").unwrap();
    assert_eq!(result.row_count(), 1);
}

#[test]
fn test_transaction_sequential_on_same_connection() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_seq (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    // First transaction
    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_seq VALUES (1)").unwrap();
    conn.commit().unwrap();

    // Second transaction
    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_seq VALUES (2)").unwrap();
    conn.commit().unwrap();

    // Third transaction with rollback
    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_seq VALUES (3)").unwrap();
    conn.rollback().unwrap();

    let result = client.query("SELECT * FROM txn_seq").unwrap();
    assert_eq!(result.row_count(), 2, "Should have rows from first two transactions only");
}

#[test]
fn test_transaction_error_nested_transaction() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();

    // Attempting nested BEGIN should fail
    let result = conn.begin();
    assert!(result.is_err());

    conn.rollback().unwrap();
}

#[test]
fn test_transaction_error_commit_without_begin() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let mut conn = client.get_connection().unwrap();

    let result = conn.commit();
    assert!(result.is_err(), "COMMIT without BEGIN should fail");
}

#[test]
fn test_transaction_rollback_without_begin_is_noop() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let mut conn = client.get_connection().unwrap();

    // SQL standard: ROLLBACK without transaction is no-op
    let result = conn.rollback();
    assert!(result.is_ok());
}

#[test]
fn test_transaction_with_select_inside() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_select (id INTEGER, value INTEGER)").unwrap();
    client.execute("INSERT INTO txn_select VALUES (1, 100)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();

    // SELECT within transaction
    let result = conn.execute("SELECT * FROM txn_select").unwrap();
    assert_eq!(result.row_count(), 1);

    conn.execute("INSERT INTO txn_select VALUES (2, 200)").unwrap();

    // SELECT should see uncommitted changes within transaction
    let result = conn.execute("SELECT * FROM txn_select").unwrap();
    assert_eq!(result.row_count(), 2);

    conn.commit().unwrap();
}

#[test]
fn test_transaction_bulk_insert_rollback() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_bulk (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    for i in 1..=100 {
        conn.execute(&format!("INSERT INTO txn_bulk VALUES ({})", i)).unwrap();
    }
    conn.rollback().unwrap();

    // All 100 inserts should be rolled back
    let result = client.query("SELECT * FROM txn_bulk").unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_transaction_bulk_insert_commit() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_bulk_commit (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    for i in 1..=50 {
        conn.execute(&format!("INSERT INTO txn_bulk_commit VALUES ({})", i)).unwrap();
    }
    conn.commit().unwrap();

    let result = client.query("SELECT * FROM txn_bulk_commit").unwrap();
    assert_eq!(result.row_count(), 50);
}

#[test]
fn test_transaction_error_recovery() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_error (id INTEGER)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("INSERT INTO txn_error VALUES (1)").unwrap();

    // Attempt invalid operation
    let _ = conn.execute("INSERT INTO nonexistent_table VALUES (1)");

    // Transaction should still be active, can be rolled back
    assert!(conn.connection().is_in_transaction());
    conn.rollback().unwrap();

    let result = client.query("SELECT * FROM txn_error").unwrap();
    assert_eq!(result.row_count(), 0);
}

#[test]
fn test_transaction_state_tracking_complete() {
    let client = Client::connect("admin", "adminpass").unwrap();

    let mut conn = client.get_connection().unwrap();

    assert!(!conn.connection().is_in_transaction());

    conn.begin().unwrap();
    assert!(conn.connection().is_in_transaction());

    conn.commit().unwrap();
    assert!(!conn.connection().is_in_transaction());

    conn.begin().unwrap();
    assert!(conn.connection().is_in_transaction());

    conn.rollback().unwrap();
    assert!(!conn.connection().is_in_transaction());
}

#[test]
fn test_transaction_update_multiple_rows() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_upd_multi (id INTEGER, category TEXT, value INTEGER)").unwrap();
    client.execute("INSERT INTO txn_upd_multi VALUES (1, 'A', 10)").unwrap();
    client.execute("INSERT INTO txn_upd_multi VALUES (2, 'A', 20)").unwrap();
    client.execute("INSERT INTO txn_upd_multi VALUES (3, 'B', 30)").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("UPDATE txn_upd_multi SET value = 99 WHERE category = 'A'").unwrap();
    conn.commit().unwrap();

    let result = client.query("SELECT * FROM txn_upd_multi WHERE value = 99").unwrap();
    assert_eq!(result.row_count(), 2, "Both 'A' category rows should be updated");

    let result = client.query("SELECT * FROM txn_upd_multi WHERE value = 30").unwrap();
    assert_eq!(result.row_count(), 1, "'B' category row should be unchanged");
}

#[test]
fn test_transaction_delete_multiple_rows() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_del_multi (id INTEGER, status TEXT)").unwrap();
    client.execute("INSERT INTO txn_del_multi VALUES (1, 'active')").unwrap();
    client.execute("INSERT INTO txn_del_multi VALUES (2, 'inactive')").unwrap();
    client.execute("INSERT INTO txn_del_multi VALUES (3, 'inactive')").unwrap();
    client.execute("INSERT INTO txn_del_multi VALUES (4, 'active')").unwrap();

    let mut conn = client.get_connection().unwrap();

    conn.begin().unwrap();
    conn.execute("DELETE FROM txn_del_multi WHERE status = 'inactive'").unwrap();
    conn.commit().unwrap();

    let result = client.query("SELECT * FROM txn_del_multi").unwrap();
    assert_eq!(result.row_count(), 2, "Only 'active' rows should remain");
}

#[test]
fn test_transaction_connection_drop_auto_rollback() {
    let client = Client::connect("admin", "adminpass").unwrap();

    client.execute("CREATE TABLE txn_drop (id INTEGER)").unwrap();

    {
        let mut conn = client.get_connection().unwrap();
        conn.begin().unwrap();
        conn.execute("INSERT INTO txn_drop VALUES (1)").unwrap();
        conn.execute("INSERT INTO txn_drop VALUES (2)").unwrap();
        // Connection dropped without commit - should auto-rollback
    }

    let result = client.query("SELECT * FROM txn_drop").unwrap();
    assert_eq!(result.row_count(), 0, "Auto-rollback on drop should discard changes");
}
