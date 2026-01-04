//! Integration tests for WAL persistence and crash recovery

use rustmemodb::{InMemoryDB, DurabilityMode};
use tempfile::TempDir;

#[test]
fn test_persistence_create_table() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = InMemoryDB::new();

    // Enable persistence
    db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
        .unwrap();

    assert!(db.is_persistence_enabled());
    assert_eq!(db.durability_mode(), Some(DurabilityMode::Sync));

    // Create table
    db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .unwrap();

    assert!(db.table_exists("users"));

    // Verify data persisted by checking WAL file exists
    let wal_path = temp_dir.path().join("rustmemodb.wal");
    assert!(wal_path.exists());
}

#[test]
fn test_persistence_drop_table() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = InMemoryDB::new();

    db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
        .unwrap();

    // Create and drop table
    db.execute("CREATE TABLE temp (id INTEGER)").unwrap();
    assert!(db.table_exists("temp"));

    db.execute("DROP TABLE temp").unwrap();
    assert!(!db.table_exists("temp"));

    // WAL should still exist
    let wal_path = temp_dir.path().join("rustmemodb.wal");
    assert!(wal_path.exists());
}

#[test]
fn test_checkpoint_creates_snapshot() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = InMemoryDB::new();

    db.enable_persistence(temp_dir.path(), DurabilityMode::Async)
        .unwrap();

    // Create some tables
    db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .unwrap();
    db.execute("CREATE TABLE products (id INTEGER, price FLOAT)")
        .unwrap();

    // Manually trigger checkpoint
    db.checkpoint().unwrap();

    // Snapshot file should exist
    let snapshot_path = temp_dir.path().join("rustmemodb.snapshot");
    assert!(snapshot_path.exists());
}

#[test]
fn test_crash_recovery_after_create_table() {
    let temp_dir = TempDir::new().unwrap();

    // Session 1: Create tables
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
            .unwrap();
        db.execute("CREATE TABLE products (id INTEGER, name TEXT, price FLOAT)")
            .unwrap();

        // Simulate crash - drop database without checkpoint
    }

    // Session 2: Recover database
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        // Tables should be recovered
        assert!(db.table_exists("users"));
        assert!(db.table_exists("products"));

        // Verify schemas
        let stats = db.table_stats("users").unwrap();
        assert_eq!(stats.column_count, 2);

        let stats = db.table_stats("products").unwrap();
        assert_eq!(stats.column_count, 3);
    }
}

#[test]
fn test_recovery_from_snapshot_and_wal() {
    let temp_dir = TempDir::new().unwrap();

    // Session 1: Create checkpoint
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
            .unwrap();

        // Create checkpoint
        db.checkpoint().unwrap();

        // Make more changes after checkpoint
        db.execute("CREATE TABLE orders (id INTEGER, total FLOAT)")
            .unwrap();

        // Simulate crash without checkpoint
    }

    // Session 2: Recover from snapshot + WAL
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        // Both tables should be recovered
        assert!(db.table_exists("users"));
        assert!(db.table_exists("orders"));
    }
}

#[test]
fn test_disable_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = InMemoryDB::new();

    // Enable
    db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
        .unwrap();
    assert!(db.is_persistence_enabled());

    // Disable
    db.disable_persistence().unwrap();
    assert!(!db.is_persistence_enabled());

    // Operations should still work (in-memory only)
    db.execute("CREATE TABLE test (id INTEGER)").unwrap();
    assert!(db.table_exists("test"));
}

#[test]
fn test_durability_mode_async() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = InMemoryDB::new();

    db.enable_persistence(temp_dir.path(), DurabilityMode::Async)
        .unwrap();

    assert_eq!(db.durability_mode(), Some(DurabilityMode::Async));

    // Operations should work
    db.execute("CREATE TABLE test (id INTEGER)").unwrap();
    assert!(db.table_exists("test"));
}

#[test]
fn test_durability_mode_none() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = InMemoryDB::new();

    db.enable_persistence(temp_dir.path(), DurabilityMode::None)
        .unwrap();

    assert_eq!(db.durability_mode(), Some(DurabilityMode::None));

    // Operations should work, but no files created
    db.execute("CREATE TABLE test (id INTEGER)").unwrap();

    // WAL file should NOT exist with DurabilityMode::None
    let wal_path = temp_dir.path().join("rustmemodb.wal");
    assert!(!wal_path.exists());
}

#[test]
fn test_multiple_create_drop_operations() {
    let temp_dir = TempDir::new().unwrap();

    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        // Create multiple tables
        db.execute("CREATE TABLE t1 (id INTEGER)").unwrap();
        db.execute("CREATE TABLE t2 (id INTEGER)").unwrap();
        db.execute("CREATE TABLE t3 (id INTEGER)").unwrap();

        // Drop one
        db.execute("DROP TABLE t2").unwrap();

        // Create another
        db.execute("CREATE TABLE t4 (id INTEGER)").unwrap();
    }

    // Recover and verify
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        assert!(db.table_exists("t1"));
        assert!(!db.table_exists("t2")); // Was dropped
        assert!(db.table_exists("t3"));
        assert!(db.table_exists("t4"));
    }
}

#[test]
fn test_checkpoint_after_many_operations() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = InMemoryDB::new();

    db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
        .unwrap();

    // Create many tables (should not trigger auto-checkpoint with default threshold)
    for i in 0..10 {
        db.execute(&format!("CREATE TABLE table_{} (id INTEGER)", i))
            .unwrap();
    }

    // Manual checkpoint
    db.checkpoint().unwrap();

    // Verify all tables exist
    for i in 0..10 {
        assert!(db.table_exists(&format!("table_{}", i)));
    }

    // Snapshot should exist
    let snapshot_path = temp_dir.path().join("rustmemodb.snapshot");
    assert!(snapshot_path.exists());
}

#[test]
fn test_cannot_enable_persistence_twice() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = InMemoryDB::new();

    db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
        .unwrap();

    // Second enable should fail
    let result = db.enable_persistence(temp_dir.path(), DurabilityMode::Sync);
    assert!(result.is_err());
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("Persistence already enabled"));
}

#[test]
fn test_empty_database_checkpoint() {
    let temp_dir = TempDir::new().unwrap();
    let mut db = InMemoryDB::new();

    db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
        .unwrap();

    // Checkpoint empty database
    db.checkpoint().unwrap();

    // Snapshot should still be created
    let snapshot_path = temp_dir.path().join("rustmemodb.snapshot");
    assert!(snapshot_path.exists());
}

// ============================================================================
// DML Persistence Tests (INSERT, UPDATE, DELETE)
// ============================================================================

#[test]
fn test_insert_persistence_and_recovery() {
    let temp_dir = TempDir::new().unwrap();

    // Session 1: Create table and insert data
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
            .unwrap();
        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
        db.execute("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap();
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)").unwrap();

        // Verify data is there
        let result = db.execute("SELECT * FROM users").unwrap();
        assert_eq!(result.row_count(), 3);

        // Simulate crash - drop database without checkpoint
    }

    // Session 2: Recover database
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        // Table and data should be recovered
        assert!(db.table_exists("users"));

        let result = db.execute("SELECT * FROM users").unwrap();
        assert_eq!(result.row_count(), 3);

        // Verify specific data
        let rows = result.rows();
        assert!(rows.iter().any(|r| r[0].to_string() == "1" && r[1].to_string() == "Alice"));
        assert!(rows.iter().any(|r| r[0].to_string() == "2" && r[1].to_string() == "Bob"));
        assert!(rows.iter().any(|r| r[0].to_string() == "3" && r[1].to_string() == "Charlie"));
    }
}

#[test]
fn test_update_persistence_and_recovery() {
    let temp_dir = TempDir::new().unwrap();

    // Session 1: Insert and update data
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        db.execute("CREATE TABLE products (id INTEGER, name TEXT, price FLOAT)")
            .unwrap();
        db.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99)").unwrap();
        db.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99)").unwrap();

        // Update prices
        db.execute("UPDATE products SET price = 899.99 WHERE id = 1").unwrap();
        db.execute("UPDATE products SET price = 19.99 WHERE id = 2").unwrap();

        // Verify updates
        let result = db.execute("SELECT * FROM products WHERE id = 1").unwrap();
        let row = &result.rows()[0];
        assert_eq!(row[2].to_string(), "899.99");

        // Simulate crash
    }

    // Session 2: Recover and verify updates
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        assert!(db.table_exists("products"));

        // Verify updated prices were recovered
        let result = db.execute("SELECT * FROM products WHERE id = 1").unwrap();
        let row = &result.rows()[0];
        assert_eq!(row[2].to_string(), "899.99");

        let result = db.execute("SELECT * FROM products WHERE id = 2").unwrap();
        let row = &result.rows()[0];
        assert_eq!(row[2].to_string(), "19.99");
    }
}

#[test]
fn test_delete_persistence_and_recovery() {
    let temp_dir = TempDir::new().unwrap();

    // Session 1: Insert and delete data
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        db.execute("CREATE TABLE items (id INTEGER, name TEXT)")
            .unwrap();
        db.execute("INSERT INTO items VALUES (1, 'Item1')").unwrap();
        db.execute("INSERT INTO items VALUES (2, 'Item2')").unwrap();
        db.execute("INSERT INTO items VALUES (3, 'Item3')").unwrap();
        db.execute("INSERT INTO items VALUES (4, 'Item4')").unwrap();

        // Delete some items
        db.execute("DELETE FROM items WHERE id = 2").unwrap();
        db.execute("DELETE FROM items WHERE id = 4").unwrap();

        // Verify deletions
        let result = db.execute("SELECT * FROM items").unwrap();
        assert_eq!(result.row_count(), 2);

        // Simulate crash
    }

    // Session 2: Recover and verify deletions
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        assert!(db.table_exists("items"));

        let result = db.execute("SELECT * FROM items").unwrap();
        assert_eq!(result.row_count(), 2);

        // Verify only items 1 and 3 remain
        let rows = result.rows();
        assert!(rows.iter().any(|r| r[0].to_string() == "1"));
        assert!(rows.iter().any(|r| r[0].to_string() == "3"));
        assert!(!rows.iter().any(|r| r[0].to_string() == "2"));
        assert!(!rows.iter().any(|r| r[0].to_string() == "4"));
    }
}

#[test]
fn test_mixed_dml_operations_recovery() {
    let temp_dir = TempDir::new().unwrap();

    // Session 1: Mixed INSERT, UPDATE, DELETE operations
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        db.execute("CREATE TABLE accounts (id INTEGER, name TEXT, balance FLOAT)")
            .unwrap();

        // Insert initial data
        db.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.0)").unwrap();
        db.execute("INSERT INTO accounts VALUES (2, 'Bob', 500.0)").unwrap();
        db.execute("INSERT INTO accounts VALUES (3, 'Charlie', 750.0)").unwrap();

        // Update balances
        db.execute("UPDATE accounts SET balance = 1200.0 WHERE id = 1").unwrap();
        db.execute("UPDATE accounts SET balance = 600.0 WHERE id = 2").unwrap();

        // Delete one account
        db.execute("DELETE FROM accounts WHERE id = 3").unwrap();

        // Insert new account
        db.execute("INSERT INTO accounts VALUES (4, 'David', 300.0)").unwrap();

        // Final state: 3 accounts (Alice=1200, Bob=600, David=300)
        let result = db.execute("SELECT * FROM accounts").unwrap();
        assert_eq!(result.row_count(), 3);

        // Simulate crash
    }

    // Session 2: Recover and verify all operations
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        assert!(db.table_exists("accounts"));

        let result = db.execute("SELECT * FROM accounts").unwrap();
        assert_eq!(result.row_count(), 3);

        let rows = result.rows();

        // Verify Alice's updated balance
        let alice = rows.iter().find(|r| r[0].to_string() == "1").unwrap();
        assert_eq!(alice[2].to_string(), "1200");

        // Verify Bob's updated balance
        let bob = rows.iter().find(|r| r[0].to_string() == "2").unwrap();
        assert_eq!(bob[2].to_string(), "600");

        // Verify Charlie was deleted
        assert!(!rows.iter().any(|r| r[0].to_string() == "3"));

        // Verify David was inserted
        let david = rows.iter().find(|r| r[0].to_string() == "4").unwrap();
        assert_eq!(david[1].to_string(), "David");
        assert_eq!(david[2].to_string(), "300");
    }
}

#[test]
fn test_dml_with_checkpoint() {
    let temp_dir = TempDir::new().unwrap();

    // Session 1: Operations with checkpoint
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        db.execute("CREATE TABLE logs (id INTEGER, message TEXT)")
            .unwrap();
        db.execute("INSERT INTO logs VALUES (1, 'First')").unwrap();
        db.execute("INSERT INTO logs VALUES (2, 'Second')").unwrap();

        // Create checkpoint
        db.checkpoint().unwrap();

        // More operations after checkpoint
        db.execute("INSERT INTO logs VALUES (3, 'Third')").unwrap();
        db.execute("UPDATE logs SET message = 'FIRST' WHERE id = 1").unwrap();
        db.execute("DELETE FROM logs WHERE id = 2").unwrap();

        // Simulate crash
    }

    // Session 2: Recover from snapshot + WAL
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(temp_dir.path(), DurabilityMode::Sync)
            .unwrap();

        let result = db.execute("SELECT * FROM logs").unwrap();
        assert_eq!(result.row_count(), 2);

        let rows = result.rows();

        // Verify row 1 was updated
        let row1 = rows.iter().find(|r| r[0].to_string() == "1").unwrap();
        assert_eq!(row1[1].to_string(), "FIRST");

        // Verify row 2 was deleted
        assert!(!rows.iter().any(|r| r[0].to_string() == "2"));

        // Verify row 3 was inserted
        assert!(rows.iter().any(|r| r[0].to_string() == "3" && r[1].to_string() == "Third"));
    }
}
