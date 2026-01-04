//! Demonstration of RustMemDB persistence with WAL (Write-Ahead Logging)
//!
//! This example shows:
//! - Enabling persistence with different durability modes
//! - Creating tables and inserting data
//! - Manual checkpoints
//! - Crash recovery
//!
//! Run with: cargo run --example persistence_demo

use rustmemodb::{InMemoryDB, DurabilityMode};
use std::fs;
use rustmemodb::DurabilityMode::Async;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = InMemoryDB::new();
    let data_dir = "./demo_data";
    db.enable_persistence(data_dir, Async)?;
    assert!(db.table_exists("users"));
    assert!(db.table_exists("products"));
    println!("✓ Tables recovered: {:?}", db.list_tables());
    Ok(())
}

fn main2() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RustMemDB Persistence Demo ===\n");

    let data_dir = "./demo_data";

    // Clean up from previous runs
    let _ = fs::remove_dir_all(data_dir);

    // ========================================================================
    // PART 1: Enable persistence and create some data
    // ========================================================================
    println!("Part 1: Creating database with persistence...");
    {
        let mut db = InMemoryDB::new();

        // Enable persistence with ASYNC mode (fast, background sync)
        db.enable_persistence(data_dir, DurabilityMode::Async)?;

        println!("✓ Persistence enabled (mode: {:?})", db.durability_mode());

        // Create tables
        db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")?;
        db.execute("CREATE TABLE products (id INTEGER, name TEXT, price FLOAT)")?;

        println!("✓ Created 2 tables");

        // Insert some data
        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")?;
        db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")?;
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)")?;

        db.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99)")?;
        db.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99)")?;

        println!("✓ Inserted data into tables");

        // Query data
        let result = db.execute("SELECT * FROM users WHERE age > 25")?;
        println!("\nUsers with age > 25:");
        for row in result.rows() {
            println!("  {:?}", row);
        }

        // Create a checkpoint (snapshot)
        db.checkpoint()?;
        println!("\n✓ Checkpoint created");

        // Database will be dropped here, simulating a "crash"
    }

    println!("\n[Simulating database crash/restart...]\n");

    // ========================================================================
    // PART 2: Recover database from persistence
    // ========================================================================
    println!("Part 2: Recovering database...");
    {
        let mut db = InMemoryDB::new();

        // Enable persistence - this will automatically recover data
        db.enable_persistence(data_dir, DurabilityMode::Async)?;

        println!("✓ Database recovered from persistence");

        // Verify tables exist
        assert!(db.table_exists("users"));
        assert!(db.table_exists("products"));
        println!("✓ Tables recovered: {:?}", db.list_tables());

        // Verify data was recovered
        let result = db.execute("SELECT * FROM users")?;
        println!("\nRecovered users ({} rows):", result.row_count());
        for row in result.rows() {
            println!("  {:?}", row);
        }

        let result = db.execute("SELECT * FROM products")?;
        println!("\nRecovered products ({} rows):", result.row_count());
        for row in result.rows() {
            println!("  {:?}", row);
        }

        // Add more data after recovery
        db.execute("INSERT INTO users VALUES (4, 'David', 28)")?;
        println!("\n✓ Added new user after recovery");

        // Final checkpoint
        db.checkpoint()?;
        println!("✓ Final checkpoint created");
    }

    // ========================================================================
    // PART 3: Demonstrate different durability modes
    // ========================================================================
    println!("\n\nPart 3: Durability Modes\n");

    // SYNC mode - fsync after each operation (slow but durable)
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence("./demo_sync", DurabilityMode::Sync)?;
        println!("✓ SYNC mode: Every operation is immediately synced to disk");
        let _ = fs::remove_dir_all("./demo_sync");
    }

    // ASYNC mode - background fsync (fast, some risk on crash)
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence("./demo_async", DurabilityMode::Async)?;
        println!("✓ ASYNC mode: Operations buffered, synced in background");
        let _ = fs::remove_dir_all("./demo_async");
    }

    // NONE mode - no persistence (in-memory only)
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence("./demo_none", DurabilityMode::None)?;
        println!("✓ NONE mode: In-memory only, no files written");
        // No files created, nothing to clean up
    }

    // ========================================================================
    // PART 4: Manual checkpoint management
    // ========================================================================
    println!("\n\nPart 4: Manual Checkpoint Management\n");
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence("./demo_checkpoint", DurabilityMode::Async)?;

        // Create lots of tables (normally would trigger auto-checkpoint at 1000 ops)
        for i in 0..10 {
            db.execute(&format!("CREATE TABLE t{} (id INTEGER)", i))?;
        }

        println!("✓ Created 10 tables");

        // Manual checkpoint to consolidate WAL
        db.checkpoint()?;
        println!("✓ Manual checkpoint created");
        println!("  - Snapshot written to disk");
        println!("  - WAL cleared and ready for new entries");

        let _ = fs::remove_dir_all("./demo_checkpoint");
    }

    // ========================================================================
    // PART 5: DML Operations with Crash Recovery
    // ========================================================================
    println!("\n\nPart 5: DML Operations with Crash Recovery\n");

    let dml_data_dir = "./demo_dml";
    let _ = fs::remove_dir_all(dml_data_dir);

    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(dml_data_dir, DurabilityMode::Sync)?;

        // Create table and insert data
        db.execute("CREATE TABLE transactions (id INTEGER, amount FLOAT, status TEXT)")?;
        db.execute("INSERT INTO transactions VALUES (1, 100.0, 'pending')")?;
        db.execute("INSERT INTO transactions VALUES (2, 250.0, 'pending')")?;
        db.execute("INSERT INTO transactions VALUES (3, 75.0, 'pending')")?;

        println!("✓ Created table and inserted 3 transactions");

        // Update some transactions
        db.execute("UPDATE transactions SET status = 'completed' WHERE id = 1")?;
        db.execute("UPDATE transactions SET amount = 300.0 WHERE id = 2")?;

        println!("✓ Updated transaction statuses and amounts");

        // Delete a transaction
        db.execute("DELETE FROM transactions WHERE id = 3")?;

        println!("✓ Deleted transaction 3");

        // Add new transaction
        db.execute("INSERT INTO transactions VALUES (4, 500.0, 'completed')")?;

        println!("✓ Added new transaction 4");

        // Show current state
        let result = db.execute("SELECT * FROM transactions")?;
        println!("\nCurrent state ({} rows):", result.row_count());
        for row in result.rows() {
            println!("  {:?}", row);
        }

        // Simulate crash
    }

    println!("\n[Simulating crash after DML operations...]");

    // Recover and verify
    {
        let mut db = InMemoryDB::new();
        db.enable_persistence(dml_data_dir, DurabilityMode::Sync)?;

        println!("\n✓ Database recovered from WAL");

        // Verify all DML operations were recovered
        let result = db.execute("SELECT * FROM transactions")?;
        println!("\nRecovered state ({} rows):", result.row_count());
        for row in result.rows() {
            println!("  {:?}", row);
        }

        assert_eq!(result.row_count(), 3);

        // Verify specific changes
        let tx1 = db.execute("SELECT * FROM transactions WHERE id = 1")?;
        assert_eq!(tx1.rows()[0][2].to_string(), "completed");

        let tx2 = db.execute("SELECT * FROM transactions WHERE id = 2")?;
        assert_eq!(tx2.rows()[0][1].to_string(), "300");

        let tx3 = db.execute("SELECT * FROM transactions WHERE id = 3")?;
        assert_eq!(tx3.row_count(), 0); // Should be deleted

        let tx4 = db.execute("SELECT * FROM transactions WHERE id = 4")?;
        assert_eq!(tx4.rows()[0][1].to_string(), "500");

        println!("\n✓ All DML operations successfully recovered!");
    }

    //let _ = fs::remove_dir_all(dml_data_dir);

    // Clean up
    //let _ = fs::remove_dir_all(data_dir);

    println!("\n=== Demo completed successfully! ===");
    Ok(())
}
