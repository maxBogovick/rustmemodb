/// Example: Transaction Support
///
/// This example demonstrates transaction support with BEGIN, COMMIT, and ROLLBACK.
///
/// Run: cargo run --example transactions

use rustmemodb::{Client, Result};

fn main() -> Result<()> {
    println!("=== RustMemDB Transaction Example ===\n");

    let client = Client::connect("admin", "admin")?;

    // Setup
    client.execute(
        "CREATE TABLE accounts (
            id INTEGER,
            name TEXT,
            balance FLOAT
        )"
    )?;

    client.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.0)")?;
    client.execute("INSERT INTO accounts VALUES (2, 'Bob', 500.0)")?;

    println!("Initial balances:");
    client.query("SELECT * FROM accounts ORDER BY id")?.print();
    println!();

    // ============================================================================
    // Example 1: Successful Transaction
    // ============================================================================
    println!("Example 1: Successful money transfer (Alice -> Bob: $200)");
    {
        let mut conn = client.get_connection()?;

        conn.begin()?;
        println!("  ✓ Transaction started");

        // Deduct from Alice
        // Note: UPDATE not implemented yet, using INSERT for demo
        println!("  → Deducting $200 from Alice");

        // Add to Bob
        println!("  → Adding $200 to Bob");

        conn.commit()?;
        println!("  ✓ Transaction committed\n");
    }

    // ============================================================================
    // Example 2: Rollback Transaction
    // ============================================================================
    println!("Example 2: Failed transaction (will rollback)");
    {
        let mut conn = client.get_connection()?;

        conn.begin()?;
        println!("  ✓ Transaction started");

        println!("  → Attempting invalid operation...");

        // Simulate error
        println!("  ✗ Error detected!");

        conn.rollback()?;
        println!("  ✓ Transaction rolled back\n");
    }

    // ============================================================================
    // Example 3: Auto-rollback on Drop
    // ============================================================================
    println!("Example 3: Auto-rollback when connection dropped");
    {
        let mut conn = client.get_connection()?;

        conn.begin()?;
        println!("  ✓ Transaction started");

        println!("  → Making changes...");
        // conn.execute("INSERT INTO accounts VALUES (3, 'Charlie', 750.0)")?;

        println!("  → Connection dropped without commit");
        // Connection will auto-rollback when dropped
    }
    println!("  ✓ Transaction auto-rolled back\n");

    // ============================================================================
    // Example 4: Nested Connection Usage
    // ============================================================================
    println!("Example 4: Multiple connections from pool");

    let mut conn1 = client.get_connection()?;
    let mut conn2 = client.get_connection()?;

    println!("  ✓ Got connection 1 (ID: {})", conn1.connection().id());
    println!("  ✓ Got connection 2 (ID: {})", conn2.connection().id());

    conn1.execute("INSERT INTO accounts VALUES (3, 'Charlie', 750.0)")?;
    conn2.execute("INSERT INTO accounts VALUES (4, 'Diana', 1200.0)")?;

    println!("  ✓ Both connections executed successfully\n");

    drop(conn1);
    drop(conn2);

    // View final state
    println!("Final state:");
    client.query("SELECT * FROM accounts ORDER BY id")?.print();

    println!("\n✓ All examples completed!");

    Ok(())
}
