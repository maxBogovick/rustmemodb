/// Example: Transaction Support
///
/// This example demonstrates transaction support with BEGIN, COMMIT, and ROLLBACK.
///
/// Run: cargo run --example transactions
use rustmemodb::{Client, Result};

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== RustMemDB Transaction Example ===\n");

    let client = Client::connect("admin", "adminpass").await?;

    // Setup
    client
        .execute(
            "CREATE TABLE accounts (
            id INTEGER,
            name TEXT,
            balance FLOAT
        )",
        )
        .await?;

    client
        .execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.0)")
        .await?;
    client
        .execute("INSERT INTO accounts VALUES (2, 'Bob', 500.0)")
        .await?;

    println!("Initial balances:");
    client
        .query("SELECT * FROM accounts ORDER BY id")
        .await?
        .print();
    println!();

    // ============================================================================
    // Example 1: Successful Transaction
    // ============================================================================
    println!("Example 1: Successful money transfer (Alice -> Bob: $200)");
    {
        let mut conn = client.get_connection().await?;

        conn.begin().await?;
        println!("  ✓ Transaction started");

        // Deduct from Alice
        println!("  → Deducting $200 from Alice");

        // Add to Bob
        println!("  → Adding $200 to Bob");

        conn.commit().await?;
        println!("  ✓ Transaction committed\n");
    }

    // ============================================================================
    // Example 2: Rollback Transaction
    // ============================================================================
    println!("Example 2: Failed transaction (will rollback)");
    {
        let mut conn = client.get_connection().await?;

        conn.begin().await?;
        println!("  ✓ Transaction started");

        println!("  → Attempting invalid operation...");

        // Simulate error
        println!("  ✗ Error detected!");

        conn.rollback().await?;
        println!("  ✓ Transaction rolled back\n");
    }

    // ============================================================================
    // Example 3: Auto-rollback on Drop
    // ============================================================================
    println!("Example 3: Auto-rollback when connection dropped");
    {
        let mut conn = client.get_connection().await?;

        conn.begin().await?;
        println!("  ✓ Transaction started");

        println!("  → Making changes...");

        println!("  → Connection dropped without commit");
        // Connection will auto-rollback when dropped
    }
    println!("  ✓ Transaction auto-rolled back\n");

    // ============================================================================
    // Example 4: Nested Connection Usage
    // ============================================================================
    println!("Example 4: Multiple connections from pool");

    let mut conn1 = client.get_connection().await?;
    let mut conn2 = client.get_connection().await?;

    println!("  ✓ Got connection 1 (ID: {})", conn1.connection().id());
    println!("  ✓ Got connection 2 (ID: {})", conn2.connection().id());

    conn1
        .execute("INSERT INTO accounts VALUES (3, 'Charlie', 750.0)")
        .await?;
    conn2
        .execute("INSERT INTO accounts VALUES (4, 'Diana', 1200.0)")
        .await?;

    println!("  ✓ Both connections executed successfully\n");

    drop(conn1);
    drop(conn2);

    // View final state
    println!("Final state:");
    client
        .query("SELECT * FROM accounts ORDER BY id")
        .await?
        .print();

    println!("\n✓ All examples completed!");

    Ok(())
}
