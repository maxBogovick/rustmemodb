/// Transaction Example
///
/// Demonstrates how to use transactions in RustMemDB
///
/// Run with: cargo run --example transactions_example

use rustmemodb::Client;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🎯 RustMemDB Transaction Example\n");
    println!("{}", "=".repeat(60));

    // Connect to the database
    let client = Client::connect("admin", "adminpass")?;

    // Create accounts table
    println!("\n📝 Creating accounts table...");
    client.execute(
        "CREATE TABLE accounts (
            id INTEGER,
            name TEXT,
            balance FLOAT
        )"
    )?;

    // Insert initial data
    println!("💰 Adding initial accounts...");
    client.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000.0)")?;
    client.execute("INSERT INTO accounts VALUES (2, 'Bob', 500.0)")?;
    client.execute("INSERT INTO accounts VALUES (3, 'Charlie', 750.0)")?;

    println!("\n📊 Initial balances:");
    let result = client.query("SELECT * FROM accounts ORDER BY id")?;
    result.print();

    // ============================================
    // Example 1: Successful Transaction (Commit)
    // ============================================
    println!("\n{}", "=".repeat(60));
    println!("Example 1: Successful Transfer (COMMIT)");
    println!("{}", "=".repeat(60));

    {
        let mut conn = client.get_connection()?;

        println!("\n🔄 Starting transaction...");
        conn.begin()?;

        println!("💸 Transferring $200 from Alice to Bob...");
        conn.execute("UPDATE accounts SET balance = balance - 200.0 WHERE name = 'Alice'")?;
        conn.execute("UPDATE accounts SET balance = balance + 200.0 WHERE name = 'Bob'")?;

        println!("\n📊 Balances within transaction:");
        let result = conn.execute("SELECT * FROM accounts ORDER BY id")?;
        result.print();

        println!("\n✅ Committing transaction...");
        conn.commit()?;
    }

    println!("\n📊 Balances after COMMIT:");
    let result = client.query("SELECT * FROM accounts ORDER BY id")?;
    result.print();

    // ============================================
    // Example 2: Rolled Back Transaction
    // ============================================
    println!("\n{}", "=".repeat(60));
    println!("Example 2: Failed Transfer (ROLLBACK)");
    println!("{}", "=".repeat(60));

    {
        let mut conn = client.get_connection()?;

        println!("\n🔄 Starting transaction...");
        conn.begin()?;

        println!("💸 Attempting to transfer $1500 from Bob to Charlie...");
        conn.execute("UPDATE accounts SET balance = balance - 1500.0 WHERE name = 'Bob'")?;
        conn.execute("UPDATE accounts SET balance = balance + 1500.0 WHERE name = 'Charlie'")?;

        println!("\n📊 Balances within transaction (Bob would have negative balance!):");
        let result = conn.execute("SELECT * FROM accounts ORDER BY id")?;
        result.print();

        println!("\n❌ Oops! Bob would have negative balance. Rolling back...");
        conn.rollback()?;
    }

    println!("\n📊 Balances after ROLLBACK (unchanged):");
    let result = client.query("SELECT * FROM accounts ORDER BY id")?;
    result.print();

    // ============================================
    // Example 3: Auto-Rollback on Drop
    // ============================================
    println!("\n{}", "=".repeat(60));
    println!("Example 3: Auto-Rollback on Connection Drop");
    println!("{}", "=".repeat(60));

    {
        let mut conn = client.get_connection()?;

        println!("\n🔄 Starting transaction...");
        conn.begin()?;

        println!("💸 Transferring $100 from Charlie to Alice...");
        conn.execute("UPDATE accounts SET balance = balance - 100.0 WHERE name = 'Charlie'")?;
        conn.execute("UPDATE accounts SET balance = balance + 100.0 WHERE name = 'Alice'")?;

        println!("\n📊 Balances within transaction:");
        let result = conn.execute("SELECT * FROM accounts ORDER BY id")?;
        result.print();

        println!("\n⚠️  Dropping connection without commit...");
        // Connection drops here, triggering auto-rollback
    }

    println!("\n📊 Balances after auto-rollback (unchanged):");
    let result = client.query("SELECT * FROM accounts ORDER BY id")?;
    result.print();

    // ============================================
    // Example 4: Complex Transaction
    // ============================================
    println!("\n{}", "=".repeat(60));
    println!("Example 4: Complex Multi-Operation Transaction");
    println!("{}", "=".repeat(60));

    {
        let mut conn = client.get_connection()?;

        println!("\n🔄 Starting transaction...");
        conn.begin()?;

        println!("📝 Performing multiple operations:");
        println!("  1. Insert new account (David)");
        conn.execute("INSERT INTO accounts VALUES (4, 'David', 0.0)")?;

        println!("  2. Transfer $50 from each person to David");
        conn.execute("UPDATE accounts SET balance = balance - 50.0 WHERE name != 'David'")?;
        conn.execute("UPDATE accounts SET balance = balance + 150.0 WHERE name = 'David'")?;

        println!("  3. Delete accounts with balance < 100");
        conn.execute("DELETE FROM accounts WHERE balance < 100")?;

        println!("\n📊 Result within transaction:");
        let result = conn.execute("SELECT * FROM accounts ORDER BY id")?;
        result.print();

        println!("\n✅ Committing transaction...");
        conn.commit()?;
    }

    println!("\n📊 Final state after complex transaction:");
    let result = client.query("SELECT * FROM accounts ORDER BY balance DESC")?;
    result.print();

    // Summary
    println!("\n{}", "=".repeat(60));
    println!("✨ Transaction Features Demonstrated:");
    println!("{}", "=".repeat(60));
    println!("✅ BEGIN - Start a transaction");
    println!("✅ COMMIT - Save all changes");
    println!("✅ ROLLBACK - Undo all changes");
    println!("✅ Auto-rollback - Automatic rollback on connection drop");
    println!("✅ MVCC - Operations see their own uncommitted changes");
    println!("✅ Atomicity - All operations succeed or all fail");
    println!("\n🎉 All transaction examples completed successfully!");

    Ok(())
}
