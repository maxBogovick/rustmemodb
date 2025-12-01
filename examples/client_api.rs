/// Example: Using the Client API (PostgreSQL/MySQL-like)
///
/// This example demonstrates the high-level Client API which provides
/// connection pooling, authentication, and transaction support.
///
/// Run: cargo run --example client_api

use rustmemodb::{Client, Result};

fn main() -> Result<()> {
    println!("=== RustMemDB Client API Example ===\n");

    // ============================================================================
    // 1. Simple Connection
    // ============================================================================
    println!("1. Connecting to database...");
    let client = Client::connect("admin", "admin")?;
    println!("   ✓ Connected successfully\n");

    // ============================================================================
    // 2. Create Schema
    // ============================================================================
    println!("2. Creating schema...");
    client.execute(
        "CREATE TABLE users (
            id INTEGER,
            username TEXT,
            email TEXT,
            age INTEGER,
            active BOOLEAN
        )"
    )?;
    println!("   ✓ Table 'users' created\n");

    // ============================================================================
    // 3. Insert Data
    // ============================================================================
    println!("3. Inserting data...");
    client.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com', 30, true)")?;
    client.execute("INSERT INTO users VALUES (2, 'bob', 'bob@example.com', 25, true)")?;
    client.execute("INSERT INTO users VALUES (3, 'charlie', 'charlie@example.com', 35, false)")?;
    client.execute("INSERT INTO users VALUES (4, 'diana', 'diana@example.com', 28, true)")?;
    println!("   ✓ 4 users inserted\n");

    // ============================================================================
    // 4. Query Data
    // ============================================================================
    println!("4. Querying data...");
    let result = client.query("SELECT * FROM users WHERE active = true")?;
    println!("   Active users: {}", result.row_count());
    result.print();
    println!();

    // ============================================================================
    // 5. Complex Queries
    // ============================================================================
    println!("5. Complex query with ORDER BY and LIMIT...");
    let result = client.query(
        "SELECT username, email, age
         FROM users
         WHERE age > 26
         ORDER BY age DESC
         LIMIT 2"
    )?;
    println!("   Top 2 users over 26:");
    result.print();
    println!();

    // ============================================================================
    // 6. Pool Statistics
    // ============================================================================
    println!("6. Connection pool statistics...");
    let stats = client.stats();
    println!("   {}", stats);
    println!();

    Ok(())
}
