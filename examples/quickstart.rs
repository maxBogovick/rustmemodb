/// Quick Start Example
///
/// Simple example showing how to use RustMemDB as a library
///
/// Run with: cargo run --example quickstart

use rustmemodb::{Client, InMemoryDB};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 RustMemDB Quick Start\n");
    println!("{}", "=".repeat(60));

    // ============================================
    // Method 1: Direct Database API
    // ============================================
    println!("\n📦 Method 1: Direct InMemoryDB API");
    println!("{}", "=".repeat(60));

    let mut db = InMemoryDB::new();

    // Create table
    db.execute(
        "CREATE TABLE users (
            id INTEGER,
            name TEXT,
            age INTEGER,
            active BOOLEAN
        )"
    )?;

    // Insert data
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, true)")?;
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, true)")?;
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35, false)")?;

    // Query data
    println!("\n📊 All users:");
    let result = db.execute("SELECT * FROM users")?;
    result.print();

    println!("\n📊 Active users over 26:");
    let result = db.execute(
        "SELECT name, age FROM users WHERE active = true AND age > 26"
    )?;
    result.print();

    // ============================================
    // Method 2: Client API (Recommended)
    // ============================================
    println!("\n{}", "=".repeat(60));
    println!("📦 Method 2: Client API with Connection Pooling");
    println!("{}", "=".repeat(60));

    // Connect to database
    let client = Client::connect("admin", "adminpass")?;

    // Create products table
    client.execute(
        "CREATE TABLE products (
            id INTEGER,
            name TEXT,
            price FLOAT,
            stock INTEGER
        )"
    )?;

    // Insert products
    client.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99, 10)")?;
    client.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99, 50)")?;
    client.execute("INSERT INTO products VALUES (3, 'Keyboard', 79.99, 30)")?;

    // Simple query
    println!("\n📊 All products:");
    let result = client.query("SELECT * FROM products")?;
    result.print();

    // Query with WHERE clause
    println!("\n📊 Products over $50:");
    let result = client.query(
        "SELECT name, price FROM products WHERE price > 50 ORDER BY price DESC"
    )?;
    result.print();

    // Update operation
    println!("\n🔄 Applying 10% discount...");
    let result = client.execute("UPDATE products SET price = price * 0.9")?;
    println!("✅ Updated {} products", result.affected_rows().unwrap_or(0));

    println!("\n📊 Products after discount:");
    let result = client.query("SELECT name, price FROM products ORDER BY price DESC")?;
    result.print();

    // ============================================
    // Method 3: Using Transactions
    // ============================================
    println!("\n{}", "=".repeat(60));
    println!("📦 Method 3: Transactions");
    println!("{}", "=".repeat(60));

    // Create orders table
    client.execute(
        "CREATE TABLE orders (
            id INTEGER,
            product_id INTEGER,
            quantity INTEGER
        )"
    )?;

    // Get a connection for transaction
    let mut conn = client.get_connection()?;

    println!("\n🔄 Starting transaction...");
    conn.begin()?;

    // Place an order (decrease stock and create order record)
    conn.execute("INSERT INTO orders VALUES (1, 1, 2)")?;
    conn.execute("UPDATE products SET stock = stock - 2 WHERE id = 1")?;

    println!("📊 Changes within transaction:");
    let result = conn.execute("SELECT * FROM products WHERE id = 1")?;
    result.print();

    println!("\n✅ Committing transaction...");
    conn.commit()?;

    println!("\n📊 Final stock after order:");
    let result = client.query("SELECT name, stock FROM products WHERE id = 1")?;
    result.print();

    // ============================================
    // Summary
    // ============================================
    println!("\n{}", "=".repeat(60));
    println!("✨ Features Used:");
    println!("{}", "=".repeat(60));
    println!("✅ CREATE TABLE - Define table schema");
    println!("✅ INSERT - Add data");
    println!("✅ SELECT - Query data with WHERE and ORDER BY");
    println!("✅ UPDATE - Modify existing data");
    println!("✅ Transactions - ACID guarantees");
    println!("✅ Connection pooling - Efficient resource management");
    println!("\n💡 Check examples/transactions_example.rs for more transaction examples!");
    println!("💡 See README.md for complete API documentation");

    Ok(())
}
