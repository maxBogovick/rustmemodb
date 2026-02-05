use rustmemodb::core::Result;
use rustmemodb::InMemoryDB;


#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 RustMemDB - Phase 2 Complete!");
    println!("   ✨ Parser → Planner → Executor Pipeline");
    println!("{}", "=".repeat(70));

    let mut db = InMemoryDB::new();

    // Create table
    println!("\n📝 Creating 'users' table...");
    db.execute(
        "CREATE TABLE users (
            id INTEGER,
            name TEXT,
            age INTEGER,
            active BOOLEAN
        )",
    ).await?;
    println!("✅ Table created");

    // Insert data
    println!("\n📥 Inserting data...");
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, true)").await?;
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, true)").await?;
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35, false)").await?;
    println!("✅ 3 rows inserted");

    // Query 1: SELECT * 
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM users");
    println!("{}", "=".repeat(70));
    let result = db.execute("SELECT * FROM users").await?;
    result.print();

    // Query 2: WHERE clause
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM users WHERE age > 26");
    println!("{}", "=".repeat(70));
    let result = db.execute("SELECT * FROM users WHERE age > 26").await?;
    result.print();

    // Query 3: Complex WHERE
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM users WHERE active = true AND age < 32");
    println!("{}", "=".repeat(70));
    let result = db.execute("SELECT * FROM users WHERE active = true AND age < 32").await?;
    result.print();

    // Query 4: LIKE pattern
    println!("\n{}", "=".repeat(70));
    println!("📊 Query: SELECT * FROM users WHERE name LIKE 'A%'");
    println!("{}", "=".repeat(70));
    let result = db.execute("SELECT * FROM users WHERE name LIKE 'A%'").await?;
    result.print();

    // Statistics
    println!("\n{}", "=".repeat(70));
    println!("📈 Database Statistics");
    println!("{}", "=".repeat(70));
    println!("Tables: {:?}", db.list_tables());
    if let Ok(stats) = db.table_stats("users").await {
        println!("{}", stats);
    }

    Ok(())
}
