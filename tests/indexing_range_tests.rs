use rustmemodb::Client;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_range_indexing() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client.execute("CREATE TABLE numbers (n INT)").await?;
    client.execute("CREATE INDEX idx_n ON numbers(n)").await?;

    // Insert 0..99
    // Since we don't have bulk insert or loops in SQL yet, we do it in loop from Rust
    for i in 0..100 {
        client
            .execute(&format!("INSERT INTO numbers VALUES ({})", i))
            .await?;
    }

    // Test > 50
    let result = client.query("SELECT * FROM numbers WHERE n > 50").await?;
    assert_eq!(result.row_count(), 49); // 51..99

    // Test >= 50
    let result = client.query("SELECT * FROM numbers WHERE n >= 50").await?;
    assert_eq!(result.row_count(), 50); // 50..99

    // Test < 10
    let result = client.query("SELECT * FROM numbers WHERE n < 10").await?;
    assert_eq!(result.row_count(), 10); // 0..9

    // Test <= 10
    let result = client.query("SELECT * FROM numbers WHERE n <= 10").await?;
    assert_eq!(result.row_count(), 11); // 0..10

    // Test BETWEEN 10 AND 20 (inclusive)
    let result = client
        .query("SELECT * FROM numbers WHERE n BETWEEN 10 AND 20")
        .await?;
    assert_eq!(result.row_count(), 11); // 10..20

    // Verify Index Usage via EXPLAIN
    let explain = client
        .query("EXPLAIN SELECT * FROM numbers WHERE n > 50")
        .await?;
    let plan: String = explain
        .rows()
        .iter()
        .map(|r| r[0].as_str().unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    println!("Plan: {}", plan);

    // We expect "index_scan: Some" or similar in Debug output of TableScanNode
    assert!(plan.contains("index_scan: Some"));
    assert!(plan.contains("Gt"));

    Ok(())
}
