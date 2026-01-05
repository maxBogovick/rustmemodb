/// Mixed type operations tests
///
/// Tests for mixed Integer/Float comparisons and arithmetic operations
/// Run with: cargo test --test mixed_type_operations_tests

use rustmemodb::Client;

#[tokio::test]
async fn test_boolean_literal_insert() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_bool2 (id INTEGER, active BOOLEAN)").await.unwrap();
    client.execute("INSERT INTO test_bool2 VALUES (1, true)").await.unwrap();
    client.execute("INSERT INTO test_bool2 VALUES (2, false)").await.unwrap();

    let result = client.query("SELECT * FROM test_bool2").await.unwrap();
    println!("Total rows: {}", result.row_count());
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_boolean_comparison_true() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_bool1 (id INTEGER, active BOOLEAN)").await.unwrap();
    client.execute("INSERT INTO test_bool1 VALUES (1, true)").await.unwrap();
    client.execute("INSERT INTO test_bool1 VALUES (2, false)").await.unwrap();

    println!("Testing: WHERE active = true");
    let result = client.query("SELECT * FROM test_bool1 WHERE active = true").await.unwrap();
    println!("Rows returned: {}", result.row_count());
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_boolean_comparison_false() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_bool (id INTEGER, active BOOLEAN)").await.unwrap();
    client.execute("INSERT INTO test_bool VALUES (1, true)").await.unwrap();
    client.execute("INSERT INTO test_bool VALUES (2, false)").await.unwrap();

    println!("Testing: WHERE active = false");
    let result = client.query("SELECT * FROM test_bool WHERE active = false").await.unwrap();
    println!("Rows returned: {}", result.row_count());
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_float_comparison() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_float (id INTEGER, score FLOAT)").await.unwrap();
    client.execute("INSERT INTO test_float VALUES (1, 95.5)").await.unwrap();
    client.execute("INSERT INTO test_float VALUES (2, 87.3)").await.unwrap();
    client.execute("INSERT INTO test_float VALUES (3, 92.1)").await.unwrap();

    println!("Testing: WHERE score > 90.0");
    let result = client.query("SELECT * FROM test_float WHERE score > 90.0").await.unwrap();
    println!("Rows returned: {}", result.row_count());
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_combined_boolean_and_float() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE mixed_data (
            id INTEGER,
            name TEXT,
            score FLOAT,
            active BOOLEAN
        )"
    ).await.unwrap();

    client.execute("INSERT INTO mixed_data VALUES (1, 'Alice', 95.5, true)").await.unwrap();
    client.execute("INSERT INTO mixed_data VALUES (2, 'Bob', 87.3, false)").await.unwrap();
    client.execute("INSERT INTO mixed_data VALUES (3, 'Charlie', 92.1, true)").await.unwrap();

    // Test each condition separately
    println!("\n=== Testing: WHERE active = true ===");
    let result = client.query("SELECT name, score FROM mixed_data WHERE active = true").await.unwrap();
    println!("Rows: {}", result.row_count());
    println!("Expected: 2 (Alice, Charlie)");

    println!("\n=== Testing: WHERE score > 90 ===");
    let result = client.query("SELECT name, score FROM mixed_data WHERE score > 90").await.unwrap();
    println!("Rows: {}", result.row_count());
    println!("Expected: 2 (Alice, Charlie)");

    println!("\n=== Testing: WHERE active = true AND score > 90 ===");
    let result = client.query(
        "SELECT name, score FROM mixed_data
         WHERE active = true
           AND score > 90"
    ).await.unwrap();
    println!("Rows: {}", result.row_count());
    println!("Expected: 2 (Alice, Charlie)");

    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_mixed_type_arithmetic() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE test_arithmetic (id INTEGER, value FLOAT)").await.unwrap();
    client.execute("INSERT INTO test_arithmetic VALUES (1, 10.5)").await.unwrap();
    client.execute("INSERT INTO test_arithmetic VALUES (2, 20.3)").await.unwrap();

    // Test Float + Integer
    println!("\n=== Testing: WHERE value + 5 > 15 ===");
    let result = client.query("SELECT * FROM test_arithmetic WHERE value + 5 > 15").await.unwrap();
    println!("Rows: {}", result.row_count());
    println!("Expected: 2 (10.5+5=15.5>15, 20.3+5=25.3>15)");
    assert_eq!(result.row_count(), 2);

    // Test Integer * Float
    println!("\n=== Testing: WHERE value * 2 > 30 ===");
    let result = client.query("SELECT * FROM test_arithmetic WHERE value * 2 > 30").await.unwrap();
    println!("Rows: {}", result.row_count());
    println!("Expected: 1 (20.3 * 2 = 40.6 > 30)");
    assert_eq!(result.row_count(), 1);
}