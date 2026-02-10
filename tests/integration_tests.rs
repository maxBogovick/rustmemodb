/// Integration tests for RustMemDB
///
/// These tests verify that all components work together correctly.
/// Run with: cargo test --test integration_tests
use rustmemodb::{InMemoryDB, Value};

#[tokio::test]
async fn test_basic_table_creation() {
    let mut db = InMemoryDB::new();

    let result = db
        .execute(
            "CREATE TABLE users (
            id INTEGER,
            name TEXT,
            age INTEGER
        )",
        )
        .await;

    assert!(result.is_ok());
    assert!(db.table_exists("users"));
}

#[tokio::test]
async fn test_insert_and_select() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)")
        .await
        .unwrap();

    let result = db.execute("SELECT * FROM users").await.unwrap();

    assert_eq!(result.row_count(), 2);
    assert_eq!(result.columns().len(), 3);
}

#[tokio::test]
async fn test_where_clause_filtering() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE products (id INTEGER, name TEXT, price FLOAT)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (1, 'Laptop', 999.99)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (2, 'Mouse', 29.99)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (3, 'Keyboard', 79.99)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM products WHERE price > 50")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2); // Laptop and Keyboard
}

#[tokio::test]
async fn test_order_by_ascending() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Charlie', 35)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Alice', 30)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Bob', 25)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT name, age FROM users ORDER BY age ASC")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 3);

    let rows = result.rows();
    // Should be sorted: Bob (25), Alice (30), Charlie (35)
    assert_eq!(rows[0][1], Value::Integer(25));
    assert_eq!(rows[1][1], Value::Integer(30));
    assert_eq!(rows[2][1], Value::Integer(35));
}

#[tokio::test]
async fn test_order_by_descending() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE products (id INTEGER, price FLOAT)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (1, 99.99)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (2, 199.99)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (3, 49.99)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM products ORDER BY price DESC")
        .await
        .unwrap();

    let rows = result.rows();
    assert_eq!(rows[0][1], Value::Float(199.99));
    assert_eq!(rows[1][1], Value::Float(99.99));
    assert_eq!(rows[2][1], Value::Float(49.99));
}

#[tokio::test]
async fn test_limit_clause() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE items (id INTEGER)").await.unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO items VALUES ({})", i))
            .await
            .unwrap();
    }

    let result = db.execute("SELECT * FROM items LIMIT 3").await.unwrap();

    assert_eq!(result.row_count(), 3);
}

#[tokio::test]
async fn test_combined_where_order_limit() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE scores (player TEXT, score INTEGER)")
        .await
        .unwrap();
    db.execute("INSERT INTO scores VALUES ('Alice', 100)")
        .await
        .unwrap();
    db.execute("INSERT INTO scores VALUES ('Bob', 150)")
        .await
        .unwrap();
    db.execute("INSERT INTO scores VALUES ('Charlie', 120)")
        .await
        .unwrap();
    db.execute("INSERT INTO scores VALUES ('Diana', 180)")
        .await
        .unwrap();

    let result = db
        .execute(
            "SELECT player, score FROM scores
         WHERE score > 100
         ORDER BY score DESC
         LIMIT 2",
        )
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);

    let rows = result.rows();
    // Should be: Diana (180), Bob (150)
    assert_eq!(rows[0][1], Value::Integer(180));
    assert_eq!(rows[1][1], Value::Integer(150));
}

#[tokio::test]
async fn test_like_operator() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER, email TEXT)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'alice@example.com')")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 'bob@test.com')")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 'charlie@example.com')")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM users WHERE email LIKE '%@example.com'")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2); // alice and charlie
}

#[tokio::test]
async fn test_between_operator() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE products (id INTEGER, price FLOAT)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (1, 10.0)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (2, 50.0)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (3, 100.0)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (4, 150.0)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM products WHERE price BETWEEN 40 AND 110")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2); // 50.0 and 100.0
}

#[tokio::test]
async fn test_is_null_operator() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE contacts (id INTEGER, phone TEXT)")
        .await
        .unwrap();
    db.execute("INSERT INTO contacts VALUES (1, '555-1234')")
        .await
        .unwrap();
    db.execute("INSERT INTO contacts VALUES (2, NULL)")
        .await
        .unwrap();
    db.execute("INSERT INTO contacts VALUES (3, '555-5678')")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM contacts WHERE phone IS NULL")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_is_not_null_operator() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE contacts (id INTEGER, phone TEXT)")
        .await
        .unwrap();
    db.execute("INSERT INTO contacts VALUES (1, '555-1234')")
        .await
        .unwrap();
    db.execute("INSERT INTO contacts VALUES (2, NULL)")
        .await
        .unwrap();
    db.execute("INSERT INTO contacts VALUES (3, '555-5678')")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM contacts WHERE phone IS NOT NULL")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_logical_and_operator() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER, age INTEGER, active BOOLEAN)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 25, true)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (2, 30, true)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (3, 35, false)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (4, 40, true)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM users WHERE age > 26 AND active = true")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2); // id 2 and 4
}

#[tokio::test]
async fn test_logical_or_operator() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE products (id INTEGER, price FLOAT, featured BOOLEAN)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (1, 100.0, false)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (2, 50.0, true)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (3, 200.0, false)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (4, 30.0, false)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM products WHERE price > 150 OR featured = true")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 2); // id 2 and 3
}

#[tokio::test]
async fn test_arithmetic_expressions() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE prices (item TEXT, base_price FLOAT)")
        .await
        .unwrap();
    db.execute("INSERT INTO prices VALUES ('A', 100.0)")
        .await
        .unwrap();
    db.execute("INSERT INTO prices VALUES ('B', 200.0)")
        .await
        .unwrap();

    // Note: This will work when expression evaluation in SELECT is implemented
    // For now, we test that WHERE clause arithmetic works
    let result = db
        .execute("SELECT * FROM prices WHERE base_price * 1.1 > 200")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 1); // Only B
}

#[tokio::test]
async fn test_multiple_tables() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .await
        .unwrap();
    db.execute("CREATE TABLE products (id INTEGER, name TEXT)")
        .await
        .unwrap();

    db.execute("INSERT INTO users VALUES (1, 'Alice')")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (1, 'Laptop')")
        .await
        .unwrap();

    let users = db.execute("SELECT * FROM users").await.unwrap();
    let products = db.execute("SELECT * FROM products").await.unwrap();

    assert_eq!(users.row_count(), 1);
    assert_eq!(products.row_count(), 1);

    let tables = db.list_tables();
    assert_eq!(tables.len(), 2);
    assert!(tables.contains(&"users".to_string()));
    assert!(tables.contains(&"products".to_string()));
}

#[tokio::test]
async fn test_table_stats() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE test (id INTEGER, data TEXT)")
        .await
        .unwrap();

    for i in 1..=100 {
        db.execute(&format!("INSERT INTO test VALUES ({}, 'data_{}')", i, i))
            .await
            .unwrap();
    }

    let stats = db.table_stats("test").await.unwrap();
    assert_eq!(stats.row_count, 100);
    assert_eq!(stats.column_count, 2);
}

#[tokio::test]
async fn test_error_table_not_found() {
    let mut db = InMemoryDB::new();

    let result = db.execute("SELECT * FROM nonexistent_table").await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_error_duplicate_table() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER)").await.unwrap();

    let result = db.execute("CREATE TABLE users (id INTEGER)").await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_error_invalid_sql() {
    let mut db = InMemoryDB::new();

    let result = db.execute("INVALID SQL STATEMENT").await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_boolean_data_type() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE flags (id INTEGER, active BOOLEAN)")
        .await
        .unwrap();
    db.execute("INSERT INTO flags VALUES (1, true)")
        .await
        .unwrap();
    db.execute("INSERT INTO flags VALUES (2, false)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM flags WHERE active = true")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_float_data_type() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE measurements (id INTEGER, value FLOAT)")
        .await
        .unwrap();
    db.execute("INSERT INTO measurements VALUES (1, 3.14159)")
        .await
        .unwrap();
    db.execute("INSERT INTO measurements VALUES (2, 2.71828)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM measurements WHERE value > 3.0")
        .await
        .unwrap();

    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_empty_table_query() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE empty (id INTEGER)").await.unwrap();

    let result = db.execute("SELECT * FROM empty").await.unwrap();

    assert_eq!(result.row_count(), 0);
}

#[tokio::test]
async fn test_projection_specific_columns() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT, age INTEGER)")
        .await
        .unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30)")
        .await
        .unwrap();

    let result = db.execute("SELECT name, age FROM users").await.unwrap();

    assert_eq!(result.columns().len(), 2);
    assert_eq!(result.columns()[0].name, "name");
    assert_eq!(result.columns()[1].name, "age");
}

#[tokio::test]
async fn test_complex_nested_conditions() {
    let mut db = InMemoryDB::new();

    db.execute(
        "CREATE TABLE products (
        id INTEGER,
        name TEXT,
        price FLOAT,
        stock INTEGER,
        featured BOOLEAN
    )",
    )
    .await
    .unwrap();

    db.execute("INSERT INTO products VALUES (1, 'A', 100.0, 10, true)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (2, 'B', 50.0, 5, false)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (3, 'C', 150.0, 0, true)")
        .await
        .unwrap();
    db.execute("INSERT INTO products VALUES (4, 'D', 75.0, 20, false)")
        .await
        .unwrap();

    let result = db
        .execute(
            "SELECT * FROM products
         WHERE (price > 60 AND stock > 0) OR featured = true",
        )
        .await
        .unwrap();

    assert_eq!(result.row_count(), 3); // A, C, D
}

#[tokio::test]
async fn test_multi_column_order_by() {
    let mut db = InMemoryDB::new();

    db.execute(
        "CREATE TABLE employees (
        name TEXT,
        department TEXT,
        salary INTEGER
    )",
    )
    .await
    .unwrap();

    db.execute("INSERT INTO employees VALUES ('Alice', 'Engineering', 100000)")
        .await
        .unwrap();
    db.execute("INSERT INTO employees VALUES ('Bob', 'Sales', 80000)")
        .await
        .unwrap();
    db.execute("INSERT INTO employees VALUES ('Charlie', 'Engineering', 120000)")
        .await
        .unwrap();
    db.execute("INSERT INTO employees VALUES ('Diana', 'Sales', 90000)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM employees ORDER BY department ASC, salary DESC")
        .await
        .unwrap();

    let rows = result.rows();
    // Engineering: Charlie (120k), Alice (100k)
    // Sales: Diana (90k), Bob (80k)
    assert_eq!(rows[0][0], Value::Text("Charlie".to_string()));
    assert_eq!(rows[1][0], Value::Text("Alice".to_string()));
    assert_eq!(rows[2][0], Value::Text("Diana".to_string()));
    assert_eq!(rows[3][0], Value::Text("Bob".to_string()));
}
