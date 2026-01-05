/// Complex queries tests
///
/// Tests for advanced SQL queries and edge cases
/// Run with: cargo test --test complex_queries_tests

use rustmemodb::{Client, Value};

#[tokio::test]
async fn test_complex_multi_table_setup() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    // Create multiple related tables
    client.execute(
        "CREATE TABLE customers1 (
            id INTEGER,
            name TEXT,
            email TEXT,
            country TEXT
        )"
    ).await.unwrap();

    client.execute(
        "CREATE TABLE orders1 (
            id INTEGER,
            customer_id INTEGER,
            amount FLOAT,
            status TEXT
        )"
    ).await.unwrap();

    client.execute(
        "CREATE TABLE products1 (
            id INTEGER,
            name TEXT,
            price FLOAT,
            category TEXT
        )"
    ).await.unwrap();

    // Insert test data
    client.execute("INSERT INTO customers1 VALUES (1, 'Alice', 'alice@example.com', 'USA')").await.unwrap();
    client.execute("INSERT INTO customers1 VALUES (2, 'Bob', 'bob@example.com', 'UK')").await.unwrap();
    client.execute("INSERT INTO customers1 VALUES (3, 'Charlie', 'charlie@example.com', 'USA')").await.unwrap();

    client.execute("INSERT INTO orders1 VALUES (1, 1, 150.0, 'completed')").await.unwrap();
    client.execute("INSERT INTO orders1 VALUES (2, 1, 200.0, 'pending')").await.unwrap();
    client.execute("INSERT INTO orders1 VALUES (3, 2, 100.0, 'completed')").await.unwrap();

    client.execute("INSERT INTO products1 VALUES (1, 'Laptop', 999.99, 'Electronics')").await.unwrap();
    client.execute("INSERT INTO products1 VALUES (2, 'Mouse', 29.99, 'Electronics')").await.unwrap();
    client.execute("INSERT INTO products1 VALUES (3, 'Desk', 299.99, 'Furniture')").await.unwrap();

    // Verify all tables created
    assert!(client.query("SELECT * FROM customers1").await.unwrap().row_count() == 3);
    assert!(client.query("SELECT * FROM orders1").await.unwrap().row_count() == 3);
    assert!(client.query("SELECT * FROM products1").await.unwrap().row_count() == 3);
}

#[tokio::test]
async fn test_boolean_not_operator() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE permissions2 (user_id INTEGER, can_read BOOLEAN, can_write BOOLEAN, can_delete BOOLEAN, is_admin BOOLEAN)").await.unwrap();

    client.execute("INSERT INTO permissions2 VALUES (1, true, true, false, false)").await.unwrap();
    client.execute("INSERT INTO permissions2 VALUES (2, true, false, false, false)").await.unwrap();
    client.execute("INSERT INTO permissions2 VALUES (3, true, true, true, true)").await.unwrap();
    client.execute("INSERT INTO permissions2 VALUES (4, false, false, false, false)").await.unwrap();

    // Тест: Users who are NOT admin
    let result = client.query(
        "SELECT user_id FROM permissions2
             WHERE NOT is_admin"
    ).await.unwrap();

    assert_eq!(result.row_count(), 3);
}

#[tokio::test]
async fn test_boolean_complex_expression() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    client.execute("CREATE TABLE permissions1 (user_id INTEGER, can_read BOOLEAN, can_write BOOLEAN, can_delete BOOLEAN, is_admin BOOLEAN)").await.unwrap();

    client.execute("INSERT INTO permissions1 VALUES (1, true, true, false, false)").await.unwrap();
    client.execute("INSERT INTO permissions1 VALUES (2, true, false, false, false)").await.unwrap();
    client.execute("INSERT INTO permissions1 VALUES (3, true, true, true, true)").await.unwrap();
    client.execute("INSERT INTO permissions1 VALUES (4, false, false, false, false)").await.unwrap();

    // Тест: Complex boolean expression
    let result = client.query(
        "SELECT user_id FROM permissions1
             WHERE can_read = true AND can_write = true AND NOT is_admin"
    ).await.unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows()[0][0].to_string(), "1");
}

#[tokio::test]
async fn test_boolean_with_parentheses() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE permissions (user_id INTEGER, can_read BOOLEAN, can_write BOOLEAN, can_delete BOOLEAN, is_admin BOOLEAN)").await.unwrap();

    client.execute("INSERT INTO permissions VALUES (1, true, true, false, false)").await.unwrap();
    client.execute("INSERT INTO permissions VALUES (2, true, false, false, false)").await.unwrap();
    client.execute("INSERT INTO permissions VALUES (3, true, true, true, true)").await.unwrap();
    client.execute("INSERT INTO permissions VALUES (4, false, false, false, false)").await.unwrap();

    // Тест: (can_write OR can_delete) AND NOT is_admin
    let result = client.query(
        "SELECT user_id FROM permissions
             WHERE (can_write = true OR can_delete = true) AND is_admin = false"
    ).await.unwrap();

    assert_eq!(result.row_count(), 1);
    assert_eq!(result.rows()[0][0].to_string(), "1");
}

#[tokio::test]
async fn test_complex_where_with_multiple_conditions() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE employees (
            id INTEGER,
            name TEXT,
            department TEXT,
            salary INTEGER,
            years_experience INTEGER,
            remote BOOLEAN
        )"
    ).await.unwrap();

    client.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 100000, 5, true)").await.unwrap();
    client.execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales', 80000, 3, false)").await.unwrap();
    client.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 120000, 8, true)").await.unwrap();
    client.execute("INSERT INTO employees VALUES (4, 'Diana', 'Marketing', 90000, 4, true)").await.unwrap();
    client.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 110000, 6, false)").await.unwrap();

    let result = client.query(
        "SELECT name, salary FROM employees
         WHERE department = 'Engineering'
           AND salary > 105000
           AND years_experience > 5
           AND remote = true"
    ).await.unwrap();

    assert_eq!(result.row_count(), 1); // Only Charlie
}

#[tokio::test]
async fn test_complex_or_conditions() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE products2 (
            id INTEGER,
            name TEXT,
            price FLOAT,
            stock INTEGER,
            featured BOOLEAN
        )"
    ).await.unwrap();

    client.execute("INSERT INTO products2 VALUES (1, 'A', 100.0, 5, false)").await.unwrap();
    client.execute("INSERT INTO products2 VALUES (2, 'B', 50.0, 0, true)").await.unwrap();
    client.execute("INSERT INTO products2 VALUES (3, 'C', 200.0, 10, false)").await.unwrap();
    client.execute("INSERT INTO products2 VALUES (4, 'D', 75.0, 3, false)").await.unwrap();
    client.execute("INSERT INTO products2 VALUES (5, 'E', 150.0, 0, true)").await.unwrap();

    let result = client.query(
        "SELECT * FROM products2
         WHERE stock = 0 OR (price > 120 AND featured = false)"
    ).await.unwrap();

    assert_eq!(result.row_count(), 3); // B (stock=0), C (price>120), E (stock=0)
}

#[tokio::test]
async fn test_complex_nested_logic() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE items (
            id INTEGER,
            category TEXT,
            subcategory TEXT,
            price FLOAT,
            available BOOLEAN
        )"
    ).await.unwrap();

    client.execute("INSERT INTO items VALUES (1, 'Electronics', 'Computers', 1000.0, true)").await.unwrap();
    client.execute("INSERT INTO items VALUES (2, 'Electronics', 'Phones', 500.0, false)").await.unwrap();
    client.execute("INSERT INTO items VALUES (3, 'Books', 'Fiction', 20.0, true)").await.unwrap();
    client.execute("INSERT INTO items VALUES (4, 'Books', 'Technical', 60.0, true)").await.unwrap();
    client.execute("INSERT INTO items VALUES (5, 'Electronics', 'Tablets', 400.0, true)").await.unwrap();

    let result = client.query(
        "SELECT * FROM items
         WHERE (category = 'Electronics' AND (price > 450 OR subcategory = 'Computers'))
            OR (category = 'Books' AND price > 50 AND available = true)"
    ).await.unwrap();

    assert_eq!(result.row_count(), 3); // Items 1, 4, 5
}

#[tokio::test]
async fn test_complex_like_patterns() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE users (id INTEGER, email TEXT, name TEXT)").await.unwrap();

    client.execute("INSERT INTO users VALUES (1, 'alice@gmail.com', 'Alice Johnson')").await.unwrap();
    client.execute("INSERT INTO users VALUES (2, 'bob.smith@yahoo.com', 'Bob Smith')").await.unwrap();
    client.execute("INSERT INTO users VALUES (3, 'charlie@gmail.com', 'Charlie Brown')").await.unwrap();
    client.execute("INSERT INTO users VALUES (4, 'diana@hotmail.com', 'Diana Prince')").await.unwrap();
    client.execute("INSERT INTO users VALUES (5, 'eve.adams@gmail.com', 'Eve Adams')").await.unwrap();

    // Gmail users with names starting with vowels
    let result = client.query(
        "SELECT * FROM users
         WHERE email LIKE '%@gmail.com'
           AND (name LIKE 'A%' OR name LIKE 'E%' OR name LIKE 'I%')"
    ).await.unwrap();

    assert_eq!(result.row_count(), 2); // Alice and Eve
}

#[tokio::test]
async fn test_complex_between_and_conditions() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE transactions (
            id INTEGER,
            amount FLOAT,
            category TEXT,
            day INTEGER
        )"
    ).await.unwrap();

    for i in 1..=30 {
        let amount = (i as f64 * 10.5) + 50.0;
        let category = if i % 2 == 0 { "income" } else { "expense" };
        client.execute(&format!(
            "INSERT INTO transactions VALUES ({}, {}, '{}', {})",
            i, amount, category, i
        )).await.unwrap();
    }

    let result = client.query(
        "SELECT * FROM transactions
         WHERE amount BETWEEN 150 AND 300
           AND category = 'income'
           AND day BETWEEN 10 AND 20"
    ).await.unwrap();

    assert!(result.row_count() > 0);
}

#[tokio::test]
async fn test_complex_multi_column_order_by() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE leaderboard (
            player TEXT,
            level INTEGER,
            score INTEGER,
            time_seconds INTEGER
        )"
    ).await.unwrap();

    client.execute("INSERT INTO leaderboard VALUES ('Alice', 10, 1000, 120)").await.unwrap();
    client.execute("INSERT INTO leaderboard VALUES ('Bob', 10, 1000, 100)").await.unwrap();
    client.execute("INSERT INTO leaderboard VALUES ('Charlie', 10, 950, 90)").await.unwrap();
    client.execute("INSERT INTO leaderboard VALUES ('Diana', 9, 1100, 150)").await.unwrap();
    client.execute("INSERT INTO leaderboard VALUES ('Eve', 10, 1000, 110)").await.unwrap();

    let result = client.query(
        "SELECT player, level, score, time_seconds FROM leaderboard
         ORDER BY level DESC, score DESC, time_seconds ASC"
    ).await.unwrap();

    let rows = result.rows();

    assert_eq!(rows[0][0], Value::Text("Bob".to_string())); // Level 10, score 1000, time 100
    assert_eq!(rows[1][0], Value::Text("Eve".to_string())); // Level 10, score 1000, time 110
}

#[tokio::test]
async fn test_complex_null_handling() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE contacts (
            id INTEGER,
            name TEXT,
            email TEXT,
            phone TEXT,
            address TEXT
        )"
    ).await.unwrap();

    client.execute("INSERT INTO contacts VALUES (1, 'Alice', 'alice@ex.com', '555-1234', '123 St')").await.unwrap();
    client.execute("INSERT INTO contacts VALUES (2, 'Bob', NULL, '555-5678', NULL)").await.unwrap();
    client.execute("INSERT INTO contacts VALUES (3, 'Charlie', 'charlie@ex.com', NULL, '456 Ave')").await.unwrap();
    client.execute("INSERT INTO contacts VALUES (4, 'Diana', NULL, NULL, NULL)").await.unwrap();

    // Find contacts with at least email OR phone
    let result = client.query(
        "SELECT name FROM contacts
         WHERE email IS NOT NULL OR phone IS NOT NULL"
    ).await.unwrap();

    assert_eq!(result.row_count(), 3); // Alice, Bob, Charlie (Diana has neither)
}

#[tokio::test]
async fn test_complex_arithmetic_in_where() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE products (
            id INTEGER,
            base_price FLOAT,
            discount_percent INTEGER,
            tax_percent INTEGER
        )"
    ).await.unwrap();

    client.execute("INSERT INTO products VALUES (1, 150.1, 10, 5)").await.unwrap();
    client.execute("INSERT INTO products VALUES (2, 200.0, 20, 5)").await.unwrap();
    client.execute("INSERT INTO products VALUES (3, 50.0, 5, 10)").await.unwrap();
    client.execute("INSERT INTO products VALUES (4, 150.0, 15, 8)").await.unwrap();

    // Find products where final price (after discount) is > 100
    let result = client.query(
        "SELECT * FROM products
         WHERE base_price * (100 - discount_percent) / 100 > 100"
    ).await.unwrap();

    assert_eq!(result.row_count(), 3); // Products 1, 2, 4
}

#[tokio::test]
async fn test_complex_string_patterns() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE files (id INTEGER, filename TEXT, extension TEXT)").await.unwrap();

    client.execute("INSERT INTO files VALUES (1, 'document.pdf', 'pdf')").await.unwrap();
    client.execute("INSERT INTO files VALUES (2, 'image.jpg', 'jpg')").await.unwrap();
    client.execute("INSERT INTO files VALUES (3, 'photo.jpeg', 'jpeg')").await.unwrap();
    client.execute("INSERT INTO files VALUES (4, 'report.doc', 'doc')").await.unwrap();
    client.execute("INSERT INTO files VALUES (5, 'data.pdf', 'pdf')").await.unwrap();

    // Find all image files (jpg or jpeg)
    let result = client.query(
        "SELECT filename FROM files
         WHERE filename LIKE '%.jpg' OR filename LIKE '%.jpeg'"
    ).await.unwrap();

    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_complex_limit_with_order_by() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE high_scores (
            player TEXT,
            score INTEGER,
            date INTEGER
        )"
    ).await.unwrap();

    for i in 0..20 {
        client.execute(&format!(
            "INSERT INTO high_scores VALUES ('Player{}', {}, {})",
            i, 1000 - (i * 50), i
        )).await.unwrap();
    }

    // Top 5 scores
    let result = client.query(
        "SELECT player, score FROM high_scores
         ORDER BY score DESC
         LIMIT 5"
    ).await.unwrap();

    assert_eq!(result.row_count(), 5);

    let rows = result.rows();
    if let Value::Integer(first_score) = rows[0][1] {
        if let Value::Integer(last_score) = rows[4][1] {
            assert!(first_score > last_score);
        }
    }
}

#[tokio::test]
async fn test_complex_boolean_logic() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE permissions3 (
            user_id INTEGER,
            can_read BOOLEAN,
            can_write BOOLEAN,
            can_delete BOOLEAN,
            is_admin BOOLEAN
        )"
    ).await.unwrap();

    client.execute("INSERT INTO permissions3 VALUES (1, true, true, false, false)").await.unwrap();
    client.execute("INSERT INTO permissions3 VALUES (2, true, false, false, false)").await.unwrap();
    client.execute("INSERT INTO permissions3 VALUES (3, true, true, true, true)").await.unwrap();
    client.execute("INSERT INTO permissions3 VALUES (4, false, false, false, false)").await.unwrap();

    // Users who can write but are not admin
    let result = client.query(
        "SELECT user_id FROM permissions3
         WHERE can_write = true AND is_admin = false"
    ).await.unwrap();

    assert_eq!(result.row_count(), 1); // Only user 1
}

#[tokio::test]
async fn test_complex_data_types_mix() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE mixed_data (
            id INTEGER,
            name TEXT,
            score FLOAT,
            active BOOLEAN,
            nullable_field INTEGER
        )"
    ).await.unwrap();

    client.execute("INSERT INTO mixed_data VALUES (1, 'Alice', 95.5, true, 100)").await.unwrap();
    client.execute("INSERT INTO mixed_data VALUES (2, 'Bob', 87.3, false, NULL)").await.unwrap();
    client.execute("INSERT INTO mixed_data VALUES (3, 'Charlie', 92.1, true, 200)").await.unwrap();

    let result = client.query(
        "SELECT name, score FROM mixed_data
         WHERE active = true
           AND score > 90
         ORDER BY score DESC"
    ).await.unwrap();

    assert_eq!(result.row_count(), 2); // Alice and Charlie
}

#[tokio::test]
async fn test_complex_edge_case_empty_strings() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute("CREATE TABLE texts (id INTEGER, content TEXT)").await.unwrap();

    client.execute("INSERT INTO texts VALUES (1, 'Hello')").await.unwrap();
    client.execute("INSERT INTO texts VALUES (2, '')").await.unwrap();
    client.execute("INSERT INTO texts VALUES (3, 'World')").await.unwrap();

    // Empty strings should not match LIKE patterns
    let result = client.query(
        "SELECT * FROM texts WHERE content LIKE 'H%'"
    ).await.unwrap();

    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn test_complex_large_dataset_query() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    client.execute(
        "CREATE TABLE large_dataset (
            id INTEGER,
            category TEXT,
            value INTEGER
        )"
    ).await.unwrap();

    // Insert 1000 rows
    for i in 0..1000 {
        let category = match i % 5 {
            0 => "A",
            1 => "B",
            2 => "C",
            3 => "D",
            _ => "E",
        };

        client.execute(&format!(
            "INSERT INTO large_dataset VALUES ({}, '{}', {})",
            i, category, i * 2
        )).await.unwrap();
    }

    // Complex query on large dataset
    let result = client.query(
        "SELECT * FROM large_dataset
         WHERE category = 'C' AND value BETWEEN 100 AND 500
         ORDER BY value DESC
         LIMIT 10"
    ).await.unwrap();

    assert_eq!(result.row_count(), 10);
}