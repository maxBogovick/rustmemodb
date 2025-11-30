// ============================================================================
// Integration Tests for ORDER BY functionality
// ============================================================================
//
// These tests verify the complete ORDER BY implementation from SQL parsing
// through execution, ensuring all components work together correctly.
//
// Test Coverage:
// - Basic ASC/DESC sorting
// - Multi-column sorting
// - NULL handling
// - Sorting with WHERE clauses
// - Sorting with LIMIT
// - Sorting different data types (INTEGER, TEXT, FLOAT)
// - Complex queries with multiple operations
// - Performance benchmarks
//
// ============================================================================

use rustmemodb::facade::InMemoryDB;
use rustmemodb::core::{Column, Result, Value};
use std::time::Instant;
use rustmemodb::DataType;
use rustmemodb::storage::{Catalog, InMemoryStorage, TableSchema};
// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

fn setup_test_db() -> InMemoryDB {
    let mut db = InMemoryDB::new();

    // Create test table
    db.execute(
        "CREATE TABLE products (
            id INTEGER,
            name TEXT,
            price INTEGER,
            category TEXT,
            rating INTEGER
        )",
    )
        .unwrap();

    // Insert test data
    db.execute("INSERT INTO products VALUES (1, 'Laptop', 1200, 'Electronics', 5)").unwrap();
    db.execute("INSERT INTO products VALUES (2, 'Mouse', 25, 'Electronics', 4)").unwrap();
    db.execute("INSERT INTO products VALUES (3, 'Desk', 300, 'Furniture', 5)").unwrap();
    db.execute("INSERT INTO products VALUES (4, 'Chair', 150, 'Furniture', 4)").unwrap();
    db.execute("INSERT INTO products VALUES (5, 'Monitor', 400, 'Electronics', 5)").unwrap();

    db
}

fn setup_db_with_nulls() -> InMemoryDB {
    let mut db = InMemoryDB::new();

    db.execute(
        "CREATE TABLE employees (
            id INTEGER,
            name TEXT,
            salary INTEGER,
            department TEXT
        )",
    )
        .unwrap();

    // Insert data with some NULL salaries
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 75000, 'Engineering')").unwrap();
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 65000, 'Sales')").unwrap();
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', NULL, 'Engineering')").unwrap();
    db.execute("INSERT INTO employees VALUES (4, 'David', 80000, 'Engineering')").unwrap();
    db.execute("INSERT INTO employees VALUES (5, 'Eve', NULL, 'Sales')").unwrap();

    db
}

/// Setup a large database for performance testing
fn setup_large_db(row_count: usize) -> InMemoryDB {
    let mut db = InMemoryDB::new();

    db.execute(
        "CREATE TABLE large_table (
            id INTEGER,
            value INTEGER,
            name TEXT,
            category INTEGER
        )",
    )
        .unwrap();

    // Insert rows in batches for efficiency
    for i in 0..row_count {
        let value = (i * 7 + 13) % 10000; // Pseudo-random values
        let category = i % 100;
        let name = format!("Item_{}", i);
        db.execute(&format!(
            "INSERT INTO large_table VALUES ({}, {}, '{}', {})",
            i, value, name, category
        ))
            .unwrap();
    }

    db
}

// ============================================================================
// BASIC SORTING TESTS
// ============================================================================

#[test]
fn test_order_by_integer_ascending() {
    let mut db = setup_test_db();

    let result = db.execute("SELECT name, price FROM products ORDER BY price ASC").unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 5);

    // Should be sorted by price: Mouse(25), Chair(150), Desk(300), Monitor(400), Laptop(1200)
    assert_eq!(rows[0][0], Value::Text("Mouse".into()));
    assert_eq!(rows[1][0], Value::Text("Chair".into()));
    assert_eq!(rows[2][0], Value::Text("Desk".into()));
    assert_eq!(rows[3][0], Value::Text("Monitor".into()));
    assert_eq!(rows[4][0], Value::Text("Laptop".into()));
}

#[test]
fn test_order_by_integer_descending() {
    let mut db = setup_test_db();

    let result = db.execute("SELECT name, price FROM products ORDER BY price DESC").unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 5);

    // Should be sorted by price DESC: Laptop(1200), Monitor(400), Desk(300), Chair(150), Mouse(25)
    assert_eq!(rows[0][0], Value::Text("Laptop".into()));
    assert_eq!(rows[1][0], Value::Text("Monitor".into()));
    assert_eq!(rows[2][0], Value::Text("Desk".into()));
    assert_eq!(rows[3][0], Value::Text("Chair".into()));
    assert_eq!(rows[4][0], Value::Text("Mouse".into()));
}

#[test]
fn test_order_by_text_ascending() {
    let mut db = setup_test_db();

    let result = db.execute("SELECT name FROM products ORDER BY name ASC").unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 5);

    // Should be sorted alphabetically
    assert_eq!(rows[0][0], Value::Text("Chair".into()));
    assert_eq!(rows[1][0], Value::Text("Desk".into()));
    assert_eq!(rows[2][0], Value::Text("Laptop".into()));
    assert_eq!(rows[3][0], Value::Text("Monitor".into()));
    assert_eq!(rows[4][0], Value::Text("Mouse".into()));
}

#[test]
fn test_order_by_text_descending() {
    let mut db = setup_test_db();

    let result = db.execute("SELECT name FROM products ORDER BY name DESC").unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 5);

    // Should be sorted reverse alphabetically
    assert_eq!(rows[0][0], Value::Text("Mouse".into()));
    assert_eq!(rows[1][0], Value::Text("Monitor".into()));
    assert_eq!(rows[2][0], Value::Text("Laptop".into()));
    assert_eq!(rows[3][0], Value::Text("Desk".into()));
    assert_eq!(rows[4][0], Value::Text("Chair".into()));
}

// ============================================================================
// MULTI-COLUMN SORTING TESTS
// ============================================================================

#[test]
fn test_order_by_multiple_columns() {
    let mut db = setup_test_db();

    // ORDER BY category ASC, price DESC
    let result = db.execute(
        "SELECT category, name, price FROM products ORDER BY category ASC, price DESC"
    ).unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 5);

    // Expected order:
    // Electronics: Laptop(1200), Monitor(400), Mouse(25)
    // Furniture: Desk(300), Chair(150)
    assert_eq!(rows[0][0], Value::Text("Electronics".into()));
    assert_eq!(rows[0][1], Value::Text("Laptop".into()));

    assert_eq!(rows[1][0], Value::Text("Electronics".into()));
    assert_eq!(rows[1][1], Value::Text("Monitor".into()));

    assert_eq!(rows[2][0], Value::Text("Electronics".into()));
    assert_eq!(rows[2][1], Value::Text("Mouse".into()));

    assert_eq!(rows[3][0], Value::Text("Furniture".into()));
    assert_eq!(rows[3][1], Value::Text("Desk".into()));

    assert_eq!(rows[4][0], Value::Text("Furniture".into()));
    assert_eq!(rows[4][1], Value::Text("Chair".into()));
}

#[test]
fn test_order_by_three_columns() {
    let mut db = setup_test_db();

    // ORDER BY rating DESC, category ASC, price ASC
    let result = db.execute(
        "SELECT rating, category, name, price FROM products ORDER BY rating DESC, category ASC, price ASC"
    ).unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 5);

    // Expected order:
    // Rating 5: Electronics (Monitor 400, Laptop 1200), Furniture (Desk 300)
    // Rating 4: Electronics (Mouse 25), Furniture (Chair 150)
    assert_eq!(rows[0][0], Value::Integer(5)); // Monitor
    assert_eq!(rows[0][2], Value::Text("Monitor".into()));

    assert_eq!(rows[1][0], Value::Integer(5)); // Laptop
    assert_eq!(rows[1][2], Value::Text("Laptop".into()));

    assert_eq!(rows[2][0], Value::Integer(5)); // Desk
    assert_eq!(rows[2][2], Value::Text("Desk".into()));

    assert_eq!(rows[3][0], Value::Integer(4)); // Mouse
    assert_eq!(rows[3][2], Value::Text("Mouse".into()));

    assert_eq!(rows[4][0], Value::Integer(4)); // Chair
    assert_eq!(rows[4][2], Value::Text("Chair".into()));
}

// ============================================================================
// NULL HANDLING TESTS
// ============================================================================

#[test]
fn test_order_by_with_nulls_ascending() {
    let mut db = setup_db_with_nulls();

    // ORDER BY salary ASC → NULLS LAST
    let result = db.execute("SELECT name, salary FROM employees ORDER BY salary ASC").unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 5);

    // Expected order: Bob(65000), Alice(75000), David(80000), Charlie(NULL), Eve(NULL)
    assert_eq!(rows[0][0], Value::Text("Bob".into()));
    assert_eq!(rows[0][1], Value::Integer(65000));

    assert_eq!(rows[1][0], Value::Text("Alice".into()));
    assert_eq!(rows[1][1], Value::Integer(75000));

    assert_eq!(rows[2][0], Value::Text("David".into()));
    assert_eq!(rows[2][1], Value::Integer(80000));

    // NULLs should be last
    assert_eq!(rows[3][1], Value::Null);
    assert_eq!(rows[4][1], Value::Null);
}

#[test]
fn test_order_by_with_nulls_descending() {
    let mut db = setup_db_with_nulls();

    // ORDER BY salary DESC → NULLS FIRST
    let result = db.execute("SELECT name, salary FROM employees ORDER BY salary DESC").unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 5);

    // Expected order: Charlie(NULL), Eve(NULL), David(80000), Alice(75000), Bob(65000)
    // NULLs should be first
    assert_eq!(rows[0][1], Value::Null);
    assert_eq!(rows[1][1], Value::Null);

    assert_eq!(rows[2][0], Value::Text("David".into()));
    assert_eq!(rows[2][1], Value::Integer(80000));

    assert_eq!(rows[3][0], Value::Text("Alice".into()));
    assert_eq!(rows[3][1], Value::Integer(75000));

    assert_eq!(rows[4][0], Value::Text("Bob".into()));
    assert_eq!(rows[4][1], Value::Integer(65000));
}

// ============================================================================
// SORTING WITH OTHER CLAUSES
// ============================================================================

#[test]
fn test_order_by_with_where() {
    let mut db = setup_test_db();

    // SELECT with WHERE and ORDER BY
    let result = db.execute(
        "SELECT name, price FROM products WHERE category = 'Electronics' ORDER BY price ASC"
    ).unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 3);

    // Should be sorted by price: Mouse(25), Monitor(400), Laptop(1200)
    assert_eq!(rows[0][0], Value::Text("Mouse".into()));
    assert_eq!(rows[1][0], Value::Text("Monitor".into()));
    assert_eq!(rows[2][0], Value::Text("Laptop".into()));
}

#[test]
fn test_order_by_with_limit() {
    let mut db = setup_test_db();

    // Get top 3 most expensive products
    let result = db.execute(
        "SELECT name, price FROM products ORDER BY price DESC LIMIT 3"
    ).unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 3);

    // Should be: Laptop(1200), Monitor(400), Desk(300)
    assert_eq!(rows[0][0], Value::Text("Laptop".into()));
    assert_eq!(rows[1][0], Value::Text("Monitor".into()));
    assert_eq!(rows[2][0], Value::Text("Desk".into()));
}

#[test]
fn test_order_by_with_where_and_limit() {
    let mut db = setup_test_db();

    // Get top 2 highest rated products in Electronics
    let result = db.execute(
        "SELECT name, rating FROM products
         WHERE category = 'Electronics'
         ORDER BY rating DESC, price DESC
         LIMIT 2"
    ).unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 2);

    // Should be: Laptop(rating 5, price 1200), Monitor(rating 5, price 400)
    assert_eq!(rows[0][0], Value::Text("Laptop".into()));
    assert_eq!(rows[1][0], Value::Text("Monitor".into()));
}

// ============================================================================
// EDGE CASES
// ============================================================================

#[test]
fn test_order_by_empty_result() {
    let mut db = setup_test_db();

    let result = db.execute(
        "SELECT name FROM products WHERE price > 10000 ORDER BY price ASC"
    ).unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 0);
}

#[test]
fn test_order_by_single_row() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE test (id INTEGER, name TEXT)").unwrap();
    db.execute("INSERT INTO test VALUES (1, 'Only')").unwrap();

    let result = db.execute("SELECT name FROM test ORDER BY id DESC").unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Text("Only".into()));
}

#[test]
fn test_order_by_all_same_values() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE test (id INTEGER, value INTEGER)").unwrap();
    db.execute("INSERT INTO test VALUES (1, 100)").unwrap();
    db.execute("INSERT INTO test VALUES (2, 100)").unwrap();
    db.execute("INSERT INTO test VALUES (3, 100)").unwrap();

    let result = db.execute("SELECT id FROM test ORDER BY value ASC").unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 3);
    // Order should be stable (maintain insertion order for equal values)
}

// ============================================================================
// COMPLEX QUERIES
// ============================================================================

#[test]
fn test_complex_query_with_everything() {
    let mut db = setup_test_db();

    // Complex query with WHERE, multi-column ORDER BY, and LIMIT
    let result = db.execute(
        "SELECT category, name, price, rating
         FROM products
         WHERE price > 100
         ORDER BY category ASC, rating DESC, price ASC
         LIMIT 4"
    ).unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 4);

    // Expected:
    // Electronics: Monitor(400, rating 5), Laptop(1200, rating 5)
    // Furniture: Desk(300, rating 5), Chair(150, rating 4)
    assert_eq!(rows[0][1], Value::Text("Monitor".into()));
    assert_eq!(rows[1][1], Value::Text("Laptop".into()));
    assert_eq!(rows[2][1], Value::Text("Desk".into()));
    assert_eq!(rows[3][1], Value::Text("Chair".into()));
}

#[test]
fn test_order_by_with_like_and_between() {
    let mut db = setup_test_db();

    let result = db.execute(
        "SELECT name, price
         FROM products
         WHERE name LIKE 'M%' AND price BETWEEN 20 AND 500
         ORDER BY price DESC"
    ).unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 2);

    // Should be: Monitor(400), Mouse(25)
    assert_eq!(rows[0][0], Value::Text("Monitor".into()));
    assert_eq!(rows[1][0], Value::Text("Mouse".into()));
}

// ============================================================================
// WILDCARD SELECT WITH ORDER BY
// ============================================================================

#[test]
fn test_select_star_with_order_by() {
    let mut db = setup_test_db();

    let result = db.execute("SELECT * FROM products ORDER BY price ASC LIMIT 2").unwrap();

    let rows = &result.rows;
    assert_eq!(rows.len(), 2);

    // Should get Mouse and Chair (cheapest two)
    assert_eq!(rows[0][1], Value::Text("Mouse".into())); // name column
    assert_eq!(rows[1][1], Value::Text("Chair".into())); // name column
}

// ============================================================================
// PERFORMANCE TESTS
// ============================================================================

#[test]
fn test_performance_sort_1000_rows() {
    let mut db = setup_large_db(1000);

    let start = Instant::now();
    let result = db.execute("SELECT * FROM large_table ORDER BY value ASC").unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 1000);

    // Verify sorted
    for i in 1..result.rows.len() {
        let prev = &result.rows[i - 1][1];
        let curr = &result.rows[i][1];
        if let (Value::Integer(a), Value::Integer(b)) = (prev, curr) {
            assert!(a <= b, "Rows not sorted: {} > {}", a, b);
        }
    }

    println!("✅ Sort 1,000 rows: {:?}", duration);
    assert!(duration.as_millis() < 1000, "Sort took too long: {:?}", duration);
}

#[test]
fn test_performance_sort_10000_rows() {
    let mut db = setup_large_db(10_000);

    let start = Instant::now();
    let result = db.execute("SELECT * FROM large_table ORDER BY value DESC").unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 10_000);

    // Verify sorted descending
    for i in 1..result.rows.len() {
        let prev = &result.rows[i - 1][1];
        let curr = &result.rows[i][1];
        if let (Value::Integer(a), Value::Integer(b)) = (prev, curr) {
            assert!(a >= b, "Rows not sorted DESC: {} < {}", a, b);
        }
    }

    println!("✅ Sort 10,000 rows: {:?}", duration);
    assert!(duration.as_millis() < 5000, "Sort took too long: {:?}", duration);
}

#[test]
fn test_performance_sort_with_limit() {
    let mut db = setup_large_db(10_000);

    // Should be fast because LIMIT is applied after sort
    let start = Instant::now();
    let result = db.execute("SELECT * FROM large_table ORDER BY value ASC LIMIT 10").unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 10);

    println!("✅ Sort 10,000 rows with LIMIT 10: {:?}", duration);
    assert!(duration.as_millis() < 5000, "Sort took too long: {:?}", duration);
}

#[test]
fn test_performance_multi_column_sort() {
    let mut db = setup_large_db(5000);

    let start = Instant::now();
    let result = db.execute(
        "SELECT * FROM large_table ORDER BY category ASC, value DESC"
    ).unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 5000);

    // Verify multi-column sort
    for i in 1..result.rows.len() {
        let prev_cat = &result.rows[i - 1][3];
        let curr_cat = &result.rows[i][3];
        let prev_val = &result.rows[i - 1][1];
        let curr_val = &result.rows[i][1];

        if let (Value::Integer(pc), Value::Integer(cc)) = (prev_cat, curr_cat) {
            if pc == cc {
                // Same category - value should be DESC
                if let (Value::Integer(pv), Value::Integer(cv)) = (prev_val, curr_val) {
                    assert!(pv >= cv, "Values not sorted DESC within category");
                }
            } else {
                // Category should be ASC
                assert!(pc < cc, "Categories not sorted ASC");
            }
        }
    }

    println!("✅ Multi-column sort 5,000 rows: {:?}", duration);
    assert!(duration.as_millis() < 3000, "Sort took too long: {:?}", duration);
}

#[test]
fn test_performance_sort_with_filter() {
    let mut db = setup_large_db(10_000);

    let start = Instant::now();
    let result = db.execute(
        "SELECT * FROM large_table WHERE category < 10 ORDER BY value ASC"
    ).unwrap();
    let duration = start.elapsed();

    // Should be ~1000 rows (10% of data where category < 10)
    assert!(result.rows.len() > 0);
    assert!(result.rows.len() <= 1000);

    println!("✅ Filter + Sort ({} rows): {:?}", result.rows.len(), duration);
    assert!(duration.as_millis() < 3000, "Query took too long: {:?}", duration);
}

#[test]
fn test_performance_sort_text_column() {
    let mut db = setup_large_db(5000);

    let start = Instant::now();
    let result = db.execute("SELECT * FROM large_table ORDER BY name ASC").unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 5000);

    // Verify text sorting
    for i in 1..result.rows.len() {
        let prev = &result.rows[i - 1][2];
        let curr = &result.rows[i][2];
        if let (Value::Text(a), Value::Text(b)) = (prev, curr) {
            assert!(a <= b, "Text not sorted: {} > {}", a, b);
        }
    }

    println!("✅ Sort 5,000 rows by TEXT: {:?}", duration);
    assert!(duration.as_millis() < 3000, "Sort took too long: {:?}", duration);
}

#[test]
fn test_performance_already_sorted_data() {
    let mut db = InMemoryDB::new();

    db.execute(
        "CREATE TABLE sorted_table (id INTEGER, value INTEGER)"
    ).unwrap();

    // Insert already sorted data
    for i in 0..5000 {
        db.execute(&format!("INSERT INTO sorted_table VALUES ({}, {})", i, i)).unwrap();
    }

    let start = Instant::now();
    let result = db.execute("SELECT * FROM sorted_table ORDER BY value ASC").unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 5000);

    println!("✅ Sort already sorted 5,000 rows: {:?}", duration);
    assert!(duration.as_millis() < 2000, "Sort took too long: {:?}", duration);
}

#[test]
fn test_performance_reverse_sorted_data() {
    let mut db = InMemoryDB::new();

    db.execute(
        "CREATE TABLE reverse_table (id INTEGER, value INTEGER)"
    ).unwrap();

    // Insert reverse sorted data
    for i in (0..5000).rev() {
        db.execute(&format!("INSERT INTO reverse_table VALUES ({}, {})", 5000 - i, i)).unwrap();
    }

    let start = Instant::now();
    let result = db.execute("SELECT * FROM reverse_table ORDER BY value ASC").unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 5000);

    // Verify it's now sorted ascending
    for i in 1..result.rows.len() {
        let prev = &result.rows[i - 1][1];
        let curr = &result.rows[i][1];
        if let (Value::Integer(a), Value::Integer(b)) = (prev, curr) {
            assert!(a <= b, "Rows not sorted");
        }
    }

    println!("✅ Sort reverse-sorted 5,000 rows: {:?}", duration);
    assert!(duration.as_millis() < 2000, "Sort took too long: {:?}", duration);
}

#[test]
fn test_performance_comparison_with_without_order_by() {
    let mut db = setup_large_db(5000);

    // Without ORDER BY
    let start_no_sort = Instant::now();
    let result_no_sort = db.execute("SELECT * FROM large_table").unwrap();
    let duration_no_sort = start_no_sort.elapsed();

    // With ORDER BY
    let start_sort = Instant::now();
    let result_sort = db.execute("SELECT * FROM large_table ORDER BY value ASC").unwrap();
    let duration_sort = start_sort.elapsed();

    assert_eq!(result_no_sort.rows.len(), result_sort.rows.len());

    println!("📊 Performance comparison (5,000 rows):");
    println!("   Without ORDER BY: {:?}", duration_no_sort);
    println!("   With ORDER BY:    {:?}", duration_sort);
    println!("   Overhead:         {:?}", duration_sort.saturating_sub(duration_no_sort));
}

// ============================================================================
// STRESS TESTS
// ============================================================================

#[test]
//#[ignore] // Run with: cargo test -- --ignored
fn test_stress_sort_50000_rows() {
    let mut db = setup_large_db(50_000);

    let start = Instant::now();
    let result = db.execute("SELECT * FROM large_table ORDER BY value ASC").unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 50_000);

    println!("🔥 Stress test - Sort 50,000 rows: {:?}", duration);
}

#[test]
//#[ignore] // Run with: cargo test -- --ignored
fn test_stress_sort_100000_rows() {
    let mut db = setup_large_db(100_000);

    let start = Instant::now();
    let result = db.execute("SELECT * FROM large_table ORDER BY value DESC").unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 100_000);

    println!("🔥 Stress test - Sort 100,000 rows: {:?}", duration);
}

#[test]
//#[ignore] // Run with: cargo test -- --ignored
fn test_stress_complex_query_large_dataset() {
    let mut db = setup_large_db(50_000);

    let start = Instant::now();
    let result = db.execute(
        "SELECT * FROM large_table
         WHERE category < 50 and name like '%tem_%'
         ORDER BY category ASC, value DESC
         LIMIT 1000"
    ).unwrap();
    let duration = start.elapsed();

    assert_eq!(result.rows.len(), 1000);

    println!("🔥 Stress test - Complex query on 50,000 rows: {:?}", duration);
}