use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;

#[test]
fn test_group_by_simple() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE sales (region TEXT, amount INTEGER)").unwrap();
    db.execute("INSERT INTO sales VALUES ('North', 100)").unwrap();
    db.execute("INSERT INTO sales VALUES ('North', 200)").unwrap();
    db.execute("INSERT INTO sales VALUES ('South', 50)").unwrap();
    db.execute("INSERT INTO sales VALUES ('South', 150)").unwrap();
    db.execute("INSERT INTO sales VALUES ('East', 300)").unwrap();

    // SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region
    let result = db.execute(
        "SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region"
    ).unwrap();

    assert_eq!(result.row_count(), 3);
    
    let rows = result.rows();
    // East -> 300
    assert_eq!(rows[0][0], Value::Text("East".into()));
    assert_eq!(rows[0][1], Value::Integer(300));
    
    // North -> 300
    assert_eq!(rows[1][0], Value::Text("North".into()));
    assert_eq!(rows[1][1], Value::Integer(300));
    
    // South -> 200
    assert_eq!(rows[2][0], Value::Text("South".into()));
    assert_eq!(rows[2][1], Value::Integer(200));
}

#[test]
fn test_group_by_count() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE logs (level TEXT, msg TEXT)").unwrap();
    db.execute("INSERT INTO logs VALUES ('INFO', 'a')").unwrap();
    db.execute("INSERT INTO logs VALUES ('INFO', 'b')").unwrap();
    db.execute("INSERT INTO logs VALUES ('ERROR', 'c')").unwrap();

    // SELECT level, COUNT(*) FROM logs GROUP BY level
    let result = db.execute(
        "SELECT level, COUNT(*) FROM logs GROUP BY level ORDER BY level"
    ).unwrap();

    assert_eq!(result.row_count(), 2);
    
    let rows = result.rows();
    // ERROR -> 1
    assert_eq!(rows[0][0], Value::Text("ERROR".into()));
    assert_eq!(rows[0][1], Value::Integer(1));
    
    // INFO -> 2
    assert_eq!(rows[1][0], Value::Text("INFO".into()));
    assert_eq!(rows[1][1], Value::Integer(2));
}

#[test]
fn test_group_by_having() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE orders (user_id INTEGER, total INTEGER)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 100)").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 200)").unwrap();
    db.execute("INSERT INTO orders VALUES (2, 50)").unwrap();
    db.execute("INSERT INTO orders VALUES (3, 500)").unwrap();

    // SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING SUM(total) > 250
    // User 1: 300 -> Keep
    // User 2: 50 -> Drop
    // User 3: 500 -> Keep
    let result = db.execute(
        "SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING SUM(total) > 250 ORDER BY user_id"
    ).unwrap();

    assert_eq!(result.row_count(), 2);
    
    let rows = result.rows();
    // User 1 -> 300
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Integer(300));
    
    // User 3 -> 500
    assert_eq!(rows[1][0], Value::Integer(3));
    assert_eq!(rows[1][1], Value::Integer(500));
}
