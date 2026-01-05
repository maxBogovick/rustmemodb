use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_group_by_simple() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE sales (region TEXT, amount INTEGER)").await.unwrap();
    db.execute("INSERT INTO sales VALUES ('North', 100)").await.unwrap();
    db.execute("INSERT INTO sales VALUES ('North', 200)").await.unwrap();
    db.execute("INSERT INTO sales VALUES ('South', 50)").await.unwrap();
    db.execute("INSERT INTO sales VALUES ('South', 150)").await.unwrap();
    db.execute("INSERT INTO sales VALUES ('East', 300)").await.unwrap();

    // SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region
    let result = db.execute(
        "SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region"
    ).await.unwrap();

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

#[tokio::test]
async fn test_group_by_count() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE logs (level TEXT, msg TEXT)").await.unwrap();
    db.execute("INSERT INTO logs VALUES ('INFO', 'a')").await.unwrap();
    db.execute("INSERT INTO logs VALUES ('INFO', 'b')").await.unwrap();
    db.execute("INSERT INTO logs VALUES ('ERROR', 'c')").await.unwrap();

    // SELECT level, COUNT(*) FROM logs GROUP BY level
    let result = db.execute(
        "SELECT level, COUNT(*) FROM logs GROUP BY level ORDER BY level"
    ).await.unwrap();

    assert_eq!(result.row_count(), 2);
    
    let rows = result.rows();
    // ERROR -> 1
    assert_eq!(rows[0][0], Value::Text("ERROR".into()));
    assert_eq!(rows[0][1], Value::Integer(1));
    
    // INFO -> 2
    assert_eq!(rows[1][0], Value::Text("INFO".into()));
    assert_eq!(rows[1][1], Value::Integer(2));
}

#[tokio::test]
async fn test_group_by_having() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE orders (user_id INTEGER, total INTEGER)").await.unwrap();
    db.execute("INSERT INTO orders VALUES (1, 100)").await.unwrap();
    db.execute("INSERT INTO orders VALUES (1, 200)").await.unwrap();
    db.execute("INSERT INTO orders VALUES (2, 50)").await.unwrap();
    db.execute("INSERT INTO orders VALUES (3, 500)").await.unwrap();

    // SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING SUM(total) > 250
    let result = db.execute(
        "SELECT user_id, SUM(total) FROM orders GROUP BY user_id HAVING SUM(total) > 250 ORDER BY user_id"
    ).await.unwrap();

    assert_eq!(result.row_count(), 2);
    
    let rows = result.rows();
    // User 1 -> 300
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(rows[0][1], Value::Integer(300));
    
    // User 3 -> 500
    assert_eq!(rows[1][0], Value::Integer(3));
    assert_eq!(rows[1][1], Value::Integer(500));
}