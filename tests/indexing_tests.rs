use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;
use rustmemodb::core::Snapshot;
use std::collections::HashSet;

#[tokio::test]
async fn test_create_index_and_use_it() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").await.unwrap();

    // Create index using SQL
    db.execute("CREATE INDEX idx_age ON users (age)").await.unwrap();

    // Insert data
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").await.unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)").await.unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)").await.unwrap();
    db.execute("INSERT INTO users VALUES (4, 'Diana', 25)").await.unwrap();

    let result = db.execute("SELECT * FROM users WHERE age = 25").await.unwrap();
    assert_eq!(result.row_count(), 2);
}

#[tokio::test]
async fn test_indexing_backend() {
    // This test uses internal APIs to verify indexing works
    use rustmemodb::storage::InMemoryStorage;
    use rustmemodb::storage::TableSchema;
    use rustmemodb::core::{Column, DataType};
    use rustmemodb::planner::logical_plan::IndexOp;
    use std::sync::Arc;
    
    let mut storage = InMemoryStorage::new();
    let schema = TableSchema::new("users", vec![
        Column::new("id", DataType::Integer),
        Column::new("age", DataType::Integer),
    ]);
    storage.create_table(schema).await.unwrap();
    
    storage.create_index("users", "age").await.unwrap();
    
    let snapshot = Snapshot {
        tx_id: 0,
        active: Arc::new(HashSet::new()),
        aborted: Arc::new(HashSet::new()),
        max_tx_id: u64::MAX,
    };

    storage.insert_row("users", vec![Value::Integer(1), Value::Integer(30)], &snapshot).await.unwrap();
    storage.insert_row("users", vec![Value::Integer(2), Value::Integer(25)], &snapshot).await.unwrap();
    storage.insert_row("users", vec![Value::Integer(3), Value::Integer(25)], &snapshot).await.unwrap();
    
    // Scan index
    let rows = storage.scan_index("users", "age", &Value::Integer(25), &None, &IndexOp::Eq, &snapshot).await.unwrap().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[1][0], Value::Integer(3));
    
    // Scan non-existent value
    let rows = storage.scan_index("users", "age", &Value::Integer(99), &None, &IndexOp::Eq, &snapshot).await.unwrap().unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn test_index_visibility_after_update() {
    use rustmemodb::storage::InMemoryStorage;
    use rustmemodb::storage::TableSchema;
    use rustmemodb::core::{Column, DataType};
    use rustmemodb::planner::logical_plan::IndexOp;
    use std::sync::Arc;

    let mut storage = InMemoryStorage::new();
    let schema = TableSchema::new("users", vec![
        Column::new("id", DataType::Integer),
        Column::new("age", DataType::Integer),
    ]);
    storage.create_table(schema).await.unwrap();
    storage.create_index("users", "age").await.unwrap();

    let snapshot_tx1 = Snapshot {
        tx_id: 1,
        active: Arc::new(HashSet::new()),
        aborted: Arc::new(HashSet::new()),
        max_tx_id: 2,
    };
    let snapshot_tx2 = Snapshot {
        tx_id: 2,
        active: Arc::new(HashSet::new()),
        aborted: Arc::new(HashSet::new()),
        max_tx_id: 3,
    };

    storage.insert_row("users", vec![Value::Integer(1), Value::Integer(25)], &snapshot_tx1).await.unwrap();

    let rows = storage.scan_table_with_ids("users", &snapshot_tx1).await.unwrap();
    let row_id = rows.first().unwrap().0;

    storage.update_row("users", row_id, vec![Value::Integer(1), Value::Integer(30)], &snapshot_tx2).await.unwrap();

    let rows_new = storage
        .scan_index("users", "age", &Value::Integer(30), &None, &IndexOp::Eq, &snapshot_tx2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows_new.len(), 1);
    assert_eq!(rows_new[0][1], Value::Integer(30));

    let rows_old = storage
        .scan_index("users", "age", &Value::Integer(25), &None, &IndexOp::Eq, &snapshot_tx2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows_old.len(), 0);

    let rows_old_snapshot = storage
        .scan_index("users", "age", &Value::Integer(25), &None, &IndexOp::Eq, &snapshot_tx1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows_old_snapshot.len(), 1);
    assert_eq!(rows_old_snapshot[0][1], Value::Integer(25));

    let rows_new_snapshot = storage
        .scan_index("users", "age", &Value::Integer(30), &None, &IndexOp::Eq, &snapshot_tx1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rows_new_snapshot.len(), 0);
}

#[tokio::test]
async fn test_index_performance_comparison() {
    use std::time::Instant;

    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE large_table (id INTEGER, val INTEGER)").await.unwrap();

    // Insert 5000 rows
    println!("Generating data...");
    for i in 0..5000 {
        let val = i % 100; // Values 0-99 repeated
        let sql = format!("INSERT INTO large_table VALUES ({}, {})", i, val);
        db.execute(&sql).await.unwrap();
    }
    println!("Data generation complete.");

    // Query BEFORE index
    let start_no_index = Instant::now();
    let result_no_index = db.execute("SELECT * FROM large_table WHERE val = 42").await.unwrap();
    let duration_no_index = start_no_index.elapsed();
    
    assert!(result_no_index.row_count() > 0);
    println!("Time without index: {:?}", duration_no_index);

    // Create Index
    println!("Creating index...");
    let start_create_index = Instant::now();
    db.execute("CREATE INDEX idx_val ON large_table (val)").await.unwrap();
    println!("Index creation took: {:?}", start_create_index.elapsed());

    // Query AFTER index
    let start_with_index = Instant::now();
    let result_with_index = db.execute("SELECT * FROM large_table WHERE val = 42").await.unwrap();
    let duration_with_index = start_with_index.elapsed();
    
    assert_eq!(result_with_index.row_count(), result_no_index.row_count());
    println!("Time with index:    {:?}", duration_with_index);

    if duration_with_index < duration_no_index {
        println!("SUCCESS: Indexing improved performance by {:.2}x", 
            duration_no_index.as_secs_f64() / duration_with_index.as_secs_f64());
    } else {
        println!("WARNING: Indexing did not improve performance (dataset might be too small)");
    }
}

#[tokio::test]
async fn test_create_index_if_not_exists_and_unique() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE users (id INTEGER, name TEXT)").await.unwrap();
    db.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob')").await.unwrap();

    db.execute("CREATE UNIQUE INDEX idx_users_id ON users (id)").await.unwrap();
    db.execute("CREATE INDEX IF NOT EXISTS idx_users_id ON users (id)").await.unwrap();

    let err = db.execute("INSERT INTO users VALUES (1, 'Charlie')").await.unwrap_err();
    assert!(err.to_string().contains("Unique constraint violation"));
}
