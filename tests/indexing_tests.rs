use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_create_index_and_use_it() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").await.unwrap();

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
    
    let mut storage = InMemoryStorage::new();
    let schema = TableSchema::new("users", vec![
        Column::new("id", DataType::Integer),
        Column::new("age", DataType::Integer),
    ]);
    storage.create_table(schema).await.unwrap();
    
    storage.create_index("users", "age").await.unwrap();
    
    storage.insert_row("users", vec![Value::Integer(1), Value::Integer(30)]).await.unwrap();
    storage.insert_row("users", vec![Value::Integer(2), Value::Integer(25)]).await.unwrap();
    storage.insert_row("users", vec![Value::Integer(3), Value::Integer(25)]).await.unwrap();
    
    // Scan index
    let rows = storage.scan_index("users", "age", &Value::Integer(25)).await.unwrap().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[1][0], Value::Integer(3));
    
    // Scan non-existent value
    let rows = storage.scan_index("users", "age", &Value::Integer(99)).await.unwrap().unwrap();
    assert_eq!(rows.len(), 0);
}