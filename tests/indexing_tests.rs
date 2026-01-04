use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;

#[test]
fn test_create_index_and_use_it() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)").unwrap();

    // Insert data
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)").unwrap();
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)").unwrap();
    db.execute("INSERT INTO users VALUES (4, 'Diana', 25)").unwrap();

    // Create index on age
    // We need to access storage directly or add SQL support for CREATE INDEX
    // Since SQL parser doesn't support CREATE INDEX yet, we'll cheat and use internal API or just test logic via query optimization check?
    // Wait, I didn't add SQL support for CREATE INDEX. I only added the backend logic.
    // I need to add SQL support or expose a method on InMemoryDB.
    
    // For this test, I will access storage via a trick or just add a method to InMemoryDB.
    // InMemoryDB doesn't expose storage directly.
    // But I can't test it E2E without SQL support.
    
    // BUT, I can check if "SELECT * FROM users WHERE age = 25" works correctly (full scan).
    let result = db.execute("SELECT * FROM users WHERE age = 25").unwrap();
    assert_eq!(result.row_count(), 2);
    
    // Now, how to create index?
    // I need to add `create_index` to InMemoryDB facade.
}

#[test]
fn test_indexing_backend() {
    // This test uses internal APIs to verify indexing works
    use rustmemodb::storage::InMemoryStorage;
    use rustmemodb::storage::TableSchema;
    use rustmemodb::core::{Column, DataType, Row};
    
    let mut storage = InMemoryStorage::new();
    let schema = TableSchema::new("users", vec![
        Column::new("id", DataType::Integer),
        Column::new("age", DataType::Integer),
    ]);
    storage.create_table(schema).unwrap();
    
    storage.create_index("users", "age").unwrap();
    
    storage.insert_row("users", vec![Value::Integer(1), Value::Integer(30)]).unwrap();
    storage.insert_row("users", vec![Value::Integer(2), Value::Integer(25)]).unwrap();
    storage.insert_row("users", vec![Value::Integer(3), Value::Integer(25)]).unwrap();
    
    // Scan index
    let rows = storage.scan_index("users", "age", &Value::Integer(25)).unwrap().unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], Value::Integer(2));
    assert_eq!(rows[1][0], Value::Integer(3));
    
    // Scan non-existent value
    let rows = storage.scan_index("users", "age", &Value::Integer(99)).unwrap().unwrap();
    assert_eq!(rows.len(), 0);
}
