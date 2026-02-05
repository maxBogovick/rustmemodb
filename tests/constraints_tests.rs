use rustmemodb::InMemoryDB;

#[tokio::test]
async fn test_primary_key_constraint() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();

    // 1. Insert valid data
    db.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
    
    // 2. Insert duplicate PK -> Should fail
    let result = db.execute("INSERT INTO users VALUES (1, 'Bob')").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Unique constraint violation"));

    // 3. Insert NULL PK -> Should fail (Parse error or Not Null violation)
    // Note: NULL in SQL integer literal is just NULL.
    let result = db.execute("INSERT INTO users VALUES (NULL, 'Charlie')").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cannot be NULL"));
}

#[tokio::test]
async fn test_unique_constraint() {
    let mut db = InMemoryDB::new();

    db.execute("CREATE TABLE emails (id INTEGER, email TEXT UNIQUE)").await.unwrap();

    db.execute("INSERT INTO emails VALUES (1, 'a@example.com')").await.unwrap();
    
    // Duplicate email -> Fail
    let result = db.execute("INSERT INTO emails VALUES (2, 'a@example.com')").await;
    assert!(result.is_err());
    
    // Different email -> OK
    db.execute("INSERT INTO emails VALUES (3, 'b@example.com')").await.unwrap();

    // Multiple NULLs allowed in UNIQUE (standard SQL behavior)
    db.execute("INSERT INTO emails VALUES (4, NULL)").await.unwrap();
    db.execute("INSERT INTO emails VALUES (5, NULL)").await.unwrap();
}

#[tokio::test]
async fn test_update_unique_constraint() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT UNIQUE)").await.unwrap();
    
    db.execute("INSERT INTO items VALUES (1, 'A')").await.unwrap();
    db.execute("INSERT INTO items VALUES (2, 'B')").await.unwrap();

    // Update to conflict with another row -> Fail
    let result = db.execute("UPDATE items SET code = 'A' WHERE id = 2").await;
    assert!(result.is_err());

    // Update to same value (no change) -> OK
    db.execute("UPDATE items SET code = 'A' WHERE id = 1").await.unwrap();
    
    // Update to new unique value -> OK
    db.execute("UPDATE items SET code = 'C' WHERE id = 2").await.unwrap();
}

#[tokio::test]
async fn test_constraint_with_index() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE indexed (val INTEGER UNIQUE)").await.unwrap();
    
    // Create index (should speed up checks)
    db.execute("CREATE INDEX IF NOT EXISTS idx_val ON indexed (val)").await.unwrap();

    db.execute("INSERT INTO indexed VALUES (10)").await.unwrap();
    
    // Duplicate with index -> Fail
    let result = db.execute("INSERT INTO indexed VALUES (10)").await;
    assert!(result.is_err());
}
