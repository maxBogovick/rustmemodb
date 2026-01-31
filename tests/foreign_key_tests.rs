use rustmemodb::core::{Result, Value, DbError};
use rustmemodb::Client;

#[tokio::test]
async fn test_foreign_key_constraint() -> Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    // 1. Create Parent Table
    client.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await?;
    client.execute("INSERT INTO users VALUES (1, 'Alice')").await?;
    client.execute("INSERT INTO users VALUES (2, 'Bob')").await?;

    // 2. Create Child Table
    client.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER REFERENCES users(id), title TEXT)").await?;

    // 3. Valid Insert
    client.execute("INSERT INTO posts VALUES (100, 1, 'Alice Post')").await?;
    client.execute("INSERT INTO posts VALUES (101, 2, 'Bob Post')").await?;

    // 4. Invalid Insert (Non-existent user)
    let res = client.execute("INSERT INTO posts VALUES (102, 999, 'Ghost Post')").await;
    match res {
        Err(DbError::ConstraintViolation(msg)) => {
            assert!(msg.contains("references non-existent key"));
        }
        _ => panic!("Expected ConstraintViolation, got {:?}", res),
    }

    // 5. Invalid Delete (Restricted)
    let res = client.execute("DELETE FROM users WHERE id = 1").await;
    match res {
        Err(DbError::ConstraintViolation(msg)) => {
            assert!(msg.contains("violates foreign key constraint"));
        }
        _ => panic!("Expected ConstraintViolation, got {:?}", res),
    }

    // 6. Valid Delete (No references)
    // First delete child
    client.execute("DELETE FROM posts WHERE user_id = 1").await?;
    // Now parent delete should succeed
    client.execute("DELETE FROM users WHERE id = 1").await?;

    Ok(())
}

#[tokio::test]
async fn test_foreign_key_null_handling() -> Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)").await?;
    client.execute("CREATE TABLE items (id INTEGER, category_id INTEGER REFERENCES categories(id))").await?;

    // Valid Insert with NULL (should skip check)
    client.execute("INSERT INTO items VALUES (1, NULL)").await?;

    Ok(())
}
