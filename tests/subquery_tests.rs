use rustmemodb::Client;
use rustmemodb::core::{Result, Value};

#[tokio::test]
async fn test_scalar_subquery() -> Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client
        .execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .await?;
    client
        .execute("INSERT INTO users VALUES (1, 'Alice')")
        .await?;
    client
        .execute("INSERT INTO users VALUES (2, 'Bob')")
        .await?;

    // Scalar subquery in WHERE
    let result = client
        .query("SELECT * FROM users WHERE id = (SELECT id FROM users WHERE name = 'Alice')")
        .await?;
    assert_eq!(result.row_count(), 1);
    let row = &result.rows()[0];
    assert_eq!(row[1], Value::Text("Alice".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_in_subquery() -> Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client
        .execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .await?;
    client
        .execute("CREATE TABLE orders (id INTEGER, user_id INTEGER)")
        .await?;

    client
        .execute("INSERT INTO users VALUES (1, 'Alice')")
        .await?;
    client
        .execute("INSERT INTO users VALUES (2, 'Bob')")
        .await?;
    client.execute("INSERT INTO orders VALUES (100, 1)").await?;

    // IN subquery
    let result = client
        .query("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")
        .await?;
    assert_eq!(result.row_count(), 1); // Only Alice has orders
    let row = &result.rows()[0];
    assert_eq!(row[1], Value::Text("Alice".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_derived_table() -> Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client
        .execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .await?;
    client
        .execute("INSERT INTO users VALUES (1, 'Alice')")
        .await?;
    client
        .execute("INSERT INTO users VALUES (2, 'Bob')")
        .await?;

    // Derived table (subquery in FROM)
    // Note: sqlparser requires aliases for derived tables usually
    let result = client
        .query("SELECT * FROM (SELECT * FROM users WHERE id > 1) AS t")
        .await?;
    assert_eq!(result.row_count(), 1);
    let row = &result.rows()[0];
    assert_eq!(row[1], Value::Text("Bob".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_exists_subquery() -> Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client
        .execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .await?;
    client
        .execute("INSERT INTO users VALUES (1, 'Alice')")
        .await?;

    // EXISTS
    let result = client
        .query("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM users WHERE id = 1)")
        .await?;
    assert_eq!(result.row_count(), 1);

    let result = client
        .query("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM users WHERE id = 999)")
        .await?;
    assert_eq!(result.row_count(), 0);

    Ok(())
}
