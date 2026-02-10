use chrono::{NaiveDate, Utc};
use rustmemodb::Client;
use rustmemodb::core::{Result, Value};
use uuid::Uuid;

#[tokio::test]
async fn test_timestamp_type() -> Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client
        .execute("CREATE TABLE logs (id INTEGER, event_time TIMESTAMP, message TEXT)")
        .await?;

    // Use current time
    let now = Utc::now();
    // Insert using string format (standard SQL way)
    let now_str = now.to_rfc3339();

    // Note: Our parser might not support literal timestamps yet without 'TIMESTAMP' keyword or string casting.
    // Let's try inserting as string and see if automatic coercion or parser handles it.
    // Currently `Value::parse_number` is used for literals. `Value::Text` is used for strings.
    // Our `InsertExecutor` calls `evaluate_literal`.
    // `evaluate_literal` calls `expected_type.is_compatible(val)`.
    // `DataType::is_compatible` allows Text -> Timestamp.

    client
        .execute(&format!(
            "INSERT INTO logs VALUES (1, '{}', 'Log 1')",
            now_str
        ))
        .await?;

    let result = client.query("SELECT * FROM logs").await?;
    let row = &result.rows()[0];
    // Check if it's stored as String (Text) or Timestamp?
    // The parser parses literals. If we passed '...', it's a string literal.
    // The executor checks compatibility. It sees Text is compatible with Timestamp.
    // BUT, it returns `Ok(val.clone())` which is `Value::Text`.
    // So storage stores `Value::Text`.
    // This is "SQLite style" typing.
    // Ideally we want strict typing where it converts to `Value::Timestamp`.

    // To support real strict typing, `InsertExecutor` should CAST the value.
    // Currently `InsertExecutor` just checks compatibility.
    // Let's verify what happens.

    match &row[1] {
        Value::Timestamp(t) => assert_eq!(t.to_rfc3339(), now_str),
        _ => panic!("Expected Timestamp, got {:?}", row[1]),
    }

    Ok(())
}

#[tokio::test]
async fn test_uuid_type() -> Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;
    client
        .execute("CREATE TABLE users (id UUID, name TEXT)")
        .await?;

    let uuid = Uuid::new_v4();
    client
        .execute(&format!("INSERT INTO users VALUES ('{}', 'Alice')", uuid))
        .await?;

    let result = client.query("SELECT * FROM users").await?;
    assert_eq!(result.row_count(), 1);

    // Again, checks storage format
    let row = &result.rows()[0];
    if let Value::Uuid(u) = &row[0] {
        assert_eq!(u, &uuid);
    } else {
        panic!("Expected UUID, got {:?}", row[0]);
    }

    Ok(())
}
