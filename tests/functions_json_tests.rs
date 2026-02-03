use rustmemodb::Client;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_functions() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;
    
    // UPPER
    let res = client.query("SELECT UPPER('abc')").await?;
    assert_eq!(res.rows()[0][0].as_str().unwrap(), "ABC");
    
    // LOWER
    let res = client.query("SELECT LOWER('ABC')").await?;
    assert_eq!(res.rows()[0][0].as_str().unwrap(), "abc");
    
    // LENGTH
    let res = client.query("SELECT LENGTH('abc')").await?;
    match &res.rows()[0][0] {
        Value::Integer(i) => assert_eq!(*i, 3),
        _ => panic!("Expected 3"),
    }
    
    // COALESCE
    let res = client.query("SELECT COALESCE(NULL, 'default')").await?;
    assert_eq!(res.rows()[0][0].as_str().unwrap(), "default");
    
    // NOW
    let res = client.query("SELECT NOW()").await?;
    // Check type
    assert!(matches!(res.rows()[0][0], Value::Timestamp(_)));
    
    Ok(())
}

#[tokio::test]
async fn test_json() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;
    
    // -> operator (Get as JSON)
    let res = client.query("SELECT '{\"a\": 1}' -> 'a'").await?;
    // Result should be JSON(1).
    match &res.rows()[0][0] {
        Value::Json(j) => assert_eq!(j.as_i64(), Some(1)),
        _ => panic!("Expected JSON"),
    }
    
    // ->> operator (Get as Text)
    let res = client.query("SELECT '{\"a\": \"hello\"}' ->> 'a'").await?;
    assert_eq!(res.rows()[0][0].as_str().unwrap(), "hello");
    
    // Nested
    let res = client.query("SELECT '{\"a\": {\"b\": 2}}' -> 'a' -> 'b'").await?;
    match &res.rows()[0][0] {
        Value::Json(j) => assert_eq!(j.as_i64(), Some(2)),
        _ => panic!("Expected JSON"),
    }
    
    Ok(())
}
