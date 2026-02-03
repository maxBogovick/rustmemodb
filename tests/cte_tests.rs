use rustmemodb::Client;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_cte_simple() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;
    
    client.execute("CREATE TABLE users (id INT, name TEXT)").await?;
    client.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')").await?;
    
    let res = client.query("
        WITH cte AS (SELECT * FROM users WHERE id = 1)
        SELECT name FROM cte
    ").await?;
    
    assert_eq!(res.row_count(), 1);
    assert_eq!(res.rows()[0][0].as_str().unwrap(), "Alice");
    
    Ok(())
}

#[tokio::test]
async fn test_cte_chained() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;
    
    client.execute("CREATE TABLE t (x INT)").await?;
    client.execute("INSERT INTO t VALUES (1)").await?;
    
    let res = client.query("
        WITH 
            a AS (SELECT x+1 as val FROM t),
            b AS (SELECT val*2 as val FROM a)
        SELECT val FROM b
    ").await?;
    
    // 1 + 1 = 2. 2 * 2 = 4.
    assert_eq!(res.row_count(), 1);
    match &res.rows()[0][0] {
        Value::Integer(i) => assert_eq!(*i, 4),
        _ => panic!("Expected 4"),
    }
    
    Ok(())
}
