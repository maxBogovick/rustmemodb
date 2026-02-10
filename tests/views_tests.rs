use rustmemodb::Client;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_views_basic() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client
        .execute("CREATE TABLE users (id INT, name TEXT)")
        .await?;
    client
        .execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .await?;

    // Create View
    client
        .execute("CREATE VIEW users_v AS SELECT id, name FROM users")
        .await?;

    // Select from View
    let res = client.query("SELECT * FROM users_v ORDER BY id").await?;
    assert_eq!(res.row_count(), 2);
    assert_eq!(res.rows()[0][1].as_str().unwrap(), "Alice");

    Ok(())
}

#[tokio::test]
async fn test_views_complex() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client.execute("CREATE TABLE t1 (a INT, b INT)").await?;
    client.execute("INSERT INTO t1 VALUES (10, 20)").await?;

    // View with alias and calculation
    client
        .execute("CREATE VIEW v1 AS SELECT a, b, a+b as sum_ab FROM t1")
        .await?;

    let res = client.query("SELECT sum_ab FROM v1 WHERE a = 10").await?;
    assert_eq!(res.row_count(), 1);

    match &res.rows()[0][0] {
        Value::Integer(i) => assert_eq!(*i, 30),
        _ => panic!("Expected integer 30"),
    }

    // Check replacement
    client
        .execute("CREATE OR REPLACE VIEW v1 AS SELECT a FROM t1")
        .await?;
    let res = client.query("SELECT * FROM v1").await?;
    assert_eq!(res.columns().len(), 1);

    // Drop view
    client.execute("DROP VIEW v1").await?;
    assert!(client.query("SELECT * FROM v1").await.is_err());

    Ok(())
}
