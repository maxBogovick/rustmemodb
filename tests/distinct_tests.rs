use rustmemodb::Client;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_distinct_select() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client.execute("CREATE TABLE t (val INT)").await?;
    client.execute("INSERT INTO t VALUES (1), (1), (2), (3), (3), (3)").await?;

    // DISTINCT SELECT
    let res = client.query("SELECT DISTINCT val FROM t").await?;
    assert_eq!(res.row_count(), 3);
    
    // COUNT DISTINCT
    let res = client.query("SELECT COUNT(DISTINCT val) FROM t").await?;
    match &res.rows()[0][0] {
        Value::Integer(i) => assert_eq!(*i, 3),
        _ => panic!("Expected 3"),
    }

    // SUM DISTINCT (1+2+3 = 6)
    let res = client.query("SELECT SUM(DISTINCT val) FROM t").await?;
    match &res.rows()[0][0] {
        Value::Integer(i) => assert_eq!(*i, 6),
        _ => panic!("Expected 6"),
    }

    Ok(())
}
