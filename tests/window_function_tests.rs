use rustmemodb::Client;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_row_number() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client.execute("CREATE TABLE t (g TEXT, x INT)").await?;
    client
        .execute("INSERT INTO t VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40), ('B', 50)")
        .await?;

    let res = client
        .query(
            "
        SELECT g, x, ROW_NUMBER() OVER (PARTITION BY g ORDER BY x) as rn
        FROM t
        ORDER BY g, x
    ",
        )
        .await?;

    assert_eq!(res.row_count(), 5);

    // Check first row (A, 10, 1)
    match &res.rows()[0][2] {
        Value::Integer(i) => assert_eq!(*i, 1),
        _ => panic!("Expected 1"),
    }
    // Check third row (B, 30, 1)
    match &res.rows()[2][2] {
        Value::Integer(i) => assert_eq!(*i, 1),
        _ => panic!("Expected 1"),
    }

    Ok(())
}

#[tokio::test]
async fn test_rank() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client.execute("CREATE TABLE t (val INT)").await?;
    client
        .execute("INSERT INTO t VALUES (10), (20), (20), (30)")
        .await?;

    let res = client
        .query(
            "
        SELECT val, RANK() OVER (ORDER BY val) as rk
        FROM t
    ",
        )
        .await?;

    let rows = res.rows();

    assert_eq!(rows[0][1], Value::Integer(1));
    assert_eq!(rows[1][1], Value::Integer(2));
    assert_eq!(rows[2][1], Value::Integer(2));
    assert_eq!(rows[3][1], Value::Integer(4));

    Ok(())
}
