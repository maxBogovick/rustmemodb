use rustmemodb::Client;

#[tokio::test]
async fn test_rename_table() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client.execute("CREATE TABLE t1 (id INT)").await?;
    client.execute("INSERT INTO t1 VALUES (1)").await?;

    client.execute("ALTER TABLE t1 RENAME TO t2").await?;

    // Check t2 exists
    let res = client.query("SELECT * FROM t2").await?;
    assert_eq!(res.row_count(), 1);

    // Check t1 gone
    assert!(client.query("SELECT * FROM t1").await.is_err());

    // Rename back
    client.execute("ALTER TABLE t2 RENAME TO t1").await?;
    assert!(client.query("SELECT * FROM t1").await.is_ok());

    Ok(())
}
