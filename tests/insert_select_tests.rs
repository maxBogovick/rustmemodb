use rustmemodb::Client;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_insert_select() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client
        .execute("CREATE TABLE source (id INT, val TEXT)")
        .await?;
    client
        .execute("INSERT INTO source VALUES (1, 'a'), (2, 'b')")
        .await?;

    client
        .execute("CREATE TABLE dest (id INT, val TEXT)")
        .await?;

    // Insert Select
    client
        .execute("INSERT INTO dest (id, val) SELECT id, val FROM source WHERE id > 1")
        .await?;

    let res = client.query("SELECT * FROM dest").await?;
    assert_eq!(res.row_count(), 1);
    match &res.rows()[0][1] {
        Value::Text(s) => assert_eq!(s, "b"),
        _ => panic!("Expected text"),
    }

    Ok(())
}
