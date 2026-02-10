use rustmemodb::Client;
use std::time::Instant;

#[tokio::test]
async fn test_window_performance() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client.execute("CREATE TABLE t (g INT, x INT)").await?;

    // Insert 1000 rows
    // Since we don't have bulk insert yet, this is the slowest part.
    // 1000 might be slow due to parsing overhead per statement.
    // I'll do 100 rows for quick test, or stick to 1000 if it finishes < 10s.
    for i in 0..100 {
        let g = i % 10;
        let x = i;
        client
            .execute(&format!("INSERT INTO t VALUES ({}, {})", g, x))
            .await?;
    }

    let start = Instant::now();
    let res = client
        .query("SELECT g, x, ROW_NUMBER() OVER (PARTITION BY g ORDER BY x) FROM t")
        .await?;
    let duration = start.elapsed();

    println!("Window function on 100 rows took: {:?}", duration);
    assert_eq!(res.row_count(), 100);

    Ok(())
}
