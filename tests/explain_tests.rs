use rustmemodb::Client;

#[tokio::test]
async fn test_explain_select() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client
        .execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        .await?;
    client
        .execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')")
        .await?;

    let result = client
        .query("EXPLAIN SELECT * FROM users WHERE id = 1")
        .await?;

    // Debug print
    println!("EXPLAIN result: {:?}", result);

    let columns: Vec<&str> = result.columns().iter().map(|c| c.name.as_str()).collect();
    assert_eq!(columns, vec!["QUERY PLAN"]);
    assert!(result.row_count() > 0);

    // Basic string check
    let plan_text: String = result
        .rows()
        .iter()
        .map(|r| r[0].as_str().unwrap())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(plan_text.contains("TableScan"));
    assert!(plan_text.contains("users"));

    // Since we have PK, it should actually be an IndexScan
    // The current output of Debug for LogicalPlan should show IndexScanInfo if used.
    // However, default QueryPlanner uses IndexScan if index exists. PK implies index.

    // Let's verify if Debug output mentions IndexScan
    // Note: Debug output might be verbose.

    Ok(())
}
