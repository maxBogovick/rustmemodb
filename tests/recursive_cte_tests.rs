use rustmemodb::Client;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_recursive_sequence() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    // Generate sequence 1..5
    let res = client
        .query(
            "
        WITH RECURSIVE t(n) AS (
            SELECT 1
            UNION ALL
            SELECT n+1 FROM t WHERE n < 5
        )
        SELECT * FROM t
    ",
        )
        .await?;

    assert_eq!(res.row_count(), 5);
    // Check last value (5)
    // Order is not guaranteed by recursive union usually, but my implementation appends in order.
    // So 1, 2, 3, 4, 5.
    match &res.rows()[4][0] {
        Value::Integer(i) => assert_eq!(*i, 5),
        _ => panic!("Expected 5"),
    }

    Ok(())
}

#[tokio::test]
async fn test_recursive_hierarchy() -> anyhow::Result<()> {
    let client = Client::connect_local("admin", "adminpass").await?;

    client
        .execute("CREATE TABLE employees (id INT, name TEXT, manager_id INT)")
        .await?;
    client
        .execute("INSERT INTO employees VALUES (1, 'CEO', NULL)")
        .await?;
    client
        .execute("INSERT INTO employees VALUES (2, 'Manager1', 1)")
        .await?;
    client
        .execute("INSERT INTO employees VALUES (3, 'Worker1', 2)")
        .await?;
    client
        .execute("INSERT INTO employees VALUES (4, 'Manager2', 1)")
        .await?;

    // Find all subordinates of CEO (id=1)
    let res = client
        .query(
            "
        WITH RECURSIVE subordinates AS (
            SELECT id, name, manager_id FROM employees WHERE id = 1
            UNION ALL
            SELECT e.id, e.name, e.manager_id 
            FROM employees e
            INNER JOIN subordinates s ON e.manager_id = s.id
        )
        SELECT * FROM subordinates
    ",
        )
        .await?;

    // Should return all 4 employees
    assert_eq!(res.row_count(), 4);

    Ok(())
}
