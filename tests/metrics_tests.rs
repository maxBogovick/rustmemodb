use rustmemodb::{InMemoryDB, Value};

#[tokio::test]
async fn test_system_query_metrics_records_sql() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER)").await.unwrap();
    db.execute("INSERT INTO t VALUES (1)").await.unwrap();
    db.execute("SELECT * FROM t").await.unwrap();

    let result = db
        .execute("SELECT * FROM system_query_metrics")
        .await
        .unwrap();
    assert!(result.row_count() >= 3);

    let mut seen_query = false;
    for row in result.rows() {
        if let Some(Value::Text(sql)) = row.get(8) {
            if sql.contains("SELECT * FROM t") {
                seen_query = true;
                break;
            }
        }
    }
    assert!(seen_query);
}

#[tokio::test]
async fn test_system_storage_metrics_lists_tables() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE storage_test (id INTEGER)")
        .await
        .unwrap();
    db.execute("INSERT INTO storage_test VALUES (1)")
        .await
        .unwrap();

    let result = db
        .execute("SELECT * FROM system_storage_metrics")
        .await
        .unwrap();
    assert!(result.row_count() >= 1);
    let mut seen = false;
    for row in result.rows() {
        if let Some(Value::Text(name)) = row.get(0) {
            if name == "storage_test" {
                seen = true;
                break;
            }
        }
    }
    assert!(seen);
}

#[tokio::test]
async fn test_system_memory_metrics_has_estimates() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE mem_test (id INTEGER)")
        .await
        .unwrap();
    db.execute("INSERT INTO mem_test VALUES (1)").await.unwrap();

    let result = db
        .execute("SELECT * FROM system_memory_metrics")
        .await
        .unwrap();
    assert!(result.row_count() >= 5);
}
