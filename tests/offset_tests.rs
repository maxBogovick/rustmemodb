use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_limit_offset() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER)").await.unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({})", i))
            .await
            .unwrap();
    }

    let res = db
        .execute("SELECT id FROM t ORDER BY id LIMIT 2 OFFSET 1")
        .await
        .unwrap();
    assert_eq!(res.row_count(), 2);
    assert_eq!(res.rows()[0][0], Value::Integer(2));
    assert_eq!(res.rows()[1][0], Value::Integer(3));
}

#[tokio::test]
async fn test_offset_without_limit() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER)").await.unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({})", i))
            .await
            .unwrap();
    }

    let res = db
        .execute("SELECT id FROM t ORDER BY id OFFSET 3")
        .await
        .unwrap();
    assert_eq!(res.row_count(), 2);
    assert_eq!(res.rows()[0][0], Value::Integer(4));
    assert_eq!(res.rows()[1][0], Value::Integer(5));
}
