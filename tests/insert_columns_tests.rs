use rustmemodb::InMemoryDB;
use rustmemodb::core::Value;

#[tokio::test]
async fn test_insert_with_column_list_values() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)")
        .await
        .unwrap();
    db.execute("INSERT INTO t (name, id) VALUES ('Alice', 1)")
        .await
        .unwrap();

    let res = db.execute("SELECT * FROM t WHERE id = 1").await.unwrap();
    assert_eq!(res.row_count(), 1);
    assert_eq!(res.rows()[0][0], Value::Integer(1));
    assert_eq!(res.rows()[0][1], Value::Text("Alice".to_string()));
    assert_eq!(res.rows()[0][2], Value::Null);
}

#[tokio::test]
async fn test_insert_with_column_list_select() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER, name TEXT, age INTEGER)")
        .await
        .unwrap();
    db.execute("INSERT INTO t (name, id) SELECT 'Bob', 2")
        .await
        .unwrap();

    let res = db.execute("SELECT * FROM t WHERE id = 2").await.unwrap();
    assert_eq!(res.row_count(), 1);
    assert_eq!(res.rows()[0][0], Value::Integer(2));
    assert_eq!(res.rows()[0][1], Value::Text("Bob".to_string()));
    assert_eq!(res.rows()[0][2], Value::Null);
}

#[tokio::test]
async fn test_update_casts_to_column_type() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER, v INTEGER)")
        .await
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1)").await.unwrap();
    db.execute("UPDATE t SET v = 1.9 WHERE id = 1")
        .await
        .unwrap();

    let res = db.execute("SELECT v FROM t WHERE id = 1").await.unwrap();
    assert_eq!(res.row_count(), 1);
    assert_eq!(res.rows()[0][0], Value::Integer(1));
}
