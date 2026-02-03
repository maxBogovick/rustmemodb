use rustmemodb::InMemoryDB;

#[tokio::test]
async fn test_in_list_basic() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").await.unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").await.unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").await.unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").await.unwrap();

    let res = db.execute("SELECT * FROM t WHERE v IN (10, 30)").await.unwrap();
    assert_eq!(res.row_count(), 2);
}

#[tokio::test]
async fn test_not_in_list() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").await.unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").await.unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").await.unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").await.unwrap();

    let res = db.execute("SELECT * FROM t WHERE v NOT IN (10, 30)").await.unwrap();
    assert_eq!(res.row_count(), 1);
}

#[tokio::test]
async fn test_in_list_null_semantics() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").await.unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").await.unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").await.unwrap();

    let res_match = db.execute("SELECT * FROM t WHERE v IN (NULL, 10)").await.unwrap();
    assert_eq!(res_match.row_count(), 1);

    let res_no_match = db.execute("SELECT * FROM t WHERE v IN (NULL, 30)").await.unwrap();
    assert_eq!(res_no_match.row_count(), 0);
}
