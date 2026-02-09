use rustmemodb::{Client, InMemoryDB, DurabilityMode, ConnectionConfig, ConnectionPool};
use rustmemodb::core::{DbError, Value, Result as DbResult};
use rustmemodb::server::PostgresServer;
use rustmemodb::connection::auth::AuthManager;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::RwLock;
use tokio::time::{timeout, Duration};

fn set_env(key: &str, value: &str) {
    unsafe { std::env::set_var(key, value) };
}

fn remove_env(key: &str) {
    unsafe { std::env::remove_var(key) };
}

async fn reserve_port() -> Option<u16> {
    match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => {
            let port = listener.local_addr().ok()?.port();
            drop(listener);
            Some(port)
        }
        Err(_) => None,
    }
}

async fn start_pg_server(port: u16) {
    let db = Arc::new(RwLock::new(InMemoryDB::new()));
    let server = PostgresServer::new(db, "127.0.0.1", port);
    tokio::spawn(async move {
        let _ = server.run().await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn fail_02_drop_connection_should_rollback_and_release_unique() {
    let client = Client::connect_local("admin", "adminpass").await.unwrap();
    client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").await.unwrap();

    {
        let mut conn = client.get_connection().await.unwrap();
        conn.begin().await.unwrap();
        conn.execute("INSERT INTO t VALUES (1)").await.unwrap();
        // Drop without rollback/close to simulate disconnect.
    }
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Expected: insert should succeed because the previous txn was rolled back on drop.
    let result = client.execute("INSERT INTO t VALUES (1)").await;
    assert!(result.is_ok(), "expected rollback on drop to release unique key");
}

#[tokio::test]
async fn fail_03_lost_update_should_be_detected() {
    let client = Client::connect_local("admin", "adminpass").await.unwrap();
    client.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await.unwrap();
    client.execute("INSERT INTO t VALUES (1, 100)").await.unwrap();

    let mut c1 = client.get_connection().await.unwrap();
    let mut c2 = client.get_connection().await.unwrap();

    c1.begin().await.unwrap();
    c2.begin().await.unwrap();

    c1.execute("UPDATE t SET v = 110 WHERE id = 1").await.unwrap();
    c2.execute("UPDATE t SET v = 120 WHERE id = 1").await.unwrap();

    c1.commit().await.unwrap();
    let res = c2.commit().await;

    assert!(res.is_err(), "expected write-write conflict on second commit");
}

#[tokio::test]
async fn fail_04_null_boolean_semantics() {
    let mut db = InMemoryDB::new();
    let result = db.execute("SELECT 1 WHERE NULL OR TRUE").await.unwrap();
    assert_eq!(result.row_count(), 1, "NULL OR TRUE should evaluate to TRUE");
}

#[tokio::test]
async fn fail_05_unique_with_aborted_tx_should_succeed() {
    let client = Client::connect_local("admin", "adminpass").await.unwrap();
    client.execute("CREATE TABLE t (id INTEGER UNIQUE)").await.unwrap();

    let mut c1 = client.get_connection().await.unwrap();
    let mut c2 = client.get_connection().await.unwrap();

    c1.begin().await.unwrap();
    c2.begin().await.unwrap();

    c1.execute("INSERT INTO t VALUES (1)").await.unwrap();
    let res2 = c2.execute("INSERT INTO t VALUES (1)").await;
    assert!(res2.is_ok(), "second insert should wait or succeed if first tx aborts");

    c1.rollback().await.unwrap();
    c2.commit().await.unwrap();

    let result = client.query("SELECT * FROM t").await.unwrap();
    assert_eq!(result.row_count(), 1);
}

#[tokio::test]
async fn fail_06_table_stats_should_reflect_visible_rows() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER)").await.unwrap();
    db.execute("INSERT INTO t VALUES (1)").await.unwrap();
    db.execute("DELETE FROM t WHERE id = 1").await.unwrap();

    let stats = db.table_stats("t").await.unwrap();
    assert_eq!(stats.row_count, 0, "row_count should exclude deleted rows");
}

#[tokio::test]
async fn fail_07_fk_update_should_be_enforced() {
    let client = Client::connect_local("admin", "adminpass").await.unwrap();
    client.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)").await.unwrap();
    client.execute("CREATE TABLE child (id INTEGER, parent_id INTEGER REFERENCES parent(id))").await.unwrap();
    client.execute("INSERT INTO parent VALUES (1)").await.unwrap();
    client.execute("INSERT INTO child VALUES (10, 1)").await.unwrap();

    let res = client.execute("UPDATE child SET parent_id = 999 WHERE id = 10").await;
    assert!(res.is_err(), "updating FK to missing parent should fail");
}

#[tokio::test]
async fn fail_08_alter_add_column_not_null_should_be_enforced() {
    let client = Client::connect_local("admin", "adminpass").await.unwrap();
    client.execute("CREATE TABLE t (id INTEGER)").await.unwrap();
    client.execute("ALTER TABLE t ADD COLUMN v INTEGER NOT NULL").await.unwrap();

    let res = client.execute("INSERT INTO t (id) VALUES (1)").await;
    assert!(res.is_err(), "NOT NULL should reject implicit NULL on added column");
}

#[tokio::test]
async fn fail_09_create_table_default_should_apply() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER, v INTEGER DEFAULT 7)").await.unwrap();
    db.execute("INSERT INTO t (id) VALUES (1)").await.unwrap();

    let res = db.execute("SELECT v FROM t WHERE id = 1").await.unwrap();
    assert_eq!(res.rows()[0][0], Value::Integer(7), "DEFAULT value should be applied");
}

#[tokio::test]
async fn fail_10_prepared_statement_timestamp_param() {
    let client = Client::connect_local("admin", "adminpass").await.unwrap();
    client.execute("CREATE TABLE t (id INTEGER, ts TIMESTAMP)").await.unwrap();

    let mut conn = client.get_connection().await.unwrap();
    let stmt = conn.connection().prepare("INSERT INTO t VALUES ($1, $2)").unwrap();
    let now = chrono::Utc::now();
    stmt.execute(&[&1, &now]).await.unwrap();

    let res = conn.execute("SELECT ts FROM t WHERE id = 1").await.unwrap();
    assert!(matches!(res.rows()[0][0], Value::Timestamp(_)), "expected TIMESTAMP value");
}

#[tokio::test]
async fn fail_12_explain_should_use_index_with_parameter() {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER)").await.unwrap();
    db.execute("CREATE INDEX idx_id ON t (id)").await.unwrap();

    let explain = db.execute("EXPLAIN SELECT * FROM t WHERE id = $1").await.unwrap();
    let plan: String = explain.rows().iter()
        .map(|r| r[0].as_str().unwrap())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(plan.contains("index_scan: Some"), "expected index scan for parameterized predicate");
}

#[tokio::test]
async fn fail_13_write_lock_should_not_block_unrelated_read() {
    set_env("RUSTMEMODB_TEST_SLOW_WRITE_MS", "200");
    let db = Arc::new(RwLock::new(InMemoryDB::new()));
    {
        let mut db_guard = db.write().await;
        db_guard.execute("CREATE TABLE a (id INTEGER)").await.unwrap();
        db_guard.execute("CREATE TABLE b (id INTEGER)").await.unwrap();
    }

    let config = ConnectionConfig::new("admin", "adminpass");
    let pool = Arc::new(ConnectionPool::new_with_db(config, db.clone()).await.unwrap());
    let writer_pool = Arc::clone(&pool);
    let writer = tokio::spawn(async move {
        let mut conn = writer_pool.get_connection().await.unwrap();
        conn.execute("INSERT INTO a VALUES (1)").await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    let reader = timeout(Duration::from_millis(50), async {
        let mut conn = pool.get_connection().await.unwrap();
        conn.execute("SELECT * FROM b").await.unwrap();
    });

    let res = reader.await;
    let _ = writer.await;
    remove_env("RUSTMEMODB_TEST_SLOW_WRITE_MS");

    assert!(res.is_ok(), "read should not be blocked by unrelated write");
}

#[tokio::test]
async fn fail_14_large_join_should_fail_gracefully() {
    set_env("RUSTMEMODB_JOIN_ROW_LIMIT", "1000");
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE a (id INTEGER)").await.unwrap();
    db.execute("CREATE TABLE b (id INTEGER)").await.unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO a VALUES ({})", i)).await.unwrap();
        db.execute(&format!("INSERT INTO b VALUES ({})", i)).await.unwrap();
    }

    let res = db.execute("SELECT * FROM a JOIN b ON a.id = b.id").await;
    assert!(res.is_err(), "expected join to enforce memory limits");
    remove_env("RUSTMEMODB_JOIN_ROW_LIMIT");
}

#[tokio::test]
async fn fail_15_auto_vacuum_should_clean_versions() {
    set_env("RUSTMEMODB_AUTOVAC_THRESHOLD", "1");
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (id INTEGER, v INTEGER)").await.unwrap();
    db.execute("INSERT INTO t VALUES (1, 1)").await.unwrap();
    for i in 0..10 {
        db.execute(&format!("UPDATE t SET v = {} WHERE id = 1", i)).await.unwrap();
    }

    let freed = db.vacuum().await.unwrap();
    assert_eq!(freed, 0, "expected auto-vacuum to have cleaned old versions");
    remove_env("RUSTMEMODB_AUTOVAC_THRESHOLD");
}

#[tokio::test]
async fn fail_16_default_admin_should_be_disabled() {
    set_env("RUSTMEMODB_DISABLE_DEFAULT_ADMIN", "1");
    let auth = AuthManager::new();
    let res = auth.authenticate("admin", "adminpass").await;
    assert!(res.is_err(), "default admin credentials should not be accepted");
    remove_env("RUSTMEMODB_DISABLE_DEFAULT_ADMIN");
}

#[tokio::test]
async fn fail_17_ssl_should_be_supported() {
    let Some(port) = reserve_port().await else { return; };
    set_env("RUSTMEMODB_SSL_TEST_ACCEPT", "1");
    start_pg_server(port).await;

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // SSLRequest: length=8, code=80877103
    let mut buf = [0u8; 8];
    buf[0..4].copy_from_slice(&8u32.to_be_bytes());
    buf[4..8].copy_from_slice(&80877103u32.to_be_bytes());
    stream.write_all(&buf).await.unwrap();

    let mut resp = [0u8; 1];
    stream.read_exact(&mut resp).await.unwrap();

    assert_eq!(resp[0], b'S', "server should accept SSL");
    remove_env("RUSTMEMODB_SSL_TEST_ACCEPT");
}

#[tokio::test]
async fn fail_18_users_should_persist_across_restart() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let auth_path = temp_dir.path().join("users.json");
    set_env("RUSTMEMODB_AUTH_FILE", auth_path.to_str().unwrap());

    let auth = AuthManager::new();
    auth.create_user("persist_user", "persistpass", vec![]).await.unwrap();

    // Simulate restart by constructing a new manager.
    let auth_after = AuthManager::new();
    let res = auth_after.authenticate("persist_user", "persistpass").await;
    assert!(res.is_ok(), "user should persist across restart");
    remove_env("RUSTMEMODB_AUTH_FILE");
}

#[tokio::test]
async fn fail_19_check_constraint_should_be_enforced() -> DbResult<()> {
    let mut db = InMemoryDB::new();
    db.execute("CREATE TABLE t (age INTEGER CHECK (age > 0))").await?;
    let res = db.execute("INSERT INTO t VALUES (-1)").await;
    let err = res.expect_err("CHECK constraint should reject invalid values");
    assert!(
        matches!(err, DbError::ConstraintViolation(_)) && err.to_string().contains("CHECK"),
        "expected CHECK constraint violation, got: {err:?}"
    );
    Ok(())
}

#[tokio::test]
async fn fail_20_conflict_should_survive_wal_recovery() {
    let temp_dir = tempfile::TempDir::new().unwrap();
    let db = Arc::new(RwLock::new(InMemoryDB::new()));
    {
        let mut db_guard = db.write().await;
        db_guard.enable_persistence(temp_dir.path(), DurabilityMode::Sync).await.unwrap();
    }

    let config = ConnectionConfig::new("admin", "adminpass");
    let pool = ConnectionPool::new_with_db(config, db.clone()).await.unwrap();
    let mut setup = pool.get_connection().await.unwrap();
    setup.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await.unwrap();
    setup.execute("INSERT INTO t VALUES (1, 1)").await.unwrap();

    let mut c1 = pool.get_connection().await.unwrap();
    let mut c2 = pool.get_connection().await.unwrap();

    c1.begin().await.unwrap();
    c2.begin().await.unwrap();
    c1.execute("UPDATE t SET v = 2 WHERE id = 1").await.unwrap();
    c2.execute("UPDATE t SET v = 3 WHERE id = 1").await.unwrap();
    c1.commit().await.unwrap();
    let res = c2.commit().await;
    assert!(res.is_err(), "conflicting commit should fail");

    drop(c1);
    drop(c2);
    drop(setup);
    drop(pool);
    drop(db);

    let db_after = Arc::new(RwLock::new(InMemoryDB::new()));
    {
        let mut db_guard = db_after.write().await;
        db_guard.enable_persistence(temp_dir.path(), DurabilityMode::Sync).await.unwrap();
    }
    let config_after = ConnectionConfig::new("admin", "adminpass");
    let pool_after = ConnectionPool::new_with_db(config_after, db_after).await.unwrap();
    let mut conn = pool_after.get_connection().await.unwrap();
    let res = conn.execute("SELECT v FROM t WHERE id = 1").await.unwrap();

    assert_eq!(res.rows()[0][0], Value::Integer(2), "recovery should reject conflicting commit");
}

#[tokio::test]
async fn fail_21_metrics_should_be_queryable() {
    let mut db = InMemoryDB::new();
    let res = db.execute("SELECT * FROM system_metrics").await;
    assert!(res.is_ok(), "expected a system metrics table or view for monitoring");
}
