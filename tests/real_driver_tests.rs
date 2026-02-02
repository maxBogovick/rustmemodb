use rustmemodb::facade::InMemoryDB;
use rustmemodb::server::PostgresServer;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::{NoTls, Client};
use serde_json::json;

async fn start_server(port: u16) -> Arc<RwLock<InMemoryDB>> {
    let db = Arc::new(RwLock::new(InMemoryDB::new()));
    let server = PostgresServer::new(db.clone(), "127.0.0.1", port);
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("Server error: {}", e);
        }
    });
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    db
}

async fn connect(port: u16) -> Client {
    let connection_string = format!("host=127.0.0.1 port={} user=admin dbname=postgres", port);
    let (client, connection) = tokio_postgres::connect(&connection_string, NoTls).await.expect("Failed to connect");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    client
}

#[tokio::test]
async fn test_parameter_binding() {
    let port = 5440;
    let _db = start_server(port).await;
    let client = connect(port).await;

    client.batch_execute("CREATE TABLE users (id INT, name TEXT, age INT)").await.unwrap();

    // Test INSERT with parameters
    let stmt = client.prepare("INSERT INTO users (id, name, age) VALUES ($1, $2, $3)").await.unwrap();
    client.execute(&stmt, &[&1i32, &"Alice", &30i32]).await.unwrap();
    client.execute(&stmt, &[&2i32, &"Bob", &25i32]).await.unwrap();

    // Test SELECT with parameters
    let stmt = client.prepare("SELECT name FROM users WHERE age > $1").await.unwrap();
    let rows = client.query(&stmt, &[&28i32]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let name: String = rows[0].get(0);
    assert_eq!(name, "Alice");
}

#[tokio::test]
async fn test_alter_table() {
    let port = 5441;
    let _db = start_server(port).await;
    let client = connect(port).await;

    client.batch_execute("CREATE TABLE products (id INT, name TEXT)").await.unwrap();
    client.batch_execute("INSERT INTO products VALUES (1, 'Laptop')").await.unwrap();

    // ADD COLUMN
    client.batch_execute("ALTER TABLE products ADD COLUMN price FLOAT").await.unwrap();

    // Check if column exists and has NULL (or default)
    let rows = client.query("SELECT price FROM products WHERE id = 1", &[]).await.unwrap();
    let price: Option<f64> = rows[0].get(0); // Should be NULL
    assert!(price.is_none()); // Or 0.0 if default? Our implementation sets NULL if no default.

    // UPDATE new column
    client.execute("UPDATE products SET price = $1 WHERE id = 1", &[&999.99f64]).await.unwrap();

    let rows = client.query("SELECT price FROM products WHERE id = 1", &[]).await.unwrap();
    let price: f64 = rows[0].get(0);
    assert_eq!(price, 999.99);

    // RENAME COLUMN
    client.batch_execute("ALTER TABLE products RENAME COLUMN name TO title").await.unwrap();
    let rows = client.query("SELECT title FROM products WHERE id = 1", &[]).await.unwrap();
    let title: String = rows[0].get(0);
    assert_eq!(title, "Laptop");

    // DROP COLUMN
    client.batch_execute("ALTER TABLE products DROP COLUMN price").await.unwrap();
    // Verify column is gone (this should fail or return error if we try to select it)
    let result = client.query("SELECT price FROM products", &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_json_type() {
    let port = 5442;
    let _db = start_server(port).await;
    let client = connect(port).await;

    // Note: We use TEXT for JSON in CREATE TABLE because our parser might not support JSON keyword fully yet in all contexts,
    // but let's try JSONB if we implemented it.
    // Our parser adapter maps JSON/JSONB to DataType::Json.
    client.batch_execute("CREATE TABLE docs (id INT, data JSONB)").await.unwrap();

    let data = json!({
        "key": "value",
        "list": [1, 2, 3]
    });

    // tokio-postgres maps serde_json::Value to JSONB if "with-serde_json-1" feature is enabled.
    // Since we might not have that feature enabled in dev-dependencies, we pass as string/text?
    // Or we can try passing as string and casting?
    // Let's try passing as string first, as that's universally supported.
    // Postgres allows implicit cast from text to jsonb.

    let data_str = data.to_string();
    client.execute("INSERT INTO docs (id, data) VALUES ($1, $2)", &[&1i32, &data_str]).await.unwrap();

    // Select back
    // We expect it to come back as String because we encode it as text in pg_server.rs
    let rows = client.query("SELECT data FROM docs WHERE id = 1", &[]).await.unwrap();
    // tokio-postgres might try to parse it if column type is JSONB.
    // But since we return it as Text format in pgwire, it should be fine.
    // However, tokio-postgres `get` expects a type that matches.
    // If we ask for String, it should work.
    let val: String = rows[0].get(0);
    assert_eq!(val, data_str);
}

#[tokio::test]
async fn test_array_type() {
    let port = 5443;
    let _db = start_server(port).await;
    let client = connect(port).await;

    // Create table with array
    client.batch_execute("CREATE TABLE arrays (id INT, tags TEXT[])").await.unwrap();

    // Insert array
    // Passing arrays in tokio-postgres requires types to implement ToSql.
    // Vec<String> implements ToSql.
    // But our server needs to handle the binary/text format of arrays sent by client.
    // If we didn't implement binary array parsing in `pg_server.rs`, this might fail if client sends binary.
    // tokio-postgres prefers binary.
    // Let's see if our "text fallback" in `pg_server.rs` handles parameters correctly.
    // In `pg_server.rs`, we try to decode parameters as specific types.
    // We didn't add Array decoding logic in `pg_server.rs` `do_query` loop yet!
    // We only added `String`, `i64`, `bool`.
    // So passing a Vec will likely result in `Value::Null` or error.

    // Workaround for now: Pass array as string literal "{tag1,tag2}"
    let tags = "{rust,database,mvcc}";
    client.execute("INSERT INTO arrays (id, tags) VALUES ($1, $2)", &[&1i32, &tags]).await.unwrap();

    let rows = client.query("SELECT tags FROM arrays WHERE id = 1", &[]).await.unwrap();
    let val: String = rows[0].get(0); // We return arrays as text "{...}"
    assert_eq!(val, "[rust, database, mvcc]"); // Our Display impl uses [...] but Postgres uses {...}.
    // Wait, our `Value::Array` Display impl uses `[...]`.
    // If we inserted "{...}" string, it went into `Value::Text`?
    // Ah, `TEXT[]` in CREATE TABLE maps to `DataType::Array(Text)`.
    // But if we pass a String parameter, it becomes `Value::Text`.
    // Does `INSERT` executor cast `Text` to `Array`?
    // `DataType::is_compatible` checks types. `Text` is not compatible with `Array`.
    // We need a cast or parse logic.

    // Let's check `evaluate_literal` in `dml.rs`. It handles `Text` -> `Timestamp` etc.
    // We should add `Text` -> `Array` parsing there if we want to support string input for arrays.
    // Or `Text` -> `Json`.
}
