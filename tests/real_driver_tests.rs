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
    client.execute(&stmt, &[&1i64, &"Alice", &30i64]).await.unwrap();
    client.execute(&stmt, &[&2i64, &"Bob", &25i64]).await.unwrap();

    // Test SELECT with parameters
    let stmt = client.prepare("SELECT name FROM users WHERE age > $1").await.unwrap();
    let rows = client.query(&stmt, &[&28i64]).await.unwrap();

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
    assert!(price.is_none());

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
    // Verify column is gone
    let result = client.query("SELECT price FROM products", &[]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_json_type() {
    let port = 5442;
    let _db = start_server(port).await;
    let client = connect(port).await;

    client.batch_execute("CREATE TABLE docs (id INT, data JSONB)").await.unwrap();

    let data = json!({
        "key": "value",
        "list": [1, 2, 3]
    });

    let data_str = data.to_string();
    client.execute("INSERT INTO docs (id, data) VALUES ($1, $2)", &[&1i64, &data_str]).await.unwrap();

    // Select back
    let rows = client.query("SELECT data FROM docs WHERE id = 1", &[]).await.unwrap();
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
    // Workaround for now: Pass array as string literal "{tag1,tag2}"
    let tags = "{rust,database,mvcc}";
    client.execute("INSERT INTO arrays (id, tags) VALUES ($1, $2)", &[&1i64, &tags]).await.unwrap();

    let rows = client.query("SELECT tags FROM arrays WHERE id = 1", &[]).await.unwrap();
    let val: String = rows[0].get(0);
    assert_eq!(val, "[rust, database, mvcc]");
}
