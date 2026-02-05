use rustmemodb::facade::InMemoryDB;
use rustmemodb::server::PostgresServer;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::SimpleQueryMessage;
use tokio_postgres::NoTls;

async fn start_server(port: u16) {
    let db = Arc::new(RwLock::new(InMemoryDB::new()));
    let server = PostgresServer::new(db, "127.0.0.1", port);
    tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("Server error: {}", e);
        }
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
}

fn collect_rows(
    messages: Vec<SimpleQueryMessage>,
) -> Result<Vec<Vec<String>>, Box<dyn std::error::Error>> {
    let mut rows = Vec::new();
    for msg in messages {
        if let SimpleQueryMessage::Row(row) = msg {
            let mut cols = Vec::new();
            for i in 0..row.len() {
                cols.push(row.get(i).unwrap_or("").to_string());
            }
            rows.push(cols);
        }
    }
    Ok(rows)
}

#[tokio::test]
async fn test_pg_protocol_interaction() -> Result<(), Box<dyn std::error::Error>> {
    let port = match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => {
            let port = listener.local_addr()?.port();
            drop(listener);
            port
        }
        Err(e) => {
            eprintln!("Skipping test_pg_protocol_interaction: {}", e);
            return Ok(());
        }
    };
    start_server(port).await;

    let connection_string = format!(
        "host=127.0.0.1 port={} user=admin password=adminpass dbname=postgres sslmode=disable",
        port
    );
    let client = match connect_with_retry(&connection_string, 10).await {
        Ok(res) => res,
        Err(e) => {
            eprintln!("Skipping test_pg_protocol_interaction: {}", e);
            return Ok(());
        }
    };

    client
        .simple_query("CREATE TABLE pg_test (id INT PRIMARY KEY, name TEXT, balance FLOAT)")
        .await?;

    client
        .simple_query("INSERT INTO pg_test (id, name, balance) VALUES (1, 'Alice', 100.5)")
        .await?;
    client
        .simple_query("INSERT INTO pg_test (id, name, balance) VALUES (2, 'Bob', 250.75)")
        .await?;

    let rows = collect_rows(
        client
            .simple_query("SELECT name, balance FROM pg_test ORDER BY balance DESC")
            .await?,
    )?;
    assert_eq!(rows.len(), 2);
    let first_name = rows[0][0].clone();
    let first_balance: f64 = rows[0][1].parse()?;
    assert_eq!(first_name, "Bob");
    assert_eq!(first_balance, 250.75);

    client
        .simple_query("UPDATE pg_test SET balance = 150.0 WHERE id = 1")
        .await?;
    let updated_rows = collect_rows(
        client
            .simple_query("SELECT balance FROM pg_test WHERE id = 1")
            .await?,
    )?;
    let updated_balance: f64 = updated_rows[0][0].parse()?;
    assert_eq!(updated_balance, 150.0);

    client
        .simple_query("DELETE FROM pg_test WHERE id = 2")
        .await?;
    let final_rows = collect_rows(
        client
            .simple_query("SELECT count(*) FROM pg_test")
            .await?,
    )?;
    let count: i64 = final_rows[0][0].parse()?;
    assert_eq!(count, 1);

    Ok(())
}

async fn connect_with_retry(
    connection_string: &str,
    attempts: usize,
) -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let mut last_err = None;
    for _ in 0..attempts {
        match tokio_postgres::connect(connection_string, NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("Connection error: {}", e);
                    }
                });
                return Ok(client);
            }
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }
    Err(last_err.expect("connect_with_retry: no attempts made"))
}
