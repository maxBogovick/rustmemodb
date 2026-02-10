/// Example: Connection Pooling
///
/// This example demonstrates connection pool behavior, configuration,
/// and performance characteristics.
///
/// Run: cargo run --example connection_pooling
use rustmemodb::{Client, ConnectionConfig, Result};
use std::thread;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== RustMemDB Connection Pooling Example ===\n");

    // ============================================================================
    // 1. Default Pool Configuration
    // ============================================================================
    println!("1. Default pool configuration:");
    let client = Client::connect("admin", "admin").await?;
    let stats = client.stats();
    println!("   {}", stats.await);
    println!();

    // ============================================================================
    // 2. Custom Pool Configuration
    // ============================================================================
    println!("2. Custom pool configuration:");
    let config = ConnectionConfig::new("admin", "admin")
        .min_connections(2)
        .max_connections(5);

    let client = Client::connect_with_config(config).await?;
    let stats = client.stats().await;
    println!("   {}", stats);
    println!();

    // Setup test table
    client
        .execute("CREATE TABLE test (id INTEGER, data TEXT)")
        .await?;

    // ============================================================================
    // 3. Pool Utilization
    // ============================================================================
    println!("3. Testing pool utilization:");

    println!("   Initial: {}", client.stats().await);

    // Get connections
    let mut conn1 = client.get_connection().await?;
    println!("   After conn1: {}", client.stats().await);

    let mut conn2 = client.get_connection().await?;
    println!("   After conn2: {}", client.stats().await);

    let mut conn3 = client.get_connection().await?;
    println!("   After conn3: {}", client.stats().await);

    // Use connections
    conn1
        .execute("INSERT INTO test VALUES (1, 'data1')")
        .await?;
    conn2
        .execute("INSERT INTO test VALUES (2, 'data2')")
        .await?;
    conn3
        .execute("INSERT INTO test VALUES (3, 'data3')")
        .await?;

    // Return to pool
    drop(conn1);
    thread::sleep(Duration::from_millis(10));
    println!("   After return conn1: {}", client.stats().await);

    drop(conn2);
    thread::sleep(Duration::from_millis(10));
    println!("   After return conn2: {}", client.stats().await);

    drop(conn3);
    thread::sleep(Duration::from_millis(10));
    println!("   After return conn3: {}", client.stats().await);
    println!();

    // ============================================================================
    // 4. Connection Reuse
    // ============================================================================
    println!("4. Connection reuse:");

    let conn_id_1 = {
        let mut conn = client.get_connection().await?;
        let id = conn.connection().id();
        println!("   First use: Connection ID {}", id);
        conn.execute("INSERT INTO test VALUES (10, 'test')").await?;
        id
    };

    thread::sleep(Duration::from_millis(10));

    let conn_id_2 = {
        let mut conn = client.get_connection().await?;
        let id = conn.connection().id();
        println!("   Second use: Connection ID {}", id);
        conn.execute("INSERT INTO test VALUES (20, 'test')").await?;
        id
    };

    if conn_id_1 == conn_id_2 {
        println!("   ✓ Connection was reused (same ID)");
    } else {
        println!("   → Different connections used");
    }
    println!();

    // ============================================================================
    // 5. Max Connections Limit
    // ============================================================================
    println!("5. Testing max connections limit:");

    let config = ConnectionConfig::new("admin", "admin")
        .max_connections(3)
        .connect_timeout(Duration::from_millis(100));

    let limited_client = Client::connect_with_config(config).await?;
    limited_client
        .execute("CREATE TABLE limited_test (id INTEGER)")
        .await?;

    println!("   Max connections: 3");

    let _c1 = limited_client.get_connection().await?;
    println!("   ✓ Connection 1 acquired");

    let _c2 = limited_client.get_connection().await?;
    println!("   ✓ Connection 2 acquired");

    let _c3 = limited_client.get_connection().await?;
    println!("   ✓ Connection 3 acquired");

    println!("   Pool: {}", limited_client.stats().await);

    println!("   → Trying to get 4th connection (should timeout)...");
    match limited_client.get_connection().await {
        Ok(_) => println!("   ✗ Got 4th connection (unexpected!)"),
        Err(_) => println!("   ✓ Timeout as expected (pool exhausted)"),
    }
    println!();

    // ============================================================================
    // 6. Connection URL
    // ============================================================================
    println!("6. Connecting via URL:");

    let url_client =
        Client::connect_url("rustmemodb://admin:admin@localhost:5432/production").await?;

    println!("   ✓ Connected via URL");
    println!("   Pool: {}", url_client.stats().await);
    println!();

    // ============================================================================
    // 7. Concurrent Access Simulation
    // ============================================================================
    println!("7. Simulating concurrent access:");

    let config = ConnectionConfig::new("admin", "admin")
        .min_connections(2)
        .max_connections(4);

    let concurrent_client = Client::connect_with_config(config).await?;
    concurrent_client
        .execute("CREATE TABLE concurrent_test (id INTEGER, thread_id INTEGER)")
        .await?;

    let mut handles = vec![];

    for thread_id in 0..4 {
        let client_clone = concurrent_client.clone_pool().await;

        let handle = tokio::spawn(async move {
            for i in 0..5 {
                let mut conn = client_clone.get_connection().await.unwrap();
                let sql = format!(
                    "INSERT INTO concurrent_test VALUES ({}, {})",
                    thread_id * 100 + i,
                    thread_id
                );
                conn.execute(&sql).await.unwrap();
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let result = concurrent_client
        .query("SELECT * FROM concurrent_test")
        .await?;
    println!("   ✓ Inserted {} rows concurrently", result.row_count());
    println!("   Final pool: {}", concurrent_client.stats().await);
    println!();

    println!("✓ All connection pooling examples completed!");

    Ok(())
}

// Helper trait to clone pool for concurrent access
trait ClonePool {
    async fn clone_pool(&self) -> Self;
}

impl ClonePool for Client {
    async fn clone_pool(&self) -> Self {
        // This is a workaround - in real implementation,
        // Client should contain Arc<ConnectionPool>
        Client::connect("admin", "admin").await.unwrap()
    }
}
