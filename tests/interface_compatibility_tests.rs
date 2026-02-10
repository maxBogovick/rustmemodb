use rustmemodb::{Client, DatabaseClient, Result};

// Example of a business logic function that is agnostic to the database implementation
async fn count_users(db: &impl DatabaseClient) -> Result<usize> {
    // This query works on both RustMemDB and Postgres/MySQL
    let result = db.query("SELECT * FROM users").await?;
    Ok(result.row_count())
}

// Example of a function that inserts data, working on any DB
async fn create_user(db: &impl DatabaseClient, id: i32, name: &str) -> Result<()> {
    let sql = format!("INSERT INTO users VALUES ({}, '{}')", id, name);
    db.execute(&sql).await?;
    Ok(())
}

#[tokio::test]
async fn test_generic_database_interface() -> Result<()> {
    // 1. Setup the In-Memory DB (Mocking a real DB)
    // Use connect_local for isolated test environment
    let client = Client::connect_local("admin", "adminpass").await?;

    // Create schema
    client
        .execute("CREATE TABLE users (id INTEGER, name TEXT)")
        .await?;

    // 2. Use the generic functions with our In-Memory DB
    create_user(&client, 1, "Alice").await?;
    create_user(&client, 2, "Bob").await?;

    // 3. Verify logic
    let count = count_users(&client).await?;
    assert_eq!(count, 2);

    // This proves that 'Client' implements 'DatabaseClient' and can be used
    // where a generic DB connection is expected.
    // In a real app, you would have:
    // struct PostgresWrapper(postgres::Client);
    // impl DatabaseClient for PostgresWrapper { ... }

    Ok(())
}

// Example of how a Real DB wrapper would look (Mock code)
/*
struct PostgresWrapper {
    inner: tokio_postgres::Client,
}

#[async_trait::async_trait]
impl DatabaseClient for PostgresWrapper {
    async fn query(&self, sql: &str) -> Result<QueryResult> {
        let rows = self.inner.query(sql, &[]).await.map_err(|e| ...)?;
        // Convert postgres rows to rustmemodb::QueryResult
        Ok(convert_postgres_rows(rows))
    }

    async fn execute(&self, sql: &str) -> Result<QueryResult> {
        let n = self.inner.execute(sql, &[]).await.map_err(|e| ...)?;
        Ok(QueryResult::updated(n as usize))
    }

    async fn ping(&self) -> Result<()> {
        self.inner.simple_query("SELECT 1").await.map(|_| ()).map_err(|e| ...)
    }
}
*/
