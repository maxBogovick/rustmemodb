use super::*;

impl PersistSession {
    /// Creates a new persistence session with a unique owned database instance.
    pub fn new(db: InMemoryDB) -> Self {
        Self {
            db: Arc::new(Mutex::new(db)),
            transaction_id: None,
        }
    }

    /// Creates a new persistence session sharing an existing database instance.
    pub fn from_shared(db: Arc<Mutex<InMemoryDB>>) -> Self {
        Self {
            db,
            transaction_id: None,
        }
    }

    /// Returns the shared database instance used by this session.
    pub fn shared_db(&self) -> Arc<Mutex<InMemoryDB>> {
        self.db.clone()
    }

    /// Creates a clone of this session associated with a specific transaction ID.
    pub fn with_transaction_id(&self, transaction_id: TransactionId) -> Self {
        Self {
            db: self.db.clone(),
            transaction_id: Some(transaction_id),
        }
    }

    /// Returns the transaction ID associated with this session, if any.
    pub fn transaction_id(&self) -> Option<TransactionId> {
        self.transaction_id
    }

    /// Executes a SQL statement within the session's context (and transaction, if active).
    pub async fn execute(&self, sql: &str) -> Result<crate::result::QueryResult> {
        let mut db = self.db.lock().await;
        db.execute_with_transaction(sql, self.transaction_id).await
    }

    /// Queries the database within the session's context. Alias for `execute`.
    pub async fn query(&self, sql: &str) -> Result<crate::result::QueryResult> {
        self.execute(sql).await
    }

    /// Checks if a row with the given persistence ID exists in the specified table.
    pub async fn persist_row_exists(&self, table_name: &str, persist_id: &str) -> Result<bool> {
        let sql = format!(
            "SELECT __persist_id FROM {} WHERE __persist_id = '{}'",
            table_name,
            sql_escape_string(persist_id)
        );
        let result = self.query(&sql).await?;
        Ok(result.row_count() > 0)
    }

    /// Deletes a row by its persistence ID from the specified table.
    pub async fn delete_persist_row(&self, table_name: &str, persist_id: &str) -> Result<()> {
        let sql = format!(
            "DELETE FROM {} WHERE __persist_id = '{}'",
            table_name,
            sql_escape_string(persist_id)
        );
        self.execute(&sql).await?;
        Ok(())
    }

    /// Ensures the internal schema registry table exists.
    pub async fn ensure_schema_registry_table(&self) -> Result<()> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (table_name TEXT PRIMARY KEY, schema_version INTEGER NOT NULL, updated_at TIMESTAMP NOT NULL)",
            PERSIST_SCHEMA_REGISTRY_TABLE
        );
        self.execute(&sql).await?;
        Ok(())
    }

    /// Retrieves the schema version for a given table from the registry.
    ///
    /// Returns `None` if the table is not registered.
    pub async fn get_table_schema_version(&self, table_name: &str) -> Result<Option<u32>> {
        self.ensure_schema_registry_table().await?;
        let sql = format!(
            "SELECT schema_version FROM {} WHERE table_name = '{}'",
            PERSIST_SCHEMA_REGISTRY_TABLE,
            sql_escape_string(table_name)
        );
        let result = self.query(&sql).await?;
        if result.row_count() == 0 {
            return Ok(None);
        }

        let Some(first_row) = result.rows().first() else {
            return Ok(None);
        };

        let Some(first_col) = first_row.first() else {
            return Ok(None);
        };

        match first_col {
            Value::Integer(v) if *v >= 0 => Ok(Some(*v as u32)),
            other => Err(DbError::ExecutionError(format!(
                "Invalid schema version value for table '{}': {}",
                table_name,
                other.type_name()
            ))),
        }
    }

    /// Sets or updates the schema version for a table in the registry.
    pub async fn set_table_schema_version(
        &self,
        table_name: &str,
        schema_version: u32,
    ) -> Result<()> {
        self.ensure_schema_registry_table().await?;
        let now = Utc::now().to_rfc3339();
        let escaped_table = sql_escape_string(table_name);

        let exists_sql = format!(
            "SELECT table_name FROM {} WHERE table_name = '{}'",
            PERSIST_SCHEMA_REGISTRY_TABLE, escaped_table
        );
        let exists = self.query(&exists_sql).await?.row_count() > 0;

        if exists {
            let update_sql = format!(
                "UPDATE {} SET schema_version = {}, updated_at = '{}' WHERE table_name = '{}'",
                PERSIST_SCHEMA_REGISTRY_TABLE, schema_version, now, escaped_table
            );
            self.execute(&update_sql).await?;
        } else {
            let insert_sql = format!(
                "INSERT INTO {} (table_name, schema_version, updated_at) VALUES ('{}', {}, '{}')",
                PERSIST_SCHEMA_REGISTRY_TABLE, escaped_table, schema_version, now
            );
            self.execute(&insert_sql).await?;
        }

        Ok(())
    }

    /// Starts a new transaction on the underlying database.
    pub async fn begin_transaction(&self) -> Result<TransactionId> {
        let db = self.db.lock().await;
        db.transaction_manager().begin().await
    }

    /// Commits an active transaction.
    pub async fn commit_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let db = self.db.lock().await;
        db.transaction_manager().commit(transaction_id).await
    }

    /// Rolls back an active transaction.
    pub async fn rollback_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        let db = self.db.lock().await;
        db.transaction_manager().rollback(transaction_id).await
    }

    /// Executes a closure within a transaction scope.
    ///
    /// If the closure returns `Ok`, the transaction is committed.
    /// If it returns `Err`, the transaction is rolled back.
    pub async fn with_transaction<F, Fut, T>(&self, op: F) -> Result<T>
    where
        F: FnOnce(PersistSession) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let transaction_id = self.begin_transaction().await?;
        let tx_session = self.with_transaction_id(transaction_id);

        let op_result = op(tx_session).await;
        match op_result {
            Ok(value) => {
                if let Err(err) = self.commit_transaction(transaction_id).await {
                    let _ = self.rollback_transaction(transaction_id).await;
                    return Err(err);
                }
                Ok(value)
            }
            Err(err) => {
                let _ = self.rollback_transaction(transaction_id).await;
                Err(err)
            }
        }
    }
}
