//! JSON Storage Adapter
//!
//! Main facade providing high-level API for JSON document operations.
//! Implements the Facade pattern to simplify complex interactions
//! between schema inference, SQL conversion, and database execution.

use crate::facade::InMemoryDB;
use crate::result::QueryResult;
use super::converter::{
    CreateTableBuilder, InsertStatementBuilder, UpdateStatementBuilder,
    DeleteStatementBuilder, JsonToValueConverter,
};
use super::error::{JsonError, JsonResult};
use super::schema_inference::{SchemaInferenceEngine, SchemaInferenceStrategy};
use super::validator::{QueryValidator, validate_collection_name, validate_field_name};
use serde_json::Value as JsonValue;
use std::sync::{Arc};
use tokio::sync::RwLock;

/// Configuration for JsonStorageAdapter
#[derive(Debug, Clone)]
pub struct JsonStorageConfig {
    /// Batch size for INSERT statements (to avoid huge queries)
    pub insert_batch_size: usize,
    /// Whether to automatically create tables if they don't exist
    pub auto_create_table: bool,
    /// Whether to validate queries for security
    pub validate_queries: bool,
}

impl Default for JsonStorageConfig {
    fn default() -> Self {
        Self {
            insert_batch_size: 1000,
            auto_create_table: true,
            validate_queries: true,
        }
    }
}

/// JSON Storage Adapter - Facade for JSON document operations
pub struct JsonStorageAdapter {
    db: Arc<RwLock<InMemoryDB>>,
    schema_engine: SchemaInferenceEngine,
    validator: QueryValidator,
    config: JsonStorageConfig,
}

impl JsonStorageAdapter {
    /// Create new adapter with default configuration
    pub fn new(db: Arc<RwLock<InMemoryDB>>) -> Self {
        Self {
            db,
            schema_engine: SchemaInferenceEngine::new(),
            validator: QueryValidator::new(),
            config: JsonStorageConfig::default(),
        }
    }

    /// Create adapter with custom configuration
    pub fn with_config(db: Arc<RwLock<InMemoryDB>>, config: JsonStorageConfig) -> Self {
        Self {
            db,
            schema_engine: SchemaInferenceEngine::new(),
            validator: QueryValidator::new(),
            config,
        }
    }

    /// Create adapter with custom schema inference strategy
    pub fn with_strategy(
        db: Arc<RwLock<InMemoryDB>>,
        strategy: Box<dyn SchemaInferenceStrategy>,
    ) -> Self {
        Self {
            db,
            schema_engine: SchemaInferenceEngine::with_strategy(strategy),
            validator: QueryValidator::new(),
            config: JsonStorageConfig::default(),
        }
    }

    /// Create or insert documents into a collection
    pub async fn create(&self, collection_name: &str, document: &str) -> JsonResult<()> {
        // Validate collection name
        validate_collection_name(collection_name)?;

        // Parse JSON document
        let json_value: JsonValue = serde_json::from_str(document)?;
        let documents = json_value.as_array()
            .ok_or_else(|| JsonError::InvalidStructure(
                "Expected JSON array of objects".to_string()
            ))?;

        if documents.is_empty() {
            return Err(JsonError::EmptyDocument);
        }

        // Check if table exists
        let table_exists = {
            let db = self.db.read().await;
            db.table_exists(collection_name)
        };

        // Create table if needed
        if !table_exists {
            if !self.config.auto_create_table {
                return Err(JsonError::ValidationError(
                    format!("Collection '{}' does not exist", collection_name)
                ));
            }

            self.create_table_from_documents(collection_name, documents).await?;
        }

        // Insert documents
        self.insert_documents(collection_name, documents).await?;

        Ok(())
    }

    /// Read documents from a collection using SQL query
    pub async fn read(&self, collection_name: &str, query: &str) -> JsonResult<String> {
        // Validate collection name
        validate_collection_name(collection_name)?;

        // Validate query if enabled
        if self.config.validate_queries {
            self.validator.validate(query, collection_name)?;
        }

        // Execute query
        let result = {
            let db = self.db.read().await;
            db.execute_readonly(query, None).await?
        };

        // Convert result to JSON
        self.query_result_to_json(&result)
    }

    /// Update documents in a collection
    pub async fn update(&self, collection_name: &str, document: &str) -> JsonResult<()> {
        // Validate collection name
        validate_collection_name(collection_name)?;

        // Parse JSON document
        let json_value: JsonValue = serde_json::from_str(document)?;
        let documents = json_value.as_array()
            .ok_or_else(|| JsonError::InvalidStructure(
                "Expected JSON array of objects".to_string()
            ))?;

        if documents.is_empty() {
            return Ok(());
        }

        // Check table exists
        {
            let db = self.db.read().await;
            if !db.table_exists(collection_name) {
                return Err(JsonError::ValidationError(
                    format!("Collection '{}' does not exist", collection_name)
                ));
            }
        }

        // Generate and execute UPDATE statements
        for doc in documents {
            let obj = doc.as_object()
                .ok_or_else(|| JsonError::InvalidStructure("Expected JSON object".to_string()))?;

            // Extract ID (required for updates)
            let id_value = obj.get("id")
                .ok_or_else(|| JsonError::MissingField("id field is required for updates".to_string()))?;

            let id_string = match id_value.as_str() {
                Some(s) => s.to_string(),
                None => id_value.to_string(),
            };
            let id_str = id_string.as_str();

            // Build UPDATE statement
            let mut builder = UpdateStatementBuilder::new(collection_name)
                .where_clause(format!("id = '{}'", id_str.replace('\'', "''")));

            // Add all fields except id to SET clause
            for (key, value) in obj {
                if key != "id" {
                    validate_field_name(key)?;
                    // Infer type from JSON value
                    let data_type = match value {
                        JsonValue::Null => crate::core::DataType::Text,
                        JsonValue::Bool(_) => crate::core::DataType::Boolean,
                        JsonValue::Number(n) => {
                            if n.is_i64() {
                                crate::core::DataType::Integer
                            } else {
                                crate::core::DataType::Float
                            }
                        }
                        JsonValue::String(_) => crate::core::DataType::Text,
                        _ => crate::core::DataType::Text,
                    };
                    let sql_value = JsonToValueConverter::convert(value, &data_type)?;
                    builder = builder.set(key.clone(), sql_value);
                }
            }

            let sql = builder.build();

            // Execute UPDATE
            let mut db = self.db.write().await;
            db.execute(&sql).await?;
        }

        Ok(())
    }

    /// Delete documents from a collection by ID
    pub async fn delete(&self, collection_name: &str, id: &str) -> JsonResult<()> {
        // Validate collection name
        validate_collection_name(collection_name)?;

        // Check table exists
        {
            let db = self.db.read().await;
            if !db.table_exists(collection_name) {
                return Err(JsonError::ValidationError(
                    format!("Collection '{}' does not exist", collection_name)
                ));
            }
        }

        // Build DELETE statement
        let sql = DeleteStatementBuilder::new(collection_name)
            .where_clause(format!("id = '{}'", id.replace('\'', "''")))
            .build();

        // Execute DELETE
        let mut db = self.db.write().await;
        db.execute(&sql).await?;

        Ok(())
    }

    /// Drop an entire collection (table)
    pub async fn drop_collection(&self, collection_name: &str) -> JsonResult<()> {
        validate_collection_name(collection_name)?;

        let mut db = self.db.write().await;
        let sql = format!("DROP TABLE IF EXISTS {}", collection_name);
        db.execute(&sql).await?;

        Ok(())
    }

    /// List all collections (tables)
    pub async fn list_collections(&self) -> Vec<String> {
        let db = self.db.read().await;
        db.list_tables()
    }

    /// Check if a collection exists
    pub async fn collection_exists(&self, collection_name: &str) -> bool {
        let db = self.db.read().await;
        db.table_exists(collection_name)
    }

    // ==================== Private Helper Methods ====================

    /// Create table from JSON documents
    async fn create_table_from_documents(
        &self,
        collection_name: &str,
        documents: &[JsonValue],
    ) -> JsonResult<()> {
        // Infer schema from documents
        let schema = self.schema_engine.infer_schema(collection_name, documents)?;

        // Generate CREATE TABLE statement
        let sql = CreateTableBuilder::from_schema(&schema).build();

        // Execute CREATE TABLE
        let mut db = self.db.write().await;
        db.execute(&sql).await?;

        Ok(())
    }

    /// Insert documents into existing table
    async fn insert_documents(
        &self,
        collection_name: &str,
        documents: &[JsonValue],
    ) -> JsonResult<()> {
        // Get table schema
        let schema = {
            let db = self.db.read().await;
            db.table_exists(collection_name)
                .then_some(())
                .ok_or_else(|| JsonError::ValidationError(
                    format!("Collection '{}' does not exist", collection_name)
                ))?;
            db.get_table_schema(collection_name).await?
        };

        // Convert JSON documents to rows
        let rows: JsonResult<Vec<_>> = documents.iter()
            .map(|doc| JsonToValueConverter::json_to_row(doc, &schema))
            .collect();
        let rows = rows?;

        // Generate INSERT statements in batches
        let column_names: Vec<String> = schema.schema().columns()
            .iter()
            .map(|col| col.name.clone())
            .collect();

        let sql_statements = InsertStatementBuilder::new(collection_name)
            .columns(column_names)
            .rows(rows)
            .build_batched(self.config.insert_batch_size);

        // Execute all INSERT statements
        let mut db = self.db.write().await;
        for sql in sql_statements {
            db.execute(&sql).await?;
        }

        Ok(())
    }

    /// Convert QueryResult to JSON string
    fn query_result_to_json(&self, result: &QueryResult) -> JsonResult<String> {
        let mut documents = Vec::new();

        for row in result.rows() {
            let mut obj = serde_json::Map::new();

            for (i, column) in result.columns().iter().enumerate() {
                let value = &row[i];
                let json_value = self.sql_value_to_json(value);
                obj.insert(column.name.clone(), json_value);
            }

            documents.push(JsonValue::Object(obj));
        }

        Ok(serde_json::to_string_pretty(&documents)?)
    }

    /// Convert SQL Value to JSON Value
    fn sql_value_to_json(&self, value: &crate::core::Value) -> JsonValue {
        match value {
            crate::core::Value::Null => JsonValue::Null,
            crate::core::Value::Integer(i) => JsonValue::Number((*i).into()),
            crate::core::Value::Float(f) => {
                serde_json::Number::from_f64(*f)
                    .map(JsonValue::Number)
                    .unwrap_or(JsonValue::Null)
            }
            crate::core::Value::Text(s) => JsonValue::String(s.clone()),
            crate::core::Value::Boolean(b) => JsonValue::Bool(*b),
            crate::core::Value::Timestamp(t) => JsonValue::String(t.to_rfc3339()),
            crate::core::Value::Date(d) => JsonValue::String(d.format("%Y-%m-%d").to_string()),
            crate::core::Value::Uuid(u) => JsonValue::String(u.to_string()),
            crate::core::Value::Array(arr) => {
                JsonValue::Array(arr.iter().map(|v| self.sql_value_to_json(v)).collect())
            }
            crate::core::Value::Json(j) => j.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_adapter() -> JsonStorageAdapter {
        let db = Arc::new(RwLock::new(InMemoryDB::new()));
        JsonStorageAdapter::new(db)
    }

    #[tokio::test]
    async fn test_create_collection() {
        let adapter = create_test_adapter();

        let doc = r#"[
            {"id": "1", "name": "Alice", "age": 30},
            {"id": "2", "name": "Bob", "age": 25}
        ]"#;

        let result = adapter.create("users", doc).await;
        assert!(result.is_ok(), "{:?}", result.err());

        assert!(adapter.collection_exists("users").await);
    }

    #[tokio::test]
    async fn test_read_collection() {
        let adapter = create_test_adapter();

        let doc = r#"[
            {"id": "1", "name": "Alice", "age": 30},
            {"id": "2", "name": "Bob", "age": 24.9}
        ]"#;

        adapter.create("users", doc).await.unwrap();

        let result = adapter.read("users", "SELECT * FROM users WHERE age > 24").await;
        assert!(result.is_ok());

        let json = result.unwrap();
        assert!(json.contains("Alice"));
    }

    #[tokio::test]
    async fn test_delete_document() {
        let adapter = create_test_adapter();

        let doc = r#"[
            {"id": "1", "name": "Alice", "age": 30}
        ]"#;

        adapter.create("users", doc).await.unwrap();
        let result = adapter.delete("users", "1").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_collection_name() {
        let adapter = create_test_adapter();

        let doc = r#"[{"id": "1"}]"#;

        assert!(adapter.create("DROP", doc).await.is_err());
        assert!(adapter.create("user-profile", doc).await.is_err());
        assert!(adapter.create("123users", doc).await.is_err());
    }
}
