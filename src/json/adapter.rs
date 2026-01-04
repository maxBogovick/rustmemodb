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
use super::validator::{QueryValidator, validate_collection_name};
use serde_json::Value as JsonValue;
use std::sync::{Arc, RwLock};

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
    ///
    /// # Arguments
    /// * `collection_name` - Name of the collection (table)
    /// * `document` - JSON string containing array of objects
    ///
    /// # Example
    /// ```ignore
    /// adapter.create("users", r#"[
    ///     {"id": "1", "name": "Alice", "age": 30},
    ///     {"id": "2", "name": "Bob", "age": 25}
    /// ]"#)?;
    /// ```
    pub fn create(&self, collection_name: &str, document: &str) -> JsonResult<()> {
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
            let db = self.db.read().unwrap();
            db.table_exists(collection_name)
        };

        // Create table if needed
        if !table_exists {
            if !self.config.auto_create_table {
                return Err(JsonError::ValidationError(
                    format!("Collection '{}' does not exist", collection_name)
                ));
            }

            self.create_table_from_documents(collection_name, documents)?;
        }

        // Insert documents
        self.insert_documents(collection_name, documents)?;

        Ok(())
    }

    /// Read documents from a collection using SQL query
    ///
    /// # Arguments
    /// * `collection_name` - Name of the collection (table)
    /// * `query` - SQL SELECT query
    ///
    /// # Example
    /// ```ignore
    /// let results = adapter.read("users", "SELECT * FROM users WHERE age > 25")?;
    /// ```
    pub fn read(&self, collection_name: &str, query: &str) -> JsonResult<String> {
        // Validate collection name
        validate_collection_name(collection_name)?;

        // Validate query if enabled
        if self.config.validate_queries {
            self.validator.validate(query, collection_name)?;
        }

        // Execute query
        let result = {
            let mut db = self.db.write().unwrap();
            db.execute(query)?
        };

        // Convert result to JSON
        self.query_result_to_json(&result)
    }

    /// Update documents in a collection
    ///
    /// # Arguments
    /// * `collection_name` - Name of the collection (table)
    /// * `document` - JSON string containing array of objects with updates
    ///
    /// # Example
    /// ```ignore
    /// adapter.update("users", r#"[
    ///     {"id": "1", "name": "Alice Smith", "age": 31}
    /// ]"#)?;
    /// ```
    ///
    /// # Note
    /// This method requires an "id" field in each document to identify
    /// which rows to update.
    pub fn update(&self, collection_name: &str, document: &str) -> JsonResult<()> {
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
            let db = self.db.read().unwrap();
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
            let mut db = self.db.write().unwrap();
            db.execute(&sql)?;
        }

        Ok(())
    }

    /// Delete documents from a collection by ID
    ///
    /// # Arguments
    /// * `collection_name` - Name of the collection (table)
    /// * `id` - ID of the document to delete
    ///
    /// # Example
    /// ```ignore
    /// adapter.delete("users", "123")?;
    /// ```
    pub fn delete(&self, collection_name: &str, id: &str) -> JsonResult<()> {
        // Validate collection name
        validate_collection_name(collection_name)?;

        // Check table exists
        {
            let db = self.db.read().unwrap();
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
        let mut db = self.db.write().unwrap();
        db.execute(&sql)?;

        Ok(())
    }

    /// Drop an entire collection (table)
    ///
    /// # Arguments
    /// * `collection_name` - Name of the collection to drop
    ///
    /// # Example
    /// ```ignore
    /// adapter.drop_collection("users")?;
    /// ```
    pub fn drop_collection(&self, collection_name: &str) -> JsonResult<()> {
        validate_collection_name(collection_name)?;

        let mut db = self.db.write().unwrap();
        let sql = format!("DROP TABLE IF EXISTS {}", collection_name);
        db.execute(&sql)?;

        Ok(())
    }

    /// List all collections (tables)
    pub fn list_collections(&self) -> Vec<String> {
        let db = self.db.read().unwrap();
        db.list_tables()
    }

    /// Check if a collection exists
    pub fn collection_exists(&self, collection_name: &str) -> bool {
        let db = self.db.read().unwrap();
        db.table_exists(collection_name)
    }

    // ==================== Private Helper Methods ====================

    /// Create table from JSON documents
    fn create_table_from_documents(
        &self,
        collection_name: &str,
        documents: &[JsonValue],
    ) -> JsonResult<()> {
        // Infer schema from documents
        let schema = self.schema_engine.infer_schema(collection_name, documents)?;

        // Generate CREATE TABLE statement
        let sql = CreateTableBuilder::from_schema(&schema).build();

        // Execute CREATE TABLE
        let mut db = self.db.write().unwrap();
        db.execute(&sql)?;

        Ok(())
    }

    /// Insert documents into existing table
    fn insert_documents(
        &self,
        collection_name: &str,
        documents: &[JsonValue],
    ) -> JsonResult<()> {
        // Get table schema
        let schema = {
            let db = self.db.read().unwrap();
            db.table_exists(collection_name)
                .then(|| ())
                .ok_or_else(|| JsonError::ValidationError(
                    format!("Collection '{}' does not exist", collection_name)
                ))?;

            // We need to get the actual schema to convert JSON to rows
            // For now, we'll re-infer it from the documents
            // TODO: Get schema from catalog
            self.schema_engine.infer_schema(collection_name, documents)?
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
        let mut db = self.db.write().unwrap();
        for sql in sql_statements {
            db.execute(&sql)?;
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
                obj.insert(column.clone(), json_value);
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

    #[test]
    fn test_create_collection() {
        let adapter = create_test_adapter();

        let doc = r#"[
            {"id": "1", "name": "Alice", "age": 30},
            {"id": "2", "name": "Bob", "age": 25}
        ]"#;

        let result = adapter.create("users", doc);
        assert!(result.is_ok());

        assert!(adapter.collection_exists("users"));
    }

    #[test]
    fn test_read_collection() {
        let adapter = create_test_adapter();

        let doc = r#"[
            {"id": "1", "name": "Alice", "age": 30},
            {"id": "2", "name": "Bob", "age": 24.9}
        ]"#;

        adapter.create("users", doc).unwrap();

        let result = adapter.read("users", "SELECT * FROM users WHERE age > 24");
        assert!(result.is_ok());

        let json = result.unwrap();
        assert!(json.contains("Alice"));
    }

    #[test]
    fn test_delete_document() {
        let adapter = create_test_adapter();

        let doc = r#"[
            {"id": "1", "name": "Alice", "age": 30}
        ]"#;

        adapter.create("users", doc).unwrap();
        let result = adapter.delete("users", "1");
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_collection_name() {
        let adapter = create_test_adapter();

        let doc = r#"[{"id": "1"}]"#;

        assert!(adapter.create("DROP", doc).is_err());
        assert!(adapter.create("user-profile", doc).is_err());
        assert!(adapter.create("123users", doc).is_err());
    }
}
