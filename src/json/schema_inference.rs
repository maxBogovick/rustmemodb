//! Schema Inference Module
//!
//! Implements Strategy pattern for inferring SQL schemas from JSON documents.
//! Supports multiple strategies:
//! - FirstDocumentStrategy: Fast, uses only first document
//! - AllDocumentsStrategy: Thorough, analyzes all documents
//! - SmartStrategy: Samples documents for balance

use super::error::{JsonError, JsonResult};
use super::validator::validate_field_name;
use crate::core::{Column, DataType};
use crate::storage::TableSchema;
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};

/// Trait for schema inference strategies (Strategy Pattern)
pub trait SchemaInferenceStrategy: Send + Sync {
    /// Infer schema from JSON documents
    fn infer_schema(
        &self,
        collection_name: &str,
        documents: &[JsonValue],
    ) -> JsonResult<TableSchema>;
}

/// Infers schema from the first document only (fast but less accurate)
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct FirstDocumentStrategy;

impl SchemaInferenceStrategy for FirstDocumentStrategy {
    fn infer_schema(
        &self,
        collection_name: &str,
        documents: &[JsonValue],
    ) -> JsonResult<TableSchema> {
        if documents.is_empty() {
            return Err(JsonError::EmptyDocument);
        }

        let first = &documents[0];
        let obj = first
            .as_object()
            .ok_or_else(|| JsonError::InvalidStructure("Expected JSON object".into()))?;

        let columns = obj
            .iter()
            .map(|(key, value)| -> JsonResult<Column> {
                validate_field_name(key)?;
                let data_type = infer_type_from_value(value);
                Ok(Column::new(key.clone(), data_type))
            })
            .collect::<JsonResult<Vec<_>>>()?;

        Ok(TableSchema::new(collection_name.to_string(), columns))
    }
}

/// Infers schema by analyzing all documents (slower but most accurate)
#[derive(Debug, Clone, Default)]
pub struct AllDocumentsStrategy;

impl SchemaInferenceStrategy for AllDocumentsStrategy {
    fn infer_schema(
        &self,
        collection_name: &str,
        documents: &[JsonValue],
    ) -> JsonResult<TableSchema> {
        if documents.is_empty() {
            return Err(JsonError::EmptyDocument);
        }

        // Collect all field names and their types across all documents
        let mut field_types: HashMap<String, Vec<DataType>> = HashMap::new();
        let mut all_fields: HashSet<String> = HashSet::new();

        for doc in documents {
            let obj = doc
                .as_object()
                .ok_or_else(|| JsonError::InvalidStructure("Expected JSON object".into()))?;

            for (key, value) in obj {
                validate_field_name(key)?;
                all_fields.insert(key.clone());
                let data_type = infer_type_from_value(value);
                field_types.entry(key.clone()).or_default().push(data_type);
            }
        }

        // Determine the most general type for each field
        let mut columns: Vec<Column> = all_fields
            .iter()
            .map(|field_name| {
                let types = field_types.get(field_name).unwrap();
                let most_general = find_most_general_type(types);

                // Field is nullable if it doesn't appear in all documents
                let is_nullable = types.len() < documents.len();

                let mut column = Column::new(field_name.clone(), most_general);
                if !is_nullable {
                    column = column.not_null();
                }
                column
            })
            .collect();

        // Sort columns for consistent ordering (id first if exists, then alphabetically)
        columns.sort_by(|a, b| match (&a.name[..], &b.name[..]) {
            ("id", _) => std::cmp::Ordering::Less,
            (_, "id") => std::cmp::Ordering::Greater,
            _ => a.name.cmp(&b.name),
        });

        Ok(TableSchema::new(collection_name.to_string(), columns))
    }
}

/// Infers schema by sampling documents (good balance)
#[derive(Debug, Clone)]
pub struct SmartStrategy {
    sample_size: usize,
}

impl SmartStrategy {
    pub fn new(sample_size: usize) -> Self {
        Self { sample_size }
    }
}

impl Default for SmartStrategy {
    fn default() -> Self {
        Self::new(100) // Sample up to 100 documents
    }
}

impl SchemaInferenceStrategy for SmartStrategy {
    fn infer_schema(
        &self,
        collection_name: &str,
        documents: &[JsonValue],
    ) -> JsonResult<TableSchema> {
        if documents.is_empty() {
            return Err(JsonError::EmptyDocument);
        }

        // Sample documents: take first, last, and evenly distributed middle ones
        let sample_indices = if documents.len() <= self.sample_size {
            (0..documents.len()).collect::<Vec<_>>()
        } else {
            let step = documents.len() / self.sample_size;
            (0..self.sample_size).map(|i| i * step).collect()
        };

        let sampled: Vec<JsonValue> = sample_indices
            .iter()
            .map(|&i| documents[i].clone())
            .collect();

        // Use AllDocumentsStrategy on the sample
        AllDocumentsStrategy.infer_schema(collection_name, &sampled)
    }
}

/// Infer DataType from a JSON value
fn infer_type_from_value(value: &JsonValue) -> DataType {
    match value {
        JsonValue::Null => DataType::Text, // Default to TEXT for null
        JsonValue::Bool(_) => DataType::Boolean,
        JsonValue::Number(n) => {
            if n.is_i64() {
                DataType::Integer
            } else {
                DataType::Float
            }
        }
        JsonValue::String(_) => DataType::Text,
        // Complex types (Object/Array) are serialized as JSON strings
        JsonValue::Array(_) | JsonValue::Object(_) => DataType::Text,
    }
}

/// Find the most general type that can represent all given types
/// Type hierarchy: Integer < Float < Text
/// Boolean remains Boolean unless mixed with others, then Text
fn find_most_general_type(types: &[DataType]) -> DataType {
    if types.is_empty() {
        return DataType::Text;
    }

    let mut has_text = false;
    let mut has_float = false;
    let mut has_integer = false;
    let mut has_boolean = false;
    let mut has_timestamp = false;
    let mut has_date = false;
    let mut has_uuid = false;
    let mut has_array = false;
    let mut has_json = false;

    for dtype in types {
        match dtype {
            DataType::Text => has_text = true,
            DataType::Float => has_float = true,
            DataType::Integer => has_integer = true,
            DataType::Boolean => has_boolean = true,
            DataType::Timestamp => has_timestamp = true,
            DataType::Date => has_date = true,
            DataType::Uuid => has_uuid = true,
            DataType::Array(_) => has_array = true,
            DataType::Json => has_json = true,
            DataType::Unknown => has_text = true,
        }
    }

    // If TEXT appears, everything becomes TEXT
    if has_text {
        return DataType::Text;
    }

    // Check for mixed types that force TEXT
    // Boolean + Numeric -> Text
    if has_boolean && (has_integer || has_float) {
        return DataType::Text;
    }

    // Complex types mixed with anything else -> Text
    // (Unless we want to support coercion like Date -> Timestamp, but simplicity first)
    if has_timestamp && (has_integer || has_float || has_boolean || has_date || has_uuid) {
        return DataType::Text;
    }
    if has_date && (has_integer || has_float || has_boolean || has_timestamp || has_uuid) {
        return DataType::Text;
    }
    if has_uuid && (has_integer || has_float || has_boolean || has_timestamp || has_date) {
        return DataType::Text;
    }

    // Single complex types
    if has_timestamp {
        return DataType::Timestamp;
    }
    if has_date {
        return DataType::Date;
    }
    if has_uuid {
        return DataType::Uuid;
    }
    if has_array {
        return DataType::Array(Box::new(DataType::Text));
    } // Fallback to array of text
    if has_json {
        return DataType::Json;
    }

    // Numeric type hierarchy
    if has_float {
        return DataType::Float;
    }

    if has_integer {
        return DataType::Integer;
    }

    if has_boolean {
        return DataType::Boolean;
    }

    DataType::Text
}

/// Default schema inference engine with configurable strategy
pub struct SchemaInferenceEngine {
    strategy: Box<dyn SchemaInferenceStrategy>,
}

impl SchemaInferenceEngine {
    /// Create engine with a specific strategy
    pub fn with_strategy(strategy: Box<dyn SchemaInferenceStrategy>) -> Self {
        Self { strategy }
    }

    /// Create engine with default strategy (SmartStrategy)
    pub fn new() -> Self {
        Self::with_strategy(Box::new(SmartStrategy::default()))
    }

    /// Infer schema from JSON documents
    pub fn infer_schema(
        &self,
        collection_name: &str,
        documents: &[JsonValue],
    ) -> JsonResult<TableSchema> {
        self.strategy.infer_schema(collection_name, documents)
    }
}

impl Default for SchemaInferenceEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_first_document_strategy() {
        let docs = vec![
            json!({"id": "1", "name": "Alice", "age": 30}),
            json!({"id": "2", "name": "Bob"}), // missing age
        ];

        let strategy = FirstDocumentStrategy;
        let schema = strategy.infer_schema("users", &docs).unwrap();

        assert_eq!(schema.name(), "users");
        assert_eq!(schema.schema().column_count(), 3);
    }

    #[test]
    fn test_all_documents_strategy() {
        let docs = vec![
            json!({"id": "1", "age": 30}),
            json!({"id": "2", "name": "Bob"}),
            json!({"id": "3", "name": "Charlie", "age": 25}),
        ];

        let strategy = AllDocumentsStrategy;
        let schema = strategy.infer_schema("users", &docs).unwrap();

        // Should have all fields: id, name, age
        assert_eq!(schema.schema().column_count(), 3);

        // id should be first
        let cols = schema.schema().columns();
        assert_eq!(cols[0].name, "id");
    }

    #[test]
    fn test_type_inference() {
        assert_eq!(infer_type_from_value(&json!(42)), DataType::Integer);
        assert_eq!(infer_type_from_value(&json!(3.14)), DataType::Float);
        assert_eq!(infer_type_from_value(&json!("hello")), DataType::Text);
        assert_eq!(infer_type_from_value(&json!(true)), DataType::Boolean);
        assert_eq!(infer_type_from_value(&json!(null)), DataType::Text);
    }

    #[test]
    fn test_most_general_type() {
        assert_eq!(
            find_most_general_type(&[DataType::Integer, DataType::Float]),
            DataType::Float
        );

        assert_eq!(
            find_most_general_type(&[DataType::Integer, DataType::Text]),
            DataType::Text
        );

        assert_eq!(
            find_most_general_type(&[DataType::Boolean, DataType::Integer]),
            DataType::Text
        );
    }
}
