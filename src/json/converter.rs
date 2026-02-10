//! JSON to SQL Converter
//!
//! Converts JSON documents to SQL statements using the Builder pattern
//! for flexible and maintainable SQL generation.

use super::error::{JsonError, JsonResult};
use crate::core::{DataType, Value};
use crate::storage::TableSchema;
use serde_json::Value as JsonValue;

/// Converts JSON values to SQL Value enum
pub struct JsonToValueConverter;

impl JsonToValueConverter {
    /// Convert a single JSON value to SQL Value
    pub fn convert(json_value: &JsonValue, expected_type: &DataType) -> JsonResult<Value> {
        match (json_value, expected_type) {
            // NULL handling
            (JsonValue::Null, _) => Ok(Value::Null),

            // Boolean
            (JsonValue::Bool(b), DataType::Boolean) => Ok(Value::Boolean(*b)),

            // Integer
            (JsonValue::Number(n), DataType::Integer) => n
                .as_i64()
                .map(Value::Integer)
                .ok_or_else(|| JsonError::TypeMismatch(format!("Cannot convert {} to INTEGER", n))),

            // Float (can accept integers too)
            (JsonValue::Number(n), DataType::Float) => n
                .as_f64()
                .map(Value::Float)
                .ok_or_else(|| JsonError::TypeMismatch(format!("Cannot convert {} to FLOAT", n))),

            // String
            (JsonValue::String(s), DataType::Text) => Ok(Value::Text(s.clone())),

            // Timestamp
            (JsonValue::String(s), DataType::Timestamp) => {
                let dt = chrono::DateTime::parse_from_rfc3339(s)
                    .map_err(|e| JsonError::TypeMismatch(format!("Invalid Timestamp: {}", e)))?;
                Ok(Value::Timestamp(dt.with_timezone(&chrono::Utc)))
            }

            // Date
            (JsonValue::String(s), DataType::Date) => {
                let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| JsonError::TypeMismatch(format!("Invalid Date: {}", e)))?;
                Ok(Value::Date(d))
            }

            // UUID
            (JsonValue::String(s), DataType::Uuid) => {
                let u = uuid::Uuid::parse_str(s)
                    .map_err(|e| JsonError::TypeMismatch(format!("Invalid UUID: {}", e)))?;
                Ok(Value::Uuid(u))
            }

            // JSON
            (v, DataType::Json) => Ok(Value::Json(v.clone())),

            // Array
            (JsonValue::Array(arr), DataType::Array(inner_type)) => {
                let mut values = Vec::new();
                for v in arr {
                    values.push(Self::convert(v, inner_type)?);
                }
                Ok(Value::Array(values))
            }

            // Complex types (Array/Object) → serialize to JSON string if target is TEXT
            (JsonValue::Array(_) | JsonValue::Object(_), DataType::Text) => {
                Ok(Value::Text(json_value.to_string()))
            }

            // Type coercion: any JSON type can be converted to TEXT
            (_, DataType::Text) => Ok(Value::Text(json_value.to_string())),

            // Type mismatch
            _ => Err(JsonError::TypeMismatch(format!(
                "Cannot convert JSON {} to SQL type {}",
                json_value, expected_type
            ))),
        }
    }

    /// Convert JSON object to Row according to schema
    pub fn json_to_row(json_obj: &JsonValue, schema: &TableSchema) -> JsonResult<Vec<Value>> {
        let obj = json_obj
            .as_object()
            .ok_or_else(|| JsonError::InvalidStructure("Expected JSON object".into()))?;

        let mut row = Vec::with_capacity(schema.schema().column_count());

        for column in schema.schema().columns() {
            let json_value = obj.get(&column.name).unwrap_or(&JsonValue::Null);

            let value = Self::convert(json_value, &column.data_type)?;

            // Validate nullable constraint
            if matches!(value, Value::Null) && !column.nullable {
                return Err(JsonError::MissingField(format!(
                    "Field '{}' is required but missing",
                    column.name
                )));
            }

            row.push(value);
        }

        Ok(row)
    }
}

/// Builder for CREATE TABLE SQL statements
pub struct CreateTableBuilder {
    table_name: String,
    columns: Vec<(String, DataType, bool)>, // (name, type, nullable)
}

impl CreateTableBuilder {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: Vec::new(),
        }
    }

    pub fn add_column(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
    ) -> Self {
        self.columns.push((name.into(), data_type, nullable));
        self
    }

    pub fn from_schema(schema: &TableSchema) -> Self {
        let mut builder = Self::new(schema.name());

        for column in schema.schema().columns() {
            builder = builder.add_column(
                column.name.clone(),
                column.data_type.clone(),
                column.nullable,
            );
        }

        builder
    }

    pub fn build(self) -> String {
        let column_defs: Vec<String> = self
            .columns
            .iter()
            .map(|(name, dtype, nullable)| {
                let null_constraint = if *nullable { "" } else { " NOT NULL" };
                format!("{} {}{}", quote_ident(name), dtype, null_constraint)
            })
            .collect();

        format!(
            "CREATE TABLE {} ({})",
            quote_ident(&self.table_name),
            column_defs.join(", ")
        )
    }
}

/// Builder for INSERT SQL statements
pub struct InsertStatementBuilder {
    table_name: String,
    columns: Vec<String>,
    values: Vec<Vec<Value>>,
}

impl InsertStatementBuilder {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn columns(mut self, columns: Vec<String>) -> Self {
        self.columns = columns;
        self
    }

    #[allow(dead_code)]
    pub fn add_row(mut self, row: Vec<Value>) -> Self {
        self.values.push(row);
        self
    }

    pub fn rows(mut self, rows: Vec<Vec<Value>>) -> Self {
        self.values = rows;
        self
    }

    /// Build a single INSERT statement (for small batches)
    #[allow(dead_code)]
    pub fn build_single(self) -> String {
        if self.values.is_empty() {
            return String::new();
        }

        let column_list = if self.columns.is_empty() {
            String::new()
        } else {
            let cols: Vec<String> = self.columns.iter().map(|c| quote_ident(c)).collect();
            format!(" ({})", cols.join(", "))
        };

        let values_str: Vec<String> = self
            .values
            .iter()
            .map(|row| {
                let values: Vec<String> = row.iter().map(format_value_for_sql).collect();
                format!("({})", values.join(", "))
            })
            .collect();

        format!(
            "INSERT INTO {}{} VALUES {}",
            quote_ident(&self.table_name),
            column_list,
            values_str.join(", ")
        )
    }

    /// Build multiple INSERT statements (for large batches to avoid query size limits)
    pub fn build_batched(self, batch_size: usize) -> Vec<String> {
        if self.values.is_empty() {
            return Vec::new();
        }

        let column_list = if self.columns.is_empty() {
            String::new()
        } else {
            let cols: Vec<String> = self.columns.iter().map(|c| quote_ident(c)).collect();
            format!(" ({})", cols.join(", "))
        };

        self.values
            .chunks(batch_size)
            .map(|chunk| {
                let values_str: Vec<String> = chunk
                    .iter()
                    .map(|row| {
                        let values: Vec<String> = row.iter().map(format_value_for_sql).collect();
                        format!("({})", values.join(", "))
                    })
                    .collect();

                format!(
                    "INSERT INTO {}{} VALUES {}",
                    quote_ident(&self.table_name),
                    column_list,
                    values_str.join(", ")
                )
            })
            .collect()
    }
}

/// Builder for UPDATE SQL statements
pub struct UpdateStatementBuilder {
    table_name: String,
    set_clause: Vec<(String, Value)>,
    where_clause: Option<String>,
}

impl UpdateStatementBuilder {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            set_clause: Vec::new(),
            where_clause: None,
        }
    }

    pub fn set(mut self, column: impl Into<String>, value: Value) -> Self {
        self.set_clause.push((column.into(), value));
        self
    }

    #[allow(dead_code)]
    pub fn set_multiple(mut self, updates: Vec<(String, Value)>) -> Self {
        self.set_clause.extend(updates);
        self
    }

    pub fn where_clause(mut self, condition: impl Into<String>) -> Self {
        self.where_clause = Some(condition.into());
        self
    }

    pub fn build(self) -> String {
        let set_parts: Vec<String> = self
            .set_clause
            .iter()
            .map(|(col, val)| format!("{} = {}", quote_ident(col), format_value_for_sql(val)))
            .collect();

        let where_part = self
            .where_clause
            .map(|w| format!(" WHERE {}", w))
            .unwrap_or_default();

        format!(
            "UPDATE {} SET {}{}",
            quote_ident(&self.table_name),
            set_parts.join(", "),
            where_part
        )
    }
}

/// Builder for DELETE SQL statements
pub struct DeleteStatementBuilder {
    table_name: String,
    where_clause: Option<String>,
}

impl DeleteStatementBuilder {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            where_clause: None,
        }
    }

    pub fn where_clause(mut self, condition: impl Into<String>) -> Self {
        self.where_clause = Some(condition.into());
        self
    }

    pub fn build(self) -> String {
        let where_part = self
            .where_clause
            .map(|w| format!(" WHERE {}", w))
            .unwrap_or_default();

        format!(
            "DELETE FROM {}{}",
            quote_ident(&self.table_name),
            where_part
        )
    }
}

/// Format SQL Value for SQL statement
fn format_value_for_sql(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Boolean(b) => b.to_string(),
        Value::Text(s) => format!("'{}'", escape_sql_string(s)),
        Value::Timestamp(t) => format!("'{}'", t.to_rfc3339()),
        Value::Date(d) => format!("'{}'", d.format("%Y-%m-%d")),
        Value::Uuid(u) => format!("'{}'", u),
        Value::Array(a) => {
            // Format as ARRAY[...]
            let elements: Vec<String> = a.iter().map(format_value_for_sql).collect();
            format!("ARRAY[{}]", elements.join(", "))
        }
        Value::Json(j) => format!("'{}'", escape_sql_string(&j.to_string())),
    }
}

/// Escape single quotes in SQL strings
fn escape_sql_string(s: &str) -> String {
    s.replace('\'', "''")
}

fn quote_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_to_value_converter() {
        assert_eq!(
            JsonToValueConverter::convert(&json!(42), &DataType::Integer).unwrap(),
            Value::Integer(42)
        );

        assert_eq!(
            JsonToValueConverter::convert(&json!(3.14), &DataType::Float).unwrap(),
            Value::Float(3.14)
        );

        assert_eq!(
            JsonToValueConverter::convert(&json!("hello"), &DataType::Text).unwrap(),
            Value::Text("hello".to_string())
        );

        assert_eq!(
            JsonToValueConverter::convert(&json!(true), &DataType::Boolean).unwrap(),
            Value::Boolean(true)
        );

        assert_eq!(
            JsonToValueConverter::convert(&json!(null), &DataType::Text).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn test_create_table_builder() {
        let sql = CreateTableBuilder::new("users")
            .add_column("id", DataType::Integer, false)
            .add_column("name", DataType::Text, true)
            .build();

        assert_eq!(
            sql,
            "CREATE TABLE \"users\" (\"id\" INTEGER NOT NULL, \"name\" TEXT)"
        );
    }

    #[test]
    fn test_insert_statement_builder() {
        let sql = InsertStatementBuilder::new("users")
            .columns(vec!["id".to_string(), "name".to_string()])
            .add_row(vec![Value::Integer(1), Value::Text("Alice".to_string())])
            .add_row(vec![Value::Integer(2), Value::Text("Bob".to_string())])
            .build_single();

        assert_eq!(
            sql,
            "INSERT INTO \"users\" (\"id\", \"name\") VALUES (1, 'Alice'), (2, 'Bob')"
        );
    }

    #[test]
    fn test_update_statement_builder() {
        let sql = UpdateStatementBuilder::new("users")
            .set("name", Value::Text("Alice Smith".to_string()))
            .set("age", Value::Integer(31))
            .where_clause("id = 1")
            .build();

        assert_eq!(
            sql,
            "UPDATE \"users\" SET \"name\" = 'Alice Smith', \"age\" = 31 WHERE id = 1"
        );
    }

    #[test]
    fn test_delete_statement_builder() {
        let sql = DeleteStatementBuilder::new("users")
            .where_clause("id = 1")
            .build();

        assert_eq!(sql, "DELETE FROM \"users\" WHERE id = 1");
    }

    #[test]
    fn test_sql_string_escaping() {
        let value = Value::Text("O'Reilly".to_string());
        assert_eq!(format_value_for_sql(&value), "'O''Reilly'");
    }
}
