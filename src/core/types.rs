use super::{DbError, Result, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::fmt;

pub type Row = Vec<Value>;

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub tx_id: u64,
    pub active: Arc<HashSet<u64>>,
    pub aborted: Arc<HashSet<u64>>,
    pub max_tx_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
    Timestamp,
    Date,
    Uuid,
    Array(Box<DataType>),
    Json,
}

impl DataType {
    pub fn is_compatible(&self, value: &Value) -> bool {
        match (self, value) {
            (_, Value::Null) => true,
            (Self::Integer, Value::Integer(_)) => true,
            (Self::Float, Value::Float(_)) => true,
            (Self::Float, Value::Integer(_)) => true, // Allow Integer -> Float coercion
            (Self::Text, Value::Text(_)) => true,
            (Self::Boolean, Value::Boolean(_)) => true,
            (Self::Timestamp, Value::Timestamp(_)) => true,
            (Self::Date, Value::Date(_)) => true,
            (Self::Uuid, Value::Uuid(_)) => true,
            // Allow string parsing for complex types
            (Self::Timestamp, Value::Text(_)) => true,
            (Self::Date, Value::Text(_)) => true,
            (Self::Uuid, Value::Text(_)) => true,

            (Self::Array(elem_type), Value::Array(arr)) => {
                arr.iter().all(|v| elem_type.is_compatible(v))
            }
            (Self::Json, Value::Json(_)) => true,
            (Self::Json, Value::Text(_)) => true, // Allow parsing JSON from text

            _ => false,
        }
    }

    pub fn can_cast_to(&self, other: &DataType) -> bool {
        match (self, other) {
            (a, b) if a == b => true,
            (Self::Integer, Self::Float) => true,
            (Self::Integer, Self::Text) => true,
            (Self::Float, Self::Text) => true,
            (Self::Boolean, Self::Text) => true,
            (Self::Timestamp, Self::Text) => true,
            (Self::Date, Self::Text) => true,
            (Self::Uuid, Self::Text) => true,
            (Self::Text, Self::Uuid) => true,
            (Self::Text, Self::Timestamp) => true,
            (Self::Text, Self::Date) => true,
            (Self::Text, Self::Json) => true,
            (Self::Json, Self::Text) => true,
            _ => false,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer => write!(f, "INTEGER"),
            Self::Float => write!(f, "FLOAT"),
            Self::Text => write!(f, "TEXT"),
            Self::Boolean => write!(f, "BOOLEAN"),
            Self::Timestamp => write!(f, "TIMESTAMP"),
            Self::Date => write!(f, "DATE"),
            Self::Uuid => write!(f, "UUID"),
            Self::Array(t) => write!(f, "{}[]", t),
            Self::Json => write!(f, "JSONB"),
        }
    }
}

// Foreign Key Definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ForeignKey {
    pub table: String,
    pub column: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub references: Option<ForeignKey>, // New field
    pub default: Option<Value>
}

impl Column {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            primary_key: false,
            unique: false,
            references: None,
            default: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self.unique = true;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn references(mut self, table: impl Into<String>, column: impl Into<String>) -> Self {
        self.references = Some(ForeignKey {
            table: table.into(),
            column: column.into(),
        });
        self
    }

    pub fn validate(&self, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            if !self.nullable {
                return Err(DbError::ConstraintViolation(format!(
                    "Column '{}' cannot be NULL",
                    self.name
                )));
            }
            return Ok(());
        }

        if !self.data_type.is_compatible(value) {
            return Err(DbError::TypeMismatch(format!(
                "Column '{}' expects type {}, got {}",
                self.name,
                self.data_type,
                value.type_name()
            )));
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub(crate) columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn find_column_index(&self, name: &str) -> Option<usize> {
        // 1. Exact match
        if let Some(idx) = self.columns.iter().position(|col| col.name == name) {
            return Some(idx);
        }

        // 2. Unqualified match (suffix)
        let matches: Vec<usize> = self.columns
            .iter()
            .enumerate()
            .filter(|(_, col)| col.name.ends_with(&format!(".{}", name)))
            .map(|(idx, _)| idx)
            .collect();

        if matches.len() == 1 {
            return Some(matches[0]);
        }

        None
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.find_column_index(name).map(|idx| &self.columns[idx])
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    pub fn merge(left: &Schema, right: &Schema) -> Self {
        let mut columns = left.columns.clone();
        columns.extend(right.columns.clone());
        Self { columns }
    }

    pub fn qualify_columns(&self, table_name: &str) -> Self {
        let columns = self.columns.iter().map(|col| {
            let mut new_col = col.clone();
            if !new_col.name.contains('.') {
                new_col.name = format!("{}.{}", table_name, col.name);
            }
            new_col
        }).collect();
        Self { columns }
    }
}
