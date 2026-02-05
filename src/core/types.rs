use super::{DbError, Result, Value};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
use std::fmt;
use chrono::{DateTime, NaiveDate, Utc, NaiveDateTime};
use uuid::Uuid;

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
    Unknown, // For parameter inference
}

impl DataType {
    pub fn is_compatible(&self, value: &Value) -> bool {
        match (self, value) {
            (_, Value::Null) => true,
            (Self::Unknown, _) => true, // Unknown accepts anything
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
            (Self::Unknown, _) => true, // Unknown can cast to anything (inferred)
            (_, Self::Unknown) => true,
            _ => false,
        }
    }

    pub fn cast_value(&self, value: &Value) -> Result<Value> {
        if matches!(value, Value::Null) {
            return Ok(Value::Null);
        }

        if self.is_exact_match(value) {
            return Ok(value.clone());
        }

        match (self, value) {
            (Self::Float, Value::Integer(i)) => Ok(Value::Float(*i as f64)),
            (Self::Integer, Value::Float(f)) => Ok(Value::Integer(*f as i64)),
            
            (Self::Timestamp, Value::Text(s)) => {
                if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
                    return Ok(Value::Timestamp(dt.with_timezone(&Utc)));
                }
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                     return Ok(Value::Timestamp(DateTime::from_utc(dt, Utc)));
                }
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                     return Ok(Value::Timestamp(DateTime::from_utc(dt, Utc)));
                }
                Err(DbError::TypeMismatch(format!("Invalid Timestamp format: {}", s)))
            },
            
            (Self::Date, Value::Text(s)) => {
                if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    Ok(Value::Date(d))
                } else {
                    Err(DbError::TypeMismatch(format!("Invalid Date format: {}", s)))
                }
            },
            
            (Self::Uuid, Value::Text(s)) => {
                if let Ok(u) = Uuid::parse_str(s) {
                    Ok(Value::Uuid(u))
                } else {
                    Err(DbError::TypeMismatch(format!("Invalid UUID format: {}", s)))
                }
            },
            
            (Self::Json, Value::Text(s)) => {
                if let Ok(json) = serde_json::from_str(s) {
                    Ok(Value::Json(json))
                } else {
                    Err(DbError::TypeMismatch(format!("Invalid JSON format: {}", s)))
                }
            },
            
            (Self::Array(inner), Value::Text(s)) => {
                // Basic array parsing "{a,b}"
                let trimmed = s.trim();
                if (trimmed.starts_with('{') && trimmed.ends_with('}')) ||
                   (trimmed.starts_with('[') && trimmed.ends_with(']')) {
                    let content = &trimmed[1..trimmed.len()-1];
                    if content.is_empty() {
                        return Ok(Value::Array(vec![]));
                    }
                    let parts: Vec<&str> = content.split(',').map(|p| p.trim()).collect();
                    let mut values = Vec::new();
                    for part in parts {
                        // TODO: handle quotes
                        let val = Value::Text(part.to_string());
                        values.push(inner.cast_value(&val)?);
                    }
                    Ok(Value::Array(values))
                } else {
                    Err(DbError::TypeMismatch(format!("Invalid Array format: {}", s)))
                }
            },

            (Self::Text, v) => Ok(Value::Text(v.to_string())),

            _ => Err(DbError::TypeMismatch(format!("Cannot cast {} to {}", value.type_name(), self))),
        }
    }

    fn is_exact_match(&self, value: &Value) -> bool {
        match (self, value) {
            (Self::Integer, Value::Integer(_)) => true,
            (Self::Float, Value::Float(_)) => true,
            (Self::Text, Value::Text(_)) => true,
            (Self::Boolean, Value::Boolean(_)) => true,
            (Self::Timestamp, Value::Timestamp(_)) => true,
            (Self::Date, Value::Date(_)) => true,
            (Self::Uuid, Value::Uuid(_)) => true,
            (Self::Json, Value::Json(_)) => true,
            (Self::Array(_), Value::Array(_)) => true, // Simplification: doesn't check inner types
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
            Self::Unknown => write!(f, "UNKNOWN"),
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

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema {
    pub(crate) columns: Vec<Column>,
    #[serde(skip, default)]
    cache: SchemaCache,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns, cache: SchemaCache::default() }
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn find_column_index(&self, name: &str) -> Option<usize> {
        let cache = self.cache.get_or_init(&self.columns);
        if let Some(idx) = cache.exact.get(name).copied() {
            return Some(idx);
        }

        if !name.contains('.') {
            return cache.unqualified.get(name).and_then(|idx| *idx);
        }

        let suffix = format!(".{}", name);
        let mut match_idx = None;
        for (idx, col) in self.columns.iter().enumerate() {
            if col.name.ends_with(&suffix) {
                if match_idx.is_some() {
                    return None;
                }
                match_idx = Some(idx);
            }
        }
        match_idx
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
        Self::new(columns)
    }

    pub fn qualify_columns(&self, table_name: &str) -> Self {
        let columns = self.columns.iter().map(|col| {
            let mut new_col = col.clone();
            if !new_col.name.contains('.') {
                new_col.name = format!("{}.{}", table_name, col.name);
            }
            new_col
        }).collect();
        Self::new(columns)
    }
}

impl Clone for Schema {
    fn clone(&self) -> Self {
        Self::new(self.columns.clone())
    }
}

#[derive(Debug, Default)]
struct SchemaCache {
    inner: OnceLock<SchemaCacheInner>,
}

#[derive(Debug)]
struct SchemaCacheInner {
    exact: HashMap<String, usize>,
    unqualified: HashMap<String, Option<usize>>,
}

impl SchemaCache {
    fn get_or_init(&self, columns: &[Column]) -> &SchemaCacheInner {
        self.inner.get_or_init(|| SchemaCacheInner::build(columns))
    }
}

impl SchemaCacheInner {
    fn build(columns: &[Column]) -> Self {
        let mut exact = HashMap::with_capacity(columns.len());
        let mut unqualified: HashMap<String, Option<usize>> = HashMap::new();

        for (idx, col) in columns.iter().enumerate() {
            exact.insert(col.name.clone(), idx);
            let key = col.name.rsplit('.').next().unwrap_or(&col.name);
            match unqualified.get(key) {
                None => {
                    unqualified.insert(key.to_string(), Some(idx));
                }
                Some(Some(_)) => {
                    unqualified.insert(key.to_string(), None);
                }
                Some(None) => {}
            }
        }

        Self { exact, unqualified }
    }
}
