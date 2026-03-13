pub mod query;
pub use query::*;

use crate::core::Value;
/// Static metadata describing a struct field for OmniEntity reflection
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldMeta {
    /// Name of the column in the database and JSON field
    pub name: &'static str,
    /// DDL type (e.g. "VARCHAR(255)")
    pub sql_type: &'static str,
    /// Indicates if this field is a part of the Primary Key
    pub is_primary_key: bool,
    /// Indicates if this field can be NULL in the database
    pub is_nullable: bool,
    /// If true, this field cannot be modified via REST PATCH/POST
    pub rest_readonly: bool,
    /// If true, this field is not exposed in REST GET responses (e.g. password hash)
    pub rest_hidden: bool,
}

/// The core schema definition implemented by `#[derive(OmniEntity)]`.
/// Provides static metadata for DDL auto-generation and REST validation.
pub trait OmniSchema {
    /// The actual table name in the relational database
    fn table_name() -> &'static str;

    /// The static array of metadata describing the entity fields
    fn fields() -> &'static [FieldMeta];
}

/// The SQL mapping contract for OmniEntity.
/// Defines how Rust models are converted to/from SQL relational rows.
pub trait SqlEntity: OmniSchema + Sized {
    /// Generates an escaped SQL projection definition.
    /// E.g., `"usr"."id" AS "usr__id", "usr"."username" AS "usr__username"`
    fn sql_projection(alias: &str) -> String {
        Self::fields()
            .iter()
            .map(|f| format!("\"{}\".\"{}\" AS \"{}__{}\"", alias, f.name, alias, f.name))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Maps a raw SQL transaction row strictly by index/offset without string hashing allocations.
    /// `row`: flat array of values returned from the DB.
    /// `offset`: starting index for this specific entity (crucial for JOINs).
    fn from_sql_row(row: &[Value], offset: usize) -> Result<Option<Self>, String>;

    /// Extracts the current entity data to a vector of `Value`s for parameterized queries.
    /// Guaranteed to prevent SQL injection.
    fn to_sql_params(&self) -> Vec<Value>;
}

/// Represents an entity's corresponding changeset struct used for PATCH REST requests.
/// Implemented by the generated `{Entity}Patch` struct.
pub trait OmniEntityPatch {
    type Target: SqlEntity;

    /// Applies the populated Option fields of this patch onto the target entity.
    fn apply_to(&self, target: &mut Self::Target);

    /// Returns the names and new values of fields that were actually modified in the patch.
    fn changed_fields(&self) -> Vec<(&'static str, Value)>;
}

/// Enables transparent conversion between Rust Enums/Newtypes and Database Native Types.
/// Typically derived via `#[derive(OmniValue)]`.
pub trait OmniValue: Sized {
    /// The underlying raw database type this maps into.
    fn into_db_value(self) -> Value;

    /// Constructs this wrapper logically from a raw database value.
    fn from_db_value(val: Value) -> Result<Self, String>;
}

// Basic impls of OmniValue for raw primitives for the macro to hit seamlessly

impl OmniValue for String {
    fn into_db_value(self) -> Value {
        Value::Text(self)
    }
    fn from_db_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Text(s) => Ok(s),
            _ => Err("Expected Text".to_string()),
        }
    }
}

impl OmniValue for i32 {
    fn into_db_value(self) -> Value {
        Value::Integer(self as i64)
    }
    fn from_db_value(val: Value) -> Result<Self, String> {
        match val {
            Value::Integer(i) => Ok(i as i32),
            _ => Err("Expected Integer (i32)".to_string()),
        }
    }
}

// Add more primitive mapping based on the specific Value variants later if needed.

impl<T: OmniValue> OmniValue for Option<T> {
    fn into_db_value(self) -> Value {
        match self {
            Some(v) => v.into_db_value(),
            None => Value::Null,
        }
    }

    fn from_db_value(val: Value) -> Result<Self, String> {
        if matches!(val, Value::Null) {
            Ok(None)
        } else {
            Ok(Some(T::from_db_value(val)?))
        }
    }
}
