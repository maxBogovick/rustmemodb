/// A homogeneous collection of persisted entities.
pub struct PersistVec<T: PersistEntityFactory> {
    pub(crate) name: String,
    pub(crate) items: Vec<T>,
}

type DynamicCreateTableSql = Arc<dyn Fn(&str) -> String + Send + Sync>;
type DynamicFromState = Arc<dyn Fn(&PersistState) -> Result<Box<dyn PersistEntity>> + Send + Sync>;
type DynamicDefaultTableName = Arc<dyn Fn() -> String + Send + Sync>;
type DynamicMigrationPlan = Arc<dyn Fn() -> PersistMigrationPlan + Send + Sync>;
type DynamicSchemaVersion = Arc<dyn Fn() -> u32 + Send + Sync>;

struct PersistTypeRegistration {
    default_table_name: DynamicDefaultTableName,
    create_table_sql: DynamicCreateTableSql,
    from_state: DynamicFromState,
    migration_plan: DynamicMigrationPlan,
    schema_version: DynamicSchemaVersion,
}

/// A heterogeneous collection of persisted entities (polymorphic storage).
pub struct HeteroPersistVec {
    pub(crate) name: String,
    pub(crate) items: Vec<Box<dyn PersistEntity>>,
    registrations: HashMap<String, PersistTypeRegistration>,
}

/// Trait for values that can be persisted in the database.
pub trait PersistValue:
    Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static
{
    fn sql_type() -> &'static str;
    fn to_sql_literal(&self) -> String;
}

/// Generic JSON-backed persistence wrapper for nested/complex values.
///
/// Use this when you want to persist arbitrary serde-compatible structures
/// without writing local wrapper types or manual `PersistValue` impls.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct PersistJson<T>(pub T);

impl<T> PersistJson<T> {
    /// Builds a new JSON wrapper value.
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// Consumes the wrapper and returns the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> std::ops::Deref for PersistJson<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for PersistJson<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T> PersistValue for PersistJson<T>
where
    T: Clone + PartialEq + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn sql_type() -> &'static str {
        "TEXT"
    }

    fn to_sql_literal(&self) -> String {
        json_to_sql_literal(&self.0)
    }
}

/// Helper to convert a Serde error into a DbError.
pub fn serde_to_db_error(context: &str, err: serde_json::Error) -> DbError {
    DbError::ExecutionError(format!("{}: {}", context, err))
}

/// Converts a generic `Value` to a SQL literal string.
pub fn value_to_sql_literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(v) => v.to_string(),
        Value::Float(v) => v.to_string(),
        Value::Text(v) => format!("'{}'", sql_escape_string(v)),
        Value::Boolean(v) => {
            if *v {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        Value::Timestamp(v) => format!("'{}'", v.to_rfc3339()),
        Value::Date(v) => format!("'{}'", v.format("%Y-%m-%d")),
        Value::Uuid(v) => format!("'{}'", v),
        Value::Array(v) => {
            let json = serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string());
            format!("'{}'", sql_escape_string(&json))
        }
        Value::Json(v) => {
            let json = v.to_string();
            format!("'{}'", sql_escape_string(&json))
        }
    }
}

/// Checks if a generic `Value` is compatible with a given SQL type.
pub fn value_matches_sql_type(value: &Value, sql_type: &str) -> bool {
    if matches!(value, Value::Null) {
        return true;
    }

    let upper = sql_type.to_ascii_uppercase();
    let base = upper
        .split(['(', ' ', '\t'])
        .next()
        .unwrap_or_default()
        .to_string();

    match base.as_str() {
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" => matches!(value, Value::Integer(_)),
        "FLOAT" | "DOUBLE" | "REAL" | "NUMERIC" | "DECIMAL" => {
            matches!(value, Value::Integer(_) | Value::Float(_))
        }
        "TEXT" | "STRING" | "CHAR" | "VARCHAR" => matches!(value, Value::Text(_)),
        "BOOL" | "BOOLEAN" => matches!(value, Value::Boolean(_)),
        "TIMESTAMP" | "DATETIME" => matches!(value, Value::Timestamp(_) | Value::Text(_)),
        "DATE" => matches!(value, Value::Date(_) | Value::Text(_)),
        "UUID" => matches!(value, Value::Uuid(_) | Value::Text(_)),
        "JSON" | "JSONB" => matches!(value, Value::Json(_) | Value::Text(_)),
        _ => true,
    }
}
