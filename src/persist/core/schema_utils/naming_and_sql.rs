/// Sanitizes a string for use as a SQL identifier (table/column name).
///
/// Converts non-alphanumeric characters to underscores and lowercases the result.
fn sanitize_sql_identifier(input: &str) -> String {
    let mut sanitized = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() {
            sanitized.push(ch.to_ascii_lowercase());
        } else {
            sanitized.push('_');
        }
    }
    sanitized
}

/// Generates a default table name based on type name and call site location.
///
/// Format: `persist_<type>_<line>_<col>`
pub fn default_table_name(type_name: &str, line: u32, column: u32) -> String {
    let sanitized = sanitize_sql_identifier(type_name);
    format!("persist_{}_{}_{}", sanitized, line, column)
}

/// Generates a stable default table name based only on the type name.
///
/// Format: `persist_<type>`
pub fn default_table_name_stable(type_name: &str) -> String {
    let sanitized = sanitize_sql_identifier(type_name);
    format!("persist_{}", sanitized)
}

/// Generates a default index name.
///
/// Format: `idx_<table>_<field>`
pub fn default_index_name(table_name: &str, field_name: &str) -> String {
    let table = sanitize_sql_identifier(table_name);
    let field = sanitize_sql_identifier(field_name);
    format!("idx_{}_{}", table, field)
}

/// Generates a new unique persistence ID (UUID v4).
pub fn new_persist_id() -> String {
    Uuid::new_v4().to_string()
}

/// Escapes a string for inclusion in a SQL literal.
///
/// Replaces single quotes with double single quotes.
pub fn sql_escape_string(value: &str) -> String {
    value.replace('\'', "''")
}

/// Serializes a value to JSON and escapes it for SQL inclusion.
pub fn json_to_sql_literal<T: Serialize>(value: &T) -> String {
    let json = serde_json::to_string(value)
        .expect("Persist JSON serialization failed in json_to_sql_literal");
    format!("'{}'", sql_escape_string(&json))
}
