//! SQL Query Validator
//!
//! Validates SQL queries for security and correctness, preventing
//! SQL injection and ensuring queries only access authorized collections.
//! Uses the Chain of Responsibility pattern for validation rules.

use crate::parser::SqlParserAdapter;
use crate::parser::ast::Statement;
use super::error::{JsonError, JsonResult};

/// Trait for validation rules (Chain of Responsibility pattern)
pub trait ValidationRule: Send + Sync {
    fn validate(&self, query: &str, collection_name: &str, parsed: &Statement) -> JsonResult<()>;
}

/// Validates that query is a SELECT statement
#[derive(Debug, Clone, Default)]
pub struct SelectOnlyRule;

impl ValidationRule for SelectOnlyRule {
    fn validate(&self, _query: &str, _collection_name: &str, parsed: &Statement) -> JsonResult<()> {
        match parsed {
            Statement::Query(_) => Ok(()),
            _ => Err(JsonError::ValidationError(
                "Only SELECT queries are allowed in read() method".to_string()
            )),
        }
    }
}

/// Validates that query only accesses the specified collection
#[derive(Debug, Clone, Default)]
pub struct SingleTableRule;

impl ValidationRule for SingleTableRule {
    fn validate(&self, _query: &str, collection_name: &str, parsed: &Statement) -> JsonResult<()> {
        let tables = extract_table_names(parsed);

        if tables.is_empty() {
            return Err(JsonError::ValidationError(
                "Query must specify a table".to_string()
            ));
        }

        if tables.len() > 1 {
            return Err(JsonError::ValidationError(
                format!("Query can only access collection '{}', but found multiple tables", collection_name)
            ));
        }

        if tables[0] != collection_name {
            return Err(JsonError::ValidationError(
                format!(
                    "Query must access collection '{}', but found '{}'",
                    collection_name, tables[0]
                )
            ));
        }

        Ok(())
    }
}

/// Validates against common SQL injection patterns
#[derive(Debug, Clone, Default)]
pub struct SqlInjectionRule;

impl ValidationRule for SqlInjectionRule {
    fn validate(&self, query: &str, _collection_name: &str, _parsed: &Statement) -> JsonResult<()> {
        let query_lower = query.to_lowercase();

        // Check for dangerous keywords that shouldn't appear in read queries
        let dangerous_patterns = [
            "drop table",
            "drop database",
            "truncate",
            "alter table",
            "create table",
            "delete from",
            "update ",
            "insert into",
            "grant ",
            "revoke ",
            "exec ",
            "execute ",
            "xp_",
            "sp_",
            "into outfile",
            "into dumpfile",
            "load_file",
        ];

        for pattern in &dangerous_patterns {
            if query_lower.contains(pattern) {
                return Err(JsonError::SqlInjectionAttempt(
                    format!("Dangerous pattern detected: {}", pattern)
                ));
            }
        }

        // Check for comment injection attempts
        if query.contains("--") || query.contains("/*") || query.contains("*/") {
            return Err(JsonError::SqlInjectionAttempt(
                "SQL comments are not allowed".to_string()
            ));
        }

        // Check for semicolon (query chaining)
        if query.matches(';').count() > 1 {
            return Err(JsonError::SqlInjectionAttempt(
                "Multiple statements (semicolons) are not allowed".to_string()
            ));
        }

        Ok(())
    }
}

/// Main query validator using Chain of Responsibility
pub struct QueryValidator {
    rules: Vec<Box<dyn ValidationRule>>,
    parser: SqlParserAdapter,
}

impl QueryValidator {
    /// Create validator with default rules
    pub fn new() -> Self {
        Self {
            rules: vec![
                Box::new(SqlInjectionRule),
                Box::new(SelectOnlyRule),
                Box::new(SingleTableRule),
            ],
            parser: SqlParserAdapter::new(),
        }
    }

    /// Create validator with custom rules
    #[allow(dead_code)]
    pub fn with_rules(rules: Vec<Box<dyn ValidationRule>>) -> Self {
        Self {
            rules,
            parser: SqlParserAdapter::new(),
        }
    }

    /// Validate a query against all rules
    pub fn validate(&self, query: &str, collection_name: &str) -> JsonResult<()> {
        // First, try to parse the query
        let statements = self.parser.parse(query)
            .map_err(|e| JsonError::ValidationError(format!("Invalid SQL: {}", e)))?;

        if statements.is_empty() {
            return Err(JsonError::ValidationError("Empty query".to_string()));
        }

        if statements.len() > 1 {
            return Err(JsonError::ValidationError(
                "Multiple statements not allowed".to_string()
            ));
        }

        let statement = &statements[0];

        // Apply all validation rules
        for rule in &self.rules {
            rule.validate(query, collection_name, statement)?;
        }

        Ok(())
    }
}

impl Default for QueryValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Extract table names from a parsed statement
fn extract_table_names(statement: &Statement) -> Vec<String> {
    match statement {
        Statement::Query(query) => {
            query.from.iter().map(|t| {
                match &t.relation {
                    crate::parser::ast::TableFactor::Table { name, .. } => name.clone(),
                }
            }).collect()
        }
        Statement::Insert(insert) => {
            vec![insert.table_name.clone()]
        }
        Statement::Update(update) => {
            vec![update.table_name.clone()]
        }
        Statement::Delete(delete) => {
            vec![delete.table_name.clone()]
        }
        Statement::CreateTable(create) => {
            vec![create.table_name.clone()]
        }
        Statement::DropTable(drop) => {
            vec![drop.table_name.clone()]
        }
        _ => vec![],
    }
}

/// Validates collection names to prevent injection
pub fn validate_collection_name(name: &str) -> JsonResult<()> {
    if name.is_empty() {
        return Err(JsonError::InvalidCollectionName("Collection name cannot be empty".to_string()));
    }

    // Must start with letter or underscore
    if !name.chars().next().unwrap().is_alphabetic() && !name.starts_with('_') {
        return Err(JsonError::InvalidCollectionName(
            "Collection name must start with a letter or underscore".to_string()
        ));
    }

    // Can only contain alphanumeric and underscores
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(JsonError::InvalidCollectionName(
            "Collection name can only contain letters, numbers, and underscores".to_string()
        ));
    }

    // Check length
    if name.len() > 64 {
        return Err(JsonError::InvalidCollectionName(
            "Collection name too long (max 64 characters)".to_string()
        ));
    }

    // Reject SQL keywords
    let sql_keywords = [
        "SELECT", "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER",
        "TABLE", "FROM", "WHERE", "JOIN", "UNION", "ORDER", "GROUP",
    ];

    if sql_keywords.iter().any(|&kw| name.eq_ignore_ascii_case(kw)) {
        return Err(JsonError::InvalidCollectionName(
            format!("Collection name cannot be SQL keyword: {}", name)
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_collection_names() {
        assert!(validate_collection_name("users").is_ok());
        assert!(validate_collection_name("user_profiles").is_ok());
        assert!(validate_collection_name("_internal").is_ok());
        assert!(validate_collection_name("data123").is_ok());
    }

    #[test]
    fn test_invalid_collection_names() {
        assert!(validate_collection_name("").is_err());
        assert!(validate_collection_name("123users").is_err());
        assert!(validate_collection_name("user-profile").is_err());
        assert!(validate_collection_name("user profile").is_err());
        assert!(validate_collection_name("SELECT").is_err());
        assert!(validate_collection_name("DROP").is_err());
    }

    #[test]
    fn test_query_validator_valid() {
        let validator = QueryValidator::new();

        assert!(validator.validate(
            "SELECT * FROM users WHERE age > 18",
            "users"
        ).is_ok());

        assert!(validator.validate(
            "SELECT id, name FROM users",
            "users"
        ).is_ok());
    }

    #[test]
    fn test_query_validator_wrong_table() {
        let validator = QueryValidator::new();

        let result = validator.validate(
            "SELECT * FROM products",
            "users"
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_query_validator_not_select() {
        let validator = QueryValidator::new();

        let result = validator.validate(
            "DELETE FROM users WHERE id = 1",
            "users"
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_sql_injection_detection() {
        let validator = QueryValidator::new();

        // SQL injection attempts
        assert!(validator.validate(
            "SELECT * FROM users; DROP TABLE users",
            "users"
        ).is_err());

        assert!(validator.validate(
            "SELECT * FROM users WHERE id = 1 OR 1=1 --",
            "users"
        ).is_err());

        assert!(validator.validate(
            "SELECT * FROM users /* comment */ WHERE id = 1",
            "users"
        ).is_err());
    }
}
