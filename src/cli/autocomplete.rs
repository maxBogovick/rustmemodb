use rustmemodb::facade::InMemoryDB;
use std::collections::HashSet;

pub struct Autocompleter {
    keywords: HashSet<String>,
}

impl Autocompleter {
    pub fn new() -> Self {
        let keywords = vec![
            "SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "WHERE", "CREATE", "TABLE", 
            "DROP", "INDEX", "INTO", "VALUES", "AND", "OR", "NOT", "NULL", "TRUE", "FALSE",
            "INTEGER", "TEXT", "BOOLEAN", "FLOAT", "PRIMARY", "KEY", "UNIQUE", "ORDER", "BY",
            "LIMIT", "BEGIN", "COMMIT", "ROLLBACK", "LIKE", "BETWEEN", "IS", "IN",
            "COUNT", "SUM", "AVG", "MIN", "MAX"
        ];
        
        Self {
            keywords: keywords.into_iter().map(|s| s.to_string()).collect(),
        }
    }

    pub async fn get_suggestions(&self, input: &str, db: &InMemoryDB) -> Vec<String> {
        let input_upper = input.to_uppercase();
        let mut suggestions = Vec::new();

        // 1. SQL Keywords
        for kw in &self.keywords {
            if kw.starts_with(&input_upper) {
                suggestions.push(kw.clone());
            }
        }

        // 2. Table Names
        let tables = db.list_tables();
        for table in tables {
            if table.to_uppercase().starts_with(&input_upper) {
                suggestions.push(table);
            }
        }

        // 3. Columns (Simple implementation: suggest all columns from all tables for now)
        // Optimization: In a real IDE, we would parse context to know which table is active.
        // Here we just dump all known columns if they match.
        // We'd need to query stats or schema. The Facade might need a method to get schema.
        // For now, let's stick to Tables + Keywords as it's safe O(1)ish.
        
        suggestions.sort();
        suggestions
    }
}
