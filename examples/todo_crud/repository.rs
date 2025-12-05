/// Repository layer for data access
///
/// Implements the Repository pattern to abstract database operations.
/// This provides a clean separation between business logic and data access.

use crate::models::{Priority, Status, Todo, TodoFilter};
use chrono::{DateTime, Utc};
use rustmemodb::{Client, QueryResult, Result as DbResult, Value};
use std::sync::Arc;

/// Repository for Todo entity - provides CRUD operations
pub struct TodoRepository {
    client: Arc<Client>,
}

impl TodoRepository {
    /// Create a new TodoRepository instance
    pub fn new(client: Arc<Client>) -> DbResult<Self> {
        let repo = Self { client };
        repo.init_schema()?;
        Ok(repo)
    }

    /// Initialize database schema
    ///
    /// Creates the todos table if it doesn't exist.
    /// Uses best practices: proper column types, NOT NULL constraints where appropriate.
    fn init_schema(&self) -> DbResult<()> {
        self.client.execute(
            "CREATE TABLE IF NOT EXISTS todos (
                id INTEGER,
                title TEXT,
                description TEXT,
                priority INTEGER,
                status TEXT,
                created_at INTEGER,
                updated_at INTEGER,
                completed_at INTEGER
            )",
        )?;
        Ok(())
    }

    /// Create a new todo
    ///
    /// # Arguments
    /// * `todo` - The todo to create
    ///
    /// # Returns
    /// * `Ok(())` on success
    pub fn create(&self, todo: &Todo) -> DbResult<()> {
        let sql = format!(
            "INSERT INTO todos VALUES ({}, '{}', {}, {}, '{}', {}, {}, {})",
            todo.id,
            self.escape_string(&todo.title),
            self.format_optional_string(&todo.description),
            todo.priority.to_i64(),
            todo.status.to_string_value(),
            todo.created_at.timestamp(),
            todo.updated_at.timestamp(),
            self.format_optional_timestamp(todo.completed_at),
        );

        self.client.execute(&sql)?;
        Ok(())
    }

    /// Find todo by ID
    pub fn find_by_id(&self, id: i64) -> DbResult<Option<Todo>> {
        let sql = format!("SELECT * FROM todos WHERE id = {}", id);
        let result = self.client.query(&sql)?;

        if result.row_count() == 0 {
            return Ok(None);
        }

        let todo = self.row_to_todo(result.rows().first().unwrap())?;
        Ok(Some(todo))
    }

    /// Find all todos with optional filtering
    pub fn find_all(&self, filter: &TodoFilter) -> DbResult<Vec<Todo>> {
        let sql = self.build_filter_query(filter);
        let result = self.client.query(&sql)?;

        let todos: DbResult<Vec<Todo>> = result.rows().iter().map(|row| self.row_to_todo(row)).collect();
        todos
    }

    /// Update an existing todo
    pub fn update(&self, todo: &Todo) -> DbResult<()> {
        let sql = format!(
            "UPDATE todos SET
                title = '{}',
                description = {},
                priority = {},
                status = '{}',
                updated_at = {},
                completed_at = {}
             WHERE id = {}",
            self.escape_string(&todo.title),
            self.format_optional_string(&todo.description),
            todo.priority.to_i64(),
            todo.status.to_string_value(),
            todo.updated_at.timestamp(),
            self.format_optional_timestamp(todo.completed_at),
            todo.id
        );

        self.client.execute(&sql)?;
        Ok(())
    }

    /// Delete todo by ID
    pub fn delete(&self, id: i64) -> DbResult<()> {
        let sql = format!("DELETE FROM todos WHERE id = {}", id);
        self.client.execute(&sql)?;
        Ok(())
    }

    /// Delete all completed todos
    pub fn delete_completed(&self) -> DbResult<usize> {
        let count_result = self.client.query("SELECT * FROM todos WHERE status = 'done'")?;
        let count = count_result.row_count();

        self.client.execute("DELETE FROM todos WHERE status = 'done'")?;
        Ok(count)
    }

    /// Get next available ID
    ///
    /// This is a simple auto-increment simulation.
    /// In production, you might want to use a sequence or UUID.
    pub fn next_id(&self) -> DbResult<i64> {
        let result = self.client.query("SELECT * FROM todos ORDER BY id DESC LIMIT 1")?;

        if result.row_count() == 0 {
            return Ok(1);
        }

        if let Some(row) = result.rows().first() {
            if let Some(Value::Integer(id)) = row.first() {
                return Ok(id + 1);
            }
        }

        Ok(1)
    }

    /// Get statistics about todos
    pub fn stats(&self) -> DbResult<TodoStats> {
        let total = self.client.query("SELECT * FROM todos")?.row_count();
        let pending = self.client.query("SELECT * FROM todos WHERE status = 'pending'")?.row_count();
        let in_progress = self
            .client
            .query("SELECT * FROM todos WHERE status = 'in_progress'")?
            .row_count();
        let completed = self.client.query("SELECT * FROM todos WHERE status = 'done'")?.row_count();

        Ok(TodoStats {
            total,
            pending,
            in_progress,
            completed,
        })
    }

    // Helper methods

    /// Build SQL query with filters
    fn build_filter_query(&self, filter: &TodoFilter) -> String {
        let mut sql = "SELECT * FROM todos".to_string();
        let mut conditions = Vec::new();

        if let Some(status) = &filter.status {
            conditions.push(format!("status = '{}'", status.to_string_value()));
        }

        if let Some(priority) = &filter.priority {
            conditions.push(format!("priority = {}", priority.to_i64()));
        }

        if let Some(search) = &filter.search_term {
            let escaped = self.escape_string(search);
            conditions.push(format!("(title LIKE '%{}%' OR description LIKE '%{}%')", escaped, escaped));
        }

        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        sql.push_str(" ORDER BY priority DESC, created_at DESC");
        sql
    }

    /// Convert database row to Todo model
    fn row_to_todo(&self, row: &[Value]) -> DbResult<Todo> {
        let id = self.extract_i64(&row[0])?;
        let title = self.extract_string(&row[1])?;
        let description = self.extract_optional_string(&row[2]);
        let priority = Priority::from_i64(self.extract_i64(&row[3])?)
            .ok_or_else(|| rustmemodb::DbError::ExecutionError("Invalid priority".into()))?;
        let status = Status::from_string_value(&self.extract_string(&row[4])?)
            .ok_or_else(|| rustmemodb::DbError::ExecutionError("Invalid status".into()))?;
        let created_at = self.timestamp_to_datetime(self.extract_i64(&row[5])?);
        let updated_at = self.timestamp_to_datetime(self.extract_i64(&row[6])?);
        let completed_at = self.extract_optional_i64(&row[7]).map(|ts| self.timestamp_to_datetime(ts));

        Ok(Todo {
            id,
            title,
            description,
            priority,
            status,
            created_at,
            updated_at,
            completed_at,
        })
    }

    // Value extraction helpers

    fn extract_i64(&self, value: &Value) -> DbResult<i64> {
        match value {
            Value::Integer(i) => Ok(*i),
            _ => Err(rustmemodb::DbError::ExecutionError("Expected integer".into())),
        }
    }

    fn extract_optional_i64(&self, value: &Value) -> Option<i64> {
        match value {
            Value::Integer(i) => Some(*i),
            Value::Null => None,
            _ => None,
        }
    }

    fn extract_string(&self, value: &Value) -> DbResult<String> {
        match value {
            Value::Text(s) => Ok(s.clone()),
            _ => Err(rustmemodb::DbError::ExecutionError("Expected string".into())),
        }
    }

    fn extract_optional_string(&self, value: &Value) -> Option<String> {
        match value {
            Value::Text(s) => Some(s.clone()),
            Value::Null => None,
            _ => None,
        }
    }

    // Formatting helpers

    fn escape_string(&self, s: &str) -> String {
        s.replace('\'', "''")
    }

    fn format_optional_string(&self, opt: &Option<String>) -> String {
        match opt {
            Some(s) => format!("'{}'", self.escape_string(s)),
            None => "NULL".to_string(),
        }
    }

    fn format_optional_timestamp(&self, opt: Option<DateTime<Utc>>) -> String {
        match opt {
            Some(dt) => dt.timestamp().to_string(),
            None => "NULL".to_string(),
        }
    }

    fn timestamp_to_datetime(&self, timestamp: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(timestamp, 0).unwrap_or_else(|| Utc::now())
    }
}

/// Statistics about todos
#[derive(Debug)]
pub struct TodoStats {
    pub total: usize,
    pub pending: usize,
    pub in_progress: usize,
    pub completed: usize,
}

impl std::fmt::Display for TodoStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Total: {} | Pending: {} | In Progress: {} | Completed: {}",
            self.total, self.pending, self.in_progress, self.completed
        )
    }
}
