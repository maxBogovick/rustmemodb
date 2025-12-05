/// Service layer for business logic
///
/// This layer implements the business rules and orchestrates operations
/// between the repository and presentation layers.

use crate::models::{Priority, Status, Todo, TodoFilter};
use crate::repository::{TodoRepository, TodoStats};
use rustmemodb::Result as DbResult;
use std::sync::Arc;

/// Service for managing todos
///
/// Provides high-level business operations with validation and error handling.
pub struct TodoService {
    repository: Arc<TodoRepository>,
}

impl TodoService {
    /// Create a new TodoService instance
    pub fn new(repository: Arc<TodoRepository>) -> Self {
        Self { repository }
    }

    /// Create a new todo with validation
    ///
    /// # Arguments
    /// * `title` - Title of the todo (required, non-empty)
    /// * `description` - Optional description
    /// * `priority` - Priority level
    ///
    /// # Returns
    /// * `Ok(Todo)` - Created todo
    /// * `Err` - Validation or database error
    pub fn create_todo(
        &self,
        title: String,
        description: Option<String>,
        priority: Priority,
    ) -> DbResult<Todo> {
        // Validation
        self.validate_title(&title)?;

        // Generate ID
        let id = self.repository.next_id()?;

        // Create todo using builder pattern
        let mut todo = Todo::builder(id, title).priority(priority);

        if let Some(desc) = description {
            todo = todo.description(desc);
        }

        let todo = todo.build();

        // Save to database
        self.repository.create(&todo)?;

        Ok(todo)
    }

    /// Get todo by ID
    pub fn get_todo(&self, id: i64) -> DbResult<Option<Todo>> {
        self.repository.find_by_id(id)
    }

    /// List all todos with optional filtering
    pub fn list_todos(&self, filter: TodoFilter) -> DbResult<Vec<Todo>> {
        self.repository.find_all(&filter)
    }

    /// List todos by status
    pub fn list_by_status(&self, status: Status) -> DbResult<Vec<Todo>> {
        let filter = TodoFilter::new().with_status(status);
        self.repository.find_all(&filter)
    }

    /// List todos by priority
    pub fn list_by_priority(&self, priority: Priority) -> DbResult<Vec<Todo>> {
        let filter = TodoFilter::new().with_priority(priority);
        self.repository.find_all(&filter)
    }

    /// Search todos by keyword in title or description
    pub fn search_todos(&self, keyword: String) -> DbResult<Vec<Todo>> {
        let filter = TodoFilter::new().with_search(keyword);
        self.repository.find_all(&filter)
    }

    /// Update todo title
    pub fn update_title(&self, id: i64, new_title: String) -> DbResult<Todo> {
        self.validate_title(&new_title)?;

        let mut todo = self
            .repository
            .find_by_id(id)?
            .ok_or_else(|| rustmemodb::DbError::ExecutionError(format!("Todo with id {} not found", id)))?;

        todo.title = new_title;
        todo.updated_at = chrono::Utc::now();

        self.repository.update(&todo)?;
        Ok(todo)
    }

    /// Update todo description
    pub fn update_description(&self, id: i64, new_description: Option<String>) -> DbResult<Todo> {
        let mut todo = self
            .repository
            .find_by_id(id)?
            .ok_or_else(|| rustmemodb::DbError::ExecutionError(format!("Todo with id {} not found", id)))?;

        todo.description = new_description;
        todo.updated_at = chrono::Utc::now();

        self.repository.update(&todo)?;
        Ok(todo)
    }

    /// Update todo priority
    pub fn update_priority(&self, id: i64, priority: Priority) -> DbResult<Todo> {
        let mut todo = self
            .repository
            .find_by_id(id)?
            .ok_or_else(|| rustmemodb::DbError::ExecutionError(format!("Todo with id {} not found", id)))?;

        todo.priority = priority;
        todo.updated_at = chrono::Utc::now();

        self.repository.update(&todo)?;
        Ok(todo)
    }

    /// Update todo status
    pub fn update_status(&self, id: i64, status: Status) -> DbResult<Todo> {
        let mut todo = self
            .repository
            .find_by_id(id)?
            .ok_or_else(|| rustmemodb::DbError::ExecutionError(format!("Todo with id {} not found", id)))?;

        todo.set_status(status);
        self.repository.update(&todo)?;
        Ok(todo)
    }

    /// Mark todo as completed
    pub fn complete_todo(&self, id: i64) -> DbResult<Todo> {
        self.update_status(id, Status::Done)
    }

    /// Mark todo as in progress
    pub fn start_todo(&self, id: i64) -> DbResult<Todo> {
        self.update_status(id, Status::InProgress)
    }

    /// Delete todo by ID
    pub fn delete_todo(&self, id: i64) -> DbResult<()> {
        // Check if exists
        if self.repository.find_by_id(id)?.is_none() {
            return Err(rustmemodb::DbError::ExecutionError(format!("Todo with id {} not found", id)));
        }

        self.repository.delete(id)
    }

    /// Delete all completed todos
    pub fn delete_completed(&self) -> DbResult<usize> {
        self.repository.delete_completed()
    }

    /// Get statistics
    pub fn get_stats(&self) -> DbResult<TodoStats> {
        self.repository.stats()
    }

    // Validation methods

    /// Validate todo title
    fn validate_title(&self, title: &str) -> DbResult<()> {
        if title.trim().is_empty() {
            return Err(rustmemodb::DbError::ConstraintViolation(
                "Title cannot be empty".to_string(),
            ));
        }

        if title.len() > 200 {
            return Err(rustmemodb::DbError::ConstraintViolation(
                "Title cannot exceed 200 characters".to_string(),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustmemodb::Client;

    fn setup_service() -> TodoService {
        let client = Arc::new(Client::connect("admin", "adminpass").unwrap());
        let repo = Arc::new(TodoRepository::new(client).unwrap());
        TodoService::new(repo)
    }

    #[test]
    fn test_create_todo() {
        let service = setup_service();
        let todo = service
            .create_todo("Test task".to_string(), None, Priority::Medium)
            .unwrap();

        assert_eq!(todo.title, "Test task");
        assert_eq!(todo.priority, Priority::Medium);
    }

    #[test]
    fn test_validation_empty_title() {
        let service = setup_service();
        let result = service.create_todo("".to_string(), None, Priority::Low);
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_long_title() {
        let service = setup_service();
        let long_title = "a".repeat(201);
        let result = service.create_todo(long_title, None, Priority::Low);
        assert!(result.is_err());
    }
}
