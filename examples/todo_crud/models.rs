/// Domain models for TodoCRUD application
///
/// This module defines the core domain models following DDD principles.

use chrono::{DateTime, Utc};
use std::fmt;

/// Todo item - core domain entity
#[derive(Debug, Clone)]
pub struct Todo {
    pub id: i64,
    pub title: String,
    pub description: Option<String>,
    pub priority: Priority,
    pub status: Status,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl Todo {
    /// Create a new Todo with default values
    pub fn new(id: i64, title: String) -> Self {
        let now = Utc::now();
        Self {
            id,
            title,
            description: None,
            priority: Priority::Medium,
            status: Status::Pending,
            created_at: now,
            updated_at: now,
            completed_at: None,
        }
    }

    /// Builder pattern for creating todos with custom values
    pub fn builder(id: i64, title: String) -> TodoBuilder {
        TodoBuilder::new(id, title)
    }

    /// Mark todo as completed
    pub fn complete(&mut self) {
        self.status = Status::Done;
        self.completed_at = Some(Utc::now());
        self.updated_at = Utc::now();
    }

    /// Update todo status
    pub fn set_status(&mut self, status: Status) {
        self.status = status;
        self.updated_at = Utc::now();

        if status == Status::Done && self.completed_at.is_none() {
            self.completed_at = Some(Utc::now());
        } else if status != Status::Done {
            self.completed_at = None;
        }
    }

    /// Check if todo is overdue (for future enhancement with due dates)
    pub fn is_completed(&self) -> bool {
        self.status == Status::Done
    }
}

/// Priority levels for todos
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Low = 1,
    Medium = 2,
    High = 3,
}

impl Priority {
    /// Parse priority from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "low" | "l" | "1" => Some(Priority::Low),
            "medium" | "med" | "m" | "2" => Some(Priority::Medium),
            "high" | "h" | "3" => Some(Priority::High),
            _ => None,
        }
    }

    /// Convert priority to integer for database storage
    pub fn to_i64(&self) -> i64 {
        *self as i64
    }

    /// Convert integer to priority
    pub fn from_i64(value: i64) -> Option<Self> {
        match value {
            1 => Some(Priority::Low),
            2 => Some(Priority::Medium),
            3 => Some(Priority::High),
            _ => None,
        }
    }
}

impl fmt::Display for Priority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Priority::Low => write!(f, "Low"),
            Priority::Medium => write!(f, "Medium"),
            Priority::High => write!(f, "High"),
        }
    }
}

/// Status of a todo item
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Pending,
    InProgress,
    Done,
}

impl Status {
    /// Parse status from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "pending" | "p" | "todo" => Some(Status::Pending),
            "inprogress" | "in_progress" | "progress" | "i" | "ip" => Some(Status::InProgress),
            "done" | "completed" | "complete" | "d" | "c" => Some(Status::Done),
            _ => None,
        }
    }

    /// Convert status to string for database storage
    pub fn to_string_value(&self) -> &'static str {
        match self {
            Status::Pending => "pending",
            Status::InProgress => "in_progress",
            Status::Done => "done",
        }
    }

    /// Convert string to status
    pub fn from_string_value(s: &str) -> Option<Self> {
        match s {
            "pending" => Some(Status::Pending),
            "in_progress" => Some(Status::InProgress),
            "done" => Some(Status::Done),
            _ => None,
        }
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Status::Pending => write!(f, "Pending"),
            Status::InProgress => write!(f, "In Progress"),
            Status::Done => write!(f, "Done"),
        }
    }
}

/// Builder for creating Todo instances with fluent API
pub struct TodoBuilder {
    todo: Todo,
}

impl TodoBuilder {
    pub fn new(id: i64, title: String) -> Self {
        Self {
            todo: Todo::new(id, title),
        }
    }

    pub fn description(mut self, description: String) -> Self {
        self.todo.description = Some(description);
        self
    }

    pub fn priority(mut self, priority: Priority) -> Self {
        self.todo.priority = priority;
        self
    }

    pub fn status(mut self, status: Status) -> Self {
        self.todo.status = status;
        if status == Status::Done {
            self.todo.completed_at = Some(Utc::now());
        }
        self
    }

    pub fn build(self) -> Todo {
        self.todo
    }
}

/// Filter criteria for querying todos
#[derive(Debug, Default)]
pub struct TodoFilter {
    pub status: Option<Status>,
    pub priority: Option<Priority>,
    pub search_term: Option<String>,
}

impl TodoFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_status(mut self, status: Status) -> Self {
        self.status = Some(status);
        self
    }

    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    pub fn with_search(mut self, term: String) -> Self {
        self.search_term = Some(term);
        self
    }

    /// Check if filter has any criteria
    pub fn is_empty(&self) -> bool {
        self.status.is_none() && self.priority.is_none() && self.search_term.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_todo_creation() {
        let todo = Todo::new(1, "Test task".to_string());
        assert_eq!(todo.id, 1);
        assert_eq!(todo.title, "Test task");
        assert_eq!(todo.status, Status::Pending);
        assert_eq!(todo.priority, Priority::Medium);
    }

    #[test]
    fn test_todo_builder() {
        let todo = Todo::builder(1, "Test".to_string())
            .description("Test description".to_string())
            .priority(Priority::High)
            .status(Status::InProgress)
            .build();

        assert_eq!(todo.priority, Priority::High);
        assert_eq!(todo.status, Status::InProgress);
        assert!(todo.description.is_some());
    }

    #[test]
    fn test_priority_parsing() {
        assert_eq!(Priority::from_str("low"), Some(Priority::Low));
        assert_eq!(Priority::from_str("HIGH"), Some(Priority::High));
        assert_eq!(Priority::from_str("m"), Some(Priority::Medium));
        assert_eq!(Priority::from_str("invalid"), None);
    }

    #[test]
    fn test_status_parsing() {
        assert_eq!(Status::from_str("pending"), Some(Status::Pending));
        assert_eq!(Status::from_str("DONE"), Some(Status::Done));
        assert_eq!(Status::from_str("ip"), Some(Status::InProgress));
        assert_eq!(Status::from_str("invalid"), None);
    }

    #[test]
    fn test_todo_complete() {
        let mut todo = Todo::new(1, "Test".to_string());
        assert_eq!(todo.status, Status::Pending);
        assert!(todo.completed_at.is_none());

        todo.complete();
        assert_eq!(todo.status, Status::Done);
        assert!(todo.completed_at.is_some());
    }
}
