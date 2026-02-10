use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{FromRow, Type};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Type, Default)]
#[serde(rename_all = "snake_case")]
#[sqlx(type_name = "todo_status", rename_all = "snake_case")]
pub enum TodoStatus {
    #[default]
    Pending,
    InProgress,
    Completed,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SortField {
    #[default]
    CreatedAt,
    UpdatedAt,
    DueAt,
    Priority,
    Title,
    Status,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Asc,
    #[default]
    Desc,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow)]
pub struct Todo {
    pub id: Uuid,
    pub title: String,
    pub description: Option<String>,
    pub status: TodoStatus,
    pub priority: i16,
    pub due_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateTodoRequest {
    pub title: String,
    pub description: Option<String>,
    pub priority: Option<i16>,
    pub due_at: Option<DateTime<Utc>>,
    pub status: Option<TodoStatus>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpdateTodoPatchRequest {
    pub title: Option<String>,
    pub description: Option<Option<String>>,
    pub priority: Option<i16>,
    pub due_at: Option<Option<DateTime<Utc>>>,
    pub status: Option<TodoStatus>,
}

impl UpdateTodoPatchRequest {
    pub fn has_changes(&self) -> bool {
        self.title.is_some()
            || self.description.is_some()
            || self.priority.is_some()
            || self.due_at.is_some()
            || self.status.is_some()
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ReplaceTodoRequest {
    pub title: String,
    pub description: Option<String>,
    pub priority: i16,
    #[serde(default)]
    pub status: TodoStatus,
    pub due_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListTodosQuery {
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_per_page")]
    pub per_page: u32,
    pub status: Option<TodoStatus>,
    pub priority: Option<i16>,
    pub search: Option<String>,
    #[serde(default)]
    pub include_deleted: bool,
    #[serde(default)]
    pub sort_by: SortField,
    #[serde(default)]
    pub order: SortOrder,
}

impl Default for ListTodosQuery {
    fn default() -> Self {
        Self {
            page: default_page(),
            per_page: default_per_page(),
            status: None,
            priority: None,
            search: None,
            include_deleted: false,
            sort_by: SortField::default(),
            order: SortOrder::default(),
        }
    }
}

impl ListTodosQuery {
    pub fn validate(&self) -> Result<(), String> {
        if self.page == 0 {
            return Err("page must be greater than 0".to_string());
        }
        if self.per_page == 0 || self.per_page > 100 {
            return Err("per_page must be between 1 and 100".to_string());
        }
        if let Some(priority) = self.priority
            && !(1..=5).contains(&priority)
        {
            return Err("priority filter must be between 1 and 5".to_string());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GetTodoQuery {
    #[serde(default)]
    pub include_deleted: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaginatedTodos {
    pub items: Vec<Todo>,
    pub page: u32,
    pub per_page: u32,
    pub total: u64,
    pub total_pages: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiResponse<T> {
    pub data: T,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiMessage {
    pub message: String,
}

const fn default_page() -> u32 {
    1
}

const fn default_per_page() -> u32 {
    20
}
