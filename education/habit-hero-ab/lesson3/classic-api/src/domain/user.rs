use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub active: bool,
    pub version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewUser {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub active: bool,
    pub version: i64,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateUserPatch {
    pub display_name: Option<String>,
    pub active: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserSortBy {
    CreatedAt,
    Email,
    DisplayName,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone)]
pub struct UserListQuery {
    pub page: u32,
    pub per_page: u32,
    pub email_contains: Option<String>,
    pub active: Option<bool>,
    pub sort_by: UserSortBy,
    pub sort_order: SortOrder,
}

impl UserListQuery {
    pub fn offset(&self) -> usize {
        usize::try_from(self.page.saturating_sub(1)).unwrap_or(usize::MAX)
            * usize::try_from(self.per_page).unwrap_or(usize::MAX)
    }
}

#[derive(Debug, Clone)]
pub struct PaginatedUsers {
    pub items: Vec<User>,
    pub page: u32,
    pub per_page: u32,
    pub total: u64,
    pub total_pages: u32,
}
