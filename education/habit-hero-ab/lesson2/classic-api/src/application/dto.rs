use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::{
    errors::DomainError,
    user::{PaginatedUsers, SortOrder, User, UserListQuery, UserSortBy},
};

#[derive(Debug, Deserialize)]
pub struct CreateUserRequest {
    pub email: String,
    pub display_name: String,
}

impl CreateUserRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        let email = self.email.trim();
        let display_name = self.display_name.trim();

        if email.is_empty() {
            return Err(DomainError::validation("email must not be blank"));
        }
        if email.len() > 320 {
            return Err(DomainError::validation(
                "email must be at most 320 characters",
            ));
        }
        if !is_valid_email(email) {
            return Err(DomainError::validation("email must be a valid address"));
        }

        if display_name.is_empty() {
            return Err(DomainError::validation("display_name must not be blank"));
        }
        if display_name.len() > 100 {
            return Err(DomainError::validation(
                "display_name must be at most 100 characters",
            ));
        }

        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub id: Uuid,
    pub email: String,
    pub display_name: String,
    pub active: bool,
    pub version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<User> for UserResponse {
    fn from(value: User) -> Self {
        Self {
            id: value.id,
            email: value.email,
            display_name: value.display_name,
            active: value.active,
            version: value.version,
            created_at: value.created_at,
            updated_at: value.updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ListUsersQueryRequest {
    #[serde(default = "default_page")]
    pub page: u32,
    #[serde(default = "default_per_page")]
    pub per_page: u32,
    #[serde(default)]
    pub email_contains: Option<String>,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub sort_by: UserSortByRequest,
    #[serde(default)]
    pub order: SortOrderRequest,
}

impl ListUsersQueryRequest {
    pub fn validate(&self) -> Result<(), DomainError> {
        if self.page == 0 {
            return Err(DomainError::validation("page must be greater than 0"));
        }
        if self.per_page == 0 || self.per_page > 100 {
            return Err(DomainError::validation(
                "per_page must be between 1 and 100",
            ));
        }
        if let Some(email_contains) = self.email_contains.as_ref()
            && email_contains.trim().is_empty()
        {
            return Err(DomainError::validation("email_contains must not be blank"));
        }
        Ok(())
    }

    pub fn into_domain(self) -> UserListQuery {
        UserListQuery {
            page: self.page,
            per_page: self.per_page,
            email_contains: self.email_contains.map(|value| value.trim().to_lowercase()),
            active: self.active,
            sort_by: self.sort_by.into_domain(),
            sort_order: self.order.into_domain(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum UserSortByRequest {
    #[default]
    CreatedAt,
    Email,
    DisplayName,
}

impl UserSortByRequest {
    fn into_domain(self) -> UserSortBy {
        match self {
            Self::CreatedAt => UserSortBy::CreatedAt,
            Self::Email => UserSortBy::Email,
            Self::DisplayName => UserSortBy::DisplayName,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SortOrderRequest {
    #[default]
    Desc,
    Asc,
}

impl SortOrderRequest {
    fn into_domain(self) -> SortOrder {
        match self {
            Self::Asc => SortOrder::Asc,
            Self::Desc => SortOrder::Desc,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct PaginatedUsersResponse {
    pub items: Vec<UserResponse>,
    pub page: u32,
    pub per_page: u32,
    pub total: u64,
    pub total_pages: u32,
}

impl From<PaginatedUsers> for PaginatedUsersResponse {
    fn from(value: PaginatedUsers) -> Self {
        Self {
            items: value.items.into_iter().map(UserResponse::from).collect(),
            page: value.page,
            per_page: value.per_page,
            total: value.total,
            total_pages: value.total_pages,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

fn is_valid_email(value: &str) -> bool {
    let Some((local, domain)) = value.split_once('@') else {
        return false;
    };

    if local.is_empty() || domain.is_empty() {
        return false;
    }

    if domain.starts_with('.') || domain.ends_with('.') || !domain.contains('.') {
        return false;
    }

    !value.contains(' ')
}

const fn default_page() -> u32 {
    1
}

const fn default_per_page() -> u32 {
    20
}
