use chrono::{DateTime, Utc};
use rustmemodb::{PersistAutonomousIntent, persist_struct};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::domain::errors::DomainError;

persist_struct!(
    pub struct User {
        // Persist-level uniqueness keeps invariants close to the entity model.
        // Repository code should not duplicate this with manual "claim" tables.
        #[persist(unique)]
        email: String,
        display_name: String,
        active: bool,
    }
);

impl User {
    pub fn id(&self) -> Result<Uuid, DomainError> {
        // Never hide id decode failures with defaults (e.g. UUID nil):
        // that would silently corrupt API behavior and observability.
        Uuid::parse_str(self.persist_id()).map_err(|err| {
            DomainError::internal(format!(
                "invalid persisted user id '{}': {err}",
                self.persist_id()
            ))
        })
    }

    pub fn version(&self) -> i64 {
        self.metadata().version
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        self.metadata().created_at
    }

    pub fn updated_at(&self) -> DateTime<Utc> {
        self.metadata().updated_at
    }
}

#[derive(Debug, Clone)]
pub struct NewUser {
    pub email: String,
    pub display_name: String,
    pub active: bool,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateUserPatch {
    pub display_name: Option<String>,
    pub active: Option<bool>,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    PersistAutonomousIntent,
)]
#[persist_intent(model = User)]
#[serde(rename_all = "snake_case")]
pub enum UserLifecycleCommand {
    #[persist_case(command = UserCommand::SetActive(true))]
    Activate,
    #[persist_case(command = UserCommand::SetActive(false))]
    Deactivate,
}

#[derive(Debug, Clone)]
pub struct UserAuditEvent {
    pub id: String,
    pub user_id: Uuid,
    pub event_type: String,
    pub message: String,
    pub resulting_version: i64,
    pub created_at: DateTime<Utc>,
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

#[derive(Clone)]
pub struct PaginatedUsers {
    pub items: Vec<User>,
    pub page: u32,
    pub per_page: u32,
    pub total: u64,
    pub total_pages: u32,
}
