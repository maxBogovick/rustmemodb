use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::{
    errors::DomainError,
    user::{
        NewUser, PaginatedUsers, UpdateUserPatch, User, UserAuditEvent, UserLifecycleCommand,
        UserListQuery,
    },
};

pub mod in_memory_user_repository;
pub mod postgres_user_repository;

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn create(&self, user: NewUser) -> Result<User, DomainError>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<User>, DomainError>;
    async fn find_by_email(&self, email: &str) -> Result<Option<User>, DomainError>;
    async fn list(&self, query: UserListQuery) -> Result<PaginatedUsers, DomainError>;
    async fn update(
        &self,
        id: Uuid,
        patch: UpdateUserPatch,
        expected_version: i64,
    ) -> Result<Option<User>, DomainError>;
    async fn delete(&self, id: Uuid, expected_version: i64) -> Result<bool, DomainError>;
    async fn apply_lifecycle_command(
        &self,
        id: Uuid,
        command: UserLifecycleCommand,
        expected_version: i64,
    ) -> Result<Option<User>, DomainError>;
    async fn bulk_apply_lifecycle_command(
        &self,
        ids: &[Uuid],
        command: UserLifecycleCommand,
    ) -> Result<u64, DomainError>;
    async fn list_events(&self, id: Uuid, limit: u32) -> Result<Vec<UserAuditEvent>, DomainError>;
}
