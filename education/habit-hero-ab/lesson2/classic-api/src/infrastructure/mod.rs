use async_trait::async_trait;
use uuid::Uuid;

use crate::domain::{
    errors::DomainError,
    user::{NewUser, PaginatedUsers, User, UserListQuery},
};

pub mod in_memory_user_repository;
pub mod postgres_user_repository;

#[async_trait]
pub trait UserRepository: Send + Sync {
    async fn create(&self, user: NewUser) -> Result<User, DomainError>;
    async fn get_by_id(&self, id: Uuid) -> Result<Option<User>, DomainError>;
    async fn find_by_email(&self, email: &str) -> Result<Option<User>, DomainError>;
    async fn list(&self, query: UserListQuery) -> Result<PaginatedUsers, DomainError>;
}
