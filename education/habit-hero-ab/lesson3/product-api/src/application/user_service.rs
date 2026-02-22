use std::sync::Arc;

use uuid::Uuid;

use crate::{
    application::dto::{
        CreateUserRequest, ListUsersQueryRequest, PaginatedUsersResponse, UpdateUserRequest,
        UserResponse,
    },
    domain::{errors::DomainError, user::NewUser},
    infrastructure::UserRepository,
};

#[derive(Clone)]
pub struct UserService {
    repository: Arc<dyn UserRepository>,
}

impl UserService {
    pub fn new(repository: Arc<dyn UserRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_user(
        &self,
        request: CreateUserRequest,
    ) -> Result<UserResponse, DomainError> {
        request.validate()?;

        let normalized_email = request.email.trim().to_lowercase();
        let normalized_display_name = request.display_name.trim().to_string();

        let created = self
            .repository
            .create(NewUser {
                email: normalized_email,
                display_name: normalized_display_name,
                active: true,
            })
            .await?;

        UserResponse::try_from(created)
    }

    pub async fn get_user(&self, id: Uuid) -> Result<UserResponse, DomainError> {
        let Some(user) = self.repository.get_by_id(id).await? else {
            return Err(DomainError::not_found("user not found"));
        };
        UserResponse::try_from(user)
    }

    pub async fn list_users(
        &self,
        query: ListUsersQueryRequest,
    ) -> Result<PaginatedUsersResponse, DomainError> {
        query.validate()?;
        let paged = self.repository.list(query.into_domain()).await?;
        PaginatedUsersResponse::try_from(paged)
    }

    pub async fn update_user(
        &self,
        id: Uuid,
        request: UpdateUserRequest,
        expected_version: i64,
    ) -> Result<UserResponse, DomainError> {
        request.validate()?;

        if self.repository.get_by_id(id).await?.is_none() {
            return Err(DomainError::not_found("user not found"));
        }

        let Some(updated) = self
            .repository
            .update(id, request.into_patch(), expected_version)
            .await?
        else {
            return Err(DomainError::conflict("expected version mismatch"));
        };

        UserResponse::try_from(updated)
    }

    pub async fn delete_user(&self, id: Uuid, expected_version: i64) -> Result<(), DomainError> {
        if self.repository.get_by_id(id).await?.is_none() {
            return Err(DomainError::not_found("user not found"));
        }

        let deleted = self.repository.delete(id, expected_version).await?;
        if !deleted {
            return Err(DomainError::conflict("expected version mismatch"));
        }

        Ok(())
    }
}
