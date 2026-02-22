use std::sync::Arc;

use uuid::Uuid;

use crate::{
    application::dto::{
        CreateUserRequest, ListUsersQueryRequest, PaginatedUsersResponse, UserResponse,
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

        if self
            .repository
            .find_by_email(&normalized_email)
            .await?
            .is_some()
        {
            return Err(DomainError::conflict("email already exists"));
        }

        let created = self
            .repository
            .create(NewUser {
                id: Uuid::new_v4(),
                email: normalized_email,
                display_name: normalized_display_name,
                active: true,
                version: 1,
            })
            .await?;

        Ok(UserResponse::from(created))
    }

    pub async fn get_user(&self, id: Uuid) -> Result<UserResponse, DomainError> {
        let Some(user) = self.repository.get_by_id(id).await? else {
            return Err(DomainError::not_found("user not found"));
        };
        Ok(UserResponse::from(user))
    }

    pub async fn list_users(
        &self,
        query: ListUsersQueryRequest,
    ) -> Result<PaginatedUsersResponse, DomainError> {
        query.validate()?;
        let paged = self.repository.list(query.into_domain()).await?;
        Ok(PaginatedUsersResponse::from(paged))
    }
}
