use std::{path::PathBuf, sync::Arc};

use uuid::Uuid;

use super::user_workspace::UserWorkspace;

use crate::{
    application::dto::{
        ApplyUserCommandRequest, BulkLifecycleCommandRequest, BulkLifecycleCommandResponse,
        CreateUserRequest, ListUserEventsQueryRequest, ListUsersQueryRequest,
        PaginatedUsersResponse, UpdateUserRequest, UserAuditEventResponse, UserEventsResponse,
        UserLifecycleCommandRequest, UserResponse,
    },
    domain::{errors::DomainError, user::NewUser},
};

#[derive(Clone)]
pub struct UserService {
    workspace: Arc<UserWorkspace>,
}

impl UserService {
    pub async fn open(data_dir: PathBuf) -> Result<Self, DomainError> {
        let workspace = Arc::new(UserWorkspace::open(data_dir).await?);
        Ok(Self::new(workspace))
    }

    pub fn new(workspace: Arc<UserWorkspace>) -> Self {
        Self { workspace }
    }

    pub async fn create_user(
        &self,
        request: CreateUserRequest,
    ) -> Result<UserResponse, DomainError> {
        request.validate()?;

        let normalized_email = request.email.trim().to_lowercase();
        let normalized_display_name = request.display_name.trim().to_string();

        let created = self
            .workspace
            .create(NewUser {
                email: normalized_email,
                display_name: normalized_display_name,
                active: true,
            })
            .await?;

        UserResponse::try_from(created)
    }

    pub async fn get_user(&self, id: Uuid) -> Result<UserResponse, DomainError> {
        let Some(user) = self.workspace.get_by_id(id).await? else {
            return Err(DomainError::not_found("user not found"));
        };
        UserResponse::try_from(user)
    }

    pub async fn list_users(
        &self,
        query: ListUsersQueryRequest,
    ) -> Result<PaginatedUsersResponse, DomainError> {
        query.validate()?;
        let paged = self.workspace.list(query.into_domain()).await?;
        PaginatedUsersResponse::try_from(paged)
    }

    pub async fn update_user(
        &self,
        id: Uuid,
        request: UpdateUserRequest,
    ) -> Result<UserResponse, DomainError> {
        request.validate()?;

        let updated = self.workspace.update(id, request.into_patch()).await?;
        UserResponse::try_from(updated)
    }

    pub async fn delete_user(&self, id: Uuid) -> Result<(), DomainError> {
        self.workspace.delete(id).await
    }

    pub async fn apply_user_lifecycle_command(
        &self,
        id: Uuid,
        request: ApplyUserCommandRequest,
    ) -> Result<UserResponse, DomainError> {
        let updated = self
            .workspace
            .apply_lifecycle_command(id, request.into_domain())
            .await?;
        UserResponse::try_from(updated)
    }

    pub async fn bulk_apply_lifecycle_command(
        &self,
        request: BulkLifecycleCommandRequest,
    ) -> Result<BulkLifecycleCommandResponse, DomainError> {
        request.validate()?;

        let requested = request.ids.len();
        let (ids, command) = request.into_domain()?;
        let processed = self
            .workspace
            .bulk_apply_lifecycle_command(&ids, command)
            .await?;

        Ok(BulkLifecycleCommandResponse {
            requested,
            processed,
            command: UserLifecycleCommandRequest::from(command),
        })
    }

    pub async fn list_user_events(
        &self,
        id: Uuid,
        query: ListUserEventsQueryRequest,
    ) -> Result<UserEventsResponse, DomainError> {
        query.validate()?;

        if self.workspace.get_by_id(id).await?.is_none() {
            return Err(DomainError::not_found("user not found"));
        }

        let items = self
            .workspace
            .list_events(id, query.limit)
            .await?
            .into_iter()
            .map(UserAuditEventResponse::from)
            .collect::<Vec<_>>();

        Ok(UserEventsResponse {
            items,
            limit: query.limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn delete_user_returns_not_found_without_pre_read() {
        let temp = tempdir().expect("temp dir");
        let service = UserService::open(temp.path().join("service_delete_not_found"))
            .await
            .expect("service should open");

        let missing_id = Uuid::new_v4();
        let result = service.delete_user(missing_id).await;
        assert!(matches!(result, Err(DomainError::NotFound(_))));
    }

    #[tokio::test]
    async fn delete_user_removes_existing_user() {
        let temp = tempdir().expect("temp dir");
        let service = UserService::open(temp.path().join("service_delete_existing"))
            .await
            .expect("service should open");

        let created = service
            .create_user(CreateUserRequest {
                email: "delete-existing@example.com".to_string(),
                display_name: "Delete Existing".to_string(),
            })
            .await
            .expect("user should be created");

        let deleted = service.delete_user(created.id).await;
        assert!(
            deleted.is_ok(),
            "delete should succeed for existing user"
        );

        let second_delete = service.delete_user(created.id).await;
        assert!(matches!(second_delete, Err(DomainError::NotFound(_))));
    }

    #[tokio::test]
    async fn update_user_returns_not_found_without_pre_read() {
        let temp = tempdir().expect("temp dir");
        let service = UserService::open(temp.path().join("service_update_not_found"))
            .await
            .expect("service should open");

        let missing_id = Uuid::new_v4();
        let result = service
            .update_user(
                missing_id,
                UpdateUserRequest {
                    display_name: Some("Updated".to_string()),
                    active: None,
                },
            )
            .await;
        assert!(matches!(result, Err(DomainError::NotFound(_))));
    }

    #[tokio::test]
    async fn update_user_updates_existing_user() {
        let temp = tempdir().expect("temp dir");
        let service = UserService::open(temp.path().join("service_update_existing"))
            .await
            .expect("service should open");

        let created = service
            .create_user(CreateUserRequest {
                email: "update-existing@example.com".to_string(),
                display_name: "Update Existing".to_string(),
            })
            .await
            .expect("user should be created");

        let updated = service
            .update_user(
                created.id,
                UpdateUserRequest {
                    display_name: Some("Updated".to_string()),
                    active: None,
                },
            )
            .await
            .expect("update should succeed for existing user");
        assert_eq!(updated.display_name, "Updated");
    }

    #[tokio::test]
    async fn apply_lifecycle_command_returns_not_found_without_pre_read() {
        let temp = tempdir().expect("temp dir");
        let service = UserService::open(temp.path().join("service_apply_not_found"))
            .await
            .expect("service should open");

        let missing_id = Uuid::new_v4();
        let result = service
            .apply_user_lifecycle_command(
                missing_id,
                ApplyUserCommandRequest {
                    command: UserLifecycleCommandRequest::Deactivate,
                },
            )
            .await;
        assert!(matches!(result, Err(DomainError::NotFound(_))));
    }

    #[tokio::test]
    async fn apply_lifecycle_command_updates_user_state() {
        let temp = tempdir().expect("temp dir");
        let service = UserService::open(temp.path().join("service_apply_existing"))
            .await
            .expect("service should open");

        let created = service
            .create_user(CreateUserRequest {
                email: "apply-existing@example.com".to_string(),
                display_name: "Apply Existing".to_string(),
            })
            .await
            .expect("user should be created");

        let updated = service
            .apply_user_lifecycle_command(
                created.id,
                ApplyUserCommandRequest {
                    command: UserLifecycleCommandRequest::Deactivate,
                },
            )
            .await
            .expect("apply command should succeed for existing user");
        assert!(!updated.active);
    }
}
