use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    domain::{
        errors::DomainError,
        user::{
            NewUser, PaginatedUsers, SortOrder, UpdateUserPatch, User, UserAuditEvent,
            UserLifecycleCommand, UserListQuery, UserSortBy,
        },
    },
    infrastructure::UserRepository,
};

#[derive(Default)]
pub struct InMemoryUserRepository {
    users_by_id: RwLock<HashMap<Uuid, User>>,
    user_id_by_email: RwLock<HashMap<String, Uuid>>,
    user_events: RwLock<Vec<UserAuditEvent>>,
}

impl InMemoryUserRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl UserRepository for InMemoryUserRepository {
    async fn create(&self, user: NewUser) -> Result<User, DomainError> {
        let mut user_id_by_email = self.user_id_by_email.write().await;
        if user_id_by_email.contains_key(&user.email) {
            return Err(DomainError::conflict("email already exists"));
        }

        let now = Utc::now();
        let created = User {
            id: user.id,
            email: user.email,
            display_name: user.display_name,
            active: user.active,
            version: user.version,
            created_at: now,
            updated_at: now,
        };

        user_id_by_email.insert(created.email.clone(), created.id);
        self.users_by_id
            .write()
            .await
            .insert(created.id, created.clone());

        Ok(created)
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<User>, DomainError> {
        let normalized = email.trim().to_lowercase();
        let user_id_by_email = self.user_id_by_email.read().await;

        let Some(user_id) = user_id_by_email.get(&normalized) else {
            return Ok(None);
        };

        Ok(self.users_by_id.read().await.get(user_id).cloned())
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<User>, DomainError> {
        Ok(self.users_by_id.read().await.get(&id).cloned())
    }

    async fn list(&self, query: UserListQuery) -> Result<PaginatedUsers, DomainError> {
        let mut items = self
            .users_by_id
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();

        if let Some(active) = query.active {
            items.retain(|item| item.active == active);
        }

        if let Some(email_contains) = &query.email_contains {
            let needle = email_contains.to_lowercase();
            items.retain(|item| item.email.contains(&needle));
        }

        items.sort_by(|left, right| {
            let ordering = match query.sort_by {
                UserSortBy::CreatedAt => left.created_at.cmp(&right.created_at),
                UserSortBy::Email => left.email.cmp(&right.email),
                UserSortBy::DisplayName => left.display_name.cmp(&right.display_name),
            };

            match query.sort_order {
                SortOrder::Asc => ordering,
                SortOrder::Desc => ordering.reverse(),
            }
        });

        let total = u64::try_from(items.len()).unwrap_or(u64::MAX);
        let per_page_u64 = u64::from(query.per_page);
        let total_pages = if total == 0 {
            0
        } else {
            total.div_ceil(per_page_u64) as u32
        };

        let offset = query.offset();
        let per_page = usize::try_from(query.per_page).unwrap_or(usize::MAX);
        let paged_items = items.into_iter().skip(offset).take(per_page).collect();

        Ok(PaginatedUsers {
            items: paged_items,
            page: query.page,
            per_page: query.per_page,
            total,
            total_pages,
        })
    }

    async fn update(
        &self,
        id: Uuid,
        patch: UpdateUserPatch,
        expected_version: i64,
    ) -> Result<Option<User>, DomainError> {
        let mut users_by_id = self.users_by_id.write().await;
        let Some(user) = users_by_id.get_mut(&id) else {
            return Ok(None);
        };

        if user.version != expected_version {
            return Err(DomainError::conflict("expected version mismatch"));
        }

        if let Some(display_name) = patch.display_name {
            user.display_name = display_name;
        }
        if let Some(active) = patch.active {
            user.active = active;
        }

        user.version += 1;
        user.updated_at = Utc::now();
        Ok(Some(user.clone()))
    }

    async fn delete(&self, id: Uuid, expected_version: i64) -> Result<bool, DomainError> {
        let mut users_by_id = self.users_by_id.write().await;
        let Some(existing) = users_by_id.get(&id) else {
            return Ok(false);
        };

        if existing.version != expected_version {
            return Err(DomainError::conflict("expected version mismatch"));
        }

        let removed = users_by_id
            .remove(&id)
            .ok_or_else(|| DomainError::internal("deleted user is missing from state"))?;
        self.user_id_by_email.write().await.remove(&removed.email);
        Ok(true)
    }

    async fn apply_lifecycle_command(
        &self,
        id: Uuid,
        command: UserLifecycleCommand,
        expected_version: i64,
    ) -> Result<Option<User>, DomainError> {
        let mut users_by_id = self.users_by_id.write().await;
        let Some(user) = users_by_id.get_mut(&id) else {
            return Ok(None);
        };

        if user.version != expected_version {
            return Err(DomainError::conflict("expected version mismatch"));
        }

        user.active = command.target_active();
        user.version += 1;
        user.updated_at = Utc::now();

        let updated = user.clone();

        self.user_events.write().await.push(UserAuditEvent {
            id: Uuid::new_v4().to_string(),
            user_id: updated.id,
            event_type: command.event_type().to_string(),
            message: command.event_message().to_string(),
            resulting_version: updated.version,
            created_at: Utc::now(),
        });

        Ok(Some(updated))
    }

    async fn bulk_apply_lifecycle_command(
        &self,
        ids: &[Uuid],
        command: UserLifecycleCommand,
    ) -> Result<u64, DomainError> {
        if ids.is_empty() {
            return Ok(0);
        }

        let ids = ids.iter().copied().collect::<HashSet<_>>();

        let mut users_by_id = self.users_by_id.write().await;
        let mut events = self.user_events.write().await;

        let mut updated = 0u64;
        for user in users_by_id.values_mut() {
            if !ids.contains(&user.id) {
                continue;
            }

            user.active = command.target_active();
            user.version += 1;
            user.updated_at = Utc::now();

            events.push(UserAuditEvent {
                id: Uuid::new_v4().to_string(),
                user_id: user.id,
                event_type: command.bulk_event_type().to_string(),
                message: command.bulk_event_message().to_string(),
                resulting_version: user.version,
                created_at: Utc::now(),
            });

            updated += 1;
        }

        Ok(updated)
    }

    async fn list_events(&self, id: Uuid, limit: u32) -> Result<Vec<UserAuditEvent>, DomainError> {
        let mut events = self
            .user_events
            .read()
            .await
            .iter()
            .filter(|event| event.user_id == id)
            .cloned()
            .collect::<Vec<_>>();

        events.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.id.cmp(&left.id))
        });

        events.truncate(usize::try_from(limit).unwrap_or(usize::MAX));
        Ok(events)
    }
}
