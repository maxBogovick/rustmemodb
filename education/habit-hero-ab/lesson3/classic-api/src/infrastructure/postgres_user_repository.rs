use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
use uuid::Uuid;

use crate::{
    domain::{
        errors::DomainError,
        user::{
            NewUser, PaginatedUsers, SortOrder, UpdateUserPatch, User, UserListQuery, UserSortBy,
        },
    },
    infrastructure::UserRepository,
};

#[derive(Clone)]
pub struct PostgresUserRepository {
    pool: PgPool,
}

impl PostgresUserRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UserRepository for PostgresUserRepository {
    async fn create(&self, user: NewUser) -> Result<User, DomainError> {
        let row = sqlx::query(
            r#"
            INSERT INTO users (id, email, display_name, active, version)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id, email, display_name, active, version, created_at, updated_at
            "#,
        )
        .bind(user.id)
        .bind(user.email)
        .bind(user.display_name)
        .bind(user.active)
        .bind(user.version)
        .fetch_one(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(row_to_user(&row))
    }

    async fn find_by_email(&self, email: &str) -> Result<Option<User>, DomainError> {
        let normalized = email.trim().to_lowercase();

        let maybe_row = sqlx::query(
            r#"
            SELECT id, email, display_name, active, version, created_at, updated_at
            FROM users
            WHERE email = $1
            "#,
        )
        .bind(normalized)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(maybe_row.as_ref().map(row_to_user))
    }

    async fn get_by_id(&self, id: Uuid) -> Result<Option<User>, DomainError> {
        let maybe_row = sqlx::query(
            r#"
            SELECT id, email, display_name, active, version, created_at, updated_at
            FROM users
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_error)?;

        Ok(maybe_row.as_ref().map(row_to_user))
    }

    async fn list(&self, query: UserListQuery) -> Result<PaginatedUsers, DomainError> {
        let offset = i64::try_from(query.offset())
            .map_err(|_| DomainError::validation("offset is too large"))?;
        let per_page = i64::from(query.per_page);

        let mut count_builder =
            QueryBuilder::<Postgres>::new("SELECT COUNT(*)::BIGINT AS count FROM users");
        let mut has_where = false;
        push_list_filters(&mut count_builder, &mut has_where, &query);

        let (total_raw,): (i64,) = count_builder
            .build_query_as()
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        let mut select_builder = QueryBuilder::<Postgres>::new(
            r#"
            SELECT id, email, display_name, active, version, created_at, updated_at
            FROM users
            "#,
        );
        let mut has_where = false;
        push_list_filters(&mut select_builder, &mut has_where, &query);

        select_builder.push(" ORDER BY ");
        select_builder.push(sort_column(query.sort_by));
        select_builder.push(" ");
        select_builder.push(sort_order(query.sort_order));
        select_builder.push(", created_at DESC");

        select_builder
            .push(" LIMIT ")
            .push_bind(per_page)
            .push(" OFFSET ")
            .push_bind(offset);

        let rows = select_builder
            .build()
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        let items = rows.iter().map(row_to_user).collect::<Vec<_>>();
        let total = u64::try_from(total_raw).unwrap_or(0);
        let total_pages = if total == 0 {
            0
        } else {
            total.div_ceil(u64::from(query.per_page)) as u32
        };

        Ok(PaginatedUsers {
            items,
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
        let mut builder = QueryBuilder::<Postgres>::new("UPDATE users SET ");
        let mut needs_comma = false;

        if let Some(display_name) = patch.display_name {
            if needs_comma {
                builder.push(", ");
            }
            builder.push("display_name = ").push_bind(display_name);
            needs_comma = true;
        }

        if let Some(active) = patch.active {
            if needs_comma {
                builder.push(", ");
            }
            builder.push("active = ").push_bind(active);
            needs_comma = true;
        }

        if !needs_comma {
            return Err(DomainError::validation(
                "at least one field must be provided for update",
            ));
        }

        builder
            .push(", version = version + 1 ")
            .push("WHERE id = ")
            .push_bind(id)
            .push(" AND version = ")
            .push_bind(expected_version)
            .push(" RETURNING id, email, display_name, active, version, created_at, updated_at");

        let maybe_row = builder
            .build()
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        Ok(maybe_row.as_ref().map(row_to_user))
    }

    async fn delete(&self, id: Uuid, expected_version: i64) -> Result<bool, DomainError> {
        let result = sqlx::query("DELETE FROM users WHERE id = $1 AND version = $2")
            .bind(id)
            .bind(expected_version)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx_error)?;

        Ok(result.rows_affected() == 1)
    }
}

fn push_list_filters(
    builder: &mut QueryBuilder<Postgres>,
    has_where: &mut bool,
    query: &UserListQuery,
) {
    if let Some(active) = query.active {
        push_where_prefix(builder, has_where);
        builder.push("active = ").push_bind(active);
    }

    if let Some(email_contains) = query.email_contains.as_ref() {
        push_where_prefix(builder, has_where);
        builder
            .push("email LIKE ")
            .push_bind(format!("%{}%", email_contains));
    }
}

fn push_where_prefix(builder: &mut QueryBuilder<Postgres>, has_where: &mut bool) {
    if !*has_where {
        builder.push(" WHERE ");
        *has_where = true;
    } else {
        builder.push(" AND ");
    }
}

fn sort_column(sort_by: UserSortBy) -> &'static str {
    match sort_by {
        UserSortBy::CreatedAt => "created_at",
        UserSortBy::Email => "email",
        UserSortBy::DisplayName => "display_name",
    }
}

fn sort_order(order: SortOrder) -> &'static str {
    match order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    }
}

fn row_to_user(row: &sqlx::postgres::PgRow) -> User {
    User {
        id: row.get::<Uuid, _>("id"),
        email: row.get::<String, _>("email"),
        display_name: row.get::<String, _>("display_name"),
        active: row.get::<bool, _>("active"),
        version: row.get::<i64, _>("version"),
        created_at: row.get::<DateTime<Utc>, _>("created_at"),
        updated_at: row.get::<DateTime<Utc>, _>("updated_at"),
    }
}

fn map_sqlx_error(error: sqlx::Error) -> DomainError {
    match error {
        sqlx::Error::Database(db_error) => {
            if db_error.code().as_deref() == Some("23505") {
                DomainError::Conflict("email already exists".to_string())
            } else {
                DomainError::Storage(db_error.to_string())
            }
        }
        other => DomainError::Storage(other.to_string()),
    }
}
