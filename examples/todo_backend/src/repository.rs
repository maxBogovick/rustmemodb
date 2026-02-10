use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use rustmemodb::{Client as RustMemDbClient, DbError as RustMemDbError, Value};
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    error::{AppError, AppResult},
    models::{
        CreateTodoRequest, ListTodosQuery, PaginatedTodos, ReplaceTodoRequest, SortField,
        SortOrder, Todo, TodoStatus, UpdateTodoPatchRequest,
    },
};

const RUSTMEMODB_SCHEMA_SQL: &str = r#"
CREATE TABLE todos (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL,
    due_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    deleted_at TIMESTAMP
)
"#;

#[async_trait]
pub trait TodoRepository: Send + Sync {
    async fn init(&self) -> AppResult<()>;
    async fn create(&self, payload: CreateTodoRequest) -> AppResult<Todo>;
    async fn list(&self, query: ListTodosQuery) -> AppResult<PaginatedTodos>;
    async fn get_by_id(&self, id: Uuid, include_deleted: bool) -> AppResult<Option<Todo>>;
    async fn patch(&self, id: Uuid, payload: UpdateTodoPatchRequest) -> AppResult<Option<Todo>>;
    async fn replace(&self, id: Uuid, payload: ReplaceTodoRequest) -> AppResult<Option<Todo>>;
    async fn delete(&self, id: Uuid) -> AppResult<bool>;
    async fn restore(&self, id: Uuid) -> AppResult<Option<Todo>>;
}

#[derive(Clone)]
pub struct PgTodoRepository {
    pool: PgPool,
}

impl PgTodoRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl TodoRepository for PgTodoRepository {
    async fn init(&self) -> AppResult<()> {
        sqlx::migrate!("./migrations").run(&self.pool).await?;
        Ok(())
    }

    async fn create(&self, payload: CreateTodoRequest) -> AppResult<Todo> {
        let status = payload.status.unwrap_or_default();
        let priority = payload.priority.unwrap_or(3);
        let completed_at = if status == TodoStatus::Completed {
            Some(Utc::now())
        } else {
            None
        };

        let todo = sqlx::query_as::<_, Todo>(
            r#"
            INSERT INTO todos (title, description, status, priority, due_at, completed_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING
                id,
                title,
                description,
                status,
                priority,
                due_at,
                completed_at,
                created_at,
                updated_at,
                deleted_at
            "#,
        )
        .bind(payload.title.trim())
        .bind(payload.description)
        .bind(status)
        .bind(priority)
        .bind(payload.due_at)
        .bind(completed_at)
        .fetch_one(&self.pool)
        .await?;

        Ok(todo)
    }

    async fn list(&self, query: ListTodosQuery) -> AppResult<PaginatedTodos> {
        let offset = (query.page.saturating_sub(1) * query.per_page) as i64;

        let mut count_builder =
            QueryBuilder::<Postgres>::new("SELECT COUNT(*)::BIGINT AS count FROM todos");
        let mut has_where = false;
        push_filters(&mut count_builder, &mut has_where, &query);

        let (total_raw,): (i64,) = count_builder.build_query_as().fetch_one(&self.pool).await?;
        let total = u64::try_from(total_raw).unwrap_or(0);

        let mut select_builder = QueryBuilder::<Postgres>::new(
            r#"
            SELECT
                id,
                title,
                description,
                status,
                priority,
                due_at,
                completed_at,
                created_at,
                updated_at,
                deleted_at
            FROM todos
            "#,
        );

        let mut has_where = false;
        push_filters(&mut select_builder, &mut has_where, &query);

        select_builder.push(" ORDER BY ");
        select_builder.push(sort_column(query.sort_by));
        select_builder.push(" ");
        select_builder.push(sort_order(query.order));
        select_builder.push(", created_at DESC");

        select_builder
            .push(" LIMIT ")
            .push_bind(i64::from(query.per_page))
            .push(" OFFSET ")
            .push_bind(offset);

        let items = select_builder
            .build_query_as::<Todo>()
            .fetch_all(&self.pool)
            .await?;

        let total_pages = if total == 0 {
            0
        } else {
            total.div_ceil(u64::from(query.per_page)) as u32
        };

        Ok(PaginatedTodos {
            items,
            page: query.page,
            per_page: query.per_page,
            total,
            total_pages,
        })
    }

    async fn get_by_id(&self, id: Uuid, include_deleted: bool) -> AppResult<Option<Todo>> {
        let todo = sqlx::query_as::<_, Todo>(
            r#"
            SELECT
                id,
                title,
                description,
                status,
                priority,
                due_at,
                completed_at,
                created_at,
                updated_at,
                deleted_at
            FROM todos
            WHERE id = $1 AND ($2 OR deleted_at IS NULL)
            "#,
        )
        .bind(id)
        .bind(include_deleted)
        .fetch_optional(&self.pool)
        .await?;

        Ok(todo)
    }

    async fn patch(&self, id: Uuid, payload: UpdateTodoPatchRequest) -> AppResult<Option<Todo>> {
        let mut builder = QueryBuilder::<Postgres>::new("UPDATE todos SET ");
        let mut first = true;

        if let Some(title) = payload.title {
            push_set_prefix(&mut builder, &mut first);
            builder.push("title = ").push_bind(title.trim().to_string());
        }

        if let Some(description) = payload.description {
            push_set_prefix(&mut builder, &mut first);
            builder.push("description = ").push_bind(description);
        }

        if let Some(priority) = payload.priority {
            push_set_prefix(&mut builder, &mut first);
            builder.push("priority = ").push_bind(priority);
        }

        if let Some(due_at) = payload.due_at {
            push_set_prefix(&mut builder, &mut first);
            builder.push("due_at = ").push_bind(due_at);
        }

        if let Some(status) = payload.status {
            push_set_prefix(&mut builder, &mut first);
            builder.push("status = ").push_bind(status);

            push_set_prefix(&mut builder, &mut first);
            builder.push("completed_at = ");
            if status == TodoStatus::Completed {
                builder.push("COALESCE(completed_at, NOW())");
            } else {
                builder.push("NULL");
            }
        }

        push_set_prefix(&mut builder, &mut first);
        builder.push("updated_at = NOW()");

        builder
            .push(
                r#"
                WHERE id =
                "#,
            )
            .push_bind(id)
            .push(
                r#"
                AND deleted_at IS NULL
                RETURNING
                    id,
                    title,
                    description,
                    status,
                    priority,
                    due_at,
                    completed_at,
                    created_at,
                    updated_at,
                    deleted_at
                "#,
            );

        let todo = builder
            .build_query_as::<Todo>()
            .fetch_optional(&self.pool)
            .await?;

        Ok(todo)
    }

    async fn replace(&self, id: Uuid, payload: ReplaceTodoRequest) -> AppResult<Option<Todo>> {
        let completed_at = if payload.status == TodoStatus::Completed {
            Some(Utc::now())
        } else {
            None
        };

        let todo = sqlx::query_as::<_, Todo>(
            r#"
            UPDATE todos
            SET
                title = $1,
                description = $2,
                status = $3,
                priority = $4,
                due_at = $5,
                completed_at = $6,
                updated_at = NOW()
            WHERE id = $7 AND deleted_at IS NULL
            RETURNING
                id,
                title,
                description,
                status,
                priority,
                due_at,
                completed_at,
                created_at,
                updated_at,
                deleted_at
            "#,
        )
        .bind(payload.title.trim())
        .bind(payload.description)
        .bind(payload.status)
        .bind(payload.priority)
        .bind(payload.due_at)
        .bind(completed_at)
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(todo)
    }

    async fn delete(&self, id: Uuid) -> AppResult<bool> {
        let result = sqlx::query(
            r#"
            UPDATE todos
            SET deleted_at = NOW(), updated_at = NOW()
            WHERE id = $1 AND deleted_at IS NULL
            "#,
        )
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn restore(&self, id: Uuid) -> AppResult<Option<Todo>> {
        let todo = sqlx::query_as::<_, Todo>(
            r#"
            UPDATE todos
            SET deleted_at = NULL, updated_at = NOW()
            WHERE id = $1 AND deleted_at IS NOT NULL
            RETURNING
                id,
                title,
                description,
                status,
                priority,
                due_at,
                completed_at,
                created_at,
                updated_at,
                deleted_at
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(todo)
    }
}

pub struct RustMemDbTodoRepository {
    client: RustMemDbClient,
}

impl RustMemDbTodoRepository {
    pub fn from_client(client: RustMemDbClient) -> Self {
        Self { client }
    }

    pub async fn connect_url(url: &str) -> AppResult<Self> {
        let client = RustMemDbClient::connect_url(url)
            .await
            .map_err(Self::map_storage_error)?;
        Ok(Self::from_client(client))
    }

    pub async fn connect(username: &str, password: &str) -> AppResult<Self> {
        let client = RustMemDbClient::connect(username, password)
            .await
            .map_err(Self::map_storage_error)?;
        Ok(Self::from_client(client))
    }

    fn map_storage_error(err: RustMemDbError) -> AppError {
        AppError::storage(err.to_string())
    }

    async fn fetch_by_id_internal(
        &self,
        id: Uuid,
        include_deleted: bool,
    ) -> AppResult<Option<Todo>> {
        let mut sql =
            "SELECT id, title, description, status, priority, due_at, completed_at, created_at, updated_at, deleted_at FROM todos WHERE id = ".to_string();
        sql.push_str(&sql_string_literal(&id.to_string()));

        if !include_deleted {
            sql.push_str(" AND deleted_at IS NULL");
        }

        let result = self
            .client
            .query(&sql)
            .await
            .map_err(Self::map_storage_error)?;

        let todo = result
            .rows()
            .first()
            .map(|row| Self::todo_from_row(row))
            .transpose()?;

        Ok(todo)
    }

    fn todo_from_row(row: &[Value]) -> AppResult<Todo> {
        if row.len() < 10 {
            return Err(AppError::storage(
                "invalid row shape for todos projection (expected 10 columns)",
            ));
        }

        Ok(Todo {
            id: parse_uuid(&row[0], "id")?,
            title: parse_required_string(&row[1], "title")?,
            description: parse_optional_string(&row[2], "description")?,
            status: parse_status(&row[3])?,
            priority: parse_priority(&row[4])?,
            due_at: parse_optional_timestamp(&row[5], "due_at")?,
            completed_at: parse_optional_timestamp(&row[6], "completed_at")?,
            created_at: parse_required_timestamp(&row[7], "created_at")?,
            updated_at: parse_required_timestamp(&row[8], "updated_at")?,
            deleted_at: parse_optional_timestamp(&row[9], "deleted_at")?,
        })
    }
}

#[async_trait]
impl TodoRepository for RustMemDbTodoRepository {
    async fn init(&self) -> AppResult<()> {
        match self.client.execute(RUSTMEMODB_SCHEMA_SQL).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let message = err.to_string().to_ascii_lowercase();
                if message.contains("already exists") {
                    Ok(())
                } else {
                    Err(Self::map_storage_error(err))
                }
            }
        }
    }

    async fn create(&self, payload: CreateTodoRequest) -> AppResult<Todo> {
        let status = payload.status.unwrap_or_default();
        let now = Utc::now();
        let todo = Todo {
            id: Uuid::new_v4(),
            title: payload.title.trim().to_string(),
            description: payload.description,
            status,
            priority: payload.priority.unwrap_or(3),
            due_at: payload.due_at,
            completed_at: if status == TodoStatus::Completed {
                Some(now)
            } else {
                None
            },
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };

        let sql = format!(
            "INSERT INTO todos (id, title, description, status, priority, due_at, completed_at, created_at, updated_at, deleted_at) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, NULL)",
            sql_string_literal(&todo.id.to_string()),
            sql_string_literal(&todo.title),
            sql_optional_string_literal(todo.description.as_deref()),
            sql_string_literal(status_as_str(todo.status)),
            todo.priority,
            sql_optional_timestamp_literal(todo.due_at),
            sql_optional_timestamp_literal(todo.completed_at),
            sql_timestamp_literal(todo.created_at),
            sql_timestamp_literal(todo.updated_at),
        );

        self.client
            .execute(&sql)
            .await
            .map_err(Self::map_storage_error)?;

        self.fetch_by_id_internal(todo.id, false)
            .await?
            .ok_or_else(|| AppError::storage("created todo cannot be fetched"))
    }

    async fn list(&self, query: ListTodosQuery) -> AppResult<PaginatedTodos> {
        let result = self
            .client
            .query(
                "SELECT id, title, description, status, priority, due_at, completed_at, created_at, updated_at, deleted_at FROM todos",
            )
            .await
            .map_err(Self::map_storage_error)?;

        let mut todos = Vec::with_capacity(result.row_count());
        for row in result.rows() {
            todos.push(Self::todo_from_row(row)?);
        }

        Ok(filter_sort_paginate(todos, query))
    }

    async fn get_by_id(&self, id: Uuid, include_deleted: bool) -> AppResult<Option<Todo>> {
        self.fetch_by_id_internal(id, include_deleted).await
    }

    async fn patch(&self, id: Uuid, payload: UpdateTodoPatchRequest) -> AppResult<Option<Todo>> {
        let Some(mut todo) = self.fetch_by_id_internal(id, false).await? else {
            return Ok(None);
        };

        if let Some(title) = payload.title {
            todo.title = title.trim().to_string();
        }

        if let Some(description) = payload.description {
            todo.description = description;
        }

        if let Some(priority) = payload.priority {
            todo.priority = priority;
        }

        if let Some(due_at) = payload.due_at {
            todo.due_at = due_at;
        }

        if let Some(status) = payload.status {
            todo.status = status;
            todo.completed_at = if status == TodoStatus::Completed {
                todo.completed_at.or_else(|| Some(Utc::now()))
            } else {
                None
            };
        }

        todo.updated_at = Utc::now();

        let sql = format!(
            "UPDATE todos SET title = {}, description = {}, status = {}, priority = {}, due_at = {}, completed_at = {}, updated_at = {} WHERE id = {} AND deleted_at IS NULL",
            sql_string_literal(&todo.title),
            sql_optional_string_literal(todo.description.as_deref()),
            sql_string_literal(status_as_str(todo.status)),
            todo.priority,
            sql_optional_timestamp_literal(todo.due_at),
            sql_optional_timestamp_literal(todo.completed_at),
            sql_timestamp_literal(todo.updated_at),
            sql_string_literal(&todo.id.to_string()),
        );

        let updated = self
            .client
            .execute(&sql)
            .await
            .map_err(Self::map_storage_error)?;

        if updated.affected_rows().unwrap_or(0) == 0 {
            return Ok(None);
        }

        self.fetch_by_id_internal(id, false).await
    }

    async fn replace(&self, id: Uuid, payload: ReplaceTodoRequest) -> AppResult<Option<Todo>> {
        let Some(existing) = self.fetch_by_id_internal(id, false).await? else {
            return Ok(None);
        };

        let status = payload.status;
        let updated_at = Utc::now();
        let completed_at = if status == TodoStatus::Completed {
            existing.completed_at.or(Some(updated_at))
        } else {
            None
        };

        let sql = format!(
            "UPDATE todos SET title = {}, description = {}, status = {}, priority = {}, due_at = {}, completed_at = {}, updated_at = {} WHERE id = {} AND deleted_at IS NULL",
            sql_string_literal(payload.title.trim()),
            sql_optional_string_literal(payload.description.as_deref()),
            sql_string_literal(status_as_str(status)),
            payload.priority,
            sql_optional_timestamp_literal(payload.due_at),
            sql_optional_timestamp_literal(completed_at),
            sql_timestamp_literal(updated_at),
            sql_string_literal(&id.to_string()),
        );

        let updated = self
            .client
            .execute(&sql)
            .await
            .map_err(Self::map_storage_error)?;

        if updated.affected_rows().unwrap_or(0) == 0 {
            return Ok(None);
        }

        self.fetch_by_id_internal(id, false).await
    }

    async fn delete(&self, id: Uuid) -> AppResult<bool> {
        let now = Utc::now();
        let sql = format!(
            "UPDATE todos SET deleted_at = {}, updated_at = {} WHERE id = {} AND deleted_at IS NULL",
            sql_timestamp_literal(now),
            sql_timestamp_literal(now),
            sql_string_literal(&id.to_string()),
        );

        let deleted = self
            .client
            .execute(&sql)
            .await
            .map_err(Self::map_storage_error)?;

        Ok(deleted.affected_rows().unwrap_or(0) > 0)
    }

    async fn restore(&self, id: Uuid) -> AppResult<Option<Todo>> {
        let now = Utc::now();
        let sql = format!(
            "UPDATE todos SET deleted_at = NULL, updated_at = {} WHERE id = {} AND deleted_at IS NOT NULL",
            sql_timestamp_literal(now),
            sql_string_literal(&id.to_string()),
        );

        let restored = self
            .client
            .execute(&sql)
            .await
            .map_err(Self::map_storage_error)?;

        if restored.affected_rows().unwrap_or(0) == 0 {
            return Ok(None);
        }

        self.fetch_by_id_internal(id, false).await
    }
}

#[derive(Debug, Default)]
pub struct InMemoryTodoRepository {
    todos: RwLock<HashMap<Uuid, Todo>>,
}

impl InMemoryTodoRepository {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl TodoRepository for InMemoryTodoRepository {
    async fn init(&self) -> AppResult<()> {
        Ok(())
    }

    async fn create(&self, payload: CreateTodoRequest) -> AppResult<Todo> {
        let now = Utc::now();
        let status = payload.status.unwrap_or_default();

        let todo = Todo {
            id: Uuid::new_v4(),
            title: payload.title.trim().to_string(),
            description: payload.description,
            status,
            priority: payload.priority.unwrap_or(3),
            due_at: payload.due_at,
            completed_at: if status == TodoStatus::Completed {
                Some(now)
            } else {
                None
            },
            created_at: now,
            updated_at: now,
            deleted_at: None,
        };

        self.todos.write().await.insert(todo.id, todo.clone());
        Ok(todo)
    }

    async fn list(&self, query: ListTodosQuery) -> AppResult<PaginatedTodos> {
        let todos = self
            .todos
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        Ok(filter_sort_paginate(todos, query))
    }

    async fn get_by_id(&self, id: Uuid, include_deleted: bool) -> AppResult<Option<Todo>> {
        let todo = self
            .todos
            .read()
            .await
            .get(&id)
            .cloned()
            .filter(|todo| include_deleted || todo.deleted_at.is_none());
        Ok(todo)
    }

    async fn patch(&self, id: Uuid, payload: UpdateTodoPatchRequest) -> AppResult<Option<Todo>> {
        let mut todos = self.todos.write().await;
        let Some(todo) = todos.get_mut(&id) else {
            return Ok(None);
        };

        if todo.deleted_at.is_some() {
            return Ok(None);
        }

        if let Some(title) = payload.title {
            todo.title = title.trim().to_string();
        }

        if let Some(description) = payload.description {
            todo.description = description;
        }

        if let Some(priority) = payload.priority {
            todo.priority = priority;
        }

        if let Some(due_at) = payload.due_at {
            todo.due_at = due_at;
        }

        if let Some(status) = payload.status {
            todo.status = status;
            todo.completed_at = if status == TodoStatus::Completed {
                todo.completed_at.or_else(|| Some(Utc::now()))
            } else {
                None
            };
        }

        todo.updated_at = Utc::now();

        Ok(Some(todo.clone()))
    }

    async fn replace(&self, id: Uuid, payload: ReplaceTodoRequest) -> AppResult<Option<Todo>> {
        let mut todos = self.todos.write().await;
        let Some(todo) = todos.get_mut(&id) else {
            return Ok(None);
        };

        if todo.deleted_at.is_some() {
            return Ok(None);
        }

        todo.title = payload.title.trim().to_string();
        todo.description = payload.description;
        todo.status = payload.status;
        todo.priority = payload.priority;
        todo.due_at = payload.due_at;
        todo.completed_at = if payload.status == TodoStatus::Completed {
            todo.completed_at.or_else(|| Some(Utc::now()))
        } else {
            None
        };
        todo.updated_at = Utc::now();

        Ok(Some(todo.clone()))
    }

    async fn delete(&self, id: Uuid) -> AppResult<bool> {
        let mut todos = self.todos.write().await;
        if let Some(todo) = todos.get_mut(&id)
            && todo.deleted_at.is_none()
        {
            todo.deleted_at = Some(Utc::now());
            todo.updated_at = Utc::now();
            return Ok(true);
        }
        Ok(false)
    }

    async fn restore(&self, id: Uuid) -> AppResult<Option<Todo>> {
        let mut todos = self.todos.write().await;
        let Some(todo) = todos.get_mut(&id) else {
            return Ok(None);
        };

        if todo.deleted_at.is_none() {
            return Ok(None);
        }

        todo.deleted_at = None;
        todo.updated_at = Utc::now();
        Ok(Some(todo.clone()))
    }
}

fn push_filters(
    builder: &mut QueryBuilder<'_, Postgres>,
    has_where: &mut bool,
    query: &ListTodosQuery,
) {
    if !query.include_deleted {
        push_condition(builder, has_where);
        builder.push("deleted_at IS NULL");
    }

    if let Some(status) = query.status {
        push_condition(builder, has_where);
        builder.push("status = ").push_bind(status);
    }

    if let Some(priority) = query.priority {
        push_condition(builder, has_where);
        builder.push("priority = ").push_bind(priority);
    }

    if let Some(search) = query
        .search
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let pattern = format!("%{search}%");
        push_condition(builder, has_where);
        builder
            .push("(title ILIKE ")
            .push_bind(pattern.clone())
            .push(" OR COALESCE(description, '') ILIKE ")
            .push_bind(pattern)
            .push(")");
    }
}

fn push_condition(builder: &mut QueryBuilder<'_, Postgres>, has_where: &mut bool) {
    if *has_where {
        builder.push(" AND ");
    } else {
        builder.push(" WHERE ");
        *has_where = true;
    }
}

fn push_set_prefix(builder: &mut QueryBuilder<'_, Postgres>, first: &mut bool) {
    if *first {
        *first = false;
    } else {
        builder.push(", ");
    }
}

fn sort_column(sort_by: SortField) -> &'static str {
    match sort_by {
        SortField::CreatedAt => "created_at",
        SortField::UpdatedAt => "updated_at",
        SortField::DueAt => "due_at",
        SortField::Priority => "priority",
        SortField::Title => "title",
        SortField::Status => "status",
    }
}

fn sort_order(order: SortOrder) -> &'static str {
    match order {
        SortOrder::Asc => "ASC",
        SortOrder::Desc => "DESC",
    }
}

fn status_order(status: TodoStatus) -> u8 {
    match status {
        TodoStatus::Pending => 0,
        TodoStatus::InProgress => 1,
        TodoStatus::Completed => 2,
        TodoStatus::Archived => 3,
    }
}

fn status_as_str(status: TodoStatus) -> &'static str {
    match status {
        TodoStatus::Pending => "pending",
        TodoStatus::InProgress => "in_progress",
        TodoStatus::Completed => "completed",
        TodoStatus::Archived => "archived",
    }
}

fn parse_status(value: &Value) -> AppResult<TodoStatus> {
    match value {
        Value::Text(raw) => match raw.to_ascii_lowercase().as_str() {
            "pending" => Ok(TodoStatus::Pending),
            "in_progress" => Ok(TodoStatus::InProgress),
            "completed" => Ok(TodoStatus::Completed),
            "archived" => Ok(TodoStatus::Archived),
            _ => Err(AppError::storage(format!("invalid todo status: {raw}"))),
        },
        _ => Err(AppError::storage("status must be text")),
    }
}

fn parse_uuid(value: &Value, field: &str) -> AppResult<Uuid> {
    match value {
        Value::Uuid(uuid) => Ok(*uuid),
        Value::Text(text) => Uuid::parse_str(text)
            .map_err(|err| AppError::storage(format!("invalid uuid in {field}: {err}"))),
        _ => Err(AppError::storage(format!(
            "field {field} must be UUID or text"
        ))),
    }
}

fn parse_required_string(value: &Value, field: &str) -> AppResult<String> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(AppError::storage(format!("field {field} must be text"))),
    }
}

fn parse_optional_string(value: &Value, field: &str) -> AppResult<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::Text(text) => Ok(Some(text.clone())),
        _ => Err(AppError::storage(format!(
            "field {field} must be text or null"
        ))),
    }
}

fn parse_priority(value: &Value) -> AppResult<i16> {
    match value {
        Value::Integer(num) => i16::try_from(*num)
            .map_err(|_| AppError::storage("priority out of i16 range".to_string())),
        Value::Float(num) => i16::try_from(*num as i64)
            .map_err(|_| AppError::storage("priority out of i16 range".to_string())),
        _ => Err(AppError::storage("priority must be numeric")),
    }
}

fn parse_required_timestamp(value: &Value, field: &str) -> AppResult<DateTime<Utc>> {
    parse_optional_timestamp(value, field)?
        .ok_or_else(|| AppError::storage(format!("field {field} must be timestamp and not null")))
}

fn parse_optional_timestamp(value: &Value, field: &str) -> AppResult<Option<DateTime<Utc>>> {
    match value {
        Value::Null => Ok(None),
        Value::Timestamp(ts) => Ok(Some(*ts)),
        Value::Text(raw) => parse_timestamp_text(raw)
            .map(Some)
            .map_err(|err| AppError::storage(format!("invalid timestamp in {field}: {err}"))),
        _ => Err(AppError::storage(format!(
            "field {field} must be timestamp or null"
        ))),
    }
}

fn parse_timestamp_text(raw: &str) -> Result<DateTime<Utc>, String> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return Ok(parsed.with_timezone(&Utc));
    }

    if let Some(stripped) = raw.strip_suffix(" UTC")
        && let Ok(parsed) = NaiveDateTime::parse_from_str(stripped, "%Y-%m-%d %H:%M:%S%.f")
    {
        return Ok(DateTime::from_naive_utc_and_offset(parsed, Utc));
    }

    if let Ok(parsed) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S") {
        return Ok(DateTime::from_naive_utc_and_offset(parsed, Utc));
    }

    if let Ok(parsed) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S") {
        return Ok(DateTime::from_naive_utc_and_offset(parsed, Utc));
    }

    Err(format!("unsupported timestamp format: {raw}"))
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_optional_string_literal(value: Option<&str>) -> String {
    value.map_or_else(|| "NULL".to_string(), sql_string_literal)
}

fn sql_timestamp_literal(value: DateTime<Utc>) -> String {
    sql_string_literal(&value.to_rfc3339())
}

fn sql_optional_timestamp_literal(value: Option<DateTime<Utc>>) -> String {
    value.map_or_else(|| "NULL".to_string(), sql_timestamp_literal)
}

fn filter_sort_paginate(mut todos: Vec<Todo>, query: ListTodosQuery) -> PaginatedTodos {
    todos.retain(|todo| query.include_deleted || todo.deleted_at.is_none());
    todos.retain(|todo| query.status.is_none_or(|status| todo.status == status));
    todos.retain(|todo| {
        query
            .priority
            .is_none_or(|priority| todo.priority == priority)
    });

    if let Some(search) = query
        .search
        .as_deref()
        .map(str::trim)
        .filter(|search| !search.is_empty())
    {
        let search = search.to_ascii_lowercase();
        todos.retain(|todo| {
            let title_match = todo.title.to_ascii_lowercase().contains(&search);
            let description_match = todo
                .description
                .as_deref()
                .unwrap_or("")
                .to_ascii_lowercase()
                .contains(&search);
            title_match || description_match
        });
    }

    todos.sort_by(|left, right| {
        let order = match query.sort_by {
            SortField::CreatedAt => left.created_at.cmp(&right.created_at),
            SortField::UpdatedAt => left.updated_at.cmp(&right.updated_at),
            SortField::DueAt => left.due_at.cmp(&right.due_at),
            SortField::Priority => left.priority.cmp(&right.priority),
            SortField::Title => left.title.cmp(&right.title),
            SortField::Status => status_order(left.status).cmp(&status_order(right.status)),
        }
        .then_with(|| left.created_at.cmp(&right.created_at));

        match query.order {
            SortOrder::Asc => order,
            SortOrder::Desc => order.reverse(),
        }
    });

    let total = todos.len() as u64;
    let start = ((query.page.saturating_sub(1)) * query.per_page) as usize;
    let end = (start + query.per_page as usize).min(todos.len());

    let items = if start >= todos.len() {
        Vec::new()
    } else {
        todos[start..end].to_vec()
    };

    let total_pages = if total == 0 {
        0
    } else {
        total.div_ceil(u64::from(query.per_page)) as u32
    };

    PaginatedTodos {
        items,
        page: query.page,
        per_page: query.per_page,
        total,
        total_pages,
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;
    use crate::models::{CreateTodoRequest, ListTodosQuery};

    #[tokio::test]
    async fn in_memory_repo_supports_basic_flow() {
        let repo = InMemoryTodoRepository::new();
        repo.init().await.expect("init should succeed");

        let created = repo
            .create(CreateTodoRequest {
                title: "todo-1".to_string(),
                description: None,
                priority: Some(2),
                due_at: None,
                status: Some(TodoStatus::Pending),
            })
            .await
            .expect("create should succeed");

        let fetched = repo
            .get_by_id(created.id, false)
            .await
            .expect("get should succeed")
            .expect("todo should exist");

        assert_eq!(fetched.title, "todo-1");

        let page = repo
            .list(ListTodosQuery::default())
            .await
            .expect("list should succeed");

        assert_eq!(page.total, 1);

        let deleted = repo
            .delete(created.id)
            .await
            .expect("delete should succeed");
        assert!(deleted);

        let hidden = repo
            .get_by_id(created.id, false)
            .await
            .expect("get should succeed");
        assert!(hidden.is_none());

        let restored = repo
            .restore(created.id)
            .await
            .expect("restore should succeed");
        assert!(restored.is_some());
    }

    #[test]
    fn status_ordering_is_stable() {
        assert!(status_order(TodoStatus::Pending) < status_order(TodoStatus::Completed));
    }

    #[test]
    fn sort_column_mapping_is_safe() {
        assert_eq!(sort_column(SortField::Priority), "priority");
        assert_eq!(sort_order(SortOrder::Asc), "ASC");
    }

    #[test]
    fn ordering_reverse_works() {
        let ord = Ordering::Less;
        assert_eq!(ord.reverse(), Ordering::Greater);
    }
}
