use sqlx::{PgPool, postgres::PgPoolOptions};
use todo_backend::{
    models::{
        CreateTodoRequest, ListTodosQuery, ReplaceTodoRequest, TodoStatus, UpdateTodoPatchRequest,
    },
    repository::{PgTodoRepository, TodoRepository},
};

async fn maybe_pool() -> Option<PgPool> {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()?;

    PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .ok()
}

#[tokio::test]
async fn postgres_repository_crud_flow() {
    let Some(pool) = maybe_pool().await else {
        eprintln!(
            "Skipping postgres_repository_crud_flow: TEST_DATABASE_URL/DATABASE_URL is not set or database is unreachable."
        );
        return;
    };

    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("migrations should run");

    sqlx::query("TRUNCATE TABLE todos RESTART IDENTITY")
        .execute(&pool)
        .await
        .expect("truncate should succeed");

    let repo = PgTodoRepository::new(pool.clone());

    let created = repo
        .create(CreateTodoRequest {
            title: "postgres todo".to_string(),
            description: Some("repo test".to_string()),
            priority: Some(2),
            due_at: None,
            status: Some(TodoStatus::Pending),
        })
        .await
        .expect("create should succeed");

    let listed = repo
        .list(ListTodosQuery::default())
        .await
        .expect("list should succeed");
    assert_eq!(listed.total, 1);

    let fetched = repo
        .get_by_id(created.id, false)
        .await
        .expect("get should succeed")
        .expect("todo should exist");
    assert_eq!(fetched.title, "postgres todo");

    let patched = repo
        .patch(
            created.id,
            UpdateTodoPatchRequest {
                title: Some("patched todo".to_string()),
                description: Some(Some("patched description".to_string())),
                priority: Some(5),
                due_at: None,
                status: Some(TodoStatus::Completed),
            },
        )
        .await
        .expect("patch should succeed")
        .expect("todo should exist");
    assert_eq!(patched.title, "patched todo");
    assert_eq!(patched.status, TodoStatus::Completed);
    assert!(patched.completed_at.is_some());

    let replaced = repo
        .replace(
            created.id,
            ReplaceTodoRequest {
                title: "replaced todo".to_string(),
                description: None,
                priority: 1,
                status: TodoStatus::InProgress,
                due_at: None,
            },
        )
        .await
        .expect("replace should succeed")
        .expect("todo should exist");
    assert_eq!(replaced.title, "replaced todo");
    assert_eq!(replaced.status, TodoStatus::InProgress);

    let deleted = repo
        .delete(created.id)
        .await
        .expect("delete should succeed");
    assert!(deleted);

    let not_found = repo
        .get_by_id(created.id, false)
        .await
        .expect("get should succeed");
    assert!(not_found.is_none());

    let restored = repo
        .restore(created.id)
        .await
        .expect("restore should succeed")
        .expect("todo should be restored");
    assert!(restored.deleted_at.is_none());
}
