use rustmemodb::Client;
use todo_backend::{
    models::{
        CreateTodoRequest, ListTodosQuery, ReplaceTodoRequest, TodoStatus, UpdateTodoPatchRequest,
    },
    repository::{RustMemDbTodoRepository, TodoRepository},
};

#[tokio::test]
async fn rustmemodb_repository_crud_flow() {
    let client = Client::connect_local("admin", "adminpass")
        .await
        .expect("local rustmemodb client should connect");
    let repo = RustMemDbTodoRepository::from_client(client);

    repo.init().await.expect("schema init should succeed");

    let created = repo
        .create(CreateTodoRequest {
            title: "rustmemodb todo".to_string(),
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
    assert_eq!(fetched.title, "rustmemodb todo");

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
