use anyhow::Result;
use chrono::Utc;
use rustmemodb::{
    PersistApp, PersistModel, persist_struct, persist_vec,
};

#[derive(Debug, Clone, PersistModel)]
#[persist_model(table = "macro_todo_items", schema_version = 2)]
pub struct TodoModel {
    pub title: String,
    pub completed: bool,
    pub priority: i64,
    pub created_at: String,
    pub updated_at: String,
}

persist_struct!(pub struct PersistedTodo from_struct = TodoModel);

persist_struct! {
    pub struct TodoTag {
        todo_id: String,
        label: String,
    }
}

persist_vec!(pub TodoVec, PersistedTodo);
persist_vec!(pub TodoTagVec, TodoTag);

#[derive(Debug, Clone)]
pub struct MacroDemoSummary {
    pub todo_count: usize,
    pub tag_count: usize,
    pub restored_todo_count: usize,
    pub invoked: usize,
    pub skipped: usize,
}

pub async fn run_macro_showcase() -> Result<MacroDemoSummary> {
    let suffix = Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| Utc::now().timestamp_millis() * 1_000_000);
    let data_root = std::env::temp_dir().join(format!("todo_macro_showcase_{suffix}"));

    let app = PersistApp::open_auto(data_root.clone()).await?;
    let mut todos = app.open_vec::<TodoVec>("macro_todos").await?;

    let todo_a_id = todos
        .create_from_draft(PersistedTodoDraft::new(
            "Write design doc".to_string(),
            false,
            2,
            Utc::now().to_rfc3339(),
            Utc::now().to_rfc3339(),
        ))
        .await?;
    let todo_b_id = todos
        .create_from_draft(PersistedTodoDraft::new(
            "Ship beta".to_string(),
            false,
            1,
            Utc::now().to_rfc3339(),
            Utc::now().to_rfc3339(),
        ))
        .await?;

    todos
        .apply_command(&todo_a_id, PersistedTodoCommand::SetPriority(3))
        .await?;
    todos
        .apply_command(
            &todo_a_id,
            PersistedTodoCommand::SetTitle("Write architecture design doc".to_string()),
        )
        .await?;
    todos
        .apply_command(&todo_b_id, PersistedTodoCommand::SetCompleted(true))
        .await?;
    todos
        .apply_command(&todo_b_id, PersistedTodoCommand::SetUpdatedAt(Utc::now().to_rfc3339()))
        .await?;

    let mut tags = app.open_vec::<TodoTagVec>("macro_tags").await?;
    tags.create(TodoTag::new(todo_a_id.clone(), "important".to_string()))
        .await?;

    let todo_count = todos.list().len();
    let tag_count = tags.list().len();
    let invoked = 2usize;
    let skipped = 0usize;

    drop(tags);
    drop(todos);

    let app_restarted = PersistApp::open_auto(data_root.clone()).await?;
    let restored = app_restarted.open_vec::<TodoVec>("macro_todos").await?;
    let restored_todo_count = restored.list().len();
    drop(restored);
    drop(app_restarted);

    let summary = MacroDemoSummary {
        todo_count,
        tag_count,
        restored_todo_count,
        invoked,
        skipped,
    };

    let _ = tokio::fs::remove_dir_all(&data_root).await;
    Ok(summary)
}
