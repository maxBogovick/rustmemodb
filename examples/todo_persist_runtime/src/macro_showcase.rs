use anyhow::Result;
use chrono::Utc;
use rustmemodb::{
    InMemoryDB, InvokeStatus, PersistModel, PersistSession, SnapshotMode, Value, persist_struct,
    persist_vec,
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
    let session = PersistSession::new(InMemoryDB::new());

    let mut todo_a = PersistedTodo::from_draft(PersistedTodoDraft::new(
        "Write design doc".to_string(),
        false,
        2,
        Utc::now().to_rfc3339(),
        Utc::now().to_rfc3339(),
    ));
    todo_a.bind_session(session.clone());
    todo_a.set_auto_persist(true)?;
    todo_a
        .apply_persisted(PersistedTodoCommand::SetPriority(3))
        .await?;
    todo_a
        .apply_persisted(PersistedTodoCommand::SetTitle(
            "Write architecture design doc".to_string(),
        ))
        .await?;

    let mut todo_b = PersistedTodo::from_parts(
        "Ship beta".to_string(),
        false,
        1,
        Utc::now().to_rfc3339(),
        Utc::now().to_rfc3339(),
    );
    todo_b.register_function("mark_done", |todo, _args| {
        todo.apply(PersistedTodoCommand::SetCompleted(true))?;
        todo.apply(PersistedTodoCommand::SetUpdatedAt(Utc::now().to_rfc3339()))?;
        Ok(Value::Boolean(true))
    });

    let mut todos = TodoVec::new("macro_todos");
    todos.add_many(vec![todo_a, todo_b]);
    todos.save_all(&session).await?;

    let outcomes = todos
        .invoke_supported("mark_done", vec![], &session)
        .await?;
    let invoked = outcomes
        .iter()
        .filter(|o| matches!(o.status, InvokeStatus::Invoked))
        .count();
    let skipped = outcomes
        .iter()
        .filter(|o| matches!(o.status, InvokeStatus::SkippedUnsupported))
        .count();

    let mut tags = TodoTagVec::new("macro_tags");
    let todo_states = todos.states();
    if let Some(first) = todo_states.first() {
        let tag = TodoTag::new(first.persist_id.clone(), "important".to_string());
        tags.add_one(tag);
        tags.save_all(&session).await?;
    }

    let snapshot = todos.snapshot(SnapshotMode::WithData);
    let restore_session = PersistSession::new(InMemoryDB::new());
    let mut restored = TodoVec::new("macro_todos_restored");
    restored.restore(snapshot, &restore_session).await?;

    Ok(MacroDemoSummary {
        todo_count: todos.len(),
        tag_count: tags.len(),
        restored_todo_count: restored.len(),
        invoked,
        skipped,
    })
}
