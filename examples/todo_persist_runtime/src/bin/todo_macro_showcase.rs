use anyhow::Result;
use todo_persist_runtime::macro_showcase::run_macro_showcase;

#[tokio::main]
async fn main() -> Result<()> {
    let summary = run_macro_showcase().await?;

    println!("=== todo_persist_runtime :: macro showcase ===");
    println!("#[derive(PersistModel)] + #[persist_model(schema_version = 2)] used for TodoModel");
    println!("persist_struct!(from_struct = TodoModel) used for PersistedTodo");
    println!(
        "persist_vec!(TodoVec, PersistedTodo) and persist_vec!(TodoTagVec, TodoTag) exercised"
    );
    println!("todos in vec: {}", summary.todo_count);
    println!("tags in vec: {}", summary.tag_count);
    println!(
        "restored todos from snapshot: {}",
        summary.restored_todo_count
    );
    println!(
        "selective invoke(mark_done): invoked={}, skipped={}",
        summary.invoked, summary.skipped
    );

    Ok(())
}
