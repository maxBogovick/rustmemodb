use crate::model::{Command, Task, TaskCommand, TaskStatus, TaskVec};
use rustmemodb::ManagedPersistVec;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{error, info};

type SharedTasks = Arc<Mutex<ManagedPersistVec<TaskVec>>>;

#[derive(Debug, Clone)]
struct DueTask {
    id: String,
    name: String,
    command: Command,
}

pub async fn start_scheduler_loop(tasks: SharedTasks) {
    info!("Scheduler loop started");
    loop {
        if let Err(e) = process_tasks(&tasks).await {
            error!("Error in scheduler loop: {}", e);
        }
        sleep(Duration::from_secs(1)).await;
    }
}

// OLD IMPLEMENTATION (kept for comparison):
//
// async fn process_tasks(app: &PersistApp) -> anyhow::Result<()> {
//     let mut tasks: ManagedPersistVec<TaskVec> = app.open_vec::<TaskVec>("tasks").await?;
//     let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
//     let tasks_to_process = tasks
//         .list()
//         .iter()
//         .filter(|task| *task.status() == TaskStatus::Pending && *task.schedule_time() <= now)
//         .map(|task| (task.persist_id().to_string(), task.name().clone(), task.command().clone()))
//         .collect::<Vec<_>>();
//     for (id, name, command) in tasks_to_process {
//         // execute side effect first, then update status
//         tasks.update(&id, |t| { t.set_status(TaskStatus::Completed); Ok(()) }).await?;
//     }
//     Ok(())
// }
//
// Why new approach is better:
// - shared managed vec (no reopen each tick)
// - task claiming via `InProgress` before side effect execution
// - explicit failed state with error reason for observability
async fn process_tasks(tasks: &SharedTasks) -> anyhow::Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    // Step 1: snapshot due pending tasks.
    let due_tasks = {
        let tasks_guard = tasks.lock().await;
        tasks_guard
            .list_filtered(|task| {
                *task.status() == TaskStatus::Pending && *task.schedule_time() <= now
            })
            .into_iter()
            .map(|task| DueTask {
                id: task.persist_id().to_string(),
                name: task.name().clone(),
                command: task.command().clone(),
            })
            .collect::<Vec<_>>()
    };

    if due_tasks.is_empty() {
        return Ok(());
    }

    // Step 2: claim tasks by transitioning to InProgress.
    let mut claimed = Vec::new();
    for due in due_tasks {
        let mut tasks_guard = tasks.lock().await;
        let found = tasks_guard
            .apply_command(&due.id, TaskCommand::SetStatus(TaskStatus::InProgress))
            .await?;
        if found {
            claimed.push(due);
        }
    }

    // Step 3: execute side-effects outside lock and persist final state.
    for due in claimed {
        info!("Executing Task: {} (ID: {})", due.name, due.id);
        let execution_result = execute_command(&due.command);

        let mut tasks_guard = tasks.lock().await;
        tasks_guard
            .update(&due.id, |task: &mut Task| {
                match &execution_result {
                    Ok(()) => {
                        task.set_status(TaskStatus::Completed);
                        task.set_last_error(None);
                    }
                    Err(err) => {
                        task.set_status(TaskStatus::Failed);
                        task.set_last_error(Some(err.clone()));
                    }
                }
                Ok(())
            })
            .await?;

        match execution_result {
            Ok(()) => info!("Task {} completed", due.id),
            Err(err) => error!("Task {} failed: {}", due.id, err),
        }
    }

    Ok(())
}

fn execute_command(command: &Command) -> Result<(), String> {
    match command {
        Command::Log(msg) => {
            println!("[SCHEDULER] LOG: {}", msg);
            Ok(())
        }
        Command::PlaySound(file) => {
            if file.trim().is_empty() {
                return Err("play command payload must not be empty".to_string());
            }
            println!("[SCHEDULER] PLAYING SOUND: {}", file);
            Ok(())
        }
    }
}
