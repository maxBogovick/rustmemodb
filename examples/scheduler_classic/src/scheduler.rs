use crate::model::{Task, TaskStatus};
use sqlx::PgPool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;
use tracing::{error, info}; // Assume types are available

pub async fn start_scheduler_loop(pool: PgPool) {
    info!("Scheduler loop started");
    loop {
        if let Err(e) = process_tasks(&pool).await {
            error!("Error in scheduler loop: {}", e);
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn process_tasks(pool: &PgPool) -> anyhow::Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

    // Start a transaction
    let mut tx = pool.begin().await?;

    // Select pending tasks due for execution with FOR UPDATE SKIP LOCKED
    // This allows multiple scheduler instances to run without conflict (if scaled horizontally)
    let tasks = sqlx::query_as::<_, Task>(
        r#"
        SELECT * FROM tasks 
        WHERE status = 'Pending' AND schedule_time <= $1
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(now)
    .fetch_all(&mut *tx)
    .await?;

    for task in tasks {
        info!("Executing Task: {} (ID: {})", task.name, task.id);

        match task.command_type.as_str() {
            "log" => println!("[SCHEDULER] LOG: {}", task.command_payload),
            "play" => println!("[SCHEDULER] PLAYING SOUND: {}", task.command_payload),
            _ => println!("[SCHEDULER] UNKNOWN COMMAND: {}", task.command_type),
        }

        // Update status to Completed
        sqlx::query("UPDATE tasks SET status = 'Completed', updated_at = NOW() WHERE id = $1")
            .bind(task.id)
            .execute(&mut *tx)
            .await?;

        info!("Task {} completed", task.id);
    }

    // Commit transaction
    tx.commit().await?;

    Ok(())
}
