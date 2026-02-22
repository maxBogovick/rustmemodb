use rustmemodb::persist::{sql_escape_string, PersistValue};
use rustmemodb::{persist_struct, persist_vec, PersistModel};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
}

impl PersistValue for TaskStatus {
    fn sql_type() -> &'static str {
        "TEXT"
    }

    fn to_sql_literal(&self) -> String {
        // Human-readable text is easier to inspect/debug than serialized JSON strings.
        let status = match self {
            Self::Pending => "Pending",
            Self::InProgress => "InProgress",
            Self::Completed => "Completed",
            Self::Failed => "Failed",
        };
        format!("'{}'", status)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Log(String),
    PlaySound(String),
}

impl PersistValue for Command {
    fn sql_type() -> &'static str {
        "TEXT"
    }

    fn to_sql_literal(&self) -> String {
        format!(
            "'{}'",
            sql_escape_string(&serde_json::to_string(self).expect("failed to serialize Command"))
        )
    }
}

// OLD IMPLEMENTATION (kept for comparison):
//
// persist_struct! {
//     pub struct Task {
//         name: String,
//         schedule_time: u64,
//         command: Command,
//         status: TaskStatus,
//     }
// }
//
// Why new approach is better:
// - `PersistModel` + `persist_struct!(from_struct=...)` generates Draft/Patch/Command types
// - API layer can use create_from_draft/patch/apply_command consistently
// - less handwritten glue code and fewer mutation mistakes
#[derive(Debug, Clone, Serialize, Deserialize, PersistModel)]
#[persist_model(table = "scheduler_tasks", schema_version = 1)]
pub struct TaskModel {
    pub name: String,
    pub schedule_time: u64, // Unix timestamp
    pub command: Command,
    pub status: TaskStatus,
    pub last_error: Option<String>,
}

persist_struct!(pub struct Task from_struct = TaskModel);

persist_vec!(pub TaskVec, Task);
