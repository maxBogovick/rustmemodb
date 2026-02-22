use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{FromRow, Row};
use time::OffsetDateTime;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]

pub enum TaskStatus {
    Pending,
    Completed,
    Failed,
}

// Manual implementation for SQLx to map TEXT column to Enum
impl sqlx::Type<sqlx::Postgres> for TaskStatus {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("TEXT")
    }
}

impl sqlx::Decode<'_, sqlx::Postgres> for TaskStatus {
    fn decode(
        value: sqlx::postgres::PgValueRef<'_>,
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let s: &str = <&str as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        match s {
            "Pending" => Ok(TaskStatus::Pending),
            "Completed" => Ok(TaskStatus::Completed),
            "Failed" => Ok(TaskStatus::Failed),
            _ => Err(format!("Unknown TaskStatus: {}", s).into()),
        }
    }
}

impl sqlx::Encode<'_, sqlx::Postgres> for TaskStatus {
    fn encode_by_ref(&self, buf: &mut sqlx::postgres::PgArgumentBuffer) -> sqlx::encode::IsNull {
        let s = match self {
            TaskStatus::Pending => "Pending",
            TaskStatus::Completed => "Completed",
            TaskStatus::Failed => "Failed",
        };
        <String as sqlx::Encode<sqlx::Postgres>>::encode_by_ref(&s.to_string(), buf)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Command {
    Log(String),
    PlaySound(String),
}

#[derive(Debug, Clone, Serialize, FromRow)]
pub struct Task {
    pub id: Uuid,
    pub name: String,
    pub schedule_time: i64,
    pub command_type: String,
    pub command_payload: String,
    pub status: TaskStatus,
    #[serde(with = "time::serde::iso8601")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::iso8601")]
    pub updated_at: OffsetDateTime,
}

#[derive(Deserialize)]
pub struct CreateTaskRequest {
    pub name: String,
    pub schedule_time: i64,
    pub command_type: String, // "log" or "play"
    pub payload: String,
}

#[derive(Serialize)]
pub struct TaskResponse {
    pub id: Uuid,
    pub name: String,
    pub schedule_time: i64,
    pub command: Command,
    pub status: TaskStatus,
}

impl From<Task> for TaskResponse {
    fn from(task: Task) -> Self {
        let command = match task.command_type.as_str() {
            "play" => Command::PlaySound(task.command_payload),
            _ => Command::Log(task.command_payload),
        };

        TaskResponse {
            id: task.id,
            name: task.name,
            schedule_time: task.schedule_time,
            command,
            status: task.status,
        }
    }
}
