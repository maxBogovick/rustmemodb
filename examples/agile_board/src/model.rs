use rustmemodb::prelude::dx::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PersistJsonValue)]
pub struct Task {
    pub id: Uuid,
    pub title: String,
    pub description: String,
    pub tags: Vec<String>,
}

impl Task {
    pub fn new(title: String, description: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            title,
            description,
            tags: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, PersistJsonValue)]
pub struct Column {
    pub id: Uuid,
    pub title: String,
    pub tasks: Vec<Task>,
}

impl Column {
    pub fn new(title: String) -> Self {
        Self {
            id: Uuid::new_v4(),
            title,
            tasks: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Autonomous)]
#[persist_model(table = "boards", schema_version = 1)]
pub struct Board {
    name: String,
    columns: PersistJson<Vec<Column>>,
}

#[expose_rest]
impl Board {
    pub fn new(name: String) -> Self {
        Self {
            name,
            columns: PersistJson::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn columns(&self) -> &[Column] {
        self.columns.as_slice()
    }

    #[command]
    pub fn rename(&mut self, name: String) -> Result<(), BoardDomainError> {
        self.name = normalize_required(name, "board name")?;
        Ok(())
    }

    #[command]
    pub fn add_column(&mut self, title: String) -> Result<Uuid, BoardDomainError> {
        let column = Column::new(normalize_required(title, "column title")?);
        let column_id = column.id;
        self.columns.push(column);
        Ok(column_id)
    }

    #[command]
    pub fn rename_column(
        &mut self,
        column_id: Uuid,
        title: String,
    ) -> Result<(), BoardDomainError> {
        let title = normalize_required(title, "column title")?;
        let column = self
            .columns
            .iter_mut()
            .find(|column| column.id == column_id)
            .ok_or(BoardDomainError::ColumnNotFound(column_id))?;
        column.title = title;
        Ok(())
    }

    #[command]
    pub fn remove_column(&mut self, column_id: Uuid) -> Result<(), BoardDomainError> {
        let index = self
            .columns
            .iter()
            .position(|column| column.id == column_id)
            .ok_or(BoardDomainError::ColumnNotFound(column_id))?;
        self.columns.remove(index);
        Ok(())
    }

    #[command]
    pub fn add_task(
        &mut self,
        column_id: Option<Uuid>,
        title: String,
        description: String,
    ) -> Result<Uuid, BoardDomainError> {
        let task = Task::new(
            normalize_required(title, "task title")?,
            normalize_required(description, "task description")?,
        );
        let task_id = task.id;
        let target_column = match column_id {
            Some(column_id) => self
                .columns
                .iter_mut()
                .find(|column| column.id == column_id)
                .ok_or(BoardDomainError::ColumnNotFound(column_id))?,
            None => self
                .columns
                .first_mut()
                .ok_or(BoardDomainError::NoColumns)?,
        };

        target_column.tasks.push(task);
        Ok(task_id)
    }

    #[command]
    pub fn update_task(
        &mut self,
        task_id: Uuid,
        title: Option<String>,
        description: Option<String>,
        tags: Option<Vec<String>>,
    ) -> Result<(), BoardDomainError> {
        let task = self
            .columns
            .iter_mut()
            .flat_map(|column| column.tasks.iter_mut())
            .find(|task| task.id == task_id)
            .ok_or(BoardDomainError::TaskNotFound(task_id))?;

        if let Some(title) = title {
            task.title = normalize_required(title, "task title")?;
        }
        if let Some(description) = description {
            task.description = normalize_required(description, "task description")?;
        }
        if let Some(tags) = tags {
            task.tags = tags;
        }
        Ok(())
    }

    #[command]
    pub fn remove_task(&mut self, task_id: Uuid) -> Result<(), BoardDomainError> {
        for column in self.columns.iter_mut() {
            if let Some(index) = column.tasks.iter().position(|task| task.id == task_id) {
                column.tasks.remove(index);
                return Ok(());
            }
        }
        Err(BoardDomainError::TaskNotFound(task_id))
    }

    #[command]
    pub fn move_task(
        &mut self,
        task_id: Uuid,
        to_column_id: Uuid,
        new_index: usize,
    ) -> Result<(), BoardDomainError> {
        let source_column_index = self
            .columns
            .iter()
            .position(|column| column.tasks.iter().any(|task| task.id == task_id))
            .ok_or(BoardDomainError::TaskNotFound(task_id))?;
        let source_task_index = self.columns[source_column_index]
            .tasks
            .iter()
            .position(|task| task.id == task_id)
            .ok_or(BoardDomainError::TaskNotFound(task_id))?;
        let task = self.columns[source_column_index].tasks.remove(source_task_index);

        let target_column_index = self
            .columns
            .iter()
            .position(|column| column.id == to_column_id)
            .ok_or(BoardDomainError::ColumnNotFound(to_column_id))?;
        let target_tasks = &mut self.columns[target_column_index].tasks;
        let bounded_index = new_index.min(target_tasks.len());
        target_tasks.insert(bounded_index, task);
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, ApiError)]
pub enum BoardDomainError {
    #[api_error(status = 422, code = "validation_error")]
    Validation(String),
    #[api_error(status = 404, code = "column_not_found")]
    ColumnNotFound(Uuid),
    #[api_error(status = 404, code = "task_not_found")]
    TaskNotFound(Uuid),
    #[api_error(status = 422, code = "no_columns")]
    NoColumns,
}

impl std::fmt::Display for BoardDomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(message) => write!(f, "{message}"),
            Self::ColumnNotFound(column_id) => write!(f, "column not found: {column_id}"),
            Self::TaskNotFound(task_id) => write!(f, "task not found: {task_id}"),
            Self::NoColumns => write!(f, "board has no columns"),
        }
    }
}

impl std::error::Error for BoardDomainError {}

fn normalize_required(value: String, field_name: &str) -> Result<String, BoardDomainError> {
    let normalized = value.trim().to_string();
    if normalized.is_empty() {
        return Err(BoardDomainError::Validation(format!(
            "{field_name} must not be empty"
        )));
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::{Board, BoardDomainError};

    #[test]
    fn board_domain_methods_manage_nested_state() {
        let mut board = Board::new("Platform".to_string());
        let backlog = board.add_column("Backlog".to_string()).expect("add column");
        let in_progress = board
            .add_column("In Progress".to_string())
            .expect("add column");
        let task_id = board
            .add_task(Some(backlog), "Design API".to_string(), "v1".to_string())
            .expect("add task");
        board
            .update_task(
                task_id,
                Some("Design API v2".to_string()),
                Some("v2".to_string()),
                Some(vec!["dx".to_string()]),
            )
            .expect("update task");
        board.move_task(task_id, in_progress, 0).expect("move task");

        let columns = board.columns();
        assert_eq!(columns[0].tasks.len(), 0);
        assert_eq!(columns[1].tasks.len(), 1);
        assert_eq!(columns[1].tasks[0].title, "Design API v2");
    }

    #[test]
    fn board_domain_methods_report_not_found_errors() {
        let mut board = Board::new("Platform".to_string());
        let missing_task = uuid::Uuid::new_v4();

        let error = board
            .remove_task(missing_task)
            .expect_err("missing task should fail");
        assert_eq!(error, BoardDomainError::TaskNotFound(missing_task));
    }

    #[test]
    fn board_domain_methods_validate_required_text_fields() {
        let mut board = Board::new("Platform".to_string());
        let error = board
            .add_column("   ".to_string())
            .expect_err("empty column title should fail");
        assert_eq!(
            error,
            BoardDomainError::Validation("column title must not be empty".to_string())
        );
    }
}
