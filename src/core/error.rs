use thiserror::Error;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Table '{0}' already exists")]
    TableExists(String),

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Column '{0}' not found in table '{1}'")]
    ColumnNotFound(String, String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("Lock error: {0}")]
    LockError(String),

    #[error("I/0 error: {0}")]
    IoError(String),
}

pub type Result<T> = std::result::Result<T, DbError>;


impl<T> From<std::sync::PoisonError<T>> for DbError {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Self::LockError(err.to_string())
    }
}