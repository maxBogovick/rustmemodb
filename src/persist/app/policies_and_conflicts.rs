/// Defines the replication mode for the persistence application.
#[derive(Debug, Clone)]
pub enum PersistReplicationMode {
    /// Synchronous replication: operations block until written to replicas.
    Sync,
    /// Async replication: operations return immediately, replication happens in background.
    AsyncBestEffort,
}

/// Configuration for replication behavior.
#[derive(Debug, Clone)]
pub struct PersistReplicationPolicy {
    /// The replication mode (Sync or Async).
    pub mode: PersistReplicationMode,
    /// List of root directories for replica storage.
    pub replica_roots: Vec<PathBuf>,
}

impl Default for PersistReplicationPolicy {
    fn default() -> Self {
        Self {
            mode: PersistReplicationMode::Sync,
            replica_roots: Vec::new(),
        }
    }
}

/// Configuration for automatic conflict retry behavior.
#[derive(Debug, Clone)]
pub struct PersistConflictRetryPolicy {
    /// Maximum number of retry attempts for a conflict.
    pub max_attempts: usize,
    /// Base duration in milliseconds for backoff calculation.
    pub base_backoff_ms: u64,
    /// Maximum duration in milliseconds for backoff.
    pub max_backoff_ms: u64,
    /// Whether to retry on Write-Write conflicts.
    ///
    /// Default is `false`. Enabling this is safe for idempotent commands or blind writes,
    /// but may not be suitable for all business logic.
    pub retry_write_write: bool,
}

impl Default for PersistConflictRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            base_backoff_ms: 5,
            max_backoff_ms: 100,
            retry_write_write: false,
        }
    }
}

/// Comprehensive policy configuration for a `PersistApp`.
#[derive(Debug, Clone)]
pub struct PersistAppPolicy {
    /// Number of operations between automatic snapshots.
    pub snapshot_every_ops: usize,
    /// Replication configuration.
    pub replication: PersistReplicationPolicy,
    /// Conflict retry configuration.
    pub conflict_retry: PersistConflictRetryPolicy,
}

impl Default for PersistAppPolicy {
    fn default() -> Self {
        Self {
            snapshot_every_ops: 50,
            replication: PersistReplicationPolicy::default(),
            conflict_retry: PersistConflictRetryPolicy::default(),
        }
    }
}

/// Simplified policy configuration for `open_auto`.
#[derive(Debug, Clone)]
pub struct PersistAppAutoPolicy {
    /// Number of operations between automatic snapshots.
    pub snapshot_every_ops: usize,
    /// Replication configuration.
    pub replication: PersistReplicationPolicy,
    /// Conflict retry configuration.
    pub conflict_retry: PersistConflictRetryPolicy,
}

impl Default for PersistAppAutoPolicy {
    fn default() -> Self {
        Self {
            snapshot_every_ops: 1,
            replication: PersistReplicationPolicy::default(),
            conflict_retry: PersistConflictRetryPolicy::default(),
        }
    }
}

impl From<PersistAppAutoPolicy> for PersistAppPolicy {
    fn from(value: PersistAppAutoPolicy) -> Self {
        Self {
            snapshot_every_ops: value.snapshot_every_ops.max(1),
            replication: value.replication,
            conflict_retry: value.conflict_retry,
        }
    }
}

/// Classifies the type of conflict encountered during a persistence operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedConflictKind {
    /// Optimistic locking failure (version mismatch).
    OptimisticLock,
    /// Concurrent write modification to the same record.
    WriteWrite,
    /// Violation of a unique constraint (e.g., duplicate email).
    UniqueConstraint,
}

impl fmt::Display for ManagedConflictKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::OptimisticLock => "optimistic_lock",
            Self::WriteWrite => "write_write",
            Self::UniqueConstraint => "unique_constraint",
        };
        write!(f, "{label}")
    }
}

/// High-level error type for domain-facing persistence APIs.
///
/// This removes low-level storage vocabulary (`DbError`, SQL/index details)
/// from application services and handlers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistDomainError {
    /// Entity is absent in the domain collection.
    NotFound,
    /// Concurrent mutation conflict (optimistic-lock / write-write).
    ConflictConcurrent(String),
    /// Uniqueness conflict (duplicate natural key, etc.).
    ConflictUnique(String),
    /// Invalid input payload or unsupported domain mutation payload.
    Validation(String),
    /// Infrastructure/runtime failure not intended for business branching.
    Internal(String),
}

impl fmt::Display for PersistDomainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => write!(f, "not found"),
            Self::ConflictConcurrent(message) => {
                write!(f, "concurrent conflict: {message}")
            }
            Self::ConflictUnique(message) => write!(f, "unique conflict: {message}"),
            Self::Validation(message) => write!(f, "validation error: {message}"),
            Self::Internal(message) => write!(f, "internal error: {message}"),
        }
    }
}

impl std::error::Error for PersistDomainError {}

/// Error wrapper for mutation APIs that can fail at two levels:
/// - domain/runtime (`PersistDomainError`)
/// - user closure/business validation (`E`)
#[derive(Debug, PartialEq, Eq)]
pub enum PersistDomainMutationError<E> {
    /// Persistence/domain-level failure (not found, conflict, infrastructure).
    Domain(PersistDomainError),
    /// User-provided mutator rejected the change with a business error.
    User(E),
}

impl<E: fmt::Display> fmt::Display for PersistDomainMutationError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Domain(err) => write!(f, "{err}"),
            Self::User(err) => write!(f, "{err}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for PersistDomainMutationError<E> {}

/// Analyzes a `DbError` and attempts to classify it into a known `ManagedConflictKind`.
///
/// Returns `None` if the error does not resemble a known conflict pattern.
pub fn classify_managed_conflict(err: &DbError) -> Option<ManagedConflictKind> {
    match err {
        DbError::ExecutionError(message) => {
            let lower = message.to_lowercase();
            if lower.contains("optimistic lock conflict") {
                return Some(ManagedConflictKind::OptimisticLock);
            }
            if lower.contains("write-write conflict detected") {
                return Some(ManagedConflictKind::WriteWrite);
            }
            None
        }
        DbError::ConstraintViolation(message) => {
            let lower = message.to_lowercase();
            if lower.contains("unique constraint violation")
                || lower.contains("unique index violation")
                || lower.contains("unique constraint")
            {
                return Some(ManagedConflictKind::UniqueConstraint);
            }
            None
        }
        _ => None,
    }
}

impl From<DbError> for PersistDomainError {
    fn from(err: DbError) -> Self {
        if let Some(kind) = classify_managed_conflict(&err) {
            return match kind {
                ManagedConflictKind::OptimisticLock | ManagedConflictKind::WriteWrite => {
                    Self::ConflictConcurrent(err.to_string())
                }
                ManagedConflictKind::UniqueConstraint => Self::ConflictUnique(err.to_string()),
            };
        }

        match err {
            DbError::ParseError(message)
            | DbError::TypeMismatch(message)
            | DbError::UnsupportedOperation(message) => Self::Validation(message),
            DbError::ConstraintViolation(message) => Self::Validation(message),
            other => Self::Internal(other.to_string()),
        }
    }
}

/// Maps a raw `DbError` to a more descriptive conflict error if applicable.
///
/// Prepend's context about the operation and the classified conflict kind.
fn map_managed_conflict_error(operation: &str, err: DbError) -> DbError {
    let Some(kind) = classify_managed_conflict(&err) else {
        return err;
    };

    let prefix = format!("Conflict({kind}) in managed operation '{operation}'");
    match err {
        DbError::ExecutionError(message) => DbError::ExecutionError(format!("{prefix}: {message}")),
        DbError::ConstraintViolation(message) => {
            DbError::ConstraintViolation(format!("{prefix}: {message}"))
        }
        other => other,
    }
}

/// Generates a default audit event type string from a command name.
///
/// Converts `PascalCase` command names to `snake_case` (e.g., `SetUserActive` -> `set_user_active`).
/// Handles abbreviations and underscores intelligently.
pub fn default_audit_event_type(command_name: &str) -> String {
    let mut normalized = String::with_capacity(command_name.len());
    let mut previous_was_separator = false;

    for (index, ch) in command_name.chars().enumerate() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() {
                if index > 0 && !previous_was_separator {
                    normalized.push('_');
                }
                normalized.push(ch.to_ascii_lowercase());
                previous_was_separator = false;
            } else {
                normalized.push(ch.to_ascii_lowercase());
                previous_was_separator = false;
            }
        } else if !previous_was_separator && !normalized.is_empty() {
            normalized.push('_');
            previous_was_separator = true;
        }
    }

    normalized.trim_matches('_').to_string()
}
