// Main runtime implementation is split into focused parts to keep
// evolution manageable while preserving one module-level visibility scope.

use super::{
    DeterministicCommandHandler, DeterministicContextCommandHandler,
    DeterministicEnvelopeCommandHandler, PersistEntityRuntime, RUNTIME_FORMAT_VERSION,
    RUNTIME_JOURNAL_FILE, RUNTIME_SNAPSHOT_FILE, RegisteredDeterministicCommand,
    RegisteredDeterministicCommandHandler, RuntimeClosureHandler, RuntimeCommandEnvelope,
    RuntimeCommandInvocation, RuntimeCommandMigrationDescriptor, RuntimeCommandMigrationRule,
    RuntimeCommandPayloadMigration, RuntimeCommandPayloadSchema, RuntimeDeterminismPolicy,
    RuntimeDeterministicContext, RuntimeDurabilityMode, RuntimeEntityKey, RuntimeEntityMailbox,
    RuntimeEntityTombstone, RuntimeEnvelopeApplyResult, RuntimeIdempotencyReceipt,
    RuntimeJournalOp, RuntimeJournalRecord, RuntimeLifecycleReport, RuntimeOperationalPolicy,
    RuntimeOutboxRecord, RuntimeOutboxStatus, RuntimePaths, RuntimeProjectionContract,
    RuntimeProjectionRow, RuntimeProjectionTable, RuntimeProjectionUndo, RuntimeReplicationMode,
    RuntimeSloMetrics, RuntimeSnapshotFile, RuntimeStats, RuntimeStoredEntity,
    build_idempotency_scope_key, build_projection_row, invoke_registered_handler,
    normalize_runtime_policy, runtime_replica_targets, validate_command_envelope,
};
use crate::core::{DbError, Result, Value};
use crate::persist::{PersistMetadata, PersistState, new_persist_id};
use chrono::Utc;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Semaphore;
use tokio::time::{Duration as TokioDuration, sleep, timeout};
use tracing::{Level, event, info_span};

include!("runtime_impl/api_registry_and_crud.rs");
include!("runtime_impl/command_and_lifecycle.rs");
include!("runtime_impl/storage_and_projection.rs");
include!("runtime_impl/internals.rs");
