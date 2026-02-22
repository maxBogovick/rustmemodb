// ============================================================================
// RustMemDB Library
// ============================================================================

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

extern crate self as rustmemodb;

pub mod connection;
pub mod core;
mod evaluator;
mod executor;
mod expression;
pub mod facade;
pub mod interface;
pub mod json;
pub mod model_lang;
mod parser;
pub mod persist;
pub mod planner;
mod plugins;
pub mod prelude;
pub mod result;
pub mod server;
pub mod storage;
pub mod transaction;
pub mod web;
pub use paste;

// Re-export main types for convenience
pub use core::{DataType, DbError, Result, Row, Value};
pub use facade::InMemoryDB;
pub use interface::{DatabaseClient, DatabaseFactory};
pub use model_lang::{
    FieldDecl, FieldType, ModelProgram, StructDecl, parse_and_materialize_models,
};
pub use persist::app::{
    LegacyPersistVecAdapter, ManagedConflictKind, ManagedPersistVec, ManagedPersistVecStats,
    PersistAggregatePage, PersistAggregateStore, PersistApp, PersistAppAutoPolicy,
    PersistAppPolicy, PersistAuditRecord, PersistAuditRecordVec, PersistAutonomousAggregate,
    PersistAutonomousCommand, PersistAutonomousModel, PersistAutonomousModelHandle,
    PersistAutonomousRecord, PersistAutonomousRestModel, PersistBackedModel, PersistCollection,
    PersistConflictRetryPolicy, PersistDomainError, PersistDomainHandle,
    PersistDomainMutationError, PersistDomainStore, PersistIdempotentCommandResult,
    PersistIndexedCollection, PersistReplicationMode, PersistReplicationPolicy, PersistTx,
    PersistWorkflowCommandModel, classify_managed_conflict,
};
pub use persist::cluster::{
    InMemoryRuntimeForwarder, RuntimeClusterApplyResult, RuntimeClusterForwarder,
    RuntimeClusterMembership, RuntimeClusterNode, RuntimeClusterQuorumStatus,
    RuntimeClusterWritePolicy, RuntimeShardLeader, RuntimeShardMovement, RuntimeShardRoute,
    RuntimeShardRoutingTable, stable_shard_for,
};
pub use persist::runtime::{
    DeterministicCommandHandler, DeterministicContextCommandHandler,
    DeterministicEnvelopeCommandHandler, PersistEntityRuntime, RuntimeBackpressurePolicy,
    RuntimeClosureHandler, RuntimeCommandEnvelope, RuntimeCommandMigrationDescriptor,
    RuntimeCommandPayloadMigration, RuntimeCommandPayloadSchema, RuntimeCompatIssue,
    RuntimeCompatReport, RuntimeConsistencyMode, RuntimeDeterminismPolicy,
    RuntimeDeterministicContext, RuntimeDurabilityMode, RuntimeEntityKey, RuntimeEntityTombstone,
    RuntimeEnvelopeApplyResult, RuntimeIdempotencyReceipt, RuntimeJournalOp, RuntimeJournalRecord,
    RuntimeLifecyclePolicy, RuntimeLifecycleReport, RuntimeOperationalPolicy, RuntimeOutboxRecord,
    RuntimeOutboxStatus, RuntimePaths, RuntimePayloadFieldContract, RuntimePayloadType,
    RuntimeProjectionContract, RuntimeProjectionField, RuntimeProjectionRow,
    RuntimeReplicationMode, RuntimeReplicationPolicy, RuntimeRetryPolicy, RuntimeSideEffectSpec,
    RuntimeSloMetrics, RuntimeSnapshotFile, RuntimeSnapshotPolicy, RuntimeSnapshotWorker,
    RuntimeStats, RuntimeStoredEntity, RuntimeTombstonePolicy, runtime_journal_compat_check,
    runtime_snapshot_compat_check, spawn_runtime_snapshot_worker,
};
pub use persist::web::{
    IDEMPOTENCY_KEY_INVALID_MESSAGE, IDEMPOTENCY_KEY_MAX_LEN, IDEMPOTENCY_KEY_TOO_LONG_MESSAGE,
    IF_MATCH_INVALID_ASCII_MESSAGE, IF_MATCH_INVALID_VERSION_MESSAGE, IF_MATCH_REQUIRED_MESSAGE,
    PersistServiceError, PersistWebInputError, PersistWebProblem, map_conflict_problem,
    normalize_idempotency_key, normalize_request_id, parse_if_match_header,
};
pub use persist::{
    FunctionDescriptor, HeteroPersistVec, HeteroPersistVecSnapshot, HeteroTypeSnapshot,
    InvokeOutcome, InvokeStatus, ObjectDescriptor, PERSIST_PUBLIC_API_VERSION_MAJOR,
    PERSIST_PUBLIC_API_VERSION_MINOR, PERSIST_PUBLIC_API_VERSION_PATCH,
    PERSIST_PUBLIC_API_VERSION_STRING, PERSIST_SCHEMA_REGISTRY_TABLE, PersistCommandContract,
    PersistCommandFieldContract, PersistCommandModel, PersistCommandName, PersistEntity,
    PersistEntityFactory, PersistJson, PersistMetadata, PersistMigrationPlan, PersistMigrationStep,
    PersistModelExt, PersistPatchContract, PersistPublicApiVersion, PersistSession, PersistState,
    PersistValue, PersistVec, PersistVecSnapshot, RestoreConflictPolicy, SnapshotMode,
    StateMigrationFn, default_schema_version, persist_public_api_version,
};
pub use result::QueryResult;
pub use rustmemodb_derive::{
    ApiError, Autonomous, PersistAutonomousIntent, PersistJsonValue, PersistModel, autonomous_impl,
    command, expose_rest, persistent, persistent_impl, query, view,
};

#[cfg(feature = "unistructgen")]
pub use unistructgen_macro::{
    generate_struct_from_env, generate_struct_from_graphql, generate_struct_from_json,
    generate_struct_from_sql, json_struct, openapi_to_rust, struct_from_external_api,
};
