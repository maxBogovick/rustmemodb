use super::*;

// Aggregate store implementation is split by concern to keep app-layer APIs compact.
include!("aggregate_store/core.rs");
include!("aggregate_store/indexed_crud_query.rs");
include!("aggregate_store/command_audit_workflow.rs");
