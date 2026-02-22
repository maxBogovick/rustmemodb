use super::*;

// Managed vec implementation is split into focused areas for easier evolution.
include!("managed_vec/base_collection.rs");
include!("managed_vec/indexed_crud.rs");
include!("managed_vec/command_model.rs");
include!("managed_vec/optimistic_workflows.rs");
include!("managed_vec/io_utils.rs");
