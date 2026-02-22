use super::*;

// Autonomous aggregate API is split by concern to keep intent-level DX readable.
include!("autonomous/core_read.rs");
include!("autonomous/conflict_and_apply.rs");
include!("autonomous/high_level_convenience.rs");
include!("autonomous/workflow_and_compat.rs");
