use super::*;

// Keep PersistVec implementation split by concern to avoid monolithic files.
include!("persist_vec_impl/basics_and_io.rs");
include!("persist_vec_impl/invoke_and_prune.rs");
include!("persist_vec_impl/snapshot_and_restore.rs");
