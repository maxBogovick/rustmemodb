//! Macro definitions for persistence.
//!
//! This module exports macros that simplify the implementation of `PersistEntity` and related traits.
//! Implementations are split into focused files to keep context manageable.

#[path = "macros/attr_helpers.rs"]
mod attr_helpers;
#[path = "macros/persist_struct.rs"]
mod persist_struct;
#[path = "macros/persist_vec.rs"]
mod persist_vec;
