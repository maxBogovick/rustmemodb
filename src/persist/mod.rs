//! # Persistence Module
//!
//! This module provides the core abstractions and implementations for `rustmemodb`'s persistence layer.
//! It handles object serialization, database schema management, transaction control, and runtime orchestration.
//!
//! Key components:
//! - `core`: Fundamental traits (`PersistEntity`, `PersistEntityFactory`) and data structures.
//! - `runtime`: Background task management for snapshots and journaling.
//! - `cluster`: Support for distributed persistence (if enabled).
//! - `macros`: Helper macros for derived implementations.

use crate::core::{DbError, Result, Value};
use crate::facade::InMemoryDB;
use crate::transaction::TransactionId;
use async_trait::async_trait;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

pub mod app;
pub mod cluster;
mod macros;
pub mod runtime;
pub mod web;

// Keep `mod.rs` as a compact public entrypoint.
include!("core/api_version.rs");
include!("core/session_and_metadata.rs");
include!("core/descriptors_and_state.rs");
include!("core/dynamic_schema_contracts.rs");
include!("core/snapshots_and_migrations.rs");
include!("core/entity_contracts.rs");
include!("core/containers_and_values.rs");

#[path = "core/hetero_vec_impl.rs"]
mod hetero_vec_impl;
#[path = "core/migration_impl.rs"]
mod migration_impl;
#[path = "core/persist_value_impls.rs"]
mod persist_value_impls;
#[path = "core/persist_vec_impl.rs"]
mod persist_vec_impl;
#[path = "core/schema_utils.rs"]
mod schema_utils;
#[path = "core/session_impl.rs"]
mod session_impl;

pub use schema_utils::{
    default_index_name, default_table_name, default_table_name_stable, dynamic_schema_from_ddl,
    dynamic_schema_from_json_schema, json_to_sql_literal, new_persist_id, sql_escape_string,
};
