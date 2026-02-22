//! Asynchronous runtime for managing persistence tasks.
//!
//! The `PersistRuntime` orchestrates background tasks such as:
//! - Periodic snapshotting of state.
//! - Journaling of events/commands.
//! - Handling asynchronous messages via `PersistEnvelope`.
//! - Managing projections and specialized update handlers.

use super::PersistState;
use crate::core::{DbError, Result, Value};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::{Mutex, Semaphore, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration as TokioDuration, sleep};
use uuid::Uuid;

pub(crate) const RUNTIME_SNAPSHOT_FILE: &str = "runtime_snapshot.json";
pub(crate) const RUNTIME_JOURNAL_FILE: &str = "runtime_journal.log";
pub(crate) const RUNTIME_FORMAT_VERSION: u16 = 1;

include!("runtime/types/handlers_and_envelope.rs");
include!("runtime/types/policy.rs");
include!("runtime/types/entity_and_journal.rs");
include!("runtime/types/projection.rs");
include!("runtime/types/stats_and_registry.rs");

#[path = "runtime/runtime_impl.rs"]
mod runtime_impl;

include!("runtime/runtime_support.rs");
