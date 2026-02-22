use crate::core::{DbError, Result};
use crate::facade::InMemoryDB;
use crate::persist::{
    PersistCommandContract, PersistCommandModel, PersistCommandName, PersistEntity,
    PersistPatchContract, PersistSession, RestoreConflictPolicy, SnapshotMode,
};
use crate::transaction::TransactionId;
use chrono::Utc;
use log::warn;
use serde::{Serialize, de::DeserializeOwned};
use std::cmp::Ordering;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs;
use tokio::sync::Mutex;

// Keep `app.rs` as a compact public entrypoint.
include!("app/collection_contracts.rs");
include!("app/policies_and_conflicts.rs");
include!("app/app_open.rs");
include!("app/store_types.rs");
include!("app/domain_handle.rs");
include!("app/autonomous_model_handle.rs");

mod aggregate_store;
mod autonomous;
mod legacy_adapter;
mod managed_vec;
