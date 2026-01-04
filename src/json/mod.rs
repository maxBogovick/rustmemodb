//! JSON Storage Adapter Module
//!
//! This module provides a high-level API for working with JSON documents
//! as if they were SQL tables, following the Facade pattern to hide
//! complexity of schema inference and SQL generation.
//!
//! # Architecture
//!
//! - `adapter.rs` - Main facade (JsonStorageAdapter)
//! - `schema_inference.rs` - Strategy pattern for schema detection
//! - `converter.rs` - JSON to SQL conversion logic
//! - `validator.rs` - SQL query validation and security
//! - `error.rs` - Domain-specific errors

mod adapter;
mod converter;
mod error;
mod schema_inference;
mod validator;

pub use adapter::JsonStorageAdapter;
pub use error::{JsonError, JsonResult};
