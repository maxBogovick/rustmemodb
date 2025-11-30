// ============================================================================
// RustMemDB Library
// ============================================================================

pub mod core;
pub mod storage;
pub mod result;
pub mod facade;
mod parser;
mod planner;
mod executor;
mod expression;
mod plugins;
mod evaluator;

// Re-export main types for convenience
pub use facade::InMemoryDB;
pub use core::{Result, DbError, Value, DataType};
pub use result::QueryResult;
