pub mod app;
pub mod config;
pub mod error;
pub mod handlers;
pub mod models;
pub mod repository;
pub mod state;

pub use app::build_router;
