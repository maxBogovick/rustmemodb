use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseBackend {
    RustMemDb,
    Postgres,
}

impl DatabaseBackend {
    fn from_env(raw: &str) -> Result<Self> {
        match raw.to_ascii_lowercase().as_str() {
            "rustmemodb" | "rustmemdb" | "rust" => Ok(Self::RustMemDb),
            "postgres" | "postgresql" | "pg" => Ok(Self::Postgres),
            _ => Err(anyhow::anyhow!(
                "DATABASE_BACKEND must be one of: rustmemodb, postgres"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub database_backend: DatabaseBackend,
    pub database_url: String,
    pub db_max_connections: u32,
    pub rustmemodb_username: String,
    pub rustmemodb_password: String,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let host = env::var("APP_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());

        let port = env::var("APP_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .context("APP_PORT must be a valid u16")?;

        let database_backend = DatabaseBackend::from_env(
            &env::var("DATABASE_BACKEND").unwrap_or_else(|_| "rustmemodb".to_string()),
        )?;

        let database_url = env::var("DATABASE_URL").unwrap_or_else(|_| {
            match database_backend {
                DatabaseBackend::RustMemDb => {
                    "rustmemodb://admin:adminpass@localhost:5432/rustmemodb"
                }
                DatabaseBackend::Postgres => "postgres://postgres:postgres@localhost:5432/todo_db",
            }
            .to_string()
        });

        let db_max_connections = env::var("DB_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u32>()
            .context("DB_MAX_CONNECTIONS must be a valid u32")?;

        let rustmemodb_username =
            env::var("RUSTMEMODB_USERNAME").unwrap_or_else(|_| "admin".to_string());
        let rustmemodb_password =
            env::var("RUSTMEMODB_PASSWORD").unwrap_or_else(|_| "adminpass".to_string());

        Ok(Self {
            host,
            port,
            database_backend,
            database_url,
            db_max_connections,
            rustmemodb_username,
            rustmemodb_password,
        })
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
