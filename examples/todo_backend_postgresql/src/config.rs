use anyhow::{Context, Result};
use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub host: String,
    pub port: u16,
    pub database_url: String,
    pub db_max_connections: u32,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let host = env::var("APP_HOST").unwrap_or_else(|_| "0.0.0.0".to_string());

        let port = env::var("APP_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse::<u16>()
            .context("APP_PORT must be a valid u16")?;

        let database_url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/todo_db".to_string());

        let db_max_connections = env::var("DB_MAX_CONNECTIONS")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u32>()
            .context("DB_MAX_CONNECTIONS must be a valid u32")?;

        Ok(Self {
            host,
            port,
            database_url,
            db_max_connections,
        })
    }

    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}
