use std::net::SocketAddr;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind_addr: SocketAddr,
    pub database_url: String,
    pub db_max_connections: u32,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env_string("HH_BIND_ADDR", "127.0.0.1:18080")
            .parse::<SocketAddr>()
            .context("HH_BIND_ADDR must be a valid host:port")?;

        let database_url = env_string(
            "HH_DATABASE_URL",
            "postgres://habit:habit@127.0.0.1:5432/habit_hero",
        );

        let db_max_connections = env_string("HH_DB_MAX_CONNECTIONS", "10")
            .parse::<u32>()
            .context("HH_DB_MAX_CONNECTIONS must be u32")?;

        Ok(Self {
            bind_addr,
            database_url,
            db_max_connections,
        })
    }
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
