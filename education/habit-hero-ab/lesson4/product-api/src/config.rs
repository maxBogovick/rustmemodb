use std::{net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub bind_addr: SocketAddr,
    pub data_dir: PathBuf,
}

impl AppConfig {
    pub fn from_env() -> Result<Self> {
        let bind_addr = env_string("HH_BIND_ADDR", "127.0.0.1:18081")
            .parse::<SocketAddr>()
            .context("HH_BIND_ADDR must be a valid host:port")?;

        let data_dir = PathBuf::from(env_string(
            "HH_DATA_DIR",
            "education/habit-hero-ab/lesson4/product-api/.data",
        ));

        Ok(Self {
            bind_addr,
            data_dir,
        })
    }
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}
