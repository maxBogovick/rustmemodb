mod cli;
mod load_test;

use crate::cli::app::App;
use crate::load_test::{LoadTestConfig, run_load_test};
use clap::{Parser, Subcommand};
use rustmemodb::InMemoryDB;
use rustmemodb::server::pg_server::PostgresServer;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Parser)]
#[command(name = "rustmemodb")]
#[command(about = "RustMemDB - In-Memory SQL Database", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the interactive TUI Client (Default)
    Cli,
    /// Start the Postgres Wire Protocol Server
    Server {
        /// Port to listen on
        #[arg(long, default_value_t = 5432)]
        port: u16,

        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
    },
    /// Run a synthetic load test with latency percentiles
    LoadTest {
        #[arg(long, default_value_t = 10)]
        duration_secs: u64,
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
        #[arg(long, default_value_t = 10000)]
        rows: usize,
        #[arg(long, default_value_t = 80)]
        read_ratio: u8,
        #[arg(long, default_value_t = 100000)]
        sample_max: usize,
        #[arg(long, default_value_t = 64)]
        payload_size: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Server { port, host }) => {
            // Share the same DB instance if we wanted hybrid mode, but here we just start server
            let db = Arc::new(RwLock::new(InMemoryDB::new()));
            let server = PostgresServer::new(db, &host, port);
            server.run().await?;
        }
        Some(Commands::LoadTest {
            duration_secs,
            concurrency,
            rows,
            read_ratio,
            sample_max,
            payload_size,
        }) => {
            let config = LoadTestConfig {
                duration_secs,
                concurrency,
                rows,
                read_ratio,
                sample_max,
                payload_size,
            };
            run_load_test(config).await?;
        }
        _ => {
            // Default to CLI
            let mut app = App::new();
            app.run().await?;
        }
    }

    Ok(())
}
