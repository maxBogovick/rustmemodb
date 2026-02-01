mod cli;

use crate::cli::app::App;
use rustmemodb::server::pg_server::PostgresServer;
use rustmemodb::InMemoryDB;
use std::error::Error;
use clap::{Parser, Subcommand};
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
        _ => {
            // Default to CLI
            let mut app = App::new();
            app.run().await?;
        }
    }

    Ok(())
}
