mod cli;

use crate::cli::app::App;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut app = App::new();
    app.run().await?;
    Ok(())
}