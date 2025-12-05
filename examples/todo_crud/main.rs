/// TodoCRUD - A comprehensive CRUD example using RustMemDB
///
/// This application demonstrates best practices for building database applications:
/// - Clean Architecture with layered design
/// - Repository Pattern for data access
/// - Service Layer for business logic
/// - Domain-Driven Design principles
/// - Error handling and validation
/// - Interactive CLI with Command Pattern
///
/// ## Architecture
///
/// ```
/// ┌─────────────────────────────────────┐
/// │         Presentation Layer          │
/// │            (CLI/UI)                 │
/// └─────────────┬───────────────────────┘
///               │
/// ┌─────────────▼───────────────────────┐
/// │         Service Layer               │
/// │      (Business Logic)               │
/// └─────────────┬───────────────────────┘
///               │
/// ┌─────────────▼───────────────────────┐
/// │       Repository Layer              │
/// │      (Data Access)                  │
/// └─────────────┬───────────────────────┘
///               │
/// ┌─────────────▼───────────────────────┐
/// │         RustMemDB                   │
/// │      (In-Memory Database)           │
/// └─────────────────────────────────────┘
/// ```
///
/// Run: cargo run --example todo_crud

mod cli;
mod models;
mod repository;
mod service;

use cli::TodoCli;
use repository::TodoRepository;
use rustmemodb::{Client, Result as DbResult};
use service::TodoService;
use std::sync::Arc;

fn main() -> DbResult<()> {
    // Initialize application
    let app = TodoApp::new()?;

    // Run the CLI
    app.run()
}

/// Main application structure
///
/// Follows the Facade pattern to provide a simple interface
/// to the entire application subsystem.
struct TodoApp {
    cli: TodoCli,
}

impl TodoApp {
    /// Create and initialize the application
    ///
    /// This demonstrates proper dependency injection and layered initialization:
    /// 1. Create database client
    /// 2. Initialize repository layer
    /// 3. Initialize service layer
    /// 4. Initialize presentation layer
    fn new() -> DbResult<Self> {
        // Layer 1: Database Connection
        // Using the high-level Client API with connection pooling
        let client = Arc::new(Client::connect("admin", "adminpass")?);

        // Layer 2: Repository (Data Access)
        // Encapsulates all database operations
        let repository = Arc::new(TodoRepository::new(client)?);

        // Layer 3: Service (Business Logic)
        // Contains validation and business rules
        let service = Arc::new(TodoService::new(repository));

        // Layer 4: Presentation (CLI)
        // Handles user interaction
        let cli = TodoCli::new(service);

        Ok(Self { cli })
    }

    /// Run the application
    fn run(&self) -> DbResult<()> {
        self.cli.run()
    }
}

/// Example usage for developers
///
/// This shows how to use the TodoCRUD components programmatically
/// without the CLI interface.
#[cfg(test)]
mod examples {
    use super::*;
    use crate::models::Priority;

    #[test]
    fn example_programmatic_usage() -> DbResult<()> {
        // Setup
        let client = Arc::new(Client::connect("admin", "adminpass")?);
        let repo = Arc::new(TodoRepository::new(client)?);
        let service = Arc::new(TodoService::new(repo));

        // Create todos
        let todo1 = service.create_todo(
            "Implement user authentication".to_string(),
            Some("Add JWT-based auth".to_string()),
            Priority::High,
        )?;

        let todo2 = service.create_todo(
            "Write documentation".to_string(),
            None,
            Priority::Medium,
        )?;

        println!("Created todo 1: {:?}", todo1);
        println!("Created todo 2: {:?}", todo2);

        // List all todos
        let todos = service.list_todos(crate::models::TodoFilter::new())?;
        println!("Total todos: {}", todos.len());

        // Update status
        service.start_todo(todo1.id)?;
        println!("Started working on todo {}", todo1.id);

        // Complete todo
        service.complete_todo(todo1.id)?;
        println!("Completed todo {}", todo1.id);

        // Search
        let results = service.search_todos("documentation".to_string())?;
        println!("Found {} matching todos", results.len());

        // Get statistics
        let stats = service.get_stats()?;
        println!("Stats: {}", stats);

        Ok(())
    }

    #[test]
    fn example_filtering() -> DbResult<()> {
        use crate::models::{Status, TodoFilter};

        let client = Arc::new(Client::connect("admin", "adminpass")?);
        let repo = Arc::new(TodoRepository::new(client)?);
        let service = Arc::new(TodoService::new(repo));

        // Create sample data
        service.create_todo("High priority task".to_string(), None, Priority::High)?;
        service.create_todo("Low priority task".to_string(), None, Priority::Low)?;

        // Filter by status
        let pending = service.list_by_status(Status::Pending)?;
        println!("Pending todos: {}", pending.len());

        // Filter by priority
        let high_priority = service.list_by_priority(Priority::High)?;
        println!("High priority todos: {}", high_priority.len());

        // Complex filter
        let filter = TodoFilter::new()
            .with_status(Status::Pending)
            .with_priority(Priority::High);

        let filtered = service.list_todos(filter)?;
        println!("High priority pending todos: {}", filtered.len());

        Ok(())
    }

    #[test]
    fn example_error_handling() -> DbResult<()> {
        let client = Arc::new(Client::connect("admin", "adminpass")?);
        let repo = Arc::new(TodoRepository::new(client)?);
        let service = Arc::new(TodoService::new(repo));

        // This will fail validation
        let result = service.create_todo("".to_string(), None, Priority::Low);
        assert!(result.is_err());
        println!("Validation error: {:?}", result.err());

        // This will fail - todo not found
        let result = service.get_todo(999);
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());

        // This will fail - deleting non-existent todo
        let result = service.delete_todo(999);
        assert!(result.is_err());
        println!("Not found error: {:?}", result.err());

        Ok(())
    }
}
