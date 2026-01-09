# TodoCRUD - RustMemDB Example Application

A comprehensive Todo list CRUD application demonstrating professional software engineering practices using RustMemDB.

## Overview

This example showcases how to build a production-ready application using RustMemDB's in-memory SQL database. It implements a complete CRUD (Create, Read, Update, Delete) application with an interactive CLI interface.

## Features

- **Full CRUD Operations**: Create, read, update, and delete todos
- **Priority Levels**: Low, Medium, High
- **Status Tracking**: Pending, In Progress, Done
- **Search & Filter**: Find todos by keyword, status, or priority
- **Statistics**: View summary of todo counts by status
- **Data Validation**: Automatic validation of inputs
- **Interactive CLI**: User-friendly command-line interface

## Architecture

The application follows **Clean Architecture** principles with clear separation of concerns:

```
┌─────────────────────────────────────┐
│      Presentation Layer             │  CLI interface (cli.rs)
│      - TodoCli                      │
└─────────────┬───────────────────────┘
              │
┌─────────────▼───────────────────────┐
│      Business Logic Layer           │  Business rules (service.rs)
│      - TodoService                  │  - Validation
│      - Validation                   │  - Workflows
└─────────────┬───────────────────────┘
              │
┌─────────────▼───────────────────────┐
│      Data Access Layer              │  Repository pattern (repository.rs)
│      - TodoRepository               │  - SQL queries
│      - SQL mapping                  │  - Data mapping
└─────────────┬───────────────────────┘
              │
┌─────────────▼───────────────────────┐
│      Domain Layer                   │  Core models (models.rs)
│      - Todo, Priority, Status       │  - Business entities
│      - Domain logic                 │  - Domain rules
└─────────────────────────────────────┘
              │
┌─────────────▼───────────────────────┐
│      RustMemDB                      │  In-memory SQL database
│      - Client API                   │
└─────────────────────────────────────┘
```

## Design Patterns Used

1. **Repository Pattern**: Abstracts data access logic
   - `TodoRepository` provides clean data access interface
   - Separates SQL queries from business logic

2. **Service Layer Pattern**: Encapsulates business logic
   - `TodoService` contains validation and workflows
   - Coordinates between repository and presentation

3. **Domain-Driven Design**: Rich domain models
   - `Todo`, `Priority`, `Status` are self-contained entities
   - Domain logic lives in domain objects

4. **Builder Pattern**: Fluent object creation
   - `TodoBuilder` for creating todos with custom values

5. **Command Pattern**: CLI command handling
   - Each CLI command is a separate method
   - Easy to extend with new commands

6. **Dependency Injection**: Loose coupling
   - Dependencies injected via constructors
   - Easy to test and swap implementations

## File Structure

```
examples/todo_crud/
├── main.rs         - Application entry point & initialization
├── models.rs       - Domain models (Todo, Priority, Status)
├── repository.rs   - Data access layer (Repository pattern)
├── service.rs      - Business logic layer
├── cli.rs          - Interactive CLI interface
└── README.md       - This file
```

## Usage

### Running the Application

```bash
cargo run --example todo_crud
```

### Available Commands

#### Create
```bash
# Create a simple todo
add Buy groceries

# Create with description
add Buy groceries --desc Milk, eggs, bread

# Create with priority
add Fix critical bug --priority high

# Create with all options
add Write tests --desc Unit tests for auth --priority high
```

#### Read
```bash
# List all todos
list

# List by status
list pending
list progress
list done

# List by priority
list --priority high

# View detailed info
view 1

# Search todos
search groceries

# Show statistics
stats
```

#### Update
```bash
# Update title
update 1 --title New title

# Update description
update 1 --desc New description

# Update priority
update 1 --priority high

# Update status
update 1 --status done

# Update multiple fields
update 1 --title New title --priority high --status progress

# Quick status updates
start 1       # Mark as in progress
complete 1    # Mark as done
```

#### Delete
```bash
# Delete a todo
delete 1

# Delete all completed todos
clear
```

#### Other
```bash
# Show help
help

# Exit application
quit
```

## Code Examples

### Programmatic Usage (Without CLI)

```rust
use rustmemodb::Client;
use std::sync::Arc;

// Initialize
let client = Arc::new(Client::connect("admin", "adminpass")?);
let repo = Arc::new(TodoRepository::new(client)?);
let service = Arc::new(TodoService::new(repo));

// Create a todo
let todo = service.create_todo(
    "Implement feature X".to_string(),
    Some("Add authentication".to_string()),
    Priority::High,
)?;

// Update status
service.start_todo(todo.id)?;
service.complete_todo(todo.id)?;

// Search
let results = service.search_todos("feature".to_string())?;

// Get statistics
let stats = service.get_stats()?;
println!("{}", stats);
```

### Filtering Examples

```rust
use models::{TodoFilter, Status, Priority};

// Filter by status
let pending = service.list_by_status(Status::Pending)?;

// Filter by priority
let high_priority = service.list_by_priority(Priority::High)?;

// Complex filter
let filter = TodoFilter::new()
    .with_status(Status::Pending)
    .with_priority(Priority::High);
let filtered = service.list_todos(filter)?;
```

## What This Example Demonstrates

### RustMemDB Features

1. **SQL Support**: CREATE TABLE, INSERT, UPDATE, DELETE, SELECT
2. **Data Types**: INTEGER, TEXT (for timestamps and enums)
3. **Filtering**: WHERE clauses with multiple conditions
4. **Ordering**: ORDER BY with multiple columns
5. **Limits**: LIMIT for pagination
6. **Pattern Matching**: LIKE operator for search
7. **Client API**: High-level connection pooling interface

### Software Engineering Best Practices

1. **Clean Architecture**: Clear separation of layers
2. **SOLID Principles**:
   - Single Responsibility: Each class has one purpose
   - Open/Closed: Easy to extend without modifying existing code
   - Dependency Inversion: Depend on abstractions, not concretions

3. **Design Patterns**: Repository, Service, Builder, Command, Facade
4. **Error Handling**: Proper Result types and error propagation
5. **Validation**: Input validation at service layer
6. **Documentation**: Comprehensive inline documentation
7. **Testing**: Unit tests and integration examples

## Key Takeaways for Developers

### Database Operations

```rust
// RustMemDB makes it easy to work with SQL
client.execute("CREATE TABLE todos (...)")?;
client.execute("INSERT INTO todos VALUES (...)")?;
let result = client.query("SELECT * FROM todos WHERE status = 'pending'")?;
```

### Repository Pattern Benefits

```rust
// Instead of SQL scattered everywhere:
let sql = "SELECT * FROM todos WHERE id = ?";
let result = client.query(&sql)?;

// Encapsulate in repository:
let todo = repository.find_by_id(id)?;
```

### Service Layer for Business Logic

```rust
// Validation logic in service layer, not in presentation
fn create_todo(&self, title: String, ...) -> Result<Todo> {
    self.validate_title(&title)?;  // Business rule
    let id = self.repository.next_id()?;
    // ... create and save
}
```

### Builder Pattern for Flexibility

```rust
// Fluent API for creating complex objects
let todo = Todo::builder(1, "Task".to_string())
    .description("Description".to_string())
    .priority(Priority::High)
    .status(Status::InProgress)
    .build();
```

## Performance Considerations

- **In-Memory Storage**: All data stored in RAM for fast access
- **Connection Pooling**: Reuses connections for better performance
- **Bulk Operations**: Use transactions for multiple operations
- **Indexing**: Consider adding indexes for frequently queried fields (future enhancement)

## Future Enhancements

Potential improvements to demonstrate more features:

1. **Due Dates**: Add deadline tracking
2. **Tags**: Categorize todos with tags
3. **Transactions**: Use BEGIN/COMMIT for complex operations
4. **Batch Operations**: Bulk create/update/delete
5. **Export/Import**: JSON/CSV export functionality
6. **User Authentication**: Multi-user support
7. **Persistence**: Save to file and reload on startup

## Testing

Run the included examples:

```bash
# Run all tests
cargo test --example todo_crud

# Run specific test
cargo test --example todo_crud example_programmatic_usage
```

## License

This example is part of the RustMemDB project.

## Contributing

This example serves as a reference implementation. Feel free to use it as a starting point for your own applications!
