# JSON Storage Module

Document-oriented API for RustMemDB that provides MongoDB-like interface with SQL query capabilities.

## Quick Example

```rust
use rustmemodb::{InMemoryDB, JsonStorageAdapter};
use std::sync::{Arc, RwLock};

let db = Arc::new(RwLock::new(InMemoryDB::new()));
let adapter = JsonStorageAdapter::new(db);

// CREATE
adapter.create("users", r#"[
    {"id": "1", "name": "Alice", "age": 30}
]"#)?;

// READ
let results = adapter.read("users", "SELECT * FROM users WHERE age > 25")?;

// UPDATE
adapter.update("users", r#"[{"id": "1", "age": 31}]"#)?;

// DELETE
adapter.delete("users", "1")?;
```

## Architecture

- **adapter.rs**: Main Facade pattern API
- **schema_inference.rs**: Strategy pattern for schema detection
- **converter.rs**: Builder pattern for SQL generation
- **validator.rs**: Chain of Responsibility for security
- **error.rs**: Domain-specific error types

## Design Patterns

1. **Facade**: JsonStorageAdapter simplifies complex operations
2. **Strategy**: Pluggable schema inference (First/All/Smart)
3. **Builder**: SQL statement builders (INSERT, UPDATE, DELETE)
4. **Chain of Responsibility**: Validation rules pipeline

## Key Features

- ✅ Automatic schema inference from JSON
- ✅ Type-safe conversions (Integer, Float, Text, Boolean)
- ✅ SQL injection prevention
- ✅ Batch insert operations
- ✅ Multiple collections support
- ✅ Comprehensive test coverage

## Testing

```bash
cargo test json::
```

## Documentation

See `/JSON_STORAGE_API.md` for comprehensive documentation.
