# TodoCRUD - Quick Start Guide

## Running the Application

```bash
cargo run --example todo_crud
```

## Quick Tutorial

### 1. Create Your First Todo

```
todo> add Buy groceries
✅ Todo created successfully!
⏸ 🟡 [1] Buy groceries - Pending
```

### 2. Create Todo with Details

```
todo> add Finish project --desc Complete RustMemDB example --priority high
✅ Todo created successfully!
⏸ 🔴 [2] Finish project - Pending
```

### 3. List All Todos

```
todo> list

📋 Todos (2):

⏸ 🔴 [2] Finish project - Pending
⏸ 🟡 [1] Buy groceries - Pending
```

### 4. Start Working on a Todo

```
todo> start 2
✅ Todo marked as in progress!
▶ 🔴 [2] Finish project - In Progress
```

### 5. View Detailed Information

```
todo> view 2

╔═══════════════════════════════════════════════════════════╗
  ID:          2
  Title:       Finish project
  Description: Complete RustMemDB example
  Priority:    High
  Status:      In Progress
  Created:     2025-12-04 21:22:34
  Updated:     2025-12-04 21:23:15
╚═══════════════════════════════════════════════════════════╝
```

### 6. Update a Todo

```
todo> update 1 --priority high --desc Buy milk, eggs, and bread
✅ Priority updated
✅ Description updated

📝 Updated todo:
⏸ 🔴 [1] Buy groceries - Pending
```

### 7. Complete a Todo

```
todo> complete 2
✅ Todo marked as completed!
✓ 🔴 [2] Finish project - Done
```

### 8. Filter Todos

```
todo> list pending

📋 Todos (1):

⏸ 🔴 [1] Buy groceries - Pending
```

```
todo> list done

📋 Todos (1):

✓ 🔴 [2] Finish project - Done
```

```
todo> list --priority high

📋 Todos (2):

✓ 🔴 [2] Finish project - Done
⏸ 🔴 [1] Buy groceries - Pending
```

### 9. Search Todos

```
todo> search project

🔍 Search results (1):

✓ 🔴 [2] Finish project - Done
```

### 10. View Statistics

```
todo> stats

📊 Statistics:
   Total: 2 | Pending: 1 | In Progress: 0 | Completed: 1
```

### 11. Delete Todos

```
todo> delete 1
✅ Todo deleted successfully

todo> clear
✅ Deleted 1 completed todo(s)
```

### 12. Get Help

```
todo> help

📚 Available Commands:
[... full help displayed ...]
```

### 13. Exit

```
todo> quit

👋 Goodbye!
```

## Command Cheatsheet

| Action | Command | Example |
|--------|---------|---------|
| **Create** | `add <title> [options]` | `add Task --desc Details --priority high` |
| **List all** | `list` | `list` |
| **Filter** | `list [pending\|progress\|done]` | `list pending` |
| **View** | `view <id>` | `view 1` |
| **Update** | `update <id> [options]` | `update 1 --title New title` |
| **Start** | `start <id>` | `start 1` |
| **Complete** | `complete <id>` | `complete 1` |
| **Search** | `search <keyword>` | `search groceries` |
| **Delete** | `delete <id>` | `delete 1` |
| **Clear done** | `clear` | `clear` |
| **Stats** | `stats` | `stats` |
| **Help** | `help` | `help` |
| **Exit** | `quit` | `quit` |

## Priority Levels

- 🔴 **High** - `high`, `h`, `3`
- 🟡 **Medium** - `medium`, `med`, `m`, `2` (default)
- 🟢 **Low** - `low`, `l`, `1`

## Status Icons

- ⏸ **Pending** - Not started yet
- ▶ **In Progress** - Currently working on
- ✓ **Done** - Completed

## Tips

1. **Shortcuts**: Most commands have short aliases (e.g., `l` for `list`, `h` for `help`)
2. **Multi-word text**: No quotes needed! Just type: `add Buy groceries and milk`
3. **Batch updates**: Update multiple fields at once: `update 1 --title New --priority high --status done`
4. **Natural language**: Use intuitive status names: `pending`, `progress`, `done`

## Testing Programmatically

Run the included tests to see how to use the TodoCRUD API programmatically:

```bash
# Run all tests
cargo test --example todo_crud

# Run specific test
cargo test --example todo_crud example_programmatic_usage -- --nocapture
```

## What's Happening Under the Hood?

This example demonstrates:

- **Clean Architecture**: Separated layers (presentation, service, repository, domain)
- **Repository Pattern**: Database abstraction layer
- **Service Layer**: Business logic and validation
- **Builder Pattern**: Fluent API for creating todos
- **Domain Models**: Rich domain entities with behavior
- **SQL Database**: All data stored in RustMemDB's in-memory SQL database

## Next Steps

1. Explore the source code in `examples/todo_crud/`
2. Read the detailed [README.md](README.md) for architecture explanation
3. Try modifying the code to add new features
4. Use this as a template for your own applications

Enjoy using TodoCRUD!
