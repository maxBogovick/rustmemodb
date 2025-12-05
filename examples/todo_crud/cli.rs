/// CLI interface for TodoCRUD application
///
/// Implements the Command pattern for handling user input and displaying results.

use crate::models::{Priority, Status, Todo, TodoFilter};
use crate::service::TodoService;
use rustmemodb::Result as DbResult;
use std::io::{self, Write};
use std::sync::Arc;

/// CLI application controller
pub struct TodoCli {
    service: Arc<TodoService>,
}

impl TodoCli {
    pub fn new(service: Arc<TodoService>) -> Self {
        Self { service }
    }

    /// Start the interactive CLI loop
    pub fn run(&self) -> DbResult<()> {
        self.print_welcome();

        loop {
            self.print_prompt();

            let input = self.read_input();
            let args: Vec<&str> = input.trim().split_whitespace().collect();

            if args.is_empty() {
                continue;
            }

            let command = args[0].to_lowercase();

            match command.as_str() {
                "help" | "h" | "?" => self.print_help(),
                "add" | "create" | "new" => self.cmd_add(&args[1..]),
                "list" | "ls" | "l" => self.cmd_list(&args[1..]),
                "view" | "show" => self.cmd_view(&args[1..]),
                "update" | "edit" => self.cmd_update(&args[1..]),
                "complete" | "done" => self.cmd_complete(&args[1..]),
                "start" => self.cmd_start(&args[1..]),
                "delete" | "rm" => self.cmd_delete(&args[1..]),
                "search" | "find" => self.cmd_search(&args[1..]),
                "stats" => self.cmd_stats(),
                "clear" => self.cmd_clear_completed(),
                "quit" | "exit" | "q" => {
                    println!("\n👋 Goodbye!");
                    break;
                }
                _ => println!("❌ Unknown command: '{}'. Type 'help' for available commands.", command),
            }
        }

        Ok(())
    }

    // Command handlers

    fn cmd_add(&self, args: &[&str]) {
        if args.is_empty() {
            println!("❌ Usage: add <title> [--desc <description>] [--priority low|medium|high]");
            return;
        }

        // Parse arguments
        let mut title = String::new();
        let mut description: Option<String> = None;
        let mut priority = Priority::Medium;

        let mut i = 0;
        while i < args.len() {
            match args[i] {
                "--desc" | "-d" => {
                    if i + 1 < args.len() {
                        description = Some(args[i + 1..].iter().take_while(|&&s| !s.starts_with("--")).copied().collect::<Vec<_>>().join(" "));
                        i += description.as_ref().unwrap().split_whitespace().count();
                    }
                }
                "--priority" | "-p" => {
                    if i + 1 < args.len() {
                        if let Some(p) = Priority::from_str(args[i + 1]) {
                            priority = p;
                        } else {
                            println!("❌ Invalid priority: {}. Using 'medium'.", args[i + 1]);
                        }
                        i += 2;
                        continue;
                    }
                }
                _ => {
                    if !args[i].starts_with("--") {
                        if title.is_empty() {
                            title = args[i..].iter().take_while(|&&s| !s.starts_with("--")).copied().collect::<Vec<_>>().join(" ");
                            i += title.split_whitespace().count();
                            continue;
                        }
                    }
                }
            }
            i += 1;
        }

        if title.is_empty() {
            println!("❌ Title is required");
            return;
        }

        match self.service.create_todo(title.clone(), description.clone(), priority) {
            Ok(todo) => {
                println!("✅ Todo created successfully!");
                self.print_todo(&todo);
            }
            Err(e) => println!("❌ Error: {}", e),
        }
    }

    fn cmd_list(&self, args: &[&str]) {
        let mut filter = TodoFilter::new();

        // Parse filter arguments
        let mut i = 0;
        while i < args.len() {
            match args[i] {
                "--status" | "-s" => {
                    if i + 1 < args.len() {
                        if let Some(status) = Status::from_str(args[i + 1]) {
                            filter = filter.with_status(status);
                        }
                        i += 2;
                        continue;
                    }
                }
                "--priority" | "-p" => {
                    if i + 1 < args.len() {
                        if let Some(priority) = Priority::from_str(args[i + 1]) {
                            filter = filter.with_priority(priority);
                        }
                        i += 2;
                        continue;
                    }
                }
                "pending" => filter = filter.with_status(Status::Pending),
                "progress" | "inprogress" => filter = filter.with_status(Status::InProgress),
                "done" | "completed" => filter = filter.with_status(Status::Done),
                _ => {}
            }
            i += 1;
        }

        match self.service.list_todos(filter) {
            Ok(todos) => {
                if todos.is_empty() {
                    println!("📝 No todos found");
                } else {
                    println!("\n📋 Todos ({}):\n", todos.len());
                    self.print_todo_list(&todos);
                }
            }
            Err(e) => println!("❌ Error: {}", e),
        }
    }

    fn cmd_view(&self, args: &[&str]) {
        if args.is_empty() {
            println!("❌ Usage: view <id>");
            return;
        }

        let id = match args[0].parse::<i64>() {
            Ok(id) => id,
            Err(_) => {
                println!("❌ Invalid ID: {}", args[0]);
                return;
            }
        };

        match self.service.get_todo(id) {
            Ok(Some(todo)) => self.print_todo_detailed(&todo),
            Ok(None) => println!("❌ Todo with ID {} not found", id),
            Err(e) => println!("❌ Error: {}", e),
        }
    }

    fn cmd_update(&self, args: &[&str]) {
        if args.is_empty() {
            println!("❌ Usage: update <id> [--title <new_title>] [--desc <new_desc>] [--priority low|medium|high] [--status pending|progress|done]");
            return;
        }

        let id = match args[0].parse::<i64>() {
            Ok(id) => id,
            Err(_) => {
                println!("❌ Invalid ID: {}", args[0]);
                return;
            }
        };

        let mut i = 1;
        let mut updated = false;

        while i < args.len() {
            match args[i] {
                "--title" | "-t" => {
                    if i + 1 < args.len() {
                        let title = args[i + 1..].iter().take_while(|&&s| !s.starts_with("--")).copied().collect::<Vec<_>>().join(" ");
                        let word_count = title.split_whitespace().count();
                        match self.service.update_title(id, title) {
                            Ok(_) => {
                                println!("✅ Title updated");
                                updated = true;
                            }
                            Err(e) => println!("❌ Error: {}", e),
                        }
                        i += word_count + 1;
                        continue;
                    }
                }
                "--desc" | "-d" => {
                    if i + 1 < args.len() {
                        let desc = args[i + 1..].iter().take_while(|&&s| !s.starts_with("--")).copied().collect::<Vec<_>>().join(" ");
                        let word_count = desc.split_whitespace().count();
                        match self.service.update_description(id, Some(desc)) {
                            Ok(_) => {
                                println!("✅ Description updated");
                                updated = true;
                            }
                            Err(e) => println!("❌ Error: {}", e),
                        }
                        i += word_count + 1;
                        continue;
                    }
                }
                "--priority" | "-p" => {
                    if i + 1 < args.len() {
                        if let Some(priority) = Priority::from_str(args[i + 1]) {
                            match self.service.update_priority(id, priority) {
                                Ok(_) => {
                                    println!("✅ Priority updated");
                                    updated = true;
                                }
                                Err(e) => println!("❌ Error: {}", e),
                            }
                        } else {
                            println!("❌ Invalid priority: {}", args[i + 1]);
                        }
                        i += 2;
                        continue;
                    }
                }
                "--status" | "-s" => {
                    if i + 1 < args.len() {
                        if let Some(status) = Status::from_str(args[i + 1]) {
                            match self.service.update_status(id, status) {
                                Ok(_) => {
                                    println!("✅ Status updated");
                                    updated = true;
                                }
                                Err(e) => println!("❌ Error: {}", e),
                            }
                        } else {
                            println!("❌ Invalid status: {}", args[i + 1]);
                        }
                        i += 2;
                        continue;
                    }
                }
                _ => {}
            }
            i += 1;
        }

        if updated {
            if let Ok(Some(todo)) = self.service.get_todo(id) {
                println!("\n📝 Updated todo:");
                self.print_todo(&todo);
            }
        }
    }

    fn cmd_complete(&self, args: &[&str]) {
        if args.is_empty() {
            println!("❌ Usage: complete <id>");
            return;
        }

        let id = match args[0].parse::<i64>() {
            Ok(id) => id,
            Err(_) => {
                println!("❌ Invalid ID: {}", args[0]);
                return;
            }
        };

        match self.service.complete_todo(id) {
            Ok(todo) => {
                println!("✅ Todo marked as completed!");
                self.print_todo(&todo);
            }
            Err(e) => println!("❌ Error: {}", e),
        }
    }

    fn cmd_start(&self, args: &[&str]) {
        if args.is_empty() {
            println!("❌ Usage: start <id>");
            return;
        }

        let id = match args[0].parse::<i64>() {
            Ok(id) => id,
            Err(_) => {
                println!("❌ Invalid ID: {}", args[0]);
                return;
            }
        };

        match self.service.start_todo(id) {
            Ok(todo) => {
                println!("✅ Todo marked as in progress!");
                self.print_todo(&todo);
            }
            Err(e) => println!("❌ Error: {}", e),
        }
    }

    fn cmd_delete(&self, args: &[&str]) {
        if args.is_empty() {
            println!("❌ Usage: delete <id>");
            return;
        }

        let id = match args[0].parse::<i64>() {
            Ok(id) => id,
            Err(_) => {
                println!("❌ Invalid ID: {}", args[0]);
                return;
            }
        };

        match self.service.delete_todo(id) {
            Ok(_) => println!("✅ Todo deleted successfully"),
            Err(e) => println!("❌ Error: {}", e),
        }
    }

    fn cmd_search(&self, args: &[&str]) {
        if args.is_empty() {
            println!("❌ Usage: search <keyword>");
            return;
        }

        let keyword = args.join(" ");

        match self.service.search_todos(keyword) {
            Ok(todos) => {
                if todos.is_empty() {
                    println!("📝 No todos found");
                } else {
                    println!("\n🔍 Search results ({}):\n", todos.len());
                    self.print_todo_list(&todos);
                }
            }
            Err(e) => println!("❌ Error: {}", e),
        }
    }

    fn cmd_stats(&self) {
        match self.service.get_stats() {
            Ok(stats) => {
                println!("\n📊 Statistics:");
                println!("   {}", stats);
            }
            Err(e) => println!("❌ Error: {}", e),
        }
    }

    fn cmd_clear_completed(&self) {
        match self.service.delete_completed() {
            Ok(count) => println!("✅ Deleted {} completed todo(s)", count),
            Err(e) => println!("❌ Error: {}", e),
        }
    }

    // Display methods

    fn print_welcome(&self) {
        println!("\n╔═══════════════════════════════════════════════════════════╗");
        println!("║          📝 TodoCRUD - RustMemDB Example App             ║");
        println!("╚═══════════════════════════════════════════════════════════╝\n");
        println!("Type 'help' to see available commands\n");
    }

    fn print_help(&self) {
        println!("\n📚 Available Commands:\n");
        println!("  CREATE:");
        println!("    add <title> [--desc <desc>] [--priority low|medium|high]");
        println!("        Create a new todo");
        println!();
        println!("  READ:");
        println!("    list [pending|progress|done] [--priority low|medium|high]");
        println!("        List all todos (with optional filters)");
        println!("    view <id>");
        println!("        View detailed information about a todo");
        println!("    search <keyword>");
        println!("        Search todos by keyword");
        println!("    stats");
        println!("        Show statistics");
        println!();
        println!("  UPDATE:");
        println!("    update <id> [--title <text>] [--desc <text>] [--priority low|medium|high] [--status pending|progress|done]");
        println!("        Update todo properties");
        println!("    complete <id>");
        println!("        Mark todo as completed");
        println!("    start <id>");
        println!("        Mark todo as in progress");
        println!();
        println!("  DELETE:");
        println!("    delete <id>");
        println!("        Delete a todo");
        println!("    clear");
        println!("        Delete all completed todos");
        println!();
        println!("  OTHER:");
        println!("    help");
        println!("        Show this help message");
        println!("    quit");
        println!("        Exit the application");
        println!();
    }

    fn print_prompt(&self) {
        print!("todo> ");
        io::stdout().flush().unwrap();
    }

    fn print_todo(&self, todo: &Todo) {
        let status_icon = match todo.status {
            Status::Pending => "⏸",
            Status::InProgress => "▶",
            Status::Done => "✓",
        };

        let priority_color = match todo.priority {
            Priority::High => "🔴",
            Priority::Medium => "🟡",
            Priority::Low => "🟢",
        };

        println!("  {} {} [{}] {} - {}", status_icon, priority_color, todo.id, todo.title, todo.status);
    }

    fn print_todo_list(&self, todos: &[Todo]) {
        for todo in todos {
            self.print_todo(todo);
        }
        println!();
    }

    fn print_todo_detailed(&self, todo: &Todo) {
        println!("\n╔═══════════════════════════════════════════════════════════╗");
        println!("  ID:          {}", todo.id);
        println!("  Title:       {}", todo.title);
        if let Some(desc) = &todo.description {
            println!("  Description: {}", desc);
        }
        println!("  Priority:    {}", todo.priority);
        println!("  Status:      {}", todo.status);
        println!("  Created:     {}", todo.created_at.format("%Y-%m-%d %H:%M:%S"));
        println!("  Updated:     {}", todo.updated_at.format("%Y-%m-%d %H:%M:%S"));
        if let Some(completed) = todo.completed_at {
            println!("  Completed:   {}", completed.format("%Y-%m-%d %H:%M:%S"));
        }
        println!("╚═══════════════════════════════════════════════════════════╝\n");
    }

    fn read_input(&self) -> String {
        let mut input = String::new();
        io::stdin().read_line(&mut input).expect("Failed to read input");
        input
    }
}
