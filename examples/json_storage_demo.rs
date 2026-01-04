//! JSON Storage API Demo
//!
//! This example demonstrates the JSON storage adapter functionality,
//! which provides a document-oriented interface on top of RustMemDB.
//!
//! Run with: cargo run --example json_storage_demo

use rustmemodb::{InMemoryDB, JsonStorageAdapter, JsonResult};
use std::sync::{Arc, RwLock};

fn main() -> JsonResult<()> {
    println!("=== RustMemDB JSON Storage Demo ===\n");

    // Initialize database
    let db = Arc::new(RwLock::new(InMemoryDB::new()));
    let adapter = JsonStorageAdapter::new(db);

    // Demo 1: Create collection with users
    println!("1. Creating 'users' collection with documents...");
    let users_doc = r#"[
        {
            "id": "1",
            "name": "Alice Johnson",
            "email": "alice@example.com",
            "age": 30,
            "active": true
        },
        {
            "id": "2",
            "name": "Bob Smith",
            "email": "bob@example.com",
            "age": 25,
            "active": true
        },
        {
            "id": "3",
            "name": "Charlie Brown",
            "email": "charlie@example.com",
            "age": 35,
            "active": false
        }
    ]"#;

    adapter.create("users", users_doc)?;
    println!("✓ Collection 'users' created with 3 documents\n");

    // Demo 2: Read all documents
    println!("2. Reading all users...");
    let all_users = adapter.read("users", "SELECT * FROM users")?;
    println!("Results:\n{}\n", all_users);

    // Demo 3: Query with filter
    println!("3. Finding active users...");
    let active_users = adapter.read(
        "users",
        "SELECT name, email FROM users WHERE active = true"
    )?;
    println!("Active users:\n{}\n", active_users);

    // Demo 4: Query with age filter
    println!("4. Finding users older than 25...");
    let older_users = adapter.read(
        "users",
        "SELECT name, age FROM users WHERE age > 25 ORDER BY age"
    )?;
    println!("Results:\n{}\n", older_users);

    // Demo 5: Update document
    println!("5. Updating user with id=1...");
    let update_doc = r#"[
        {
            "id": "1",
            "name": "Alice Johnson-Smith",
            "email": "alice.smith@example.com",
            "age": 31,
            "active": true
        }
    ]"#;
    adapter.update("users", update_doc)?;
    println!("✓ User updated\n");

    // Verify update
    println!("6. Verifying update...");
    let updated_user = adapter.read("users", "SELECT * FROM users WHERE id = '1'")?;
    println!("Updated user:\n{}\n", updated_user);

    // Demo 7: Delete document
    println!("7. Deleting user with id=3...");
    adapter.delete("users", "3")?;
    println!("✓ User deleted\n");

    // Verify deletion
    println!("8. Remaining users...");
    let remaining = adapter.read("users", "SELECT id, name FROM users ORDER BY id")?;
    println!("Results:\n{}\n", remaining);

    // Demo 9: Create another collection (products)
    println!("9. Creating 'products' collection...");
    let products_doc = r#"[
        {
            "id": "p1",
            "name": "Laptop",
            "price": 999.99,
            "in_stock": true,
            "quantity": 50
        },
        {
            "id": "p2",
            "name": "Mouse",
            "price": 29.99,
            "in_stock": true,
            "quantity": 200
        },
        {
            "id": "p3",
            "name": "Keyboard",
            "price": 79.99,
            "in_stock": false,
            "quantity": 0
        }
    ]"#;

    adapter.create("products", products_doc)?;
    println!("✓ Collection 'products' created\n");

    // Demo 10: Query products
    println!("10. Finding in-stock products under $100...");
    let affordable_products = adapter.read(
        "products",
        "SELECT name, price, quantity FROM products WHERE in_stock = true AND price < 100"
    )?;
    println!("Results:\n{}\n", affordable_products);

    // Demo 11: List all collections
    println!("11. Listing all collections...");
    let collections = adapter.list_collections();
    println!("Collections: {:?}\n", collections);

    // Demo 12: Complex query with aggregation-like operations
    println!("12. Getting product statistics...");
    let all_products = adapter.read(
        "products",
        "SELECT name, price FROM products ORDER BY price DESC"
    )?;
    println!("Products by price:\n{}\n", all_products);

    // Demo 13: Test schema inference with mixed types
    println!("13. Testing schema inference with mixed data...");
    let mixed_doc = r#"[
        {
            "id": "1",
            "score": 100,
            "grade": "A"
        },
        {
            "id": "2",
            "score": 85,
            "grade": "B"
        }
    ]"#;

    adapter.create("grades", mixed_doc)?;
    let grades = adapter.read("grades", "SELECT * FROM grades")?;
    println!("Grades:\n{}\n", grades);

    // Demo 14: Security validation
    println!("14. Testing security validation...");
    println!("Attempting SQL injection (should fail):");

    let injection_result = adapter.read(
        "users",
        "SELECT * FROM users; DROP TABLE users;"
    );

    match injection_result {
        Ok(_) => println!("✗ Security validation failed!"),
        Err(e) => println!("✓ Security validation passed: {}\n", e),
    }

    // Demo 15: Invalid collection name
    println!("15. Testing invalid collection name (should fail):");
    let invalid_result = adapter.create("DROP", r#"[{"id": "1"}]"#);

    match invalid_result {
        Ok(_) => println!("✗ Validation failed!"),
        Err(e) => println!("✓ Validation passed: {}\n", e),
    }

    // Demo 16: Drop collection
    println!("16. Dropping 'grades' collection...");
    adapter.drop_collection("grades")?;
    println!("✓ Collection dropped\n");

    println!("17. Final collection list:");
    let final_collections = adapter.list_collections();
    println!("Collections: {:?}\n", final_collections);

    // Summary
    println!("=== Demo Complete ===");
    println!("\nKey Features Demonstrated:");
    println!("✓ Automatic schema inference from JSON");
    println!("✓ CRUD operations (Create, Read, Update, Delete)");
    println!("✓ SQL query support for flexible data retrieval");
    println!("✓ Multiple collections support");
    println!("✓ Security validation and SQL injection prevention");
    println!("✓ Type inference (integers, floats, text, boolean)");
    println!("✓ Collection management");

    Ok(())
}
