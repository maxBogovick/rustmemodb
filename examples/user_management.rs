/// Example: User Management and Authentication
///
/// This example demonstrates user creation, authentication, and permissions.
///
/// Run: cargo run --example user_management

use rustmemodb::{Client, Permission, Result};

fn main() -> Result<()> {
    println!("=== RustMemDB User Management Example ===\n");

    let client = Client::connect("admin", "admin")?;
    let auth = client.auth_manager();

    // ============================================================================
    // 1. List Default Users
    // ============================================================================
    println!("1. Default users:");
    let users = auth.list_users()?;
    for username in &users {
        println!("   - {}", username);
    }
    println!();

    // ============================================================================
    // 2. Create New Users
    // ============================================================================
    println!("2. Creating new users...");

    // Read-only user
    auth.create_user(
        "alice",
        "alice_password",
        vec![Permission::Select],
    )?;
    println!("   ✓ Created user 'alice' (SELECT only)");

    // Read-write user
    auth.create_user(
        "bob",
        "bob_password",
        vec![
            Permission::Select,
            Permission::Insert,
            Permission::Update,
            Permission::Delete,
        ],
    )?;
    println!("   ✓ Created user 'bob' (full DML)");

    // Schema admin
    auth.create_user(
        "charlie",
        "charlie_password",
        vec![
            Permission::Select,
            Permission::Insert,
            Permission::CreateTable,
            Permission::DropTable,
        ],
    )?;
    println!("   ✓ Created user 'charlie' (DDL + DML)\n");

    // ============================================================================
    // 3. Authenticate Users
    // ============================================================================
    println!("3. Testing authentication...");

    match auth.authenticate("alice", "alice_password") {
        Ok(user) => println!("   ✓ Alice authenticated successfully"),
        Err(_) => println!("   ✗ Alice authentication failed"),
    }

    match auth.authenticate("bob", "wrong_password") {
        Ok(_) => println!("   ✗ Bob authenticated with wrong password!"),
        Err(_) => println!("   ✓ Bob rejected with wrong password"),
    }

    match auth.authenticate("nonexistent", "password") {
        Ok(_) => println!("   ✗ Non-existent user authenticated!"),
        Err(_) => println!("   ✓ Non-existent user rejected"),
    }
    println!();

    // ============================================================================
    // 4. Check Permissions
    // ============================================================================
    println!("4. Checking permissions...");

    let alice = auth.get_user("alice")?;
    println!("   Alice permissions:");
    println!("     - SELECT: {}", alice.has_permission(Permission::Select));
    println!("     - INSERT: {}", alice.has_permission(Permission::Insert));
    println!("     - CREATE TABLE: {}", alice.has_permission(Permission::CreateTable));
    println!();

    let bob = auth.get_user("bob")?;
    println!("   Bob permissions:");
    println!("     - SELECT: {}", bob.has_permission(Permission::Select));
    println!("     - INSERT: {}", bob.has_permission(Permission::Insert));
    println!("     - UPDATE: {}", bob.has_permission(Permission::Update));
    println!("     - DELETE: {}", bob.has_permission(Permission::Delete));
    println!();

    // ============================================================================
    // 5. Grant/Revoke Permissions
    // ============================================================================
    println!("5. Modifying permissions...");

    println!("   Alice before: INSERT = {}",
        auth.get_user("alice")?.has_permission(Permission::Insert));

    auth.grant_permission("alice", Permission::Insert)?;
    println!("   ✓ Granted INSERT to Alice");

    println!("   Alice after: INSERT = {}",
        auth.get_user("alice")?.has_permission(Permission::Insert));

    auth.revoke_permission("alice", Permission::Insert)?;
    println!("   ✓ Revoked INSERT from Alice");

    println!("   Alice final: INSERT = {}",
        auth.get_user("alice")?.has_permission(Permission::Insert));
    println!();

    // ============================================================================
    // 6. Update Password
    // ============================================================================
    println!("6. Updating password...");

    auth.update_password("bob", "new_secure_password")?;
    println!("   ✓ Updated Bob's password");

    match auth.authenticate("bob", "bob_password") {
        Ok(_) => println!("   ✗ Old password still works!"),
        Err(_) => println!("   ✓ Old password rejected"),
    }

    match auth.authenticate("bob", "new_secure_password") {
        Ok(_) => println!("   ✓ New password works"),
        Err(_) => println!("   ✗ New password failed!"),
    }
    println!();

    // ============================================================================
    // 7. Connect as Different Users
    // ============================================================================
    println!("7. Connecting as different users...");

    // Connect as Alice (read-only)
    let alice_client = Client::connect("alice", "alice_password")?;
    println!("   ✓ Connected as Alice");

    // Alice can read
    alice_client.execute("CREATE TABLE test (id INTEGER)")?;
    match alice_client.query("SELECT * FROM test") {
        Ok(_) => println!("   ✓ Alice can SELECT"),
        Err(_) => println!("   ✗ Alice cannot SELECT"),
    }

    // Connect as Bob
    let bob_client = Client::connect("bob", "new_secure_password")?;
    println!("   ✓ Connected as Bob");
    println!();

    // ============================================================================
    // 8. List All Users
    // ============================================================================
    println!("8. Final user list:");
    let users = auth.list_users()?;
    for username in users {
        let user = auth.get_user(&username)?;
        println!("   - {} (admin: {})", username, user.is_admin());
    }
    println!();

    // ============================================================================
    // 9. Delete User
    // ============================================================================
    println!("9. Deleting user...");

    auth.delete_user("charlie")?;
    println!("   ✓ Deleted user 'charlie'");

    match auth.get_user("charlie") {
        Ok(_) => println!("   ✗ Charlie still exists!"),
        Err(_) => println!("   ✓ Charlie no longer exists")
    }
    println!();

    println!("✓ All user management examples completed!");

    Ok(())
}
