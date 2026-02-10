/// Example: User Management and Authentication
///
/// This example demonstrates user creation, authentication, and permissions.
///
/// Run: cargo run --example user_management
use rustmemodb::{Client, Permission, Result};

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== RustMemDB User Management Example ===\n");

    let client = Client::connect("admin", "adminpass").await?;
    let auth = client.auth_manager();

    // ============================================================================
    // 1. List Default Users
    // ============================================================================
    println!("1. Default users:");
    let users = auth.list_users().await?;
    for username in &users {
        println!("   - {}", username);
    }
    println!();

    // ============================================================================
    // 2. Create New Users
    // ============================================================================
    println!("2. Creating new users...");

    // Read-only user
    auth.create_user("alice", "alice_password", vec![Permission::Select])
        .await?;
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
    )
    .await?;
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
    )
    .await?;
    println!("   ✓ Created user 'charlie' (DDL + DML)\n");

    // ============================================================================
    // 3. Authenticate Users
    // ============================================================================
    println!("3. Testing authentication...");

    match auth.authenticate("alice", "alice_password").await {
        Ok(_) => println!("   ✓ Alice authenticated successfully"),
        Err(_) => println!("   ✗ Alice authentication failed"),
    }

    match auth.authenticate("bob", "wrong_password").await {
        Ok(_) => println!("   ✗ Bob authenticated with wrong password!"),
        Err(_) => println!("   ✓ Bob rejected with wrong password"),
    }

    match auth.authenticate("nonexistent", "password").await {
        Ok(_) => println!("   ✗ Non-existent user authenticated!"),
        Err(_) => println!("   ✓ Non-existent user rejected"),
    }
    println!();

    // ============================================================================
    // 4. Check Permissions
    // ============================================================================
    println!("4. Checking permissions...");

    let alice = auth.get_user("alice").await?;
    println!("   Alice permissions:");
    println!(
        "     - SELECT: {}",
        alice.has_permission(Permission::Select)
    );
    println!(
        "     - INSERT: {}",
        alice.has_permission(Permission::Insert)
    );
    println!(
        "     - CREATE TABLE: {}",
        alice.has_permission(Permission::CreateTable)
    );
    println!();

    let bob = auth.get_user("bob").await?;
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

    println!(
        "{}",
        "   Alice before: INSERT = ".to_string()
            + &auth
                .get_user("alice")
                .await?
                .has_permission(Permission::Insert)
                .to_string()
    );

    auth.grant_permission("alice", Permission::Insert).await?;
    println!("   ✓ Granted INSERT to Alice");

    println!(
        "{}",
        "   Alice after: INSERT = ".to_string()
            + &auth
                .get_user("alice")
                .await?
                .has_permission(Permission::Insert)
                .to_string()
    );

    auth.revoke_permission("alice", Permission::Insert).await?;
    println!("   ✓ Revoked INSERT from Alice");

    println!(
        "{}",
        "   Alice final: INSERT = ".to_string()
            + &auth
                .get_user("alice")
                .await?
                .has_permission(Permission::Insert)
                .to_string()
    );
    println!();

    // ============================================================================
    // 6. Update Password
    // ============================================================================
    println!("6. Updating password...");

    auth.update_password("bob", "new_secure_password").await?;
    println!("   ✓ Updated Bob's password");

    match auth.authenticate("bob", "bob_password").await {
        Ok(_) => println!("   ✗ Old password still works!"),
        Err(_) => println!("   ✓ Old password rejected"),
    }

    match auth.authenticate("bob", "new_secure_password").await {
        Ok(_) => println!("   ✓ New password works"),
        Err(_) => println!("   ✗ New password failed!"),
    }
    println!();

    // ============================================================================
    // 7. Connect as Different Users
    // ============================================================================
    println!("7. Connecting as different users...");

    // Connect as Alice (read-only)
    let alice_client = Client::connect("alice", "alice_password").await?;
    println!("   ✓ Connected as Alice");

    // Alice can read
    let _ = alice_client.execute("CREATE TABLE test (id INTEGER)").await;
    match alice_client.query("SELECT * FROM test").await {
        Ok(_) => println!("   ✓ Alice can SELECT"),
        Err(_) => println!("   ✗ Alice cannot SELECT"),
    }

    // Connect as Bob
    let _bob_client = Client::connect("bob", "new_secure_password").await?;
    println!("   ✓ Connected as Bob");
    println!();

    // ============================================================================
    // 8. List All Users
    // ============================================================================
    println!("8. Final user list:");
    let users = auth.list_users().await?;
    for username in users {
        let user = auth.get_user(&username).await?;
        println!("   - {} (admin: {})", username, user.is_admin());
    }
    println!();

    // ============================================================================
    // 9. Delete User
    // ============================================================================
    println!("9. Deleting user...");

    auth.delete_user("charlie").await?;
    println!("   ✓ Deleted user 'charlie'");

    match auth.get_user("charlie").await {
        Ok(_) => println!("   ✗ Charlie still exists!"),
        Err(_) => println!("   ✓ Charlie no longer exists"),
    }
    println!();

    println!("✓ All user management examples completed!");

    Ok(())
}
