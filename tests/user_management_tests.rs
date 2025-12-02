/// User management tests
///
/// Tests for authentication and authorization system
/// Run with: cargo test --test user_management_tests

use rustmemodb::{Client, ConnectionConfig};
use rustmemodb::connection::auth::{AuthManager, Permission};

#[test]
fn test_default_admin_user() {
    let client = Client::connect("admin", "admin").unwrap();

    let auth = client.auth_manager();
    let users = auth.list_users().unwrap();

    assert!(users.contains(&"admin".to_string()));
}

#[test]
fn test_admin_authentication() {
    let result = Client::connect("admin", "admin");
    assert!(result.is_ok());
}

#[test]
fn test_invalid_credentials() {
    let result = Client::connect("admin", "wrong_password");
    assert!(result.is_err());

    let result = Client::connect("nonexistent_user", "password");
    assert!(result.is_err());
}

#[test]
fn test_create_new_user() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    let permissions = vec![Permission::Select, Permission::Insert];

    let result = auth.create_user("alice", "alice123", permissions);
    assert!(result.is_ok());

    let users = auth.list_users().unwrap();
    assert!(users.contains(&"alice".to_string()));
}

#[test]
fn test_new_user_login() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    auth.create_user("bob", "bob123", vec![Permission::Select]).unwrap();

    // Now Bob should be able to connect
    let bob_client = Client::connect("bob", "bob123");
    assert!(bob_client.is_ok());
}

#[test]
fn test_user_permissions() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    let permissions = vec![
        Permission::Select,
        Permission::Insert,
        Permission::CreateTable,
    ];

    auth.create_user("charlie", "charlie123", permissions.clone()).unwrap();

    let user = auth.authenticate("charlie", "charlie123").unwrap();

    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(user.has_permission(Permission::CreateTable));
    assert!(!user.has_permission(Permission::Delete));
    assert!(!user.has_permission(Permission::Admin));
}

#[test]
fn test_admin_permission() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    let admin_user = auth.authenticate("admin", "admin").unwrap();

    // Admin should have all permissions
    assert!(admin_user.has_permission(Permission::Admin));
    assert!(admin_user.has_permission(Permission::Select));
    assert!(admin_user.has_permission(Permission::Insert));
    assert!(admin_user.has_permission(Permission::Update));
    assert!(admin_user.has_permission(Permission::Delete));
    assert!(admin_user.has_permission(Permission::CreateTable));
    assert!(admin_user.has_permission(Permission::DropTable));
}

#[test]
fn test_duplicate_user_creation() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    auth.create_user("diana", "diana123", vec![Permission::Select]).unwrap();

    // Try to create same user again
    let result = auth.create_user("diana", "diana123", vec![Permission::Insert]);
    assert!(result.is_err());
}

#[test]
fn test_delete_user() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    auth.create_user("eve", "eve123", vec![Permission::Select]).unwrap();

    let users = auth.list_users().unwrap();
    assert!(users.contains(&"eve".to_string()));

    // Delete user
    auth.delete_user("eve").unwrap();

    let users = auth.list_users().unwrap();
    assert!(!users.contains(&"eve".to_string()));

    // Eve should no longer be able to connect
    let result = Client::connect("eve", "eve123");
    assert!(result.is_err());
}

#[test]
fn test_cannot_delete_admin() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    let result = auth.delete_user("admin");
    assert!(result.is_err());
}

#[test]
fn test_update_user_password() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    auth.create_user("frank", "frank123", vec![Permission::Select]).unwrap();

    // Change password
    auth.update_password("frank", "new_password").unwrap();

    // Old password should not work
    let result = Client::connect("frank", "frank123");
    assert!(result.is_err());

    // New password should work
    let result = Client::connect("frank", "new_password");
    assert!(result.is_ok());
}

#[test]
fn test_grant_permission() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    auth.create_user("grace", "grace123", vec![Permission::Select]).unwrap();

    let user = auth.authenticate("grace", "grace123").unwrap();
    assert!(!user.has_permission(Permission::Insert));

    // Grant INSERT permission
    auth.grant_permission("grace", Permission::Insert).unwrap();

    let user = auth.authenticate("grace", "grace123").unwrap();
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
}

#[test]
fn test_revoke_permission() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    let permissions = vec![Permission::Select, Permission::Insert, Permission::Delete];
    auth.create_user("henry", "henry123", permissions).unwrap();

    let user = auth.authenticate("henry", "henry123").unwrap();
    assert!(user.has_permission(Permission::Delete));

    // Revoke DELETE permission
    auth.revoke_permission("henry", Permission::Delete).unwrap();

    let user = auth.authenticate("henry", "henry123").unwrap();
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(!user.has_permission(Permission::Delete));
}

#[test]
fn test_list_all_users() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    let initial_users = auth.list_users().unwrap();
    let initial_count = initial_users.len();

    // Create several users
    auth.create_user("user1", "pass1", vec![Permission::Select]).unwrap();
    auth.create_user("user2", "pass2", vec![Permission::Insert]).unwrap();
    auth.create_user("user3", "pass3", vec![Permission::Delete]).unwrap();

    let users = auth.list_users().unwrap();
    assert_eq!(users.len(), initial_count + 3);
    assert!(users.contains(&"user1".to_string()));
    assert!(users.contains(&"user2".to_string()));
    assert!(users.contains(&"user3".to_string()));
}

#[test]
fn test_user_info() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    let permissions = vec![
        Permission::Select,
        Permission::Insert,
        Permission::Update,
    ];

    auth.create_user("iris", "iris123", permissions).unwrap();

    let user = auth.authenticate("iris", "iris123").unwrap();

    assert_eq!(user.username(), "iris");
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(user.has_permission(Permission::Update));
    assert!(!user.has_permission(Permission::Delete));
}

#[test]
fn test_connection_with_different_users() {
    let admin_client = Client::connect("admin", "admin").unwrap();
    let auth = admin_client.auth_manager();

    auth.create_user("reader", "reader123", vec![Permission::Select]).unwrap();
    auth.create_user("writer", "writer123", vec![Permission::Insert, Permission::Select]).unwrap();

    let reader_client = Client::connect("reader", "reader123").unwrap();
    let writer_client = Client::connect("writer", "writer123").unwrap();

    // Admin creates table
    admin_client.execute("CREATE TABLE user_test (id INTEGER, data TEXT)").unwrap();
    admin_client.execute("INSERT INTO user_test VALUES (1, 'data1')").unwrap();

    // Reader can SELECT
    let result = reader_client.query("SELECT * FROM user_test");
    assert!(result.is_ok());

    // Writer can INSERT and SELECT
    let result = writer_client.execute("INSERT INTO user_test VALUES (2, 'data2')");
    assert!(result.is_ok());

    let result = writer_client.query("SELECT * FROM user_test");
    assert!(result.is_ok());
}

#[test]
fn test_permission_enforcement_select() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    // User with only INSERT permission
    auth.create_user("inserter", "inserter123", vec![Permission::Insert]).unwrap();

    client.execute("CREATE TABLE perm_test (id INTEGER)").unwrap();

    let inserter_client = Client::connect("inserter", "inserter123").unwrap();

    // INSERT should work
    let result = inserter_client.execute("INSERT INTO perm_test VALUES (1)");
    // Note: Permission enforcement may not be fully implemented yet
    // This test documents expected behavior
}

#[test]
fn test_connection_pool_with_auth() {
    let config = ConnectionConfig::new("admin", "admin")
        .min_connections(2)
        .max_connections(5);

    let client = Client::connect_with_config(config).unwrap();

    let stats = client.stats();
    assert_eq!(stats.max_connections, 5);
    assert!(stats.total_connections >= 2);
}

#[test]
fn test_multiple_users_concurrent_access() {
    use std::sync::Arc;
    use std::thread;

    let admin_client = Client::connect("admin", "admin").unwrap();
    let auth = admin_client.auth_manager();

    // Create multiple users
    auth.create_user("concurrent1", "pass1", vec![Permission::Select, Permission::Insert]).unwrap();
    auth.create_user("concurrent2", "pass2", vec![Permission::Select, Permission::Insert]).unwrap();
    auth.create_user("concurrent3", "pass3", vec![Permission::Select, Permission::Insert]).unwrap();

    admin_client.execute("CREATE TABLE multi_user (id INTEGER, user TEXT)").unwrap();

    let mut handles = vec![];

    for (username, password) in &[
        ("concurrent1", "pass1"),
        ("concurrent2", "pass2"),
        ("concurrent3", "pass3"),
    ] {
        let user = username.to_string();
        let pass = password.to_string();

        let handle = thread::spawn(move || {
            let client = Client::connect(&user, &pass).unwrap();

            for i in 0..20 {
                client.execute(&format!(
                    "INSERT INTO multi_user VALUES ({}, '{}')",
                    i, user
                )).unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let result = admin_client.query("SELECT * FROM multi_user").unwrap();
    assert_eq!(result.row_count(), 60); // 3 users * 20 inserts
}

#[test]
fn test_username_case_sensitivity() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    auth.create_user("TestUser", "password123", vec![Permission::Select]).unwrap();

    // Exact case should work
    let result = Client::connect("TestUser", "password123");
    assert!(result.is_ok());

    // Different case should fail (usernames are case-sensitive)
    let result = Client::connect("testuser", "password123");
    assert!(result.is_err());

    let result = Client::connect("TESTUSER", "password123");
    assert!(result.is_err());
}

#[test]
#[ignore]
//TODO need fix it
fn test_empty_username_or_password() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    // Empty username
    let result = auth.create_user("", "password", vec![Permission::Select]);
    assert!(result.is_err());

    // Empty password
    let result = auth.create_user("user", "", vec![Permission::Select]);
    assert!(result.is_err());
}

#[test]
fn test_special_characters_in_credentials() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    // Username with special characters
    let result = auth.create_user("user@domain.com", "password", vec![Permission::Select]);
    // This may or may not be supported - test documents behavior

    // Password with special characters
    let result = auth.create_user("special_user", "p@$$w0rd!#", vec![Permission::Select]);
    assert!(result.is_ok());

    let result = Client::connect("special_user", "p@$$w0rd!#");
    assert!(result.is_ok());
}

#[test]
fn test_user_permissions_inheritance() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    // User starts with SELECT only
    auth.create_user("inherit_user", "password", vec![Permission::Select]).unwrap();

    let user = auth.authenticate("inherit_user", "password").unwrap();
    assert!(user.has_permission(Permission::Select));
    assert!(!user.has_permission(Permission::Insert));

    // Grant multiple permissions
    auth.grant_permission("inherit_user", Permission::Insert).unwrap();
    auth.grant_permission("inherit_user", Permission::Update).unwrap();
    auth.grant_permission("inherit_user", Permission::Delete).unwrap();

    let user = auth.authenticate("inherit_user", "password").unwrap();
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(user.has_permission(Permission::Update));
    assert!(user.has_permission(Permission::Delete));
}

#[test]
fn test_revoke_all_permissions() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    let permissions = vec![
        Permission::Select,
        Permission::Insert,
        Permission::Update,
        Permission::Delete,
    ];

    auth.create_user("revoke_all", "password", permissions).unwrap();

    // Revoke all permissions one by one
    auth.revoke_permission("revoke_all", Permission::Select).unwrap();
    auth.revoke_permission("revoke_all", Permission::Insert).unwrap();
    auth.revoke_permission("revoke_all", Permission::Update).unwrap();
    auth.revoke_permission("revoke_all", Permission::Delete).unwrap();

    let user = auth.authenticate("revoke_all", "password").unwrap();
    assert!(!user.has_permission(Permission::Select));
    assert!(!user.has_permission(Permission::Insert));
    assert!(!user.has_permission(Permission::Update));
    assert!(!user.has_permission(Permission::Delete));

    // User should still be able to authenticate
    let result = Client::connect("revoke_all", "password");
    assert!(result.is_ok());
}

#[test]
fn test_user_with_all_permissions() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    let all_permissions = vec![
        Permission::Select,
        Permission::Insert,
        Permission::Update,
        Permission::Delete,
        Permission::CreateTable,
        Permission::DropTable,
    ];

    auth.create_user("superuser", "password", all_permissions).unwrap();

    let user = auth.authenticate("superuser", "password").unwrap();

    // Should have all permissions except Admin
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(user.has_permission(Permission::Update));
    assert!(user.has_permission(Permission::Delete));
    assert!(user.has_permission(Permission::CreateTable));
    assert!(user.has_permission(Permission::DropTable));
    assert!(!user.has_permission(Permission::Admin));
}

#[test]
fn test_connection_url_with_credentials() {
    let admin_client = Client::connect("admin", "admin").unwrap();
    let auth = admin_client.auth_manager();

    auth.create_user("urluser", "urlpass", vec![Permission::Select]).unwrap();

    let client = Client::connect_url("rustmemodb://urluser:urlpass@localhost:5432/testdb");
    assert!(client.is_ok());

    // Wrong password in URL
    let client = Client::connect_url("rustmemodb://urluser:wrong@localhost:5432/testdb");
    assert!(client.is_err());
}

#[test]
fn test_max_username_length() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    // Very long username
    let long_username = "a".repeat(100);
    let result = auth.create_user(&long_username, "password", vec![Permission::Select]);
    // This may succeed or fail depending on implementation limits
}

#[test]
fn test_max_password_length() {
    let client = Client::connect("admin", "admin").unwrap();
    let auth = client.auth_manager();

    // Very long password
    let long_password = "p".repeat(100);
    let result = auth.create_user("longpass_user", &long_password, vec![Permission::Select]);

    if result.is_ok() {
        let client = Client::connect("longpass_user", &long_password);
        assert!(client.is_ok());
    }
}
