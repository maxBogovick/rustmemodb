/// User management tests
///
/// Tests for authentication and authorization system
/// Run with: cargo test --test user_management_tests

use rustmemodb::{Client, ConnectionConfig};
use rustmemodb::connection::auth::{Permission};

#[tokio::test]
async fn test_default_admin_user() {
    let client = Client::connect("admin", "adminpass").await.unwrap();

    let auth = client.auth_manager();
    let users = auth.list_users().await.unwrap();

    assert!(users.contains(&"admin".to_string()));
}

#[tokio::test]
async fn test_admin_authentication() {
    let result = Client::connect("admin", "adminpass").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_invalid_credentials() {
    let result = Client::connect("admin", "wrong_password").await;
    assert!(result.is_err());

    let result = Client::connect("nonexistent_user", "password").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_create_new_user() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    let permissions = vec![Permission::Select, Permission::Insert];

    let result = auth.create_user("alice", "alice123", permissions).await;
    assert!(result.is_ok());

    let users = auth.list_users().await.unwrap();
    assert!(users.contains(&"alice".to_string()));
}

#[tokio::test]
async fn test_new_user_login() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    auth.create_user("bob", "bob123000", vec![Permission::Select]).await.unwrap();

    // Now Bob should be able to connect
    let bob_client = Client::connect("bob", "bob123000").await;
    assert!(bob_client.is_ok());
}

#[tokio::test]
async fn test_user_permissions() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    let permissions = vec![
        Permission::Select,
        Permission::Insert,
        Permission::CreateTable,
    ];

    auth.create_user("charlie", "charlie123", permissions.clone()).await.unwrap();

    let user = auth.authenticate("charlie", "charlie123").await.unwrap();

    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(user.has_permission(Permission::CreateTable));
    assert!(!user.has_permission(Permission::Delete));
    assert!(!user.has_permission(Permission::Admin));
}

#[tokio::test]
async fn test_admin_permission() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    let admin_user = auth.authenticate("admin", "adminpass").await.unwrap();

    // Admin should have all permissions
    assert!(admin_user.has_permission(Permission::Admin));
    assert!(admin_user.has_permission(Permission::Select));
    assert!(admin_user.has_permission(Permission::Insert));
    assert!(admin_user.has_permission(Permission::Update));
    assert!(admin_user.has_permission(Permission::Delete));
    assert!(admin_user.has_permission(Permission::CreateTable));
    assert!(admin_user.has_permission(Permission::DropTable));
}

#[tokio::test]
async fn test_permission_enforcement_on_queries() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    if auth.user_exists("reader_perm").await.unwrap() {
        let _ = auth.delete_user("reader_perm").await;
    }
    auth.create_user("reader_perm", "reader123", vec![Permission::Select]).await.unwrap();

    let reader = Client::connect("reader_perm", "reader123").await.unwrap();

    let result = reader.execute("CREATE TABLE denied_tbl (id INTEGER)").await;
    assert!(result.is_err());

    let result = reader.execute("INSERT INTO denied_tbl VALUES (1)").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_duplicate_user_creation() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    auth.create_user("diana", "diana123", vec![Permission::Select]).await.unwrap();

    // Try to create same user again
    let result = auth.create_user("diana", "diana123", vec![Permission::Insert]).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_delete_user() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    auth.create_user("eve", "eve123456", vec![Permission::Select]).await.unwrap();

    let users = auth.list_users().await.unwrap();
    assert!(users.contains(&"eve".to_string()));

    // Delete user
    auth.delete_user("eve").await.unwrap();

    let users = auth.list_users().await.unwrap();
    assert!(!users.contains(&"eve".to_string()));

    // Eve should no longer be able to connect
    let result = Client::connect("eve", "eve123456").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_cannot_delete_admin() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    let result = auth.delete_user("admin").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_update_user_password() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    auth.create_user("frank", "frank123", vec![Permission::Select]).await.unwrap();

    // Change password
    auth.update_password("frank", "new_password").await.unwrap();

    // Old password should not work
    let result = Client::connect("frank", "frank123").await;
    assert!(result.is_err());

    // New password should work
    let result = Client::connect("frank", "new_password").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_grant_permission() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    auth.create_user("grace", "grace123", vec![Permission::Select]).await.unwrap();

    let user = auth.authenticate("grace", "grace123").await.unwrap();
    assert!(!user.has_permission(Permission::Insert));

    // Grant INSERT permission
    auth.grant_permission("grace", Permission::Insert).await.unwrap();

    let user = auth.authenticate("grace", "grace123").await.unwrap();
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
}

#[tokio::test]
async fn test_revoke_permission() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    let permissions = vec![Permission::Select, Permission::Insert, Permission::Delete];
    auth.create_user("henry", "henry123", permissions).await.unwrap();

    let user = auth.authenticate("henry", "henry123").await.unwrap();
    assert!(user.has_permission(Permission::Delete));

    // Revoke DELETE permission
    auth.revoke_permission("henry", Permission::Delete).await.unwrap();

    let user = auth.authenticate("henry", "henry123").await.unwrap();
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(!user.has_permission(Permission::Delete));
}

#[tokio::test]
async fn test_user_info() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    let permissions = vec![
        Permission::Select,
        Permission::Insert,
        Permission::Update,
    ];

    auth.create_user("iris", "iris1234", permissions).await.unwrap();

    let user = auth.authenticate("iris", "iris1234").await.unwrap();

    assert_eq!(user.username(), "iris");
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(user.has_permission(Permission::Update));
    assert!(!user.has_permission(Permission::Delete));
}

#[tokio::test]
async fn test_connection_with_different_users() {
    let admin_client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = admin_client.auth_manager();

    auth.create_user("reader", "reader123", vec![Permission::Select]).await.unwrap();
    auth.create_user("writer", "writer123", vec![Permission::Insert, Permission::Select]).await.unwrap();

    let reader_client = Client::connect("reader", "reader123").await.unwrap();
    let writer_client = Client::connect("writer", "writer123").await.unwrap();

    // Admin creates table
    admin_client.execute("CREATE TABLE user_test (id INTEGER, data TEXT)").await.unwrap();
    admin_client.execute("INSERT INTO user_test VALUES (1, 'data1')").await.unwrap();

    // Reader can SELECT
    let result = reader_client.query("SELECT * FROM user_test").await;
    assert!(result.is_ok());

    // Writer can INSERT and SELECT
    let result = writer_client.execute("INSERT INTO user_test VALUES (2, 'data2')").await;
    assert!(result.is_ok());

    let result = writer_client.query("SELECT * FROM user_test").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_connection_pool_with_auth() {
    let config = ConnectionConfig::new("admin", "adminpass")
        .min_connections(2)
        .max_connections(5);

    let client = Client::connect_with_config(config).await.unwrap();

    let stats = client.stats().await;
    assert_eq!(stats.max_connections, 5);
    assert!(stats.total_connections >= 2);
}

#[tokio::test]
async fn test_multiple_users_concurrent_access() {
    let admin_client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = admin_client.auth_manager();

    // Create multiple users
    auth.create_user("concurrent1", "pass1000", vec![Permission::Select, Permission::Insert]).await.unwrap();
    auth.create_user("concurrent2", "pass2000", vec![Permission::Select, Permission::Insert]).await.unwrap();
    auth.create_user("concurrent3", "pass3000", vec![Permission::Select, Permission::Insert]).await.unwrap();

    admin_client.execute("CREATE TABLE multi_user (id INTEGER, user TEXT)").await.unwrap();

    let mut handles = vec![];

    for (username, password) in &[
        ("concurrent1", "pass1000"),
        ("concurrent2", "pass2000"),
        ("concurrent3", "pass3000"),
    ] {
        let user = username.to_string();
        let pass = password.to_string();

        let handle = tokio::spawn(async move {
            let client = Client::connect(&user, &pass).await.unwrap();

            for i in 0..20 {
                client.execute(&format!(
                    "INSERT INTO multi_user VALUES ({}, '{}')",
                    i, user
                )).await.unwrap();
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let result = admin_client.query("SELECT * FROM multi_user").await.unwrap();
    assert_eq!(result.row_count(), 60); // 3 users * 20 inserts
}

#[tokio::test]
async fn test_username_case_sensitivity() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    auth.create_user("TestUser", "password123", vec![Permission::Select]).await.unwrap();

    // Exact case should work
    let result = Client::connect("TestUser", "password123").await;
    assert!(result.is_ok());

    // Different case should fail (usernames are case-sensitive)
    let result = Client::connect("testuser", "password123").await;
    assert!(result.is_err());

    let result = Client::connect("TESTUSER", "password123").await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_special_characters_in_credentials() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    // Password with special characters
    let result = auth.create_user("special_user", "p@$$w0rd!#", vec![Permission::Select]).await;
    assert!(result.is_ok());

    let result = Client::connect("special_user", "p@$$w0rd!#").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_user_permissions_inheritance() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    // User starts with SELECT only
    auth.create_user("inherit_user", "password", vec![Permission::Select]).await.unwrap();

    let user = auth.authenticate("inherit_user", "password").await.unwrap();
    assert!(user.has_permission(Permission::Select));
    assert!(!user.has_permission(Permission::Insert));

    // Grant multiple permissions
    auth.grant_permission("inherit_user", Permission::Insert).await.unwrap();
    auth.grant_permission("inherit_user", Permission::Update).await.unwrap();
    auth.grant_permission("inherit_user", Permission::Delete).await.unwrap();

    let user = auth.authenticate("inherit_user", "password").await.unwrap();
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(user.has_permission(Permission::Update));
    assert!(user.has_permission(Permission::Delete));
}

#[tokio::test]
async fn test_revoke_all_permissions() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    let permissions = vec![
        Permission::Select,
        Permission::Insert,
        Permission::Update,
        Permission::Delete,
    ];

    auth.create_user("revoke_all", "password", permissions).await.unwrap();

    // Revoke all permissions one by one
    auth.revoke_permission("revoke_all", Permission::Select).await.unwrap();
    auth.revoke_permission("revoke_all", Permission::Insert).await.unwrap();
    auth.revoke_permission("revoke_all", Permission::Update).await.unwrap();
    auth.revoke_permission("revoke_all", Permission::Delete).await.unwrap();

    let user = auth.authenticate("revoke_all", "password").await.unwrap();
    assert!(!user.has_permission(Permission::Select));
    assert!(!user.has_permission(Permission::Insert));
    assert!(!user.has_permission(Permission::Update));
    assert!(!user.has_permission(Permission::Delete));

    // User should still be able to authenticate
    let result = Client::connect("revoke_all", "password").await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_user_with_all_permissions() {
    let client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = client.auth_manager();

    let all_permissions = vec![
        Permission::Select,
        Permission::Insert,
        Permission::Update,
        Permission::Delete,
        Permission::CreateTable,
        Permission::DropTable,
    ];

    auth.create_user("superuser", "password", all_permissions).await.unwrap();

    let user = auth.authenticate("superuser", "password").await.unwrap();

    // Should have all permissions except Admin
    assert!(user.has_permission(Permission::Select));
    assert!(user.has_permission(Permission::Insert));
    assert!(user.has_permission(Permission::Update));
    assert!(user.has_permission(Permission::Delete));
    assert!(user.has_permission(Permission::CreateTable));
    assert!(user.has_permission(Permission::DropTable));
    assert!(!user.has_permission(Permission::Admin));
}

#[tokio::test]
async fn test_connection_url_with_credentials() {
    let admin_client = Client::connect("admin", "adminpass").await.unwrap();
    let auth = admin_client.auth_manager();

    auth.create_user("urluser", "urlpass80", vec![Permission::Select]).await.unwrap();

    let client = Client::connect_url("rustmemodb://urluser:urlpass80@localhost:5432/testdb").await;
    assert!(client.is_ok());

    // Wrong password in URL
    let client = Client::connect_url("rustmemodb://urluser:wrong@localhost:5432/testdb").await;
    assert!(client.is_err());
}
