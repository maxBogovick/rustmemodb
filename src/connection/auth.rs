use crate::core::{DbError, Result};
use std::collections::HashMap;
use std::sync::{Arc};
use tokio::sync::RwLock;
use lazy_static::lazy_static;

/// User permission level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    /// Execute SELECT queries
    Select,
    /// Execute INSERT queries
    Insert,
    /// Execute UPDATE queries
    Update,
    /// Execute DELETE queries
    Delete,
    /// Create tables
    CreateTable,
    /// Drop tables
    DropTable,
    /// Administrative privileges
    Admin,
}

/// User account
#[derive(Debug, Clone)]
pub struct User {
    username: String,
    password_hash: String,
    permissions: Vec<Permission>,
}

impl User {
    /// Creates a new user
    pub fn new(username: String, password_hash: String, permissions: Vec<Permission>) -> Self {
        Self {
            username,
            password_hash,
            permissions,
        }
    }

    /// Returns the username
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the password hash (internal use only)
    pub(crate) fn password_hash(&self) -> &str {
        &self.password_hash
    }

    /// Returns the user's permission list
    pub fn permissions(&self) -> &[Permission] {
        &self.permissions
    }

    /// Checks if user has a specific permission
    pub fn has_permission(&self, permission: Permission) -> bool {
        self.permissions.contains(&Permission::Admin) || self.permissions.contains(&permission)
    }

    /// Checks if user is an administrator
    #[inline]
    pub fn is_admin(&self) -> bool {
        self.permissions.contains(&Permission::Admin)
    }

    /// Updates the password hash
    fn set_password_hash(&mut self, hash: String) {
        self.password_hash = hash;
    }

    /// Adds a permission if it doesn't already exist
    fn add_permission(&mut self, permission: Permission) -> bool {
        if !self.permissions.contains(&permission) {
            self.permissions.push(permission);
            true
        } else {
            false
        }
    }

    /// Removes a permission
    fn remove_permission(&mut self, permission: Permission) -> bool {
        let len_before = self.permissions.len();
        self.permissions.retain(|p| p != &permission);
        len_before != self.permissions.len()
    }
}

/// Authentication and authorization manager
///
/// Manages user accounts and their access permissions
pub struct AuthManager {
    users: RwLock<HashMap<String, User>>,
}

// Global singleton instance of AuthManager
lazy_static! {
    static ref GLOBAL_AUTH_MANAGER: Arc<AuthManager> = Arc::new(AuthManager::new());
}

impl AuthManager {
    /// Get the global AuthManager instance
    ///
    /// Returns a reference to the singleton AuthManager that is shared across all connections.
    /// This ensures that users created in one connection are available in all other connections.
    pub fn global() -> &'static Arc<AuthManager> {
        &GLOBAL_AUTH_MANAGER
    }

    const DEFAULT_ADMIN_USERNAME: &'static str = "admin";
    const DEFAULT_ADMIN_PASSWORD: &'static str = "adminpass";

    /// Creates a new manager with default administrator
    pub fn new() -> Self {
        Self::with_admin(Self::DEFAULT_ADMIN_USERNAME, Self::DEFAULT_ADMIN_PASSWORD)
    }

    /// Creates a manager with custom administrator credentials
    pub fn with_admin(username: &str, password: &str) -> Self {
        let mut users = HashMap::new();

        let admin_user = User::new(
            username.to_string(),
            Self::hash_password(password),
            vec![Permission::Admin],
        );

        users.insert(username.to_string(), admin_user);

        Self {
            users: RwLock::new(users),
        }
    }

    /// Hashes a password using bcrypt
    ///
    /// Uses bcrypt with default cost factor (12) for secure password hashing.
    /// Each hash includes a random salt, so the same password will produce different hashes.
    fn hash_password(password: &str) -> String {
        bcrypt::hash(password, bcrypt::DEFAULT_COST)
            .expect("Failed to hash password")
    }

    /// Verifies password against bcrypt hash
    ///
    /// Returns true if the password matches the hash, false otherwise.
    fn verify_password(password: &str, hash: &str) -> bool {
        bcrypt::verify(password, hash).unwrap_or(false)
    }

    /// Authenticates a user
    pub async fn authenticate(&self, username: &str, password: &str) -> Result<User> {
        let users = self.users.read().await;

        let user = users.get(username)
            .ok_or_else(|| DbError::ExecutionError("Invalid username or password".into()))?;

        if !Self::verify_password(password, &user.password_hash) {
            return Err(DbError::ExecutionError("Invalid username or password".into()));
        }

        Ok(user.clone())
    }

    /// Creates a new user
    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        permissions: Vec<Permission>,
    ) -> Result<()> {
        self.validate_username(username)?;
        self.validate_password(password)?;  // Validate BEFORE hashing

        let mut users = self.users.write().await;

        if users.contains_key(username) {
            return Err(DbError::ExecutionError(
                format!("User '{}' already exists", username)
            ));
        }

        let user = User::new(
            username.to_string(),
            Self::hash_password(password),
            permissions,
        );

        users.insert(username.to_string(), user);

        Ok(())
    }

    /// Deletes a user
    pub async fn delete_user(&self, username: &str) -> Result<()> {
        let mut users = self.users.write().await;

        // Check if we're deleting the last administrator
        let user_to_delete = users.get(username)
            .ok_or_else(|| DbError::ExecutionError(
                format!("User '{}' not found", username)
            ))?;

        if user_to_delete.is_admin() {
            let admin_count = users.values().filter(|u| u.is_admin()).count();

            if admin_count <= 1 {
                return Err(DbError::ExecutionError(
                    "Cannot delete the last admin user".into()
                ));
            }
        }

        users.remove(username);

        Ok(())
    }

    /// Updates a user's password
    pub async fn update_password(&self, username: &str, new_password: &str) -> Result<()> {
        self.validate_password(new_password)?;

        let mut users = self.users.write().await;

        let user = users.get_mut(username)
            .ok_or_else(|| DbError::ExecutionError(
                format!("User '{}' not found", username)
            ))?;

        user.set_password_hash(Self::hash_password(new_password));

        Ok(())
    }

    /// Grants a permission to a user
    pub async fn grant_permission(&self, username: &str, permission: Permission) -> Result<()> {
        let mut users = self.users.write().await;

        let user = users.get_mut(username)
            .ok_or_else(|| DbError::ExecutionError(
                format!("User '{}' not found", username)
            ))?;

        user.add_permission(permission);

        Ok(())
    }

    /// Revokes a permission from a user
    pub async fn revoke_permission(&self, username: &str, permission: Permission) -> Result<()> {
        let mut users = self.users.write().await;

        let user = users.get(username)
            .ok_or_else(|| DbError::ExecutionError(
                format!("User '{}' not found", username)
            ))?;

        // Prevent revoking Admin permission from the last administrator
        if permission == Permission::Admin && user.is_admin() {
            let admin_count = users.values().filter(|u| u.is_admin()).count();

            if admin_count <= 1 {
                return Err(DbError::ExecutionError(
                    "Cannot revoke admin permission from the last admin user".into()
                ));
            }
        }

        // Now we can safely get mutable reference
        let user = users.get_mut(username).unwrap();
        user.remove_permission(permission);

        Ok(())
    }

    /// Returns a list of all users
    pub async fn list_users(&self) -> Result<Vec<String>> {
        let users = self.users.read().await;

        let mut usernames: Vec<String> = users.keys().cloned().collect();
        usernames.sort();

        Ok(usernames)
    }

    /// Gets user information
    pub async fn get_user(&self, username: &str) -> Result<User> {
        let users = self.users.read().await;

        users.get(username)
            .cloned()
            .ok_or_else(|| DbError::ExecutionError(
                format!("User '{}' not found", username)
            ))
    }

    /// Checks if a user exists
    pub async fn user_exists(&self, username: &str) -> Result<bool> {
        let users = self.users.read().await;

        Ok(users.contains_key(username))
    }

    /// Returns the number of users
    pub async fn user_count(&self) -> Result<usize> {
        let users = self.users.read().await;

        Ok(users.len())
    }

    /// Validates username
    fn validate_username(&self, username: &str) -> Result<()> {
        if username.is_empty() {
            return Err(DbError::ExecutionError("Username cannot be empty".into()));
        }

        if username.len() > 50 {
            return Err(DbError::ExecutionError("Username too long (max 50 characters)".into()));
        }

        Ok(())
    }

    /// Validates password complexity
    ///
    /// Enforces the following rules:
    /// - Minimum 8 characters
    /// - Cannot be empty
    fn validate_password(&self, password: &str) -> Result<()> {
        if password.is_empty() {
            return Err(DbError::ExecutionError("Password cannot be empty".into()));
        }

        if password.len() < 8 {
            return Err(DbError::ExecutionError(
                "Password must be at least 8 characters long".into()
            ));
        }

        Ok(())
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_default_admin_user() {
        let auth = AuthManager::new();
        let user = auth.authenticate("admin", "adminpass").await.unwrap();
        assert!(user.is_admin());
        assert_eq!(user.username(), "admin");
    }

    #[tokio::test]
    async fn test_create_user() {
        let auth = AuthManager::new();

        auth.create_user("alice", "password123", vec![Permission::Select]).await.unwrap();

        let user = auth.authenticate("alice", "password123").await.unwrap();
        assert_eq!(user.username(), "alice");
        assert!(user.has_permission(Permission::Select));
        assert!(!user.has_permission(Permission::Insert));
    }

    #[tokio::test]
    async fn test_invalid_credentials() {
        let auth = AuthManager::new();
        assert!(auth.authenticate("admin", "wrongpass").await.is_err());
        assert!(auth.authenticate("nonexistent", "password123").await.is_err());
    }

    #[tokio::test]
    async fn test_duplicate_user() {
        let auth = AuthManager::new();
        auth.create_user("bob", "password1234", vec![]).await.unwrap();

        let result = auth.create_user("bob", "password1234", vec![]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_update_password() {
        let auth = AuthManager::new();
        auth.create_user("charlie", "oldpassword", vec![]).await.unwrap();

        auth.update_password("charlie", "newpassword").await.unwrap();

        assert!(auth.authenticate("charlie", "oldpassword").await.is_err());
        assert!(auth.authenticate("charlie", "newpassword").await.is_ok());
    }

    #[tokio::test]
    async fn test_grant_revoke_permission() {
        let auth = AuthManager::new();
        auth.create_user("diana", "password1234", vec![]).await.unwrap();

        auth.grant_permission("diana", Permission::Select).await.unwrap();
        let user = auth.get_user("diana").await.unwrap();
        assert!(user.has_permission(Permission::Select));

        auth.revoke_permission("diana", Permission::Select).await.unwrap();
        let user = auth.get_user("diana").await.unwrap();
        assert!(!user.has_permission(Permission::Select));
    }

    #[tokio::test]
    async fn test_admin_permission_overrides() {
        let user = User::new(
            "admin".to_string(),
            "hash".to_string(),
            vec![Permission::Admin],
        );

        assert!(user.has_permission(Permission::Select));
        assert!(user.has_permission(Permission::Insert));
        assert!(user.has_permission(Permission::Delete));
        assert!(user.has_permission(Permission::CreateTable));
    }

    #[tokio::test]
    async fn test_cannot_delete_last_admin() {
        let auth = AuthManager::new();
        let result = auth.delete_user("admin").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cannot_revoke_last_admin_permission() {
        let auth = AuthManager::new();
        let result = auth.revoke_permission("admin", Permission::Admin).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_users() {
        let auth = AuthManager::new();
        auth.create_user("user1", "password1234", vec![]).await.unwrap();
        auth.create_user("user2", "password1234", vec![]).await.unwrap();

        let users = auth.list_users().await.unwrap();
        assert!(users.contains(&"admin".to_string()));
        assert!(users.contains(&"user1".to_string()));
        assert!(users.contains(&"user2".to_string()));
    }

    #[tokio::test]
    async fn test_user_exists() {
        let auth = AuthManager::new();
        assert!(auth.user_exists("admin").await.unwrap());
        assert!(!auth.user_exists("nonexistent").await.unwrap());
    }

    #[tokio::test]
    async fn test_user_count() {
        let auth = AuthManager::new();
        assert_eq!(auth.user_count().await.unwrap(), 1);

        auth.create_user("user1", "password1234", vec![]).await.unwrap();
        assert_eq!(auth.user_count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_validate_username() {
        let auth = AuthManager::new();

        // Пустое имя
        assert!(auth.create_user("", "password123", vec![]).await.is_err());

        // Слишком длинное имя
        let long_name = "a".repeat(51);
        assert!(auth.create_user(&long_name, "password123", vec![]).await.is_err());
    }

    #[tokio::test]
    async fn test_validate_password() {
        let auth = AuthManager::new();

        // Короткий пароль (меньше 8 символов)
        let result = auth.create_user("test", "short", vec![]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("at least 8 characters"));

        // Пустой пароль
        let result = auth.create_user("test2", "", vec![]).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cannot be empty"));

        // Валидный пароль (8+ символов)
        assert!(auth.create_user("test3", "validpass123", vec![]).await.is_ok());
    }
}