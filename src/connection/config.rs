use std::time::Duration;

/// Database connection configuration
///
/// Similar to PostgreSQL/MySQL connection strings
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Database host (for future client-server mode)
    pub host: String,

    /// Database port (for future client-server mode)
    pub port: u16,

    /// Database name
    pub database: String,

    /// Username for authentication
    pub username: String,

    /// Password for authentication
    pub password: String,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// Query timeout
    pub query_timeout: Option<Duration>,

    /// Maximum number of connections in pool
    pub max_connections: usize,

    /// Minimum number of connections in pool
    pub min_connections: usize,

    /// Connection idle timeout
    pub idle_timeout: Option<Duration>,

    /// Maximum connection lifetime
    pub max_lifetime: Option<Duration>,
}

impl ConnectionConfig {
    /// Create a new connection configuration
    pub fn new(username: &str, password: &str) -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432, // Default PostgreSQL port
            database: "rustmemodb".to_string(),
            username: username.to_string(),
            password: password.to_string(),
            connect_timeout: Duration::from_secs(30),
            query_timeout: None,
            max_connections: 10,
            min_connections: 1,
            idle_timeout: Some(Duration::from_secs(600)), // 10 minutes
            max_lifetime: Some(Duration::from_secs(1800)), // 30 minutes
        }
    }

    /// Set the database name
    pub fn database(mut self, database: &str) -> Self {
        self.database = database.to_string();
        self
    }

    /// Set the host
    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    /// Set the port
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set connection timeout
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set query timeout
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }

    /// Set maximum connections
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Set minimum connections
    pub fn min_connections(mut self, min: usize) -> Self {
        self.min_connections = min;
        self
    }

    /// Set idle timeout
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    /// Set maximum lifetime
    pub fn max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = Some(lifetime);
        self
    }

    /// Parse from connection string
    ///
    /// Format: "rustmemodb://username:password@host:port/database"
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let config = ConnectionConfig::from_url(
    ///     "rustmemodb://admin:secret@localhost:5432/mydb"
    /// )?;
    /// ```
    pub fn from_url(url: &str) -> Result<Self, String> {
        // Simple URL parsing (use url crate in production)
        if !url.starts_with("rustmemodb://") {
            return Err("URL must start with 'rustmemodb://'".to_string());
        }

        let url = &url["rustmemodb://".len()..];

        // Parse username:password@host:port/database
        let parts: Vec<&str> = url.split('@').collect();
        if parts.len() != 2 {
            return Err("Invalid URL format".to_string());
        }

        let auth_parts: Vec<&str> = parts[0].split(':').collect();
        if auth_parts.len() != 2 {
            return Err("Invalid credentials format".to_string());
        }

        let username = auth_parts[0];
        let password = auth_parts[1];

        let host_parts: Vec<&str> = parts[1].split('/').collect();
        if host_parts.len() != 2 {
            return Err("Invalid host/database format".to_string());
        }

        let host_port: Vec<&str> = host_parts[0].split(':').collect();
        let host = host_port[0];
        let port = if host_port.len() > 1 {
            host_port[1].parse().map_err(|_| "Invalid port".to_string())?
        } else {
            5432
        };

        let database = host_parts[1];

        Ok(Self::new(username, password)
            .host(host)
            .port(port)
            .database(database))
    }

    /// Convert to connection string
    pub fn to_url(&self) -> String {
        format!(
            "rustmemodb://{}:{}@{}:{}/{}",
            self.username,
            "***", // Don't expose password
            self.host,
            self.port,
            self.database
        )
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.username.is_empty() {
            return Err("Username cannot be empty".to_string());
        }

        if self.password.is_empty() {
            return Err("Password cannot be empty".to_string());
        }

        if self.max_connections == 0 {
            return Err("max_connections must be > 0".to_string());
        }

        if self.min_connections > self.max_connections {
            return Err("min_connections cannot exceed max_connections".to_string());
        }

        Ok(())
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self::new("admin", "adminpass")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ConnectionConfig::default();
        assert_eq!(config.username, "admin");
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 5432);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ConnectionConfig::new("user", "pass")
            .host("example.com")
            .port(3306)
            .database("mydb")
            .max_connections(20);

        assert_eq!(config.host, "example.com");
        assert_eq!(config.port, 3306);
        assert_eq!(config.database, "mydb");
        assert_eq!(config.max_connections, 20);
    }

    #[test]
    fn test_from_url() {
        let config = ConnectionConfig::from_url(
            "rustmemodb://alice:secret@db.example.com:5432/production"
        ).unwrap();

        assert_eq!(config.username, "alice");
        assert_eq!(config.password, "secret");
        assert_eq!(config.host, "db.example.com");
        assert_eq!(config.port, 5432);
        assert_eq!(config.database, "production");
    }

    #[test]
    fn test_from_url_default_port() {
        let config = ConnectionConfig::from_url(
            "rustmemodb://user:pass@localhost/testdb"
        ).unwrap();

        assert_eq!(config.port, 5432);
    }

    #[test]
    fn test_invalid_url() {
        assert!(ConnectionConfig::from_url("invalid://url").is_err());
        assert!(ConnectionConfig::from_url("rustmemodb://noat").is_err());
    }

    #[test]
    fn test_validate() {
        let valid = ConnectionConfig::new("user", "pass");
        assert!(valid.validate().is_ok());

        let invalid_username = ConnectionConfig::new("", "pass");
        assert!(invalid_username.validate().is_err());

        let invalid_max_conn = ConnectionConfig::new("user", "pass")
            .max_connections(0);
        assert!(invalid_max_conn.validate().is_err());

        let invalid_min_max = ConnectionConfig::new("user", "pass")
            .min_connections(10)
            .max_connections(5);
        assert!(invalid_min_max.validate().is_err());
    }

    #[test]
    fn test_to_url_hides_password() {
        let config = ConnectionConfig::new("alice", "secret123")
            .host("example.com")
            .database("mydb");

        let url = config.to_url();
        assert!(!url.contains("secret123"));
        assert!(url.contains("***"));
    }
}
