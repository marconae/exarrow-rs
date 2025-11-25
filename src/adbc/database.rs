//! ADBC Database implementation.
//!
//! This module provides the `Database` type which acts as a factory for
//! creating database connections.

use crate::adbc::Connection;
use crate::connection::params::ConnectionParams;
use crate::error::ConnectionError;
use std::str::FromStr;

/// ADBC Database connection factory.
///
/// The `Database` type represents a database connection configuration and
/// serves as a factory for creating `Connection` instances. It encapsulates
/// the connection parameters and provides methods to establish connections.
///
/// # Example
///
/// ```no_run
/// use exarrow_rs::adbc::Database;
/// use std::str::FromStr;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create database from connection string
/// let database = Database::from_str("exasol://user:pass@localhost:8563")?;
///
/// // Connect to the database
/// let connection = database.connect().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Database {
    /// Connection parameters
    params: ConnectionParams,
    /// Original connection string (for display purposes)
    connection_string: String,
}

impl Database {
    /// Create a new Database instance from connection parameters.
    ///
    /// # Arguments
    ///
    /// * `params` - The connection parameters
    ///
    /// # Example
    ///
    /// ```
    /// use exarrow_rs::adbc::Database;
    /// use exarrow_rs::connection::ConnectionBuilder;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let params = ConnectionBuilder::new()
    ///     .host("localhost")
    ///     .port(8563)
    ///     .username("sys")
    ///     .password("exasol")
    ///     .build()?;
    ///
    /// let database = Database::new(params);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(params: ConnectionParams) -> Self {
        // Reconstruct a safe connection string for display (without password)
        let connection_string = format!(
            "exasol://{}@{}:{}{}",
            params.username,
            params.host,
            params.port,
            params
                .schema
                .as_ref()
                .map(|s| format!("/{}", s))
                .unwrap_or_default()
        );

        Self {
            params,
            connection_string,
        }
    }

    /// Get the connection parameters.
    ///
    /// # Returns
    ///
    /// A reference to the connection parameters.
    pub fn params(&self) -> &ConnectionParams {
        &self.params
    }

    /// Get the connection string (without password).
    ///
    /// # Returns
    ///
    /// A safe display version of the connection string.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Establish a connection to the database.
    ///
    /// This creates a new `Connection` instance and establishes a connection
    /// to the Exasol database using the configured parameters.
    ///
    /// # Returns
    ///
    /// A connected `Connection` instance.
    ///
    /// # Errors
    ///
    /// Returns `ConnectionError` if the connection fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use exarrow_rs::adbc::Database;
    /// use std::str::FromStr;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let database = Database::from_str("exasol://user:pass@localhost:8563")?;
    /// let connection = database.connect().await?;
    ///
    /// // Use the connection...
    ///
    /// connection.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(&self) -> Result<Connection, ConnectionError> {
        Connection::from_params(self.params.clone()).await
    }

    /// Test the connection without establishing a persistent connection.
    ///
    /// This attempts to connect to the database and immediately closes the
    /// connection, verifying that the connection parameters are valid.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the connection test succeeds.
    ///
    /// # Errors
    ///
    /// Returns `ConnectionError` if the connection test fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use exarrow_rs::adbc::Database;
    /// use std::str::FromStr;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let database = Database::from_str("exasol://user:pass@localhost:8563")?;
    ///
    /// // Test the connection
    /// database.test_connection().await?;
    ///
    /// println!("Connection test successful!");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn test_connection(&self) -> Result<(), ConnectionError> {
        let connection = self.connect().await?;
        connection.close().await?;
        Ok(())
    }
}

impl FromStr for Database {
    type Err = ConnectionError;

    /// Parse a connection string to create a Database instance.
    ///
    /// # Arguments
    ///
    /// * `s` - Connection string in the format:
    ///   `exasol://[username[:password]@]host[:port][/schema][?param=value&...]`
    ///
    /// # Returns
    ///
    /// A `Database` instance configured with the parsed parameters.
    ///
    /// # Errors
    ///
    /// Returns `ConnectionError` if the connection string is invalid.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use exarrow_rs::adbc::Database;
    /// use std::str::FromStr;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let database = Database::from_str("exasol://user:pass@localhost:8563/MY_SCHEMA")?;
    /// # Ok(())
    /// # }
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let params = ConnectionParams::from_str(s)?;
        Ok(Self::new(params))
    }
}

impl std::fmt::Display for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Database({})", self.connection_string)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectionBuilder;

    #[test]
    fn test_database_creation() {
        let params = ConnectionBuilder::new()
            .host("localhost")
            .port(8563)
            .username("test")
            .password("secret")
            .build()
            .unwrap();

        let database = Database::new(params);
        assert!(database.connection_string().contains("localhost"));
        assert!(database.connection_string().contains("test"));
        // Password should not appear in connection string
        assert!(!database.connection_string().contains("secret"));
    }

    #[test]
    fn test_database_from_str_basic() {
        let database = Database::from_str("exasol://user@localhost").unwrap();
        assert_eq!(database.params().host, "localhost");
        assert_eq!(database.params().port, 8563);
        assert_eq!(database.params().username, "user");
    }

    #[test]
    fn test_database_from_str_with_port() {
        let database = Database::from_str("exasol://user@localhost:9000").unwrap();
        assert_eq!(database.params().port, 9000);
    }

    #[test]
    fn test_database_from_str_with_password() {
        let database = Database::from_str("exasol://user:pass@localhost").unwrap();
        assert_eq!(database.params().username, "user");
        // Password should be set internally but not exposed
        assert!(database.connection_string().contains("user"));
        assert!(!database.connection_string().contains("pass"));
    }

    #[test]
    fn test_database_from_str_with_schema() {
        let database = Database::from_str("exasol://user@localhost/MY_SCHEMA").unwrap();
        assert_eq!(database.params().schema, Some("MY_SCHEMA".to_string()));
        assert!(database.connection_string().contains("MY_SCHEMA"));
    }

    #[test]
    fn test_database_from_str_full() {
        let database =
            Database::from_str("exasol://admin:secret@db.example.com:9000/PROD?timeout=30")
                .unwrap();

        assert_eq!(database.params().host, "db.example.com");
        assert_eq!(database.params().port, 9000);
        assert_eq!(database.params().username, "admin");
        assert_eq!(database.params().schema, Some("PROD".to_string()));
    }

    #[test]
    fn test_database_from_str_invalid() {
        let result = Database::from_str("invalid://connection");
        assert!(result.is_err());

        let result = Database::from_str("");
        assert!(result.is_err());

        let result = Database::from_str("postgres://user@host");
        assert!(result.is_err());
    }

    #[test]
    fn test_database_display() {
        let database = Database::from_str("exasol://user@localhost/SCHEMA").unwrap();
        let display = format!("{}", database);
        assert!(display.contains("Database"));
        assert!(display.contains("localhost"));
        assert!(display.contains("SCHEMA"));
    }

    #[test]
    fn test_database_debug() {
        let database = Database::from_str("exasol://user@localhost").unwrap();
        let debug = format!("{:?}", database);
        assert!(debug.contains("Database"));
        assert!(debug.contains("params"));
    }
}
