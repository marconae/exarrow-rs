//! ADBC Driver implementation.
//!
//! This module provides the `Driver` type which contains metadata about the
//! exarrow-rs driver and serves as a factory for creating `Database` instances.

use crate::adbc::Database;
use crate::error::ConnectionError;
use std::str::FromStr;

/// ADBC Driver for Exasol.
///
/// The `Driver` type represents the exarrow-rs driver and provides metadata
/// about the driver implementation. It serves as the entry point for creating
/// database connections.
///
/// # Example
///
/// ```
/// use exarrow_rs::adbc::Driver;
///
/// let driver = Driver::new();
/// println!("Driver: {} v{}", driver.name(), driver.version());
/// println!("Vendor: {}", driver.vendor());
/// ```
#[derive(Debug, Clone)]
pub struct Driver {
    /// Driver name
    name: String,
    /// Driver version
    version: String,
    /// Vendor name
    vendor: String,
    /// Driver description
    description: String,
}

impl Driver {
    /// Create a new Driver instance.
    ///
    /// This initializes the driver with metadata from the library.
    ///
    /// # Example
    ///
    /// ```
    /// use exarrow_rs::adbc::Driver;
    ///
    /// let driver = Driver::new();
    /// assert_eq!(driver.name(), "exarrow-rs");
    /// ```
    pub fn new() -> Self {
        Self {
            name: "exarrow-rs".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            vendor: "exarrow-rs contributors".to_string(),
            description: "ADBC-compatible driver for Exasol with Arrow data format support"
                .to_string(),
        }
    }

    /// Get the driver name.
    ///
    /// # Returns
    ///
    /// The name of the driver ("exarrow-rs").
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the driver version.
    ///
    /// # Returns
    ///
    /// The version string from the Cargo.toml.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Get the vendor name.
    ///
    /// # Returns
    ///
    /// The vendor name ("exarrow-rs contributors").
    pub fn vendor(&self) -> &str {
        &self.vendor
    }

    /// Get the driver description.
    ///
    /// # Returns
    ///
    /// A description of the driver's capabilities.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Open a database connection factory.
    ///
    /// This parses the connection string and creates a `Database` instance
    /// that can be used to establish connections.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - Connection string in the format:
    ///   `exasol://[username[:password]@]host[:port][/schema][?param=value&...]`
    ///
    /// # Returns
    ///
    /// A `Database` instance configured with the connection parameters.
    ///
    /// # Errors
    ///
    /// Returns `ConnectionError` if the connection string is invalid.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use exarrow_rs::adbc::Driver;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let driver = Driver::new();
    /// let database = driver.open("exasol://user:pass@localhost:8563")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn open(&self, connection_string: &str) -> Result<Database, ConnectionError> {
        Database::from_str(connection_string)
    }

    /// Check if a connection string is valid.
    ///
    /// This validates the connection string format without establishing a connection.
    ///
    /// # Arguments
    ///
    /// * `connection_string` - The connection string to validate.
    ///
    /// # Returns
    ///
    /// `true` if the connection string is valid, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```
    /// use exarrow_rs::adbc::Driver;
    ///
    /// let driver = Driver::new();
    /// assert!(driver.validate_connection_string("exasol://user@localhost"));
    /// assert!(!driver.validate_connection_string("invalid://connection"));
    /// ```
    pub fn validate_connection_string(&self, connection_string: &str) -> bool {
        Database::from_str(connection_string).is_ok()
    }
}

impl Default for Driver {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for Driver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} v{} ({})", self.name, self.version, self.vendor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_driver_creation() {
        let driver = Driver::new();
        assert_eq!(driver.name(), "exarrow-rs");
        assert_eq!(driver.vendor(), "exarrow-rs contributors");
        assert!(!driver.version().is_empty());
        assert!(!driver.description().is_empty());
    }

    #[test]
    fn test_driver_default() {
        let driver = Driver::default();
        assert_eq!(driver.name(), "exarrow-rs");
    }

    #[test]
    fn test_driver_display() {
        let driver = Driver::new();
        let display = format!("{}", driver);
        assert!(display.contains("exarrow-rs"));
        assert!(display.contains("contributors"));
    }

    #[test]
    fn test_driver_open_valid() {
        let driver = Driver::new();
        let result = driver.open("exasol://user@localhost");
        assert!(result.is_ok());
    }

    #[test]
    fn test_driver_open_invalid() {
        let driver = Driver::new();
        let result = driver.open("invalid://connection");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_connection_string() {
        let driver = Driver::new();

        assert!(driver.validate_connection_string("exasol://user@localhost"));
        assert!(driver.validate_connection_string("exasol://user:pass@host:8563"));
        assert!(driver.validate_connection_string("exasol://user@host/schema"));

        assert!(!driver.validate_connection_string(""));
        assert!(!driver.validate_connection_string("invalid"));
        assert!(!driver.validate_connection_string("postgres://user@host"));
    }
}
