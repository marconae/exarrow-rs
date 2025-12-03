//! ADBC (Arrow Database Connectivity) interface implementation.
//!
//! This module provides the ADBC-compatible interface for the exarrow-rs driver,
//! offering a high-level API for connecting to Exasol databases and executing queries.
//!
//! # v2.0.0 Breaking Changes
//!
//! - Connection now owns the transport directly
//! - `create_statement()` is now synchronous and returns a pure data container
//! - Use `execute_statement()` instead of `Statement::execute()`
//! - Use `Connection::prepare()` instead of `Statement::prepare()`
//!
//! # Architecture
//!
//! The ADBC interface is organized into four main components:
//! - `Driver` - Driver metadata and factory for creating databases
//! - `Database` - Database connection factory with connection string parsing
//! - `Connection` - Active database connection for executing queries
//! - `Statement` - SQL statement data container with parameter binding
//!
//! # Example
//!
//! ```no_run
//! use exarrow_rs::adbc::{Driver, Database, Connection};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create driver
//! let driver = Driver::new();
//!
//! // Open database
//! let database = driver.open("exasol://user:pass@localhost:8563")?;
//!
//! // Connect
//! let mut connection = database.connect().await?;
//!
//! // Execute query (new API)
//! let stmt = connection.create_statement("SELECT * FROM my_table");
//! let results = connection.execute_statement(&stmt).await?;
//!
//! // Or use convenience method
//! let results = connection.execute("SELECT * FROM my_table").await?;
//!
//! // Close connection
//! connection.close().await?;
//! # Ok(())
//! # }
//! ```

pub mod connection;
pub mod database;
pub mod driver;
pub mod statement;

// Re-export commonly used types
pub use connection::Connection;
pub use database::Database;
pub use driver::Driver;
pub use statement::{Parameter, Statement, StatementType};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify that key types are exported and accessible
        // This is a compile-time check more than a runtime check
        let driver = Driver::new();
        assert_eq!(driver.name(), "exarrow-rs");
    }
}
