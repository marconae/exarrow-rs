//! Query execution and result handling.
//!
//! This module provides the core query execution functionality for exarrow-rs,
//! including SQL statement data structures, prepared statements, and result handling.
//!
//! # v2.0.0 Changes
//!
//! Statement is now a pure data container. Execution is performed by Connection.
//!
//! # Overview
//!
//! The query module is organized into:
//! - `statement` - SQL statement data container with parameter binding
//! - `prepared` - Prepared statement handling for parameterized queries
//! - `results` - Result set iteration and metadata handling
//!
//! # Example
//!
//! ```no_run
//! use exarrow_rs::adbc::Connection;
//!
//! # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
//! // Create a statement (now synchronous)
//! let mut stmt = connection.create_statement("SELECT * FROM users WHERE age > ?");
//!
//! // Bind parameters
//! stmt.bind(0, 18)?;
//!
//! // Execute via Connection
//! let result_set = connection.execute_statement(&stmt).await?;
//!
//! // Iterate over results
//! for batch in result_set.into_iterator()? {
//!     let batch = batch?;
//!     println!("Batch rows: {}", batch.num_rows());
//! }
//! # Ok(())
//! # }
//! ```

pub mod prepared;
pub mod results;
pub mod statement;

// Re-export commonly used types
pub use prepared::PreparedStatement;
pub use results::{QueryMetadata, ResultSet, ResultSetIterator};
pub use statement::{Parameter, Statement, StatementType};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify that key types are exported and accessible
        // This is a compile-time check more than a runtime check
        let _: Option<StatementType> = None;
        let _: Option<Parameter> = None;
    }

    #[test]
    fn test_prepared_statement_export() {
        // Verify that PreparedStatement is accessible
        // This is a compile-time check
        fn _takes_prepared_stmt(_stmt: PreparedStatement) {}
    }
}
