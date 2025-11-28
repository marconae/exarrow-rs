//! Query execution and result handling.
//!
//! This module provides the core query execution functionality for exarrow-rs,
//! including SQL statement preparation, parameter binding, and result set handling.
//!
//! # Overview
//!
//! The query module is organized into:
//! - `statement` - SQL statement execution and parameter binding
//! - `prepared` - Prepared statement handling for parameterized queries
//! - `results` - Result set iteration and metadata handling
//!
//! # Example
//!
//! ```no_run
//! use exarrow_rs::query::{Statement, StatementBuilder};
//! use exarrow_rs::transport::WebSocketTransport;
//! use std::sync::Arc;
//! use tokio::sync::Mutex;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let transport = Arc::new(Mutex::new(WebSocketTransport::new()));
//!
//! // Create and execute a statement
//! let mut stmt = StatementBuilder::new(transport)
//!     .sql("SELECT * FROM users WHERE age > ?")
//!     .timeout_ms(30_000)
//!     .build();
//!
//! // Bind parameters
//! stmt.bind(0, 18)?;
//!
//! // Execute query
//! let result_set = stmt.execute().await?;
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
pub use statement::{Parameter, Statement, StatementBuilder, StatementType};

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
