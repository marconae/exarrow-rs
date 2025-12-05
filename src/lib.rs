//! # exarrow-rs
//!
//! ADBC-compatible driver for Exasol with Apache Arrow data format support.
//!
//! This library provides high-performance database connectivity for Exasol using
//! the ADBC (Arrow Database Connectivity) interface. Phase 1 implements the
//! WebSocket-based transport protocol, with future plans for Arrow-native gRPC.
//!
//! ## Example
//!
//! ```no_run
//! # use exarrow_rs::*;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create driver and open database
//! let driver = Driver::new();
//! let database = driver.open("exasol://sys:exasol@localhost:8563")?;
//!
//! // Connect to the database
//! let mut connection = database.connect().await?;
//!
//! // Execute a query
//! let results = connection
//!     .query("SELECT * FROM my_table")
//!     .await?;
//!
//! // Process Arrow RecordBatch
//! for batch in results {
//!     println!("Rows: {}", batch.num_rows());
//! }
//!
//! // Close connection
//! connection.close().await?;
//! # Ok(())
//! # }
//! ```

// Module declarations
pub mod adbc;
pub mod arrow_conversion;
pub mod connection;
pub mod error;
pub mod query;
pub mod transport;
pub mod types;

// FFI module for C-compatible ADBC export (conditionally compiled)
#[cfg(feature = "ffi")]
pub mod adbc_ffi;

// Re-export public API
pub use adbc::{Connection, Database, Driver, Statement};
pub use arrow_conversion::ArrowConverter;
pub use error::{ConnectionError, ConversionError, ExasolError, QueryError};
pub use types::{ExasolType, TypeMapper};

// Re-export FFI types when ffi feature is enabled
#[cfg(feature = "ffi")]
pub use adbc_ffi::{FfiConnection, FfiDatabase, FfiDriver, FfiStatement};
