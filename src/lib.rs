//! # exarrow-rs
//!
//! ADBC-compatible driver for Exasol with Apache Arrow data format support.
//!
//! This library provides high-performance database connectivity for Exasol using
//! the ADBC (Arrow Database Connectivity) interface. It enables efficient data transfer
//! using the Apache Arrow columnar format, making it ideal for analytical workloads
//! and data science applications.
//!
//! ## Features
//!
//! - **ADBC Interface**: Standard Arrow Database Connectivity driver
//! - **Query Execution**: Execute SQL queries and retrieve results as Arrow RecordBatches
//! - **Bulk Import**: Import data from CSV, Parquet, and Arrow RecordBatches
//! - **Bulk Export**: Export data to CSV, Parquet, and Arrow RecordBatches
//! - **Streaming**: Memory-efficient streaming for large datasets
//! - **Compression**: Support for gzip, bzip2, snappy, lz4, and zstd compression
//!
//! ## Connect
//!
//! ```no_run
//! use exarrow_rs::adbc::{Driver, Database, Connection};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let driver = Driver::new();
//!     // Schema in the URI is automatically opened with OPEN SCHEMA after login.
//!     let database = driver.open(
//!         "exasol://sys:exasol@localhost:8563/my_schema?validateservercertificate=0"
//!     )?;
//!     let mut connection = database.connect().await?;
//!     println!("Connected: session {}", connection.session_id());
//!     connection.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Execute a query
//!
//! ```no_run
//! use exarrow_rs::adbc::{Driver, Connection};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let driver = Driver::new();
//!     let database = driver.open(
//!         "exasol://sys:exasol@localhost:8563/my_schema?validateservercertificate=0"
//!     )?;
//!     let mut connection = database.connect().await?;
//!
//!     let batches = connection.query("SELECT id, name FROM users ORDER BY id").await?;
//!     let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
//!     println!("Fetched {} rows across {} batch(es)", total_rows, batches.len());
//!
//!     connection.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Import data
//!
//! ```no_run
//! use exarrow_rs::adbc::{Driver, Connection};
//! use exarrow_rs::import::{CsvImportOptions, ParquetImportOptions};
//! use std::path::PathBuf;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let driver = Driver::new();
//!     let database = driver.open(
//!         "exasol://sys:exasol@localhost:8563/my_schema?validateservercertificate=0"
//!     )?;
//!     let mut connection = database.connect().await?;
//!
//!     // Import CSV files
//!     let csv_files = vec![PathBuf::from("/data/part1.csv"), PathBuf::from("/data/part2.csv")];
//!     let rows = connection
//!         .import_csv_from_files("users", csv_files, CsvImportOptions::default())
//!         .await?;
//!     println!("Imported {} rows from CSV", rows);
//!
//!     // Import Parquet files
//!     let parquet_files = vec![PathBuf::from("/data/snapshot.parquet")];
//!     let rows = connection
//!         .import_parquet_from_files("users", parquet_files, ParquetImportOptions::default())
//!         .await?;
//!     println!("Imported {} rows from Parquet", rows);
//!
//!     connection.close().await?;
//!     Ok(())
//! }
//! ```

// Module declarations
pub mod adbc;
pub mod arrow_conversion;
pub mod connection;
pub mod error;
pub mod export;
pub mod import;
pub mod query;
pub mod transport;
pub mod types;

// FFI module for C-compatible ADBC export (conditionally compiled)
#[cfg(feature = "ffi")]
pub mod adbc_ffi;

// ADBC Interface Types
/// Re-export ADBC driver and connection types.
pub use adbc::{Connection, Database, Driver, Statement};

// Arrow Conversion
/// Re-export Arrow conversion utilities.
pub use arrow_conversion::ArrowConverter;

// Error Types
/// Re-export error types for convenient error handling.
pub use error::{ConnectionError, ConversionError, ExasolError, QueryError};

// Type System
/// Re-export type mapping utilities.
pub use types::{ExasolType, TypeMapper};

// Import Types
/// CSV import options and functions.
///
/// The CSV import module provides functionality for importing data from CSV files,
/// streams, iterators, and custom callbacks into Exasol tables.
///
/// # Example
///
pub use import::{
    import_from_arrow_ipc,
    import_from_callback,
    import_from_file,
    import_from_iter,
    import_from_parquet,
    import_from_parquet_stream,
    import_from_record_batch,
    import_from_record_batches,
    import_from_stream,
    // Arrow import
    ArrowImportOptions,
    ArrowToCsvWriter,
    // CSV import
    CsvImportOptions,
    CsvWriterOptions,
    DataPipeSender,
    // Error type
    ImportError,
    // Parquet import
    ParquetImportOptions,
};

// Export Types
/// CSV export options and functions.
///
/// The export module provides functionality for exporting data from Exasol tables
/// or query results to files, streams, in-memory lists, or custom callbacks.
///
/// # Example
///
pub use export::{
    csv_to_record_batches,
    exasol_types_to_arrow_schema,
    export_to_arrow_ipc,
    export_to_callback,
    export_to_file,
    export_to_list,
    export_to_parquet,
    export_to_parquet_stream,
    export_to_parquet_via_transport,
    export_to_record_batches,
    export_to_stream,
    // Arrow export
    ArrowExportOptions,
    // CSV export
    CsvExportOptions,
    CsvToArrowReader,
    DataPipeReceiver,
    ExportError,
    // Parquet export
    ParquetCompression,
    ParquetExportOptions,
};

// Query Builder Types
/// Query builder types for constructing IMPORT and EXPORT SQL statements.
///
/// These types provide a builder pattern for constructing Exasol IMPORT and EXPORT
/// SQL statements with full control over CSV format options, compression, and
/// encoding settings.
///
/// # Export Query Example
///
///
/// # Import Query Example
///
pub use query::export::{Compression, DelimitMode, ExportQuery, ExportSource, RowSeparator};
pub use query::import::{
    Compression as ImportCompression, ImportQuery, RowSeparator as ImportRowSeparator, TrimMode,
};

// Query Execution Types
pub use query::statement::{Parameter, StatementType};
/// Query execution and result handling types.
pub use query::{PreparedStatement, QueryMetadata, ResultSet, ResultSetIterator};

// FFI Types (when ffi feature is enabled)
/// Re-export FFI types when ffi feature is enabled.
#[cfg(feature = "ffi")]
pub use adbc_ffi::{FfiConnection, FfiDatabase, FfiDriver, FfiStatement};
