//! ADBC FFI-compatible trait implementations.
//!
//! This module provides wrapper types that implement the `adbc_core` traits,
//! enabling the exarrow-rs driver to be exported as a C-compatible shared library.
//!
//! The wrappers bridge the async Rust implementation to the synchronous ADBC C API
//! using a tokio runtime for async-to-sync conversion.
//!
//! # FFI Export
//!
//! When built with `--features ffi`, the library exports a C-compatible shared library
//! with the entry point `ExarrowDriverInit`. This can be loaded by ADBC driver managers.
//!
//! ## Building the FFI Library
//!
//! ```bash
//! # Build in release mode (recommended for production)
//! cargo build --release --features ffi
//!
//! # Build in debug mode (for development)
//! cargo build --features ffi
//! ```
//!
//! ## Exported Symbol
//!
//! - **Function name**: `ExarrowDriverInit`
//! - **Library location**:
//!   - macOS: `target/release/libexarrow_rs.dylib`
//!   - Linux: `target/release/libexarrow_rs.so`
//!   - Windows: `target/release/exarrow_rs.dll`
//!
//! ## Connection URI Format
//!
//! The driver accepts URIs in the following format:
//!
//! ```text
//! exasol://[username[:password]@]host[:port][/schema]
//! ```
//!
//! Examples:
//! - `exasol://sys:exasol@localhost:8563`
//! - `exasol://admin:secret@192.168.1.100:8563/my_schema`
//! - `exasol://sys:exasol@exasol-server.example.com:8563`
//!
//! ## Using with ADBC Driver Manager (Rust)
//!
//! ```rust,ignore
//! use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
//! use adbc_core::{Connection, Database, Driver, Statement};
//! use adbc_driver_manager::ManagedDriver;
//!
//! // Load the driver
//! let mut driver = ManagedDriver::load_dynamic_from_filename(
//!     "target/release/libexarrow_rs.dylib",  // or .so on Linux
//!     Some(b"ExarrowDriverInit"),
//!     AdbcVersion::V110,
//! ).expect("Failed to load driver");
//!
//! // Create database with connection URI
//! let uri = "exasol://sys:exasol@localhost:8563";
//! let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri.to_string()))];
//! let db = driver.new_database_with_opts(opts).expect("Failed to create database");
//!
//! // Create connection
//! let mut conn = db.new_connection().expect("Failed to connect");
//!
//! // Create and execute statement
//! let mut stmt = conn.new_statement().expect("Failed to create statement");
//! stmt.set_sql_query("SELECT 1 AS result").expect("Failed to set query");
//!
//! // Execute and get results as Arrow RecordBatch
//! let reader = stmt.execute().expect("Failed to execute");
//! for batch in reader {
//!     let batch = batch.expect("Failed to read batch");
//!     println!("Got {} rows", batch.num_rows());
//! }
//! ```
//!
//! ## Using with ADBC Driver Manager (Python)
//!
//! ```python
//! import adbc_driver_manager
//!
//! # Load the exarrow driver
//! with adbc_driver_manager.AdbcDriver(
//!     "/path/to/libexarrow_rs.dylib",
//!     entrypoint="ExarrowDriverInit"
//! ) as driver:
//!     with adbc_driver_manager.AdbcDatabase(
//!         driver=driver,
//!         uri="exasol://sys:exasol@localhost:8563"
//!     ) as db:
//!         with adbc_driver_manager.AdbcConnection(db) as conn:
//!             with adbc_driver_manager.AdbcStatement(conn) as stmt:
//!                 stmt.set_sql_query("SELECT 1 AS result")
//!                 reader, _ = stmt.execute_query()
//!                 table = reader.read_all()
//!                 print(table.to_pandas())
//! ```
//!
//! ## Supported ADBC Features
//!
//! | Feature | Status |
//! |---------|--------|
//! | `execute` (SELECT queries) | Supported |
//! | `execute_update` (INSERT/UPDATE/DELETE) | Supported |
//! | `get_info` | Supported |
//! | `get_table_types` | Supported |
//! | `commit` / `rollback` | Supported |
//! | `get_objects` | Not yet implemented |
//! | `get_table_schema` | Not yet implemented |
//! | Partitioned results | Not supported |
//! | Substrait plans | Not supported |
//!
//! # Running Integration Tests
//!
//! To test the FFI driver with the ADBC driver manager:
//!
//! ```bash
//! # First, build the FFI library
//! cargo build --release --features ffi
//!
//! # Start Exasol Docker (if not already running)
//! docker run -d --name exasol-test -p 8563:8563 --privileged exasol/docker-db:latest
//!
//! # Wait for database to be ready (1-2 minutes)
//! docker logs exasol-test 2>&1 | grep -i "started"
//!
//! # Run the driver manager integration tests
//! cargo test --test driver_manager_tests -- --ignored
//!
//! # Run with verbose output
//! cargo test --test driver_manager_tests -- --ignored --nocapture
//! ```

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{Optionable, PartitionedResult};
use arrow::compute::concat_batches;
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;
use tokio::runtime::Runtime;

use crate::adbc::Connection as ExaConnection;
use crate::error::ExasolError;

/// Global tokio runtime for async-to-sync bridging.
/// This is lazily initialized on first use.
fn get_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime for ADBC FFI")
    })
}

/// Convert an ExasolError to an ADBC Error.
fn to_adbc_error(err: impl std::error::Error) -> AdbcError {
    AdbcError::with_message_and_status(err.to_string(), AdbcStatus::Internal)
}

// -----------------------------------------------------------------------------
// FFI Driver
// -----------------------------------------------------------------------------

/// FFI-compatible ADBC Driver wrapper.
///
/// This wraps the internal `Driver` type to implement `adbc_core::Driver`.
/// The driver is loaded via the `ExarrowDriverInit` entry point.
///
/// # Example
///
/// ```rust,ignore
/// use adbc_driver_manager::ManagedDriver;
/// use adbc_core::options::AdbcVersion;
///
/// let driver = ManagedDriver::load_dynamic_from_filename(
///     "target/release/libexarrow_rs.dylib",
///     Some(b"ExarrowDriverInit"),
///     AdbcVersion::V110,
/// ).expect("Failed to load driver");
/// ```
#[derive(Debug, Default)]
pub struct FfiDriver;

impl adbc_core::Driver for FfiDriver {
    type DatabaseType = FfiDatabase;

    fn new_database(&mut self) -> AdbcResult<Self::DatabaseType> {
        Ok(FfiDatabase::new())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> AdbcResult<Self::DatabaseType> {
        let mut db = FfiDatabase::new();
        for (key, value) in opts {
            db.set_option(key, value)?;
        }
        Ok(db)
    }
}

// -----------------------------------------------------------------------------
// FFI Database
// -----------------------------------------------------------------------------

/// FFI-compatible ADBC Database wrapper.
///
/// This stores connection options and creates connections on demand.
/// The primary option is `OptionDatabase::Uri` which should contain
/// the Exasol connection string.
///
/// # Connection URI Format
///
/// ```text
/// exasol://[username[:password]@]host[:port][/schema]
/// ```
///
/// # Example
///
/// ```rust,ignore
/// use adbc_core::options::{OptionDatabase, OptionValue};
///
/// let opts = vec![
///     (OptionDatabase::Uri, OptionValue::String("exasol://sys:exasol@localhost:8563".to_string())),
/// ];
/// let db = driver.new_database_with_opts(opts)?;
/// ```
pub struct FfiDatabase {
    /// URI for the connection (exasol://user:pass@host:port/schema)
    uri: Option<String>,
    /// Username override
    username: Option<String>,
    /// Password override
    password: Option<String>,
    /// Custom options
    options: std::collections::HashMap<String, OptionValue>,
}

impl FfiDatabase {
    fn new() -> Self {
        Self {
            uri: None,
            username: None,
            password: None,
            options: std::collections::HashMap::new(),
        }
    }

    /// Build connection parameters from stored options.
    /// This rebuilds the URI with any override credentials.
    fn build_connection_uri(&self) -> AdbcResult<String> {
        let uri = self.uri.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status(
                "Database URI not set. Set adbc.exasol.uri option.",
                AdbcStatus::InvalidState,
            )
        })?;

        // If no overrides, use URI as-is
        if self.username.is_none() && self.password.is_none() {
            return Ok(uri.clone());
        }

        // Parse the URI and rebuild with overrides
        // URI format: exasol://[user[:pass]@]host[:port][/schema][?params]
        let uri_str = uri.as_str();
        if !uri_str.starts_with("exasol://") {
            return Err(AdbcError::with_message_and_status(
                "URI must start with exasol://",
                AdbcStatus::InvalidArguments,
            ));
        }

        let after_scheme = &uri_str[9..]; // Skip "exasol://"

        // Find @, /, ? positions
        let at_pos = after_scheme.rfind('@');
        let (host_part, orig_user, orig_pass) = if let Some(at) = at_pos {
            let auth_part = &after_scheme[..at];
            let host_part = &after_scheme[at + 1..];
            let (user, pass) = if let Some(colon) = auth_part.find(':') {
                (&auth_part[..colon], Some(&auth_part[colon + 1..]))
            } else {
                (auth_part, None)
            };
            (host_part, Some(user), pass)
        } else {
            (after_scheme, None, None)
        };

        // Use overrides or originals
        let user = self.username.as_deref().or(orig_user).unwrap_or("sys");
        let pass = self.password.as_deref().or(orig_pass).unwrap_or("");

        // Rebuild URI
        if pass.is_empty() {
            Ok(format!("exasol://{}@{}", user, host_part))
        } else {
            Ok(format!("exasol://{}:{}@{}", user, pass, host_part))
        }
    }
}

impl Optionable for FfiDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        match key {
            OptionDatabase::Uri => {
                if let OptionValue::String(s) = value {
                    self.uri = Some(s);
                } else {
                    return Err(AdbcError::with_message_and_status(
                        "URI must be a string",
                        AdbcStatus::InvalidArguments,
                    ));
                }
            }
            OptionDatabase::Username => {
                if let OptionValue::String(s) = value {
                    self.username = Some(s);
                } else {
                    return Err(AdbcError::with_message_and_status(
                        "Username must be a string",
                        AdbcStatus::InvalidArguments,
                    ));
                }
            }
            OptionDatabase::Password => {
                if let OptionValue::String(s) = value {
                    self.password = Some(s);
                } else {
                    return Err(AdbcError::with_message_and_status(
                        "Password must be a string",
                        AdbcStatus::InvalidArguments,
                    ));
                }
            }
            OptionDatabase::Other(key) => {
                self.options.insert(key, value);
            }
            _ => {
                // Handle any future additions to the enum
                return Err(AdbcError::with_message_and_status(
                    "Unsupported database option",
                    AdbcStatus::NotImplemented,
                ));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
        match key {
            OptionDatabase::Uri => self.uri.clone().ok_or_else(|| {
                AdbcError::with_message_and_status("URI not set", AdbcStatus::NotFound)
            }),
            OptionDatabase::Username => self.username.clone().ok_or_else(|| {
                AdbcError::with_message_and_status("Username not set", AdbcStatus::NotFound)
            }),
            OptionDatabase::Password => Err(AdbcError::with_message_and_status(
                "Password cannot be retrieved",
                AdbcStatus::InvalidArguments,
            )),
            OptionDatabase::Other(key) => {
                if let Some(OptionValue::String(s)) = self.options.get(&key) {
                    Ok(s.clone())
                } else {
                    Err(AdbcError::with_message_and_status(
                        format!("Option {} not found or not a string", key),
                        AdbcStatus::NotFound,
                    ))
                }
            }
            _ => Err(AdbcError::with_message_and_status(
                "Option not found",
                AdbcStatus::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
        if let OptionDatabase::Other(key) = key {
            if let Some(OptionValue::Bytes(b)) = self.options.get(&key) {
                return Ok(b.clone());
            }
        }
        Err(AdbcError::with_message_and_status(
            "Option not found or not bytes",
            AdbcStatus::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
        if let OptionDatabase::Other(key) = key {
            if let Some(OptionValue::Int(i)) = self.options.get(&key) {
                return Ok(*i);
            }
        }
        Err(AdbcError::with_message_and_status(
            "Option not found or not an integer",
            AdbcStatus::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
        if let OptionDatabase::Other(key) = key {
            if let Some(OptionValue::Double(d)) = self.options.get(&key) {
                return Ok(*d);
            }
        }
        Err(AdbcError::with_message_and_status(
            "Option not found or not a double",
            AdbcStatus::NotFound,
        ))
    }
}

impl adbc_core::Database for FfiDatabase {
    type ConnectionType = FfiConnection;

    fn new_connection(&self) -> AdbcResult<Self::ConnectionType> {
        let uri = self.build_connection_uri()?;
        Ok(FfiConnection::new(uri))
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> AdbcResult<Self::ConnectionType> {
        let uri = self.build_connection_uri()?;
        let mut conn = FfiConnection::new(uri);
        for (key, value) in opts {
            conn.set_option(key, value)?;
        }
        Ok(conn)
    }
}

// -----------------------------------------------------------------------------
// FFI Connection
// -----------------------------------------------------------------------------

/// FFI-compatible ADBC Connection wrapper.
///
/// Represents an active connection to an Exasol database. The connection
/// is lazily established on first use (when creating a statement or
/// performing a transaction operation).
pub struct FfiConnection {
    /// Connection URI
    uri: String,
    /// The actual connection (lazily initialized)
    inner: Option<ExaConnection>,
    /// Pre-init options
    options: std::collections::HashMap<String, OptionValue>,
    /// Auto-commit mode
    auto_commit: bool,
    /// Current schema
    current_schema: Option<String>,
}

impl FfiConnection {
    fn new(uri: String) -> Self {
        Self {
            uri,
            inner: None,
            options: std::collections::HashMap::new(),
            auto_commit: true,
            current_schema: None,
        }
    }

    /// Ensure the connection is established.
    fn ensure_connected(&mut self) -> AdbcResult<&mut ExaConnection> {
        if self.inner.is_none() {
            let uri = self.uri.clone();
            let conn = get_runtime()
                .block_on(async {
                    let params: crate::connection::ConnectionParams = uri.parse()?;
                    ExaConnection::from_params(params).await
                })
                .map_err(to_adbc_error)?;
            self.inner = Some(conn);
        }
        Ok(self.inner.as_mut().unwrap())
    }
}

impl Optionable for FfiConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        match key {
            OptionConnection::AutoCommit => {
                if let OptionValue::String(s) = value {
                    self.auto_commit = s == "true" || s == "1";
                } else {
                    return Err(AdbcError::with_message_and_status(
                        "AutoCommit must be a string ('true' or 'false')",
                        AdbcStatus::InvalidArguments,
                    ));
                }
            }
            OptionConnection::CurrentSchema => {
                if let OptionValue::String(s) = value {
                    self.current_schema = Some(s);
                } else {
                    return Err(AdbcError::with_message_and_status(
                        "CurrentSchema must be a string",
                        AdbcStatus::InvalidArguments,
                    ));
                }
            }
            OptionConnection::ReadOnly | OptionConnection::IsolationLevel => {
                // Store as other option
                self.options.insert(key.as_ref().to_string(), value);
            }
            OptionConnection::CurrentCatalog => {
                // Exasol doesn't have catalogs in the traditional sense
                return Err(AdbcError::with_message_and_status(
                    "Exasol does not support catalogs",
                    AdbcStatus::NotImplemented,
                ));
            }
            OptionConnection::Other(key) => {
                self.options.insert(key, value);
            }
            _ => {
                // Handle any future additions to the enum
                return Err(AdbcError::with_message_and_status(
                    "Unsupported connection option",
                    AdbcStatus::NotImplemented,
                ));
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
        match key {
            OptionConnection::AutoCommit => {
                Ok(if self.auto_commit { "true" } else { "false" }.to_string())
            }
            OptionConnection::CurrentSchema => self.current_schema.clone().ok_or_else(|| {
                AdbcError::with_message_and_status("CurrentSchema not set", AdbcStatus::NotFound)
            }),
            OptionConnection::Other(key) => {
                if let Some(OptionValue::String(s)) = self.options.get(&key) {
                    Ok(s.clone())
                } else {
                    Err(AdbcError::with_message_and_status(
                        format!("Option {} not found", key),
                        AdbcStatus::NotFound,
                    ))
                }
            }
            _ => Err(AdbcError::with_message_and_status(
                "Option not found",
                AdbcStatus::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
        if let OptionConnection::Other(key) = key {
            if let Some(OptionValue::Bytes(b)) = self.options.get(&key) {
                return Ok(b.clone());
            }
        }
        Err(AdbcError::with_message_and_status(
            "Option not found or not bytes",
            AdbcStatus::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
        if let OptionConnection::Other(key) = key {
            if let Some(OptionValue::Int(i)) = self.options.get(&key) {
                return Ok(*i);
            }
        }
        Err(AdbcError::with_message_and_status(
            "Option not found or not an integer",
            AdbcStatus::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
        if let OptionConnection::Other(key) = key {
            if let Some(OptionValue::Double(d)) = self.options.get(&key) {
                return Ok(*d);
            }
        }
        Err(AdbcError::with_message_and_status(
            "Option not found or not a double",
            AdbcStatus::NotFound,
        ))
    }
}

/// A simple RecordBatchReader implementation that yields batches from a Vec.
struct VecRecordBatchReader {
    schema: Arc<Schema>,
    batches: std::vec::IntoIter<RecordBatch>,
}

impl VecRecordBatchReader {
    fn new(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: batches.into_iter(),
        }
    }

    fn empty(schema: Arc<Schema>) -> Self {
        Self::new(schema, vec![])
    }
}

impl Iterator for VecRecordBatchReader {
    type Item = Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

impl RecordBatchReader for VecRecordBatchReader {
    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }
}

impl adbc_core::Connection for FfiConnection {
    type StatementType = FfiStatement;

    fn new_statement(&mut self) -> AdbcResult<Self::StatementType> {
        self.ensure_connected()?;
        Ok(FfiStatement::new(self.uri.clone()))
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        // Cancel is not directly supported in our async implementation
        Err(AdbcError::with_message_and_status(
            "Cancel not implemented",
            AdbcStatus::NotImplemented,
        ))
    }

    fn get_info(
        &self,
        _codes: Option<HashSet<InfoCode>>,
    ) -> AdbcResult<impl RecordBatchReader + Send> {
        // Return driver info as a RecordBatch
        use arrow_array::builder::{StringBuilder, UInt32Builder};
        use arrow_schema::{DataType, Field};

        // Build info schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("info_name", DataType::UInt32, false),
            Field::new("info_value", DataType::Utf8, true),
        ]));

        // Build info data
        let mut name_builder = UInt32Builder::new();
        let mut value_builder = StringBuilder::new();

        // Add driver info
        name_builder.append_value(0); // VendorName
        value_builder.append_value("Exasol");

        name_builder.append_value(100); // DriverName
        value_builder.append_value("exarrow-rs");

        name_builder.append_value(101); // DriverVersion
        value_builder.append_value(env!("CARGO_PKG_VERSION"));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(name_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )
        .map_err(|e| AdbcError::with_message_and_status(e.to_string(), AdbcStatus::Internal))?;

        Ok(VecRecordBatchReader::new(schema, vec![batch]))
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> AdbcResult<impl RecordBatchReader + Send> {
        // This would require querying the Exasol metadata tables
        // For now, return not implemented
        Err::<VecRecordBatchReader, _>(AdbcError::with_message_and_status(
            "get_objects not yet implemented",
            AdbcStatus::NotImplemented,
        ))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> AdbcResult<Schema> {
        // This would require querying Exasol metadata
        Err(AdbcError::with_message_and_status(
            "get_table_schema not yet implemented",
            AdbcStatus::NotImplemented,
        ))
    }

    fn get_table_types(&self) -> AdbcResult<impl RecordBatchReader + Send> {
        use arrow_array::builder::StringBuilder;
        use arrow_schema::{DataType, Field};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));

        let mut builder = StringBuilder::new();
        builder.append_value("TABLE");
        builder.append_value("VIEW");
        builder.append_value("SYSTEM TABLE");

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(builder.finish())])
            .map_err(|e| AdbcError::with_message_and_status(e.to_string(), AdbcStatus::Internal))?;

        Ok(VecRecordBatchReader::new(schema, vec![batch]))
    }

    fn get_statistic_names(&self) -> AdbcResult<impl RecordBatchReader + Send> {
        use arrow_schema::{DataType, Field};
        let schema = Arc::new(Schema::new(vec![
            Field::new("statistic_name", DataType::Utf8, false),
            Field::new("statistic_key", DataType::Int16, false),
        ]));
        Ok(VecRecordBatchReader::empty(schema))
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> AdbcResult<impl RecordBatchReader + Send> {
        Err::<VecRecordBatchReader, _>(AdbcError::with_message_and_status(
            "get_statistics not yet implemented",
            AdbcStatus::NotImplemented,
        ))
    }

    fn commit(&mut self) -> AdbcResult<()> {
        let conn = self.ensure_connected()?;
        get_runtime().block_on(conn.commit()).map_err(to_adbc_error)
    }

    fn rollback(&mut self) -> AdbcResult<()> {
        let conn = self.ensure_connected()?;
        get_runtime()
            .block_on(conn.rollback())
            .map_err(to_adbc_error)
    }

    fn read_partition(
        &self,
        _partition: impl AsRef<[u8]>,
    ) -> AdbcResult<impl RecordBatchReader + Send> {
        Err::<VecRecordBatchReader, _>(AdbcError::with_message_and_status(
            "Partitioned results not supported",
            AdbcStatus::NotImplemented,
        ))
    }
}

// -----------------------------------------------------------------------------
// FFI Statement
// -----------------------------------------------------------------------------

/// FFI-compatible ADBC Statement wrapper.
///
/// Used to execute SQL queries and retrieve results as Arrow RecordBatches.
///
/// # Example
///
/// ```rust,ignore
/// let mut stmt = conn.new_statement()?;
/// stmt.set_sql_query("SELECT * FROM my_table")?;
/// let reader = stmt.execute()?;
/// for batch in reader {
///     // Process Arrow RecordBatch
/// }
/// ```
pub struct FfiStatement {
    /// Connection URI (for creating connections)
    uri: String,
    /// SQL query
    sql: Option<String>,
    /// Bound parameters as RecordBatch
    #[allow(dead_code)]
    bound_data: Option<RecordBatch>,
    /// Statement options
    options: std::collections::HashMap<String, OptionValue>,
}

impl FfiStatement {
    fn new(uri: String) -> Self {
        Self {
            uri,
            sql: None,
            bound_data: None,
            options: std::collections::HashMap::new(),
        }
    }
}

impl Optionable for FfiStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        match key {
            OptionStatement::Other(key) => {
                self.options.insert(key, value);
            }
            _ => {
                // Store standard options as strings
                self.options.insert(key.as_ref().to_string(), value);
            }
        }
        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> AdbcResult<String> {
        let key_str = match key {
            OptionStatement::Other(ref k) => k.as_str(),
            _ => key.as_ref(),
        };
        if let Some(OptionValue::String(s)) = self.options.get(key_str) {
            Ok(s.clone())
        } else {
            Err(AdbcError::with_message_and_status(
                "Option not found or not a string",
                AdbcStatus::NotFound,
            ))
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> AdbcResult<Vec<u8>> {
        let key_str = match key {
            OptionStatement::Other(ref k) => k.as_str(),
            _ => key.as_ref(),
        };
        if let Some(OptionValue::Bytes(b)) = self.options.get(key_str) {
            Ok(b.clone())
        } else {
            Err(AdbcError::with_message_and_status(
                "Option not found or not bytes",
                AdbcStatus::NotFound,
            ))
        }
    }

    fn get_option_int(&self, key: Self::Option) -> AdbcResult<i64> {
        let key_str = match key {
            OptionStatement::Other(ref k) => k.as_str(),
            _ => key.as_ref(),
        };
        if let Some(OptionValue::Int(i)) = self.options.get(key_str) {
            Ok(*i)
        } else {
            Err(AdbcError::with_message_and_status(
                "Option not found or not an integer",
                AdbcStatus::NotFound,
            ))
        }
    }

    fn get_option_double(&self, key: Self::Option) -> AdbcResult<f64> {
        let key_str = match key {
            OptionStatement::Other(ref k) => k.as_str(),
            _ => key.as_ref(),
        };
        if let Some(OptionValue::Double(d)) = self.options.get(key_str) {
            Ok(*d)
        } else {
            Err(AdbcError::with_message_and_status(
                "Option not found or not a double",
                AdbcStatus::NotFound,
            ))
        }
    }
}

impl adbc_core::Statement for FfiStatement {
    fn bind(&mut self, batch: RecordBatch) -> AdbcResult<()> {
        self.bound_data = Some(batch);
        Ok(())
    }

    fn bind_stream(&mut self, mut reader: Box<dyn RecordBatchReader + Send>) -> AdbcResult<()> {
        // Collect all batches from the stream
        let mut batches = Vec::new();
        for batch_result in reader.by_ref() {
            let batch = batch_result.map_err(|e| {
                AdbcError::with_message_and_status(e.to_string(), AdbcStatus::Internal)
            })?;
            batches.push(batch);
        }

        // Concatenate batches if there are multiple
        if batches.is_empty() {
            self.bound_data = None;
        } else if batches.len() == 1 {
            self.bound_data = Some(batches.remove(0));
        } else {
            // Use arrow's concat_batches
            let schema = batches[0].schema();
            let combined = concat_batches(&schema, &batches).map_err(|e| {
                AdbcError::with_message_and_status(e.to_string(), AdbcStatus::Internal)
            })?;
            self.bound_data = Some(combined);
        }
        Ok(())
    }

    fn execute(&mut self) -> AdbcResult<impl RecordBatchReader + Send> {
        let sql = self.sql.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("SQL query not set", AdbcStatus::InvalidState)
        })?;

        // Create a connection and execute
        let uri = self.uri.clone();
        let sql = sql.clone();

        let result = get_runtime().block_on(async {
            let params: crate::connection::ConnectionParams = uri.parse()?;
            let mut conn = ExaConnection::from_params(params).await?;
            let batches = conn.query(&sql).await?;
            conn.close().await?;
            Ok::<_, ExasolError>(batches)
        });

        let batches = result.map_err(to_adbc_error)?;

        // Determine schema from first batch or create empty schema
        let schema = if batches.is_empty() {
            Arc::new(Schema::empty())
        } else {
            batches[0].schema()
        };

        Ok(VecRecordBatchReader::new(schema, batches))
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        let sql = self.sql.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("SQL query not set", AdbcStatus::InvalidState)
        })?;

        // Create a connection and execute
        let uri = self.uri.clone();
        let sql = sql.clone();

        let result = get_runtime().block_on(async {
            let params: crate::connection::ConnectionParams = uri.parse()?;
            let mut conn = ExaConnection::from_params(params).await?;
            let count = conn.execute_update(&sql).await?;
            conn.close().await?;
            Ok::<_, ExasolError>(count)
        });

        let count = result.map_err(to_adbc_error)?;
        Ok(Some(count))
    }

    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        // We would need to prepare the query to get the schema without executing
        Err(AdbcError::with_message_and_status(
            "execute_schema not yet implemented",
            AdbcStatus::NotImplemented,
        ))
    }

    fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult> {
        Err(AdbcError::with_message_and_status(
            "Partitioned execution not supported",
            AdbcStatus::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        // Would need to prepare the statement to determine parameter schema
        Err(AdbcError::with_message_and_status(
            "get_parameter_schema not yet implemented",
            AdbcStatus::NotImplemented,
        ))
    }

    fn prepare(&mut self) -> AdbcResult<()> {
        // Exasol WebSocket protocol doesn't have explicit prepare
        // We just validate the SQL is set
        if self.sql.is_none() {
            return Err(AdbcError::with_message_and_status(
                "SQL query not set",
                AdbcStatus::InvalidState,
            ));
        }
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> AdbcResult<()> {
        self.sql = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> AdbcResult<()> {
        Err(AdbcError::with_message_and_status(
            "Substrait plans not supported",
            AdbcStatus::NotImplemented,
        ))
    }

    fn cancel(&mut self) -> AdbcResult<()> {
        Err(AdbcError::with_message_and_status(
            "Cancel not implemented",
            AdbcStatus::NotImplemented,
        ))
    }
}

// -----------------------------------------------------------------------------
// FFI Export
// -----------------------------------------------------------------------------

// Export the driver using the adbc_ffi macro.
// The exported function will be named `ExarrowDriverInit`.
adbc_ffi::export_driver!(ExarrowDriverInit, FfiDriver);

#[cfg(test)]
mod tests {
    use super::*;
    use adbc_core::{Driver, Statement};

    #[test]
    fn test_ffi_driver_creation() {
        let mut driver = FfiDriver;
        let db = driver.new_database();
        assert!(db.is_ok());
    }

    #[test]
    fn test_ffi_database_options() {
        let mut db = FfiDatabase::new();

        // Set URI
        db.set_option(
            OptionDatabase::Uri,
            "exasol://user:pass@localhost:8563".into(),
        )
        .unwrap();

        // Get URI
        let uri = db.get_option_string(OptionDatabase::Uri).unwrap();
        assert_eq!(uri, "exasol://user:pass@localhost:8563");

        // Set username override
        db.set_option(OptionDatabase::Username, "admin".into())
            .unwrap();
        let username = db.get_option_string(OptionDatabase::Username).unwrap();
        assert_eq!(username, "admin");
    }

    #[test]
    fn test_ffi_database_build_uri_with_overrides() {
        let mut db = FfiDatabase::new();

        // Set base URI
        db.set_option(
            OptionDatabase::Uri,
            "exasol://user:pass@localhost:8563/schema".into(),
        )
        .unwrap();

        // Override username
        db.set_option(OptionDatabase::Username, "admin".into())
            .unwrap();

        // Override password
        db.set_option(OptionDatabase::Password, "secret".into())
            .unwrap();

        let uri = db.build_connection_uri().unwrap();
        assert!(uri.contains("admin"));
        assert!(uri.contains("secret"));
        assert!(uri.contains("localhost:8563/schema"));
    }

    #[test]
    fn test_ffi_connection_options() {
        let mut conn = FfiConnection::new("exasol://user@localhost:8563".to_string());

        // Set auto-commit
        conn.set_option(OptionConnection::AutoCommit, "false".into())
            .unwrap();
        let auto_commit = conn
            .get_option_string(OptionConnection::AutoCommit)
            .unwrap();
        assert_eq!(auto_commit, "false");
    }

    #[test]
    fn test_ffi_statement_sql() {
        let mut stmt = FfiStatement::new("exasol://user@localhost:8563".to_string());

        stmt.set_sql_query("SELECT 1").unwrap();
        assert_eq!(stmt.sql, Some("SELECT 1".to_string()));
    }
}
