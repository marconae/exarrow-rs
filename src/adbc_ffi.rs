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
//!
//! Examples:
//! - `exasol://sys:exasol@localhost:8563`
//! - `exasol://admin:secret@192.168.1.100:8563/my_schema`
//! - `exasol://sys:exasol@exasol-server.example.com:8563`
//!
//! ## Using with ADBC Driver Manager (Rust)
//!
//!
//! ## Using with ADBC Driver Manager (Python)
//!
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
//! | `get_objects` | Supported |
//! | `get_table_schema` | Supported |
//! | `get_parameter_schema` | Supported |
//! | `bulk_ingestion` | Supported |
//! | Partitioned results | Not supported |
//! | Substrait plans | Not supported |
//!
//! # Running Integration Tests
//!
//! To test the FFI driver with the ADBC driver manager:
//!

use std::collections::HashSet;
use std::sync::{Arc, OnceLock};

use adbc_core::error::{Error as AdbcError, Result as AdbcResult, Status as AdbcStatus};
use adbc_core::options::{
    InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{Optionable, PartitionedResult};
use arrow::array::{Array, RecordBatch, RecordBatchReader};
use arrow::compute::concat_batches;
use arrow::datatypes::Schema;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;

use crate::adbc::Connection as ExaConnection;
use crate::error::{ExasolError, QueryError};
use crate::transport::messages::DataType as TransportDataType;
use crate::transport::protocol::PreparedStatementHandle;
use crate::types::{ExasolType, TypeMapper};

/// Global tokio runtime for async-to-sync bridging.
///
/// Uses 2 worker threads so the I/O reactor remains available
/// even when one worker is parked in `block_on` during import.
fn get_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime for ADBC FFI")
    })
}

/// Convert an ExasolError to an ADBC Error.
fn to_adbc_error(err: impl std::error::Error) -> AdbcError {
    AdbcError::with_message_and_status(err.to_string(), AdbcStatus::Internal)
}

/// Column metadata for GetObjects result: (schema, table) -> Vec<(col_name, ordinal, type_name)>.
type ColumnMetadataMap = std::collections::HashMap<(String, String), Vec<(String, i32, String)>>;

/// Quote an Exasol identifier with double quotes and escape embedded double quotes.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build a fully-qualified, properly-quoted table name.
///
/// If `schema` is provided, it is quoted separately and prepended.
/// If `schema` is None but `table` contains a dot, the first dot is treated
/// as a schema/table separator so that `"SCHEMA.TABLE"` becomes `"SCHEMA"."TABLE"`
/// rather than a single quoted identifier containing a literal dot.
fn build_qualified_table_name(schema: Option<&str>, table: &str) -> String {
    if let Some(schema) = schema {
        format!("{}.{}", quote_identifier(schema), quote_identifier(table))
    } else if let Some((schema_part, table_part)) = table.split_once('.') {
        format!(
            "{}.{}",
            quote_identifier(schema_part),
            quote_identifier(table_part)
        )
    } else {
        quote_identifier(table)
    }
}

/// Generate a CREATE TABLE DDL statement from an Arrow Schema.
///
/// Maps each Arrow field to an Exasol type and respects nullability constraints.
/// The `table_name` should already be a properly quoted/qualified identifier.
pub(crate) fn generate_create_table_ddl(table_name: &str, schema: &Schema) -> AdbcResult<String> {
    let mut columns = Vec::new();

    for field in schema.fields() {
        let exasol_type = TypeMapper::arrow_to_exasol(field.data_type()).map_err(|e| {
            AdbcError::with_message_and_status(
                format!(
                    "Cannot map Arrow type {:?} for column '{}': {}",
                    field.data_type(),
                    field.name(),
                    e
                ),
                AdbcStatus::InvalidArguments,
            )
        })?;

        let ddl_type = exasol_type.to_ddl_type();
        let escaped_name = format!("\"{}\"", field.name().replace('"', "\"\""));

        let col_def = if !field.is_nullable() {
            format!("{} {} NOT NULL", escaped_name, ddl_type)
        } else {
            format!("{} {}", escaped_name, ddl_type)
        };
        columns.push(col_def);
    }

    Ok(format!(
        "CREATE TABLE {} ({})",
        table_name,
        columns.join(", ")
    ))
}

/// Parse an Exasol type name string from system views into an ExasolType.
///
/// Handles formats like "DECIMAL(18,0)", "VARCHAR(100)", "BOOLEAN", "TIMESTAMP",
/// "TIMESTAMP WITH LOCAL TIME ZONE", "DATE", "DOUBLE", etc.
fn parse_exasol_type_string(type_str: &str) -> AdbcResult<ExasolType> {
    let type_upper = type_str.trim().to_uppercase();

    if type_upper == "BOOLEAN" {
        return Ok(ExasolType::Boolean);
    }
    if type_upper == "DATE" {
        return Ok(ExasolType::Date);
    }
    if type_upper == "DOUBLE" || type_upper == "DOUBLE PRECISION" {
        return Ok(ExasolType::Double);
    }
    if type_upper.starts_with("TIMESTAMP") && type_upper.ends_with("WITH LOCAL TIME ZONE") {
        return Ok(ExasolType::Timestamp {
            with_local_time_zone: true,
        });
    }
    if type_upper.starts_with("TIMESTAMP") {
        return Ok(ExasolType::Timestamp {
            with_local_time_zone: false,
        });
    }
    if type_upper.starts_with("INTERVAL YEAR") && type_upper.contains("TO MONTH") {
        return Ok(ExasolType::IntervalYearToMonth);
    }
    if type_upper.starts_with("INTERVAL DAY") && type_upper.contains("TO SECOND") {
        let precision = extract_last_param(&type_upper).unwrap_or(3);
        return Ok(ExasolType::IntervalDayToSecond {
            precision: precision as u8,
        });
    }
    if type_upper.starts_with("GEOMETRY") {
        let srid = extract_single_param(&type_upper).map(|v| v as i32);
        return Ok(ExasolType::Geometry { srid });
    }
    if type_upper.starts_with("HASHTYPE") {
        let byte_size = extract_single_param(&type_upper).unwrap_or(16);
        return Ok(ExasolType::Hashtype {
            byte_size: byte_size as usize,
        });
    }
    if type_upper.starts_with("CHAR") && !type_upper.starts_with("CHAR VARYING") {
        let size = extract_single_param(&type_upper).unwrap_or(1);
        return Ok(ExasolType::Char {
            size: size as usize,
        });
    }
    if type_upper.starts_with("VARCHAR") || type_upper.starts_with("CHAR VARYING") {
        let size = extract_single_param(&type_upper).unwrap_or(2000000);
        return Ok(ExasolType::Varchar {
            size: size as usize,
        });
    }
    if type_upper.starts_with("DECIMAL") || type_upper.starts_with("NUMERIC") {
        let (p, s) = extract_two_params(&type_upper).unwrap_or((18, 0));
        return Ok(ExasolType::Decimal {
            precision: p as u8,
            scale: s as i8,
        });
    }
    if type_upper == "BIGINT"
        || type_upper == "INT"
        || type_upper == "INTEGER"
        || type_upper == "SMALLINT"
        || type_upper == "TINYINT"
    {
        return Ok(ExasolType::Decimal {
            precision: 18,
            scale: 0,
        });
    }
    if type_upper == "FLOAT" || type_upper == "REAL" {
        return Ok(ExasolType::Double);
    }
    if type_upper == "CLOB" || type_upper.starts_with("LONG VARCHAR") {
        return Ok(ExasolType::Varchar { size: 2000000 });
    }

    Err(AdbcError::with_message_and_status(
        format!("Unknown Exasol type: {}", type_str),
        AdbcStatus::Internal,
    ))
}

fn extract_single_param(s: &str) -> Option<i64> {
    let start = s.find('(')?;
    let end = s.find(')')?;
    s[start + 1..end].trim().parse().ok()
}

fn extract_last_param(s: &str) -> Option<i64> {
    let start = s.rfind('(')?;
    let end = s.rfind(')')?;
    if start < end {
        s[start + 1..end].trim().parse().ok()
    } else {
        None
    }
}

fn extract_two_params(s: &str) -> Option<(i64, i64)> {
    let start = s.find('(')?;
    let end = s.find(')')?;
    let inner = &s[start + 1..end];
    let parts: Vec<&str> = inner.split(',').collect();
    if parts.len() == 2 {
        let a = parts[0].trim().parse().ok()?;
        let b = parts[1].trim().parse().ok()?;
        Some((a, b))
    } else if parts.len() == 1 {
        let a = parts[0].trim().parse().ok()?;
        Some((a, 0))
    } else {
        None
    }
}

/// Convert a transport DataType to an ExasolType for Arrow conversion.
fn transport_datatype_to_exasol(dt: &TransportDataType) -> AdbcResult<ExasolType> {
    let type_upper = dt.type_name.to_uppercase();

    match type_upper.as_str() {
        "BOOLEAN" => Ok(ExasolType::Boolean),
        "DATE" => Ok(ExasolType::Date),
        "DOUBLE" | "DOUBLE PRECISION" => Ok(ExasolType::Double),
        "TIMESTAMP" => Ok(ExasolType::Timestamp {
            with_local_time_zone: dt.with_local_time_zone.unwrap_or(false),
        }),
        "TIMESTAMP WITH LOCAL TIME ZONE" => Ok(ExasolType::Timestamp {
            with_local_time_zone: true,
        }),
        "CHAR" => Ok(ExasolType::Char {
            size: dt.size.unwrap_or(1) as usize,
        }),
        "VARCHAR" => Ok(ExasolType::Varchar {
            size: dt.size.unwrap_or(2000000) as usize,
        }),
        "DECIMAL" => {
            let precision = dt.precision.unwrap_or(18) as u8;
            let scale = dt.scale.unwrap_or(0) as i8;
            Ok(ExasolType::Decimal { precision, scale })
        }
        "GEOMETRY" => Ok(ExasolType::Geometry { srid: None }),
        "HASHTYPE" => Ok(ExasolType::Hashtype {
            byte_size: dt.size.unwrap_or(16) as usize,
        }),
        "INTERVAL DAY TO SECOND" => Ok(ExasolType::IntervalDayToSecond {
            precision: dt.fraction.unwrap_or(3) as u8,
        }),
        "INTERVAL YEAR TO MONTH" => Ok(ExasolType::IntervalYearToMonth),
        _ => Err(AdbcError::with_message_and_status(
            format!("Unknown transport type: {}", dt.type_name),
            AdbcStatus::Internal,
        )),
    }
}

/// Build the Arrow schema for the ADBC GetObjects result.
fn build_get_objects_schema() -> Schema {
    use arrow::datatypes::{DataType, Field, Fields};

    let column_fields = Fields::from(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("ordinal_position", DataType::Int32, false),
        Field::new("xdbc_type_name", DataType::Utf8, true),
    ]);

    let constraint_fields = Fields::from(vec![
        Field::new("constraint_name", DataType::Utf8, true),
        Field::new("constraint_type", DataType::Utf8, false),
    ]);

    let table_fields = Fields::from(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
        Field::new(
            "table_columns",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(column_fields),
                true,
            ))),
            true,
        ),
        Field::new(
            "table_constraints",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(constraint_fields),
                true,
            ))),
            true,
        ),
    ]);

    let schema_fields = Fields::from(vec![
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new(
            "db_schema_tables",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(table_fields),
                true,
            ))),
            true,
        ),
    ]);

    Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new(
            "catalog_db_schemas",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(schema_fields),
                true,
            ))),
            true,
        ),
    ])
}

/// Extract an i32 value from an Arrow array at the given index.
/// Handles various numeric array types that Exasol might return.
fn get_int_value(array: &dyn arrow::array::Array, idx: usize) -> Option<i32> {
    use arrow::array::{Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::DataType;

    match array.data_type() {
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(arr.value(idx))
            }
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(arr.value(idx) as i32)
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(arr.value(idx) as i32)
            }
        }
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()?;
            if arr.is_null(idx) {
                None
            } else {
                Some(arr.value(idx) as i32)
            }
        }
        _ => None,
    }
}

/// Extract a nullable boolean from a column value.
/// Exasol returns COLUMN_IS_NULLABLE as a BOOLEAN or string.
fn get_nullable_value(array: &dyn arrow::array::Array, idx: usize) -> bool {
    use arrow::array::{BooleanArray, StringArray};
    use arrow::datatypes::DataType;

    match array.data_type() {
        DataType::Boolean => {
            if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                if arr.is_null(idx) {
                    true
                } else {
                    arr.value(idx)
                }
            } else {
                true
            }
        }
        DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                if arr.is_null(idx) {
                    true
                } else {
                    let val = arr.value(idx).to_uppercase();
                    val == "TRUE" || val == "YES" || val == "1"
                }
            } else {
                true
            }
        }
        _ => true,
    }
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
///
/// # Example
///
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
    /// The actual connection (lazily initialized), shared with statements
    inner: Option<Arc<Mutex<ExaConnection>>>,
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
    fn ensure_connected(&mut self) -> AdbcResult<Arc<Mutex<ExaConnection>>> {
        if self.inner.is_none() {
            let uri = self.uri.clone();
            let conn = get_runtime()
                .block_on(async {
                    let params: crate::connection::ConnectionParams = uri.parse()?;
                    ExaConnection::from_params(params).await
                })
                .map_err(to_adbc_error)?;
            self.inner = Some(Arc::new(Mutex::new(conn)));
        }
        Ok(Arc::clone(self.inner.as_ref().unwrap()))
    }
}

impl Drop for FfiConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.inner.take() {
            // Attempt graceful shutdown: close WebSocket and session
            // to prevent lingering I/O tasks on the static runtime.
            let _ = get_runtime().block_on(async {
                let conn = conn.lock().await;
                conn.shutdown().await
            });
        }
    }
}

impl Optionable for FfiConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> AdbcResult<()> {
        match key {
            OptionConnection::AutoCommit => {
                if let OptionValue::String(s) = value {
                    let new_auto_commit = s == "true" || s == "1";
                    let old_auto_commit = self.auto_commit;
                    self.auto_commit = new_auto_commit;

                    if self.inner.is_some() {
                        if old_auto_commit && !new_auto_commit {
                            let conn_arc = self.ensure_connected()?;
                            get_runtime()
                                .block_on(async {
                                    let mut conn = conn_arc.lock().await;
                                    conn.begin_transaction().await
                                })
                                .map_err(to_adbc_error)?;
                        } else if !old_auto_commit && new_auto_commit {
                            let conn_arc = self.ensure_connected()?;
                            get_runtime()
                                .block_on(async {
                                    let mut conn = conn_arc.lock().await;
                                    if conn.in_transaction() {
                                        conn.commit().await?;
                                    }
                                    Ok::<(), QueryError>(())
                                })
                                .map_err(to_adbc_error)?;
                        }
                    }
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
    type Item = Result<RecordBatch, arrow::error::ArrowError>;

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
        let conn = self.ensure_connected()?;
        let mut stmt = FfiStatement::with_connection(Some(conn), self.uri.clone());
        stmt.auto_commit = self.auto_commit;
        Ok(stmt)
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
        use arrow::array::builder::{StringBuilder, UInt32Builder};
        use arrow::datatypes::{DataType, Field};

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
        depth: ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> AdbcResult<impl RecordBatchReader + Send> {
        use arrow::array::builder::{
            ArrayBuilder, Int32Builder, ListBuilder, StringBuilder, StructBuilder,
        };
        use arrow::datatypes::{DataType, Field, Fields};

        // Check catalog filter - Exasol only has "EXA"
        if let Some(cat) = catalog {
            if !cat.eq_ignore_ascii_case("EXA") {
                // Return empty result with correct schema
                let schema = Arc::new(build_get_objects_schema());
                return Ok(VecRecordBatchReader::empty(schema));
            }
        }

        let conn_arc = self.inner.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status(
                "Connection not established. Call new_statement() or connect first.",
                AdbcStatus::InvalidState,
            )
        })?;
        let conn_arc = Arc::clone(conn_arc);

        // Build schema list if depth >= Schemas
        let include_schemas = matches!(
            depth,
            ObjectDepth::Schemas | ObjectDepth::Tables | ObjectDepth::Columns | ObjectDepth::All
        );
        let include_tables = matches!(
            depth,
            ObjectDepth::Tables | ObjectDepth::Columns | ObjectDepth::All
        );
        let include_columns = matches!(depth, ObjectDepth::Columns | ObjectDepth::All);

        // Query schemas
        let schemas: Vec<String> = if include_schemas {
            let mut sql = "SELECT SCHEMA_NAME FROM SYS.EXA_ALL_SCHEMAS".to_string();
            if let Some(schema_filter) = db_schema {
                sql.push_str(&format!(
                    " WHERE SCHEMA_NAME LIKE '{}'",
                    schema_filter.replace('\'', "''")
                ));
            }
            sql.push_str(" ORDER BY SCHEMA_NAME");

            let batches = {
                let conn_arc = Arc::clone(&conn_arc);
                get_runtime()
                    .block_on(async {
                        let mut conn = conn_arc.lock().await;
                        conn.query(&sql).await
                    })
                    .map_err(to_adbc_error)?
            };

            let mut result = Vec::new();
            for batch in &batches {
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        AdbcError::with_message_and_status(
                            "Expected string column for SCHEMA_NAME",
                            AdbcStatus::Internal,
                        )
                    })?;
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        result.push(col.value(i).to_string());
                    }
                }
            }
            result
        } else {
            vec![]
        };

        // Query tables grouped by schema
        let tables: std::collections::HashMap<String, Vec<(String, String)>> = if include_tables {
            let mut sql =
                "SELECT OBJECT_NAME, OBJECT_TYPE, ROOT_NAME FROM SYS.EXA_ALL_OBJECTS".to_string();
            let mut conditions = vec!["OBJECT_TYPE IN ('TABLE', 'VIEW')".to_string()];
            if let Some(schema_filter) = db_schema {
                conditions.push(format!(
                    "ROOT_NAME LIKE '{}'",
                    schema_filter.replace('\'', "''")
                ));
            }
            if let Some(tbl_name) = table_name {
                conditions.push(format!(
                    "OBJECT_NAME LIKE '{}'",
                    tbl_name.replace('\'', "''")
                ));
            }
            if let Some(ref tbl_types) = table_type {
                let types_str: Vec<String> = tbl_types
                    .iter()
                    .map(|t| format!("'{}'", t.replace('\'', "''")))
                    .collect();
                conditions.push(format!("OBJECT_TYPE IN ({})", types_str.join(",")));
            }
            if !conditions.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&conditions.join(" AND "));
            }
            sql.push_str(" ORDER BY ROOT_NAME, OBJECT_NAME");

            let batches = {
                let conn_arc = Arc::clone(&conn_arc);
                get_runtime()
                    .block_on(async {
                        let mut conn = conn_arc.lock().await;
                        conn.query(&sql).await
                    })
                    .map_err(to_adbc_error)?
            };

            let mut result: std::collections::HashMap<String, Vec<(String, String)>> =
                std::collections::HashMap::new();
            for batch in &batches {
                let name_col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        AdbcError::with_message_and_status(
                            "Expected string column",
                            AdbcStatus::Internal,
                        )
                    })?;
                let type_col = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        AdbcError::with_message_and_status(
                            "Expected string column",
                            AdbcStatus::Internal,
                        )
                    })?;
                let schema_col = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .ok_or_else(|| {
                        AdbcError::with_message_and_status(
                            "Expected string column",
                            AdbcStatus::Internal,
                        )
                    })?;
                for i in 0..batch.num_rows() {
                    let schema_name = schema_col.value(i).to_string();
                    let tbl_name = name_col.value(i).to_string();
                    let tbl_type = type_col.value(i).to_string();
                    result
                        .entry(schema_name)
                        .or_default()
                        .push((tbl_name, tbl_type));
                }
            }
            result
        } else {
            std::collections::HashMap::new()
        };

        // Query columns grouped by (schema, table)
        let columns: ColumnMetadataMap = if include_columns {
            let mut sql = "SELECT COLUMN_NAME, COLUMN_ORDINAL_POSITION, COLUMN_TYPE, COLUMN_SCHEMA, COLUMN_TABLE FROM SYS.EXA_ALL_COLUMNS".to_string();
            let mut conditions = Vec::new();
            if let Some(schema_filter) = db_schema {
                conditions.push(format!(
                    "COLUMN_SCHEMA LIKE '{}'",
                    schema_filter.replace('\'', "''")
                ));
            }
            if let Some(tbl_name) = table_name {
                conditions.push(format!(
                    "COLUMN_TABLE LIKE '{}'",
                    tbl_name.replace('\'', "''")
                ));
            }
            if let Some(col_name) = column_name {
                conditions.push(format!(
                    "COLUMN_NAME LIKE '{}'",
                    col_name.replace('\'', "''")
                ));
            }
            if !conditions.is_empty() {
                sql.push_str(" WHERE ");
                sql.push_str(&conditions.join(" AND "));
            }
            sql.push_str(" ORDER BY COLUMN_SCHEMA, COLUMN_TABLE, COLUMN_ORDINAL_POSITION");

            let batches = {
                let conn_arc = Arc::clone(&conn_arc);
                get_runtime()
                    .block_on(async {
                        let mut conn = conn_arc.lock().await;
                        conn.query(&sql).await
                    })
                    .map_err(to_adbc_error)?
            };

            let mut result: ColumnMetadataMap = ColumnMetadataMap::new();
            for batch in &batches {
                let col_name_arr = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                let col_ordinal = batch.column(1);
                let col_type_arr = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                let col_schema_arr = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                let col_table_arr = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();

                for i in 0..batch.num_rows() {
                    let schema_name = col_schema_arr.value(i).to_string();
                    let tbl_name_val = col_table_arr.value(i).to_string();
                    let name = col_name_arr.value(i).to_string();
                    // COLUMN_ORDINAL_POSITION can be various numeric types
                    let ordinal = get_int_value(col_ordinal.as_ref(), i).unwrap_or(i as i32 + 1);
                    let type_name = col_type_arr.value(i).to_string();
                    result
                        .entry((schema_name, tbl_name_val))
                        .or_default()
                        .push((name, ordinal, type_name));
                }
            }
            result
        } else {
            std::collections::HashMap::new()
        };

        // Build the nested Arrow structure
        let get_objects_schema = build_get_objects_schema();

        // column struct fields
        let column_fields = Fields::from(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, false),
            Field::new("xdbc_type_name", DataType::Utf8, true),
        ]);

        // constraint struct fields (empty list)
        let constraint_fields = Fields::from(vec![
            Field::new("constraint_name", DataType::Utf8, true),
            Field::new("constraint_type", DataType::Utf8, false),
        ]);

        // table struct fields
        let table_fields = Fields::from(vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new(
                "table_columns",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(column_fields.clone()),
                    true,
                ))),
                true,
            ),
            Field::new(
                "table_constraints",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(constraint_fields.clone()),
                    true,
                ))),
                true,
            ),
        ]);

        // schema struct fields
        let schema_struct_fields = Fields::from(vec![
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new(
                "db_schema_tables",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Struct(table_fields.clone()),
                    true,
                ))),
                true,
            ),
        ]);

        // Build arrays using builders
        let mut catalog_name_builder = StringBuilder::new();

        // Schema list builder
        let schema_struct_builder = StructBuilder::from_fields(schema_struct_fields.clone(), 0);
        let mut schema_list_builder = ListBuilder::new(schema_struct_builder);

        catalog_name_builder.append_value("EXA");

        let builder_err = |field: &str| {
            AdbcError::with_message_and_status(
                format!("Failed to access Arrow builder for field '{field}'"),
                AdbcStatus::Internal,
            )
        };

        if include_schemas {
            for schema_name in &schemas {
                // Access the struct builder for this schema entry
                let struct_builder = schema_list_builder.values();
                let schema_name_builder = struct_builder
                    .field_builder::<StringBuilder>(0)
                    .ok_or_else(|| builder_err("db_schema_name"))?;
                schema_name_builder.append_value(schema_name);

                // Tables list for this schema
                let tables_list_builder = struct_builder
                    .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(1)
                    .ok_or_else(|| builder_err("db_schema_tables"))?;

                if include_tables {
                    if let Some(schema_tables) = tables.get(schema_name) {
                        for (tbl_name, tbl_type) in schema_tables {
                            let table_struct = tables_list_builder
                                .values()
                                .as_any_mut()
                                .downcast_mut::<StructBuilder>()
                                .ok_or_else(|| builder_err("db_schema_tables downcast"))?;

                            // table_name
                            table_struct
                                .field_builder::<StringBuilder>(0)
                                .ok_or_else(|| builder_err("table_name"))?
                                .append_value(tbl_name);
                            // table_type
                            table_struct
                                .field_builder::<StringBuilder>(1)
                                .ok_or_else(|| builder_err("table_type"))?
                                .append_value(tbl_type);

                            // table_columns
                            let columns_list = table_struct
                                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
                                .ok_or_else(|| builder_err("table_columns"))?;
                            if include_columns {
                                let key = (schema_name.clone(), tbl_name.clone());
                                if let Some(cols) = columns.get(&key) {
                                    for (col_name, ordinal, type_name) in cols {
                                        let col_struct = columns_list
                                            .values()
                                            .as_any_mut()
                                            .downcast_mut::<StructBuilder>()
                                            .ok_or_else(|| builder_err("table_columns downcast"))?;
                                        col_struct
                                            .field_builder::<StringBuilder>(0)
                                            .ok_or_else(|| builder_err("column_name"))?
                                            .append_value(col_name);
                                        col_struct
                                            .field_builder::<Int32Builder>(1)
                                            .ok_or_else(|| builder_err("ordinal_position"))?
                                            .append_value(*ordinal);
                                        col_struct
                                            .field_builder::<StringBuilder>(2)
                                            .ok_or_else(|| builder_err("xdbc_type_name"))?
                                            .append_value(type_name);
                                        col_struct.append(true);
                                    }
                                }
                                columns_list.append(true);
                            } else {
                                columns_list.append(false); // null
                            }

                            // table_constraints (always null/empty)
                            let constraints_list = table_struct
                                .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(3)
                                .ok_or_else(|| builder_err("table_constraints"))?;
                            constraints_list.append(false); // null

                            table_struct.append(true);
                        }
                    }
                    tables_list_builder.append(true);
                } else {
                    tables_list_builder.append(false); // null
                }

                struct_builder.append(true);
            }
            schema_list_builder.append(true);
        } else {
            schema_list_builder.append(false); // null
        }

        let schema = Arc::new(get_objects_schema);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(catalog_name_builder.finish()),
                Arc::new(schema_list_builder.finish()),
            ],
        )
        .map_err(|e| AdbcError::with_message_and_status(e.to_string(), AdbcStatus::Internal))?;

        Ok(VecRecordBatchReader::new(schema, vec![batch]))
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> AdbcResult<Schema> {
        use arrow::datatypes::Field;

        let conn_arc = self.inner.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status(
                "Connection not established. Call new_statement() or connect first.",
                AdbcStatus::InvalidState,
            )
        })?;
        let conn_arc = Arc::clone(conn_arc);

        let mut sql = format!(
            "SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_MAXSIZE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, COLUMN_IS_NULLABLE FROM SYS.EXA_ALL_COLUMNS WHERE COLUMN_TABLE = '{}'",
            table_name.replace('\'', "''")
        );
        if let Some(schema) = db_schema {
            sql.push_str(&format!(
                " AND COLUMN_SCHEMA = '{}'",
                schema.replace('\'', "''")
            ));
        }
        sql.push_str(" ORDER BY COLUMN_ORDINAL_POSITION");

        let batches = get_runtime()
            .block_on(async {
                let mut conn = conn_arc.lock().await;
                conn.query(&sql).await
            })
            .map_err(to_adbc_error)?;

        let mut fields = Vec::new();
        let mut found_any = false;

        for batch in &batches {
            let col_name_arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    AdbcError::with_message_and_status(
                        "Expected string column for COLUMN_NAME",
                        AdbcStatus::Internal,
                    )
                })?;
            let col_type_arr = batch
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .ok_or_else(|| {
                    AdbcError::with_message_and_status(
                        "Expected string column for COLUMN_TYPE",
                        AdbcStatus::Internal,
                    )
                })?;
            let col_nullable = batch.column(5);

            for i in 0..batch.num_rows() {
                found_any = true;
                let name = col_name_arr.value(i).to_string();
                let type_str = col_type_arr.value(i).to_string();

                // Parse nullable
                let nullable = get_nullable_value(col_nullable.as_ref(), i);

                // Parse the type string to ExasolType, then convert to Arrow
                let exasol_type = parse_exasol_type_string(&type_str)?;
                let arrow_type =
                    TypeMapper::exasol_to_arrow(&exasol_type, nullable).map_err(|e| {
                        AdbcError::with_message_and_status(e.to_string(), AdbcStatus::Internal)
                    })?;

                fields.push(Field::new(name, arrow_type, nullable));
            }
        }

        if !found_any {
            return Err(AdbcError::with_message_and_status(
                format!("Table '{}' not found", table_name),
                AdbcStatus::NotFound,
            ));
        }

        Ok(Schema::new(fields))
    }

    fn get_table_types(&self) -> AdbcResult<impl RecordBatchReader + Send> {
        use arrow::array::builder::StringBuilder;
        use arrow::datatypes::{DataType, Field};

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
        use arrow::datatypes::{DataType, Field};
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
        if self.auto_commit {
            return Err(AdbcError::with_message_and_status(
                "Cannot commit when autocommit is enabled",
                AdbcStatus::InvalidState,
            ));
        }
        let conn_arc = self.ensure_connected()?;
        get_runtime()
            .block_on(async {
                let mut conn = conn_arc.lock().await;
                conn.commit().await
            })
            .map_err(to_adbc_error)
    }

    fn rollback(&mut self) -> AdbcResult<()> {
        if self.auto_commit {
            return Err(AdbcError::with_message_and_status(
                "Cannot rollback when autocommit is enabled",
                AdbcStatus::InvalidState,
            ));
        }
        let conn_arc = self.ensure_connected()?;
        get_runtime()
            .block_on(async {
                let mut conn = conn_arc.lock().await;
                conn.rollback().await
            })
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
pub struct FfiStatement {
    /// Shared connection handle from the parent FfiConnection
    conn: Option<Arc<Mutex<ExaConnection>>>,
    /// Connection URI (fallback for ephemeral connections)
    uri: String,
    /// SQL query
    sql: Option<String>,
    /// Bound parameters as RecordBatch
    bound_data: Option<RecordBatch>,
    /// Statement options
    options: std::collections::HashMap<String, OptionValue>,
    /// Prepared statement handle with parameter metadata
    prepared: Option<PreparedStatementHandle>,
    /// Autocommit state inherited from parent connection
    auto_commit: bool,
}

impl FfiStatement {
    #[cfg(test)]
    fn new(uri: String) -> Self {
        Self {
            conn: None,
            uri,
            sql: None,
            bound_data: None,
            options: std::collections::HashMap::new(),
            prepared: None,
            auto_commit: true,
        }
    }

    fn with_connection(conn: Option<Arc<Mutex<ExaConnection>>>, uri: String) -> Self {
        Self {
            conn,
            uri,
            sql: None,
            bound_data: None,
            options: std::collections::HashMap::new(),
            prepared: None,
            auto_commit: true,
        }
    }

    fn execute_bulk_ingest(&mut self) -> AdbcResult<i64> {
        let target_table = self
            .options
            .get("adbc.ingest.target_table")
            .and_then(|v| match v {
                OptionValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .ok_or_else(|| {
                AdbcError::with_message_and_status(
                    "Target table not set for bulk ingestion",
                    AdbcStatus::InvalidState,
                )
            })?;

        if let Some(OptionValue::String(s)) = self.options.get("adbc.ingest.temporary") {
            if s == "true" {
                return Err(AdbcError::with_message_and_status(
                    "Exasol does not support temporary tables",
                    AdbcStatus::NotImplemented,
                ));
            }
        }

        let target_schema =
            self.options
                .get("adbc.ingest.target_db_schema")
                .and_then(|v| match v {
                    OptionValue::String(s) => Some(s.clone()),
                    _ => None,
                });

        let ingest_mode = self
            .options
            .get("adbc.ingest.mode")
            .and_then(|v| match v {
                OptionValue::String(s) => Some(s.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "adbc.ingest.mode.append".to_string());

        match ingest_mode.as_str() {
            "adbc.ingest.mode.create"
            | "adbc.ingest.mode.append"
            | "adbc.ingest.mode.create_append"
            | "adbc.ingest.mode.replace" => {}
            other => {
                return Err(AdbcError::with_message_and_status(
                    format!("Unknown ingest mode: {}", other),
                    AdbcStatus::InvalidArguments,
                ));
            }
        }

        let qualified_name = build_qualified_table_name(target_schema.as_deref(), &target_table);

        let batch = self.bound_data.take().ok_or_else(|| {
            AdbcError::with_message_and_status(
                "No data bound for bulk ingestion",
                AdbcStatus::InvalidState,
            )
        })?;

        let conn_arc = self.conn.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("No connection available", AdbcStatus::InvalidState)
        })?;
        let conn_arc = Arc::clone(conn_arc);

        let arrow_schema = batch.schema();

        match ingest_mode.as_str() {
            "adbc.ingest.mode.create" => {
                let ddl = generate_create_table_ddl(&qualified_name, &arrow_schema)?;
                let conn_arc2 = Arc::clone(&conn_arc);
                get_runtime()
                    .block_on(async {
                        let mut conn = conn_arc2.lock().await;
                        conn.execute_update(&ddl).await
                    })
                    .map_err(to_adbc_error)?;
            }
            "adbc.ingest.mode.create_append" => {
                let ddl = generate_create_table_ddl(&qualified_name, &arrow_schema)?;
                let ddl = ddl.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");
                let conn_arc2 = Arc::clone(&conn_arc);
                get_runtime()
                    .block_on(async {
                        let mut conn = conn_arc2.lock().await;
                        conn.execute_update(&ddl).await
                    })
                    .map_err(to_adbc_error)?;
            }
            "adbc.ingest.mode.replace" => {
                let drop_ddl = format!("DROP TABLE IF EXISTS {}", qualified_name);
                let create_ddl = generate_create_table_ddl(&qualified_name, &arrow_schema)?;
                let conn_arc2 = Arc::clone(&conn_arc);
                get_runtime()
                    .block_on(async {
                        let mut conn = conn_arc2.lock().await;
                        conn.execute_update(&drop_ddl).await?;
                        conn.execute_update(&create_ddl).await
                    })
                    .map_err(to_adbc_error)?;
            }
            _ => {
                // Append mode (default): no DDL needed, table must already exist
            }
        }

        let import_options = crate::import::arrow::ArrowImportOptions::default();
        let row_count = get_runtime()
            .block_on(async {
                let mut conn = conn_arc.lock().await;
                conn.import_from_record_batch(&qualified_name, &batch, import_options)
                    .await
            })
            .map_err(to_adbc_error)?;

        Ok(row_count as i64)
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
        if self.options.contains_key("adbc.ingest.target_table") {
            self.execute_bulk_ingest()?;
            return Ok(VecRecordBatchReader::new(Arc::new(Schema::empty()), vec![]));
        }

        let sql = self.sql.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("SQL query not set", AdbcStatus::InvalidState)
        })?;

        let sql = sql.clone();

        let batches = if let Some(ref conn_arc) = self.conn {
            // Reuse the shared connection
            let conn_arc = Arc::clone(conn_arc);
            match get_runtime().block_on(async {
                let mut conn = conn_arc.lock().await;
                conn.query(&sql).await
            }) {
                Ok(batches) => batches,
                Err(QueryError::NoResultSet(_)) => vec![],
                Err(e) => return Err(to_adbc_error(e)),
            }
        } else {
            // Fallback: ephemeral connection (no parent connection available)
            let uri = self.uri.clone();
            get_runtime()
                .block_on(async {
                    let params: crate::connection::ConnectionParams = uri.parse()?;
                    let mut conn = ExaConnection::from_params(params).await?;
                    let result = conn.query(&sql).await;
                    conn.close().await?;
                    match result {
                        Ok(batches) => Ok(batches),
                        Err(QueryError::NoResultSet(_)) => Ok(vec![]),
                        Err(e) => Err(ExasolError::from(e)),
                    }
                })
                .map_err(to_adbc_error)?
        };

        // Determine schema from first batch or create empty schema
        let schema = if batches.is_empty() {
            Arc::new(Schema::empty())
        } else {
            batches[0].schema()
        };

        Ok(VecRecordBatchReader::new(schema, batches))
    }

    fn execute_update(&mut self) -> AdbcResult<Option<i64>> {
        if self.options.contains_key("adbc.ingest.target_table") {
            let count = self.execute_bulk_ingest()?;
            return Ok(Some(count));
        }

        let sql = self.sql.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("SQL query not set", AdbcStatus::InvalidState)
        })?;

        let sql = sql.clone();

        let count = if let Some(ref conn_arc) = self.conn {
            // Reuse the shared connection
            let conn_arc = Arc::clone(conn_arc);
            get_runtime()
                .block_on(async {
                    let mut conn = conn_arc.lock().await;
                    conn.execute_update(&sql).await
                })
                .map_err(to_adbc_error)?
        } else {
            // Fallback: ephemeral connection (no parent connection available)
            let uri = self.uri.clone();
            get_runtime()
                .block_on(async {
                    let params: crate::connection::ConnectionParams = uri.parse()?;
                    let mut conn = ExaConnection::from_params(params).await?;
                    let count = conn.execute_update(&sql).await?;
                    conn.close().await?;
                    Ok::<_, ExasolError>(count)
                })
                .map_err(to_adbc_error)?
        };

        Ok(Some(count))
    }

    fn execute_schema(&mut self) -> AdbcResult<Schema> {
        // Prepare if not already done
        if self.prepared.is_none() {
            self.prepare()?;
        }

        let prepared = self.prepared.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("Statement not prepared", AdbcStatus::InvalidState)
        })?;

        // Use the result columns from the prepared statement response
        // For now, we need to actually execute a describe-style query
        // The prepared statement handle doesn't directly give us result columns,
        // so we re-query the connection to get the result set metadata
        let sql = self.sql.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("SQL query not set", AdbcStatus::InvalidState)
        })?;

        let conn_arc = self.conn.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("No connection available", AdbcStatus::InvalidState)
        })?;
        let conn_arc = Arc::clone(conn_arc);
        let sql = sql.clone();
        let _handle = prepared.handle;

        // Execute to get schema - we need to query and just extract the schema
        let batches = get_runtime()
            .block_on(async {
                let mut conn = conn_arc.lock().await;
                conn.query(&sql).await
            })
            .map_err(to_adbc_error)?;

        if batches.is_empty() {
            Ok(Schema::empty())
        } else {
            Ok(batches[0].schema().as_ref().clone())
        }
    }

    fn execute_partitions(&mut self) -> AdbcResult<PartitionedResult> {
        Err(AdbcError::with_message_and_status(
            "Partitioned execution not supported",
            AdbcStatus::NotImplemented,
        ))
    }

    fn get_parameter_schema(&self) -> AdbcResult<Schema> {
        use arrow::datatypes::Field;

        let prepared = self.prepared.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status(
                "Statement not prepared. Call prepare() first.",
                AdbcStatus::InvalidState,
            )
        })?;

        let mut fields = Vec::new();
        for (i, param_type) in prepared.parameter_types.iter().enumerate() {
            let exasol_type = transport_datatype_to_exasol(param_type)?;
            let arrow_type = TypeMapper::exasol_to_arrow(&exasol_type, true).map_err(|e| {
                AdbcError::with_message_and_status(e.to_string(), AdbcStatus::Internal)
            })?;
            fields.push(Field::new(format!("param_{}", i), arrow_type, true));
        }

        Ok(Schema::new(fields))
    }

    fn prepare(&mut self) -> AdbcResult<()> {
        let sql = self.sql.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("SQL query not set", AdbcStatus::InvalidState)
        })?;
        let sql = sql.clone();

        let conn_arc = self.conn.as_ref().ok_or_else(|| {
            AdbcError::with_message_and_status("No connection available", AdbcStatus::InvalidState)
        })?;
        let conn_arc = Arc::clone(conn_arc);

        let prepared_stmt = get_runtime()
            .block_on(async {
                let mut conn = conn_arc.lock().await;
                conn.prepare(&sql).await
            })
            .map_err(to_adbc_error)?;

        // Store the handle metadata (parameter types, num_params)
        self.prepared = Some(PreparedStatementHandle::new(
            prepared_stmt.handle(),
            prepared_stmt.parameter_count() as i32,
            prepared_stmt.parameter_types().to_vec(),
        ));

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
    use arrow::datatypes::{DataType, Field};

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

    #[test]
    fn test_generate_create_table_ddl_basic() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]);

        let ddl = generate_create_table_ddl("test_table", &schema).unwrap();
        assert!(ddl.starts_with("CREATE TABLE test_table ("));
        assert!(ddl.contains("\"id\" DECIMAL(18,0) NOT NULL"));
        assert!(ddl.contains("\"name\" VARCHAR(2000000)"));
        assert!(ddl.contains("\"score\" DOUBLE"));
    }

    #[test]
    fn test_generate_create_table_ddl_all_not_null() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Date32, false),
        ]);

        let ddl = generate_create_table_ddl("my_table", &schema).unwrap();
        assert!(ddl.contains("\"a\" BOOLEAN NOT NULL"));
        assert!(ddl.contains("\"b\" DATE NOT NULL"));
    }

    #[test]
    fn test_generate_create_table_ddl_empty_schema() {
        let schema = Schema::new(Vec::<Field>::new());
        let ddl = generate_create_table_ddl("empty_table", &schema).unwrap();
        assert_eq!(ddl, "CREATE TABLE empty_table ()");
    }

    #[test]
    fn test_generate_create_table_ddl_column_name_escaping() {
        let schema = Schema::new(vec![Field::new(
            "col with \"quotes\"",
            DataType::Utf8,
            true,
        )]);
        let ddl = generate_create_table_ddl("t", &schema).unwrap();
        assert!(ddl.contains("\"col with \"\"quotes\"\"\""));
    }

    #[test]
    fn test_parse_exasol_type_string_varchar() {
        let t = parse_exasol_type_string("VARCHAR(200)").unwrap();
        assert_eq!(t, ExasolType::Varchar { size: 200 });
    }

    #[test]
    fn test_parse_exasol_type_string_decimal() {
        let t = parse_exasol_type_string("DECIMAL(10,3)").unwrap();
        assert_eq!(
            t,
            ExasolType::Decimal {
                precision: 10,
                scale: 3
            }
        );
    }

    #[test]
    fn test_parse_exasol_type_string_boolean() {
        let t = parse_exasol_type_string("BOOLEAN").unwrap();
        assert_eq!(t, ExasolType::Boolean);
    }

    #[test]
    fn test_parse_exasol_type_string_timestamp_with_tz() {
        let t = parse_exasol_type_string("TIMESTAMP WITH LOCAL TIME ZONE").unwrap();
        assert_eq!(
            t,
            ExasolType::Timestamp {
                with_local_time_zone: true
            }
        );
    }

    #[test]
    fn test_parse_exasol_type_timestamp_with_precision() {
        for input in &["TIMESTAMP(3)", "TIMESTAMP(6)", "TIMESTAMP(9)", "TIMESTAMP"] {
            let t = parse_exasol_type_string(input).unwrap();
            assert_eq!(
                t,
                ExasolType::Timestamp {
                    with_local_time_zone: false
                },
                "Failed for input: {}",
                input
            );
        }
    }

    #[test]
    fn test_parse_exasol_type_timestamp_with_local_time_zone_precision() {
        for input in &[
            "TIMESTAMP WITH LOCAL TIME ZONE",
            "TIMESTAMP(3) WITH LOCAL TIME ZONE",
            "TIMESTAMP(6) WITH LOCAL TIME ZONE",
        ] {
            let t = parse_exasol_type_string(input).unwrap();
            assert_eq!(
                t,
                ExasolType::Timestamp {
                    with_local_time_zone: true
                },
                "Failed for input: {}",
                input
            );
        }
    }

    #[test]
    fn test_parse_exasol_type_string_unknown() {
        let result = parse_exasol_type_string("UNKNOWN_TYPE");
        assert!(result.is_err());
    }

    #[test]
    fn test_transport_datatype_to_exasol_decimal() {
        let dt = TransportDataType {
            type_name: "DECIMAL".to_string(),
            precision: Some(18),
            scale: Some(0),
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        };
        let exasol = transport_datatype_to_exasol(&dt).unwrap();
        assert_eq!(
            exasol,
            ExasolType::Decimal {
                precision: 18,
                scale: 0
            }
        );
    }

    #[test]
    fn test_transport_datatype_to_exasol_varchar() {
        let dt = TransportDataType {
            type_name: "VARCHAR".to_string(),
            precision: None,
            scale: None,
            size: Some(100),
            character_set: Some("UTF8".to_string()),
            with_local_time_zone: None,
            fraction: None,
        };
        let exasol = transport_datatype_to_exasol(&dt).unwrap();
        assert_eq!(exasol, ExasolType::Varchar { size: 100 });
    }

    #[test]
    fn test_get_parameter_schema_not_prepared() {
        let stmt = FfiStatement::new("exasol://user@localhost:8563".to_string());
        let result = stmt.get_parameter_schema();
        assert!(result.is_err());
    }

    #[test]
    fn test_get_objects_invalid_catalog() {
        let conn = FfiConnection {
            uri: "exasol://user@localhost:8563".to_string(),
            inner: None,
            options: std::collections::HashMap::new(),
            auto_commit: true,
            current_schema: None,
        };

        // With a catalog filter that doesn't match "EXA", should return empty
        let result = adbc_core::Connection::get_objects(
            &conn,
            ObjectDepth::Catalogs,
            Some("OTHER"),
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_get_objects_no_connection() {
        let conn = FfiConnection {
            uri: "exasol://user@localhost:8563".to_string(),
            inner: None,
            options: std::collections::HashMap::new(),
            auto_commit: true,
            current_schema: None,
        };

        // Catalogs-only depth with matching catalog should fail (no connection)
        let result = adbc_core::Connection::get_objects(
            &conn,
            ObjectDepth::Schemas,
            Some("EXA"),
            None,
            None,
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_get_table_schema_no_connection() {
        let conn = FfiConnection {
            uri: "exasol://user@localhost:8563".to_string(),
            inner: None,
            options: std::collections::HashMap::new(),
            auto_commit: true,
            current_schema: None,
        };

        let result = adbc_core::Connection::get_table_schema(&conn, None, None, "SOME_TABLE");
        assert!(result.is_err());
    }

    #[test]
    fn test_build_get_objects_schema() {
        let schema = build_get_objects_schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "catalog_name");
        assert_eq!(schema.field(1).name(), "catalog_db_schemas");
    }

    #[test]
    fn test_bulk_ingest_temporary_not_supported() {
        let mut stmt = FfiStatement::new("exasol://user@localhost:8563".to_string());
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String("test_table".to_string()),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.temporary".to_string()),
            OptionValue::String("true".to_string()),
        )
        .unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();
        stmt.bind(batch).unwrap();

        let result = stmt.execute_update();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.status, AdbcStatus::NotImplemented);
        assert!(err.message.contains("temporary tables"));
    }

    #[test]
    fn test_bulk_ingest_no_bound_data() {
        let mut stmt = FfiStatement::new("exasol://user@localhost:8563".to_string());
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String("test_table".to_string()),
        )
        .unwrap();

        let result = stmt.execute_update();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("No data bound"));
    }

    #[test]
    fn test_bulk_ingest_no_connection() {
        let mut stmt = FfiStatement::new("exasol://user@localhost:8563".to_string());
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String("test_table".to_string()),
        )
        .unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();
        stmt.bind(batch).unwrap();

        let result = stmt.execute_update();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("No connection available"));
    }

    #[test]
    fn test_bulk_ingest_unknown_mode() {
        let mut stmt = FfiStatement::new("exasol://user@localhost:8563".to_string());
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String("test_table".to_string()),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.mode".to_string()),
            OptionValue::String("adbc.ingest.mode.unknown".to_string()),
        )
        .unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
        )
        .unwrap();
        stmt.bind(batch).unwrap();

        let result = stmt.execute_update();
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("Unknown ingest mode"));
    }

    #[test]
    fn test_bulk_ingest_detection_in_execute() {
        let mut stmt = FfiStatement::new("exasol://user@localhost:8563".to_string());
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String("test_table".to_string()),
        )
        .unwrap();

        // Without bound data, should return error (not "SQL query not set")
        let result = stmt.execute();
        match result {
            Ok(_) => panic!("Expected error for missing bound data"),
            Err(err) => assert!(
                err.message.contains("No data bound"),
                "Expected 'No data bound' error, got: {}",
                err.message
            ),
        }
    }

    #[test]
    fn test_autocommit_default_true() {
        let conn = FfiConnection::new("exasol://user@localhost:8563".to_string());
        assert!(conn.auto_commit);
    }

    #[test]
    fn test_autocommit_set_false() {
        let mut conn = FfiConnection::new("exasol://user@localhost:8563".to_string());
        conn.set_option(OptionConnection::AutoCommit, "false".into())
            .unwrap();
        assert!(!conn.auto_commit);
    }

    #[test]
    fn test_statement_inherits_autocommit() {
        let mut stmt = FfiStatement::new("exasol://user@localhost:8563".to_string());
        assert!(stmt.auto_commit);
        stmt.auto_commit = false;
        assert!(!stmt.auto_commit);
    }

    #[test]
    fn test_parse_interval_year_to_month_with_precision() {
        let t = parse_exasol_type_string("INTERVAL YEAR(2) TO MONTH").unwrap();
        assert_eq!(t, ExasolType::IntervalYearToMonth);

        let t = parse_exasol_type_string("INTERVAL YEAR(5) TO MONTH").unwrap();
        assert_eq!(t, ExasolType::IntervalYearToMonth);
    }

    #[test]
    fn test_parse_interval_year_to_month_bare() {
        let t = parse_exasol_type_string("INTERVAL YEAR TO MONTH").unwrap();
        assert_eq!(t, ExasolType::IntervalYearToMonth);
    }

    #[test]
    fn test_parse_interval_day_to_second_with_precision() {
        let t = parse_exasol_type_string("INTERVAL DAY(2) TO SECOND(3)").unwrap();
        assert_eq!(t, ExasolType::IntervalDayToSecond { precision: 3 });

        let t = parse_exasol_type_string("INTERVAL DAY(9) TO SECOND(9)").unwrap();
        assert_eq!(t, ExasolType::IntervalDayToSecond { precision: 9 });
    }

    #[test]
    fn test_parse_interval_day_to_second_bare() {
        let t = parse_exasol_type_string("INTERVAL DAY TO SECOND").unwrap();
        assert_eq!(t, ExasolType::IntervalDayToSecond { precision: 3 });

        let t = parse_exasol_type_string("INTERVAL DAY TO SECOND(6)").unwrap();
        assert_eq!(t, ExasolType::IntervalDayToSecond { precision: 6 });
    }
}
