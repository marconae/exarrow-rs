//! Parquet export functionality for exarrow-rs.
//!
//! This module provides functionality to export data from Exasol to Parquet format.
//! Exasol exports data as CSV via HTTP transport; this module converts that CSV
//! to Arrow RecordBatches and then writes Parquet files.
//!
//! # Architecture
//!
//! 1. Execute EXPORT SQL to receive CSV via HTTP transport
//! 2. Parse CSV and convert to Arrow RecordBatches
//! 3. Write RecordBatches to Parquet format
//!
//! # Example
//!
//! ```ignore
//! use exarrow_rs::export::parquet::{export_to_parquet, ParquetExportOptions, ExportSource};
//! use std::path::Path;
//!
//! // Export a table to Parquet
//! let rows = export_to_parquet(
//!     &mut session,
//!     ExportSource::Table { schema: None, name: "users".into(), columns: vec![] },
//!     Path::new("/tmp/users.parquet"),
//!     ParquetExportOptions::default(),
//! ).await?;
//! ```

use std::io::{Cursor, Write};
use std::path::Path;
use std::sync::Arc;

use tokio::io::{AsyncWrite, AsyncWriteExt};

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression as ParquetCompressionCodec, Encoding};
use parquet::file::properties::WriterProperties;
use thiserror::Error;

use crate::types::{
    conversion::{
        exasol_type_to_arrow as exasol_type_to_arrow_impl,
        parse_date_to_days as parse_date_to_days_impl,
        parse_decimal_to_i128 as parse_decimal_to_i128_impl,
        parse_timestamp_to_micros as parse_timestamp_to_micros_impl,
    },
    ExasolType,
};

/// Errors that can occur during Parquet export operations.
#[derive(Error, Debug)]
pub enum ParquetExportError {
    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Parquet error
    #[error("Parquet error: {0}")]
    Parquet(String),

    /// Arrow error
    #[error("Arrow error: {0}")]
    Arrow(String),

    /// CSV parsing error
    #[error("CSV parsing error at row {row}: {message}")]
    CsvParse { row: usize, message: String },

    /// Schema error
    #[error("Schema error: {0}")]
    Schema(String),
}

impl From<arrow::error::ArrowError> for ParquetExportError {
    fn from(err: arrow::error::ArrowError) -> Self {
        ParquetExportError::Arrow(err.to_string())
    }
}

impl From<parquet::errors::ParquetError> for ParquetExportError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        ParquetExportError::Parquet(err.to_string())
    }
}

/// Compression options for Parquet export.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ParquetCompression {
    /// No compression
    None,
    /// Snappy compression (fast, moderate ratio)
    #[default]
    Snappy,
    /// Gzip compression (slower, better ratio)
    Gzip,
    /// LZ4 compression (very fast, lower ratio)
    Lz4,
    /// Zstd compression (good balance of speed and ratio)
    Zstd,
}

impl ParquetCompression {
    /// Convert to parquet compression codec.
    fn to_codec(self) -> ParquetCompressionCodec {
        match self {
            ParquetCompression::None => ParquetCompressionCodec::UNCOMPRESSED,
            ParquetCompression::Snappy => ParquetCompressionCodec::SNAPPY,
            ParquetCompression::Gzip => ParquetCompressionCodec::GZIP(Default::default()),
            ParquetCompression::Lz4 => ParquetCompressionCodec::LZ4,
            ParquetCompression::Zstd => ParquetCompressionCodec::ZSTD(Default::default()),
        }
    }
}

/// Options for Parquet export.
#[derive(Debug, Clone)]
pub struct ParquetExportOptions {
    /// Number of rows per batch (default: 1024)
    pub batch_size: usize,
    /// Compression type (default: Snappy)
    pub compression: ParquetCompression,
    /// Whether CSV has column names header row (default: true)
    pub with_column_names: bool,
    /// Column separator for CSV parsing (default: ',')
    pub column_separator: char,
    /// Column delimiter for CSV parsing (default: '"')
    pub column_delimiter: char,
    /// NULL value representation (default: empty string)
    pub null_value: Option<String>,
    /// Exasol host for HTTP transport connection.
    /// This is typically the same host as the WebSocket connection.
    pub host: String,
    /// Exasol port for HTTP transport connection.
    /// This is typically the same port as the WebSocket connection.
    pub port: u16,
    /// Whether to use TLS for the HTTP transport tunnel.
    /// Default is `false` because the main WebSocket connection typically
    /// already handles TLS encryption.
    pub use_tls: bool,
}

impl Default for ParquetExportOptions {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            compression: ParquetCompression::default(),
            with_column_names: true,
            column_separator: ',',
            column_delimiter: '"',
            null_value: None,
            host: String::new(),
            port: 0,
            use_tls: false,
        }
    }
}

impl ParquetExportOptions {
    /// Create a new `ParquetExportOptions` with default values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[must_use]
    pub fn with_compression(mut self, compression: ParquetCompression) -> Self {
        self.compression = compression;
        self
    }

    #[must_use]
    pub fn with_column_names(mut self, with_column_names: bool) -> Self {
        self.with_column_names = with_column_names;
        self
    }

    #[must_use]
    pub fn with_column_separator(mut self, separator: char) -> Self {
        self.column_separator = separator;
        self
    }

    #[must_use]
    pub fn with_column_delimiter(mut self, delimiter: char) -> Self {
        self.column_delimiter = delimiter;
        self
    }

    #[must_use]
    pub fn with_null_value(mut self, null_value: impl Into<String>) -> Self {
        self.null_value = Some(null_value.into());
        self
    }

    /// Set the Exasol host for HTTP transport connection.
    ///
    /// This should be the same host as used for the WebSocket connection.
    #[must_use]
    pub fn exasol_host(mut self, host: impl Into<String>) -> Self {
        self.host = host.into();
        self
    }

    /// Set the Exasol port for HTTP transport connection.
    ///
    /// This should be the same port as used for the WebSocket connection.
    #[must_use]
    pub fn exasol_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Sets whether to use TLS for the HTTP transport tunnel.
    #[must_use]
    pub fn use_tls(mut self, v: bool) -> Self {
        self.use_tls = v;
        self
    }
}

/// Export data from Exasol to a Parquet file.
///
/// This function exports data from Exasol (table or query) directly to a Parquet file.
/// The data flows through the following stages:
/// 1. Query Exasol for column metadata
/// 2. Build Arrow schema from metadata
/// 3. Parse CSV data into RecordBatches
/// 4. Write RecordBatches to Parquet file
///
/// # Arguments
///
/// * `csv_data` - CSV data as bytes (simulating what would come from HTTP transport)
/// * `schema` - Arrow schema for the data
/// * `file_path` - Path to write the Parquet file
/// * `options` - Export options
///
/// # Returns
///
/// The number of rows exported.
///
/// # Errors
///
/// Returns `ParquetExportError` if:
/// - CSV parsing fails
/// - Arrow conversion fails
/// - Parquet writing fails
/// - IO operations fail
pub async fn export_to_parquet(
    csv_data: &[u8],
    schema: Arc<Schema>,
    file_path: &Path,
    options: ParquetExportOptions,
) -> Result<u64, ParquetExportError> {
    let file = std::fs::File::create(file_path)?;
    export_to_parquet_stream(csv_data, schema, file, options).await
}

/// Export data from Exasol to a Parquet stream.
///
/// This function exports data from Exasol (table or query) to any writer implementing `Write`.
/// Useful for writing to network streams, in-memory buffers, or custom destinations.
///
/// # Arguments
///
/// * `csv_data` - CSV data as bytes (simulating what would come from HTTP transport)
/// * `schema` - Arrow schema for the data
/// * `writer` - Any type implementing `Write`
/// * `options` - Export options
///
/// # Returns
///
/// The number of rows exported.
///
/// # Errors
///
/// Returns `ParquetExportError` if:
/// - CSV parsing fails
/// - Arrow conversion fails
/// - Parquet writing fails
pub async fn export_to_parquet_stream<W: Write + Send>(
    csv_data: &[u8],
    schema: Arc<Schema>,
    writer: W,
    options: ParquetExportOptions,
) -> Result<u64, ParquetExportError> {
    // Parse CSV data into record batches
    let batches = csv_to_record_batches(csv_data, &schema, &options)?;

    // Calculate total rows
    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    // Create writer properties with compression
    let props = WriterProperties::builder()
        .set_compression(options.compression.to_codec())
        .set_encoding(Encoding::PLAIN)
        .build();

    // Create Parquet writer
    let mut parquet_writer = ArrowWriter::try_new(writer, schema.clone(), Some(props))?;

    // Write all batches
    for batch in batches {
        parquet_writer.write(&batch)?;
    }

    // Close the writer to flush remaining data
    parquet_writer.close()?;

    Ok(total_rows)
}

/// Export data from Exasol to an async Parquet stream.
///
/// This function exports data from Exasol (table or query) to any writer implementing `AsyncWrite`.
/// Useful for writing to async network streams, tokio files, or custom async destinations.
///
/// Since Parquet writers are synchronous, this function writes to an in-memory buffer first,
/// then asynchronously writes the buffer to the output.
///
/// # Arguments
///
/// * `csv_data` - CSV data as bytes (simulating what would come from HTTP transport)
/// * `schema` - Arrow schema for the data
/// * `writer` - Any type implementing `AsyncWrite + Unpin`
/// * `options` - Export options
///
/// # Returns
///
/// The number of rows exported.
///
/// # Errors
///
/// Returns `ParquetExportError` if:
/// - CSV parsing fails
/// - Arrow conversion fails
/// - Parquet writing fails
/// - Async IO fails
pub async fn export_to_parquet_async_stream<W: AsyncWrite + Unpin + Send>(
    csv_data: &[u8],
    schema: Arc<Schema>,
    writer: &mut W,
    options: ParquetExportOptions,
) -> Result<u64, ParquetExportError> {
    // Parse CSV data into record batches
    let batches = csv_to_record_batches(csv_data, &schema, &options)?;

    // Calculate total rows
    let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    // Create writer properties with compression
    let props = WriterProperties::builder()
        .set_compression(options.compression.to_codec())
        .set_encoding(Encoding::PLAIN)
        .build();

    // Write to in-memory buffer first (Parquet writer is synchronous)
    let mut buffer = Cursor::new(Vec::new());
    {
        let mut parquet_writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))?;

        // Write all batches
        for batch in batches {
            parquet_writer.write(&batch)?;
        }

        // Close the writer to flush remaining data
        parquet_writer.close()?;
    }

    // Asynchronously write the buffer to the output
    let data = buffer.into_inner();
    writer.write_all(&data).await?;
    writer.flush().await?;

    Ok(total_rows)
}

/// Convert CSV data to Arrow RecordBatches.
///
/// # Arguments
///
/// * `csv_data` - CSV data as bytes
/// * `schema` - Arrow schema for the data
/// * `options` - Export options containing CSV parsing settings
///
/// # Returns
///
/// A vector of RecordBatches.
pub fn csv_to_record_batches(
    csv_data: &[u8],
    schema: &Schema,
    options: &ParquetExportOptions,
) -> Result<Vec<RecordBatch>, ParquetExportError> {
    let csv_str = std::str::from_utf8(csv_data).map_err(|e| ParquetExportError::CsvParse {
        row: 0,
        message: format!("Invalid UTF-8: {}", e),
    })?;

    let lines: Vec<&str> = csv_str.lines().collect();

    if lines.is_empty() {
        return Ok(vec![RecordBatch::new_empty(Arc::new(schema.clone()))]);
    }

    // Skip header row if present
    let data_start = if options.with_column_names { 1 } else { 0 };
    let data_lines = &lines[data_start..];

    if data_lines.is_empty() {
        return Ok(vec![RecordBatch::new_empty(Arc::new(schema.clone()))]);
    }

    // Process data in batches
    let mut batches = Vec::new();
    for chunk in data_lines.chunks(options.batch_size) {
        let batch = csv_chunk_to_record_batch(chunk, schema, options, data_start)?;
        batches.push(batch);
    }

    Ok(batches)
}

/// Convert a chunk of CSV lines to a RecordBatch.
fn csv_chunk_to_record_batch(
    lines: &[&str],
    schema: &Schema,
    options: &ParquetExportOptions,
    row_offset: usize,
) -> Result<RecordBatch, ParquetExportError> {
    let num_columns = schema.fields().len();

    // Parse all rows first
    let parsed_rows: Result<Vec<Vec<Option<String>>>, ParquetExportError> = lines
        .iter()
        .enumerate()
        .map(|(idx, line)| parse_csv_line(line, options, row_offset + idx, num_columns))
        .collect();
    let parsed_rows = parsed_rows?;

    // Build arrays for each column
    let arrays: Result<Vec<ArrayRef>, ParquetExportError> = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(col_idx, field)| {
            let column_values: Vec<Option<&str>> = parsed_rows
                .iter()
                .map(|row| row.get(col_idx).and_then(|v| v.as_deref()))
                .collect();

            build_array_from_csv_column(&column_values, field, col_idx, options)
        })
        .collect();

    let arrays = arrays?;

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| ParquetExportError::Arrow(e.to_string()))
}

/// Parse a CSV line into column values.
fn parse_csv_line(
    line: &str,
    options: &ParquetExportOptions,
    row_idx: usize,
    expected_columns: usize,
) -> Result<Vec<Option<String>>, ParquetExportError> {
    let mut values = Vec::with_capacity(expected_columns);
    let mut current_value = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if c == options.column_delimiter {
            if in_quotes {
                // Check for escaped quote (doubled delimiter)
                if chars.peek() == Some(&options.column_delimiter) {
                    current_value.push(c);
                    chars.next(); // Skip the second quote
                } else {
                    in_quotes = false;
                }
            } else {
                in_quotes = true;
            }
        } else if c == options.column_separator && !in_quotes {
            values.push(parse_csv_value(&current_value, options));
            current_value.clear();
        } else {
            current_value.push(c);
        }
    }

    // Push the last value
    values.push(parse_csv_value(&current_value, options));

    // Validate column count
    if values.len() != expected_columns {
        return Err(ParquetExportError::CsvParse {
            row: row_idx,
            message: format!(
                "Expected {} columns, found {}",
                expected_columns,
                values.len()
            ),
        });
    }

    Ok(values)
}

/// Parse a single CSV value, handling NULL values.
fn parse_csv_value(value: &str, options: &ParquetExportOptions) -> Option<String> {
    let trimmed = value.trim();

    // Check for NULL value
    if let Some(ref null_val) = options.null_value {
        if trimmed == null_val {
            return None;
        }
    }

    // Empty string is treated as NULL if no explicit null value is set
    if trimmed.is_empty() && options.null_value.is_none() {
        return None;
    }

    Some(trimmed.to_string())
}

/// Build an Arrow array from CSV column values.
fn build_array_from_csv_column(
    values: &[Option<&str>],
    field: &Field,
    col_idx: usize,
    _options: &ParquetExportOptions,
) -> Result<ArrayRef, ParquetExportError> {
    match field.data_type() {
        DataType::Boolean => build_boolean_array_from_csv(values, col_idx),
        DataType::Utf8 => build_string_array_from_csv(values, col_idx),
        DataType::Float64 => build_float64_array_from_csv(values, col_idx),
        DataType::Decimal128(precision, scale) => {
            build_decimal128_array_from_csv(values, *precision, *scale, col_idx)
        }
        DataType::Date32 => build_date32_array_from_csv(values, col_idx),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            build_timestamp_array_from_csv(values, tz.clone(), col_idx)
        }
        DataType::Int64 => build_int64_array_from_csv(values, col_idx),
        _ => Err(ParquetExportError::Schema(format!(
            "Unsupported data type for Parquet export: {:?}",
            field.data_type()
        ))),
    }
}

/// Build a Boolean array from CSV string values.
fn build_boolean_array_from_csv(
    values: &[Option<&str>],
    col_idx: usize,
) -> Result<ArrayRef, ParquetExportError> {
    let mut builder = BooleanBuilder::with_capacity(values.len());

    for (row_idx, value) in values.iter().enumerate() {
        match value {
            None => builder.append_null(),
            Some(s) => {
                let lower = s.to_lowercase();
                let bool_val = match lower.as_str() {
                    "true" | "1" | "yes" | "t" | "y" => true,
                    "false" | "0" | "no" | "f" | "n" => false,
                    _ => {
                        return Err(ParquetExportError::CsvParse {
                            row: row_idx,
                            message: format!("Invalid boolean value at column {}: {}", col_idx, s),
                        })
                    }
                };
                builder.append_value(bool_val);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a String array from CSV string values.
fn build_string_array_from_csv(
    values: &[Option<&str>],
    _col_idx: usize,
) -> Result<ArrayRef, ParquetExportError> {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 32);

    for value in values {
        match value {
            None => builder.append_null(),
            Some(s) => builder.append_value(s),
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a Float64 array from CSV string values.
fn build_float64_array_from_csv(
    values: &[Option<&str>],
    col_idx: usize,
) -> Result<ArrayRef, ParquetExportError> {
    let mut builder = Float64Builder::with_capacity(values.len());

    for (row_idx, value) in values.iter().enumerate() {
        match value {
            None => builder.append_null(),
            Some(s) => {
                let float_val = s.parse::<f64>().map_err(|_| ParquetExportError::CsvParse {
                    row: row_idx,
                    message: format!("Invalid float value at column {}: {}", col_idx, s),
                })?;
                builder.append_value(float_val);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Build a Decimal128 array from CSV string values.
fn build_decimal128_array_from_csv(
    values: &[Option<&str>],
    precision: u8,
    scale: i8,
    col_idx: usize,
) -> Result<ArrayRef, ParquetExportError> {
    let mut builder = Decimal128Builder::with_capacity(values.len())
        .with_precision_and_scale(precision, scale)
        .map_err(|e| ParquetExportError::Arrow(e.to_string()))?;

    for (row_idx, value) in values.iter().enumerate() {
        match value {
            None => builder.append_null(),
            Some(s) => {
                let decimal_val = parse_decimal_to_i128(s, scale, row_idx, col_idx)?;
                builder.append_value(decimal_val);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse a decimal string to i128.
fn parse_decimal_to_i128(
    value_str: &str,
    scale: i8,
    row_idx: usize,
    col_idx: usize,
) -> Result<i128, ParquetExportError> {
    parse_decimal_to_i128_impl(value_str, scale).map_err(|e| ParquetExportError::CsvParse {
        row: row_idx,
        message: format!("at column {}: {}", col_idx, e),
    })
}

/// Build a Date32 array from CSV string values.
fn build_date32_array_from_csv(
    values: &[Option<&str>],
    col_idx: usize,
) -> Result<ArrayRef, ParquetExportError> {
    let mut builder = Date32Builder::with_capacity(values.len());

    for (row_idx, value) in values.iter().enumerate() {
        match value {
            None => builder.append_null(),
            Some(s) => {
                let days = parse_date_to_days(s, row_idx, col_idx)?;
                builder.append_value(days);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse a date string to days since Unix epoch.
fn parse_date_to_days(
    date_str: &str,
    row_idx: usize,
    col_idx: usize,
) -> Result<i32, ParquetExportError> {
    parse_date_to_days_impl(date_str).map_err(|e| ParquetExportError::CsvParse {
        row: row_idx,
        message: format!("at column {}: {}", col_idx, e),
    })
}

/// Build a Timestamp array from CSV string values.
fn build_timestamp_array_from_csv(
    values: &[Option<&str>],
    _tz: Option<Arc<str>>,
    col_idx: usize,
) -> Result<ArrayRef, ParquetExportError> {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());

    for (row_idx, value) in values.iter().enumerate() {
        match value {
            None => builder.append_null(),
            Some(s) => {
                let micros = parse_timestamp_to_micros(s, row_idx, col_idx)?;
                builder.append_value(micros);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Parse a timestamp string to microseconds since Unix epoch.
fn parse_timestamp_to_micros(
    timestamp_str: &str,
    row_idx: usize,
    col_idx: usize,
) -> Result<i64, ParquetExportError> {
    parse_timestamp_to_micros_impl(timestamp_str).map_err(|e| ParquetExportError::CsvParse {
        row: row_idx,
        message: format!("at column {}: {}", col_idx, e),
    })
}

/// Build an Int64 array from CSV string values.
fn build_int64_array_from_csv(
    values: &[Option<&str>],
    col_idx: usize,
) -> Result<ArrayRef, ParquetExportError> {
    use arrow::array::Int64Builder;

    let mut builder = Int64Builder::with_capacity(values.len());

    for (row_idx, value) in values.iter().enumerate() {
        match value {
            None => builder.append_null(),
            Some(s) => {
                let int_val = s.parse::<i64>().map_err(|_| ParquetExportError::CsvParse {
                    row: row_idx,
                    message: format!("Invalid integer value at column {}: {}", col_idx, s),
                })?;
                builder.append_value(int_val);
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Create an Arrow schema from Exasol column types.
///
/// This function maps Exasol types to Arrow types for Parquet export.
pub fn exasol_types_to_arrow_schema(
    column_names: &[String],
    column_types: &[ExasolType],
) -> Result<Schema, ParquetExportError> {
    if column_names.len() != column_types.len() {
        return Err(ParquetExportError::Schema(format!(
            "Column names count ({}) doesn't match types count ({})",
            column_names.len(),
            column_types.len()
        )));
    }

    let fields: Result<Vec<Field>, ParquetExportError> = column_names
        .iter()
        .zip(column_types.iter())
        .map(|(name, exasol_type)| {
            let arrow_type = exasol_type_to_arrow(exasol_type)?;
            Ok(Field::new(name, arrow_type, true)) // All columns nullable
        })
        .collect();

    Ok(Schema::new(fields?))
}

/// Convert an Exasol type to Arrow DataType.
fn exasol_type_to_arrow(exasol_type: &ExasolType) -> Result<DataType, ParquetExportError> {
    // Check for unsupported types in Parquet export first
    match exasol_type {
        ExasolType::IntervalYearToMonth
        | ExasolType::IntervalDayToSecond { .. }
        | ExasolType::Geometry { .. }
        | ExasolType::Hashtype { .. } => {
            return Err(ParquetExportError::Schema(format!(
                "Unsupported Exasol type for Parquet export: {:?}",
                exasol_type
            )));
        }
        _ => {}
    }

    // Use shared implementation for supported types
    exasol_type_to_arrow_impl(exasol_type).map_err(ParquetExportError::Schema)
}

// =============================================================================
// Transport-integrated export functions
// =============================================================================

use crate::query::export::ExportSource;
use crate::transport::TransportProtocol;

/// Exports data from an Exasol table or query to a Parquet file via transport.
///
/// This function exports data from Exasol (table or query) directly to a Parquet file
/// using the HTTP transport layer.
///
/// # Arguments
///
/// * `transport` - Transport for executing SQL
/// * `source` - The data source (table or query)
/// * `file_path` - Path to write the Parquet file
/// * `options` - Export options
///
/// # Returns
///
/// The number of rows exported.
///
/// # Errors
///
/// Returns `ExportError` if the export fails.
pub async fn export_to_parquet_via_transport<T: TransportProtocol + ?Sized>(
    transport: &mut T,
    source: ExportSource,
    file_path: &Path,
    options: ParquetExportOptions,
) -> Result<u64, crate::export::csv::ExportError> {
    use crate::export::csv::{export_to_list, CsvExportOptions};

    // Get the data as CSV via the existing export function
    // Note: We always disable column names in the CSV export for Parquet
    // because we don't want header rows mixed with data rows.
    let csv_options = CsvExportOptions::default()
        .column_separator(options.column_separator)
        .column_delimiter(options.column_delimiter)
        .with_column_names(false)
        .exasol_host(&options.host)
        .exasol_port(options.port)
        .use_tls(options.use_tls);

    // Get the CSV data as a list of rows
    let rows = export_to_list(transport, source, csv_options).await?;

    if rows.is_empty() {
        return Ok(0);
    }

    // We need a schema to create Parquet. Without column type info, use strings.
    let num_columns = rows.first().map(|r| r.len()).unwrap_or(0);
    let fields: Vec<Field> = (0..num_columns)
        .map(|i| Field::new(format!("col{}", i), DataType::Utf8, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));

    // Convert rows to CSV bytes
    let mut csv_bytes = Vec::new();
    for row in &rows {
        let line = row.join(&options.column_separator.to_string());
        csv_bytes.extend_from_slice(line.as_bytes());
        csv_bytes.push(b'\n');
    }

    // Use the existing export function
    let parquet_options = ParquetExportOptions {
        with_column_names: false, // We already have data rows
        ..options
    };

    export_to_parquet(&csv_bytes, schema, file_path, parquet_options)
        .await
        .map_err(|e| crate::export::csv::ExportError::CsvParseError {
            row: 0,
            message: e.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ==========================================================================
    // Tests for ParquetCompression
    // ==========================================================================

    #[test]
    fn test_parquet_compression_default() {
        let compression = ParquetCompression::default();
        assert_eq!(compression, ParquetCompression::Snappy);
    }

    #[test]
    fn test_parquet_compression_to_codec() {
        assert!(matches!(
            ParquetCompression::None.to_codec(),
            ParquetCompressionCodec::UNCOMPRESSED
        ));
        assert!(matches!(
            ParquetCompression::Snappy.to_codec(),
            ParquetCompressionCodec::SNAPPY
        ));
        assert!(matches!(
            ParquetCompression::Gzip.to_codec(),
            ParquetCompressionCodec::GZIP(_)
        ));
        assert!(matches!(
            ParquetCompression::Lz4.to_codec(),
            ParquetCompressionCodec::LZ4
        ));
        assert!(matches!(
            ParquetCompression::Zstd.to_codec(),
            ParquetCompressionCodec::ZSTD(_)
        ));
    }

    // ==========================================================================
    // Tests for ParquetExportOptions
    // ==========================================================================

    #[test]
    fn test_parquet_export_options_default() {
        let options = ParquetExportOptions::default();
        assert_eq!(options.batch_size, 1024);
        assert_eq!(options.compression, ParquetCompression::Snappy);
        assert!(options.with_column_names);
        assert_eq!(options.column_separator, ',');
        assert_eq!(options.column_delimiter, '"');
        assert!(options.null_value.is_none());
        assert_eq!(options.host, "");
        assert_eq!(options.port, 0);
        assert!(!options.use_tls);
    }

    #[test]
    fn test_parquet_export_options_builder() {
        let options = ParquetExportOptions::new()
            .with_batch_size(2048)
            .with_compression(ParquetCompression::Gzip)
            .with_column_names(false)
            .with_column_separator(';')
            .with_column_delimiter('\'')
            .with_null_value("\\N")
            .exasol_host("exasol.example.com")
            .exasol_port(8563)
            .use_tls(true);

        assert_eq!(options.batch_size, 2048);
        assert_eq!(options.compression, ParquetCompression::Gzip);
        assert!(!options.with_column_names);
        assert_eq!(options.column_separator, ';');
        assert_eq!(options.column_delimiter, '\'');
        assert_eq!(options.null_value, Some("\\N".to_string()));
        assert_eq!(options.host, "exasol.example.com");
        assert_eq!(options.port, 8563);
        assert!(options.use_tls);
    }

    #[test]
    fn test_parquet_export_options_use_tls_builder() {
        assert!(ParquetExportOptions::default().use_tls(true).use_tls);
        assert!(!ParquetExportOptions::default().use_tls(false).use_tls);
    }

    // ==========================================================================
    // Tests for CSV parsing
    // ==========================================================================

    #[test]
    fn test_parse_csv_line_simple() {
        let options = ParquetExportOptions::default();
        let result = parse_csv_line("1,Alice,true", &options, 0, 3).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("1".to_string()));
        assert_eq!(result[1], Some("Alice".to_string()));
        assert_eq!(result[2], Some("true".to_string()));
    }

    #[test]
    fn test_parse_csv_line_with_quotes() {
        let options = ParquetExportOptions::default();
        let result = parse_csv_line("1,\"Hello, World\",true", &options, 0, 3).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("1".to_string()));
        assert_eq!(result[1], Some("Hello, World".to_string()));
        assert_eq!(result[2], Some("true".to_string()));
    }

    #[test]
    fn test_parse_csv_line_with_escaped_quotes() {
        let options = ParquetExportOptions::default();
        let result = parse_csv_line("1,\"Say \"\"Hello\"\"\",true", &options, 0, 3).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[1], Some("Say \"Hello\"".to_string()));
    }

    #[test]
    fn test_parse_csv_line_with_empty_value() {
        let options = ParquetExportOptions::default();
        let result = parse_csv_line("1,,true", &options, 0, 3).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some("1".to_string()));
        assert_eq!(result[1], None); // Empty value is NULL
        assert_eq!(result[2], Some("true".to_string()));
    }

    #[test]
    fn test_parse_csv_line_with_explicit_null() {
        let options = ParquetExportOptions {
            null_value: Some("NULL".to_string()),
            ..Default::default()
        };
        let result = parse_csv_line("1,NULL,true", &options, 0, 3).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[1], None);
    }

    #[test]
    fn test_parse_csv_line_wrong_column_count() {
        let options = ParquetExportOptions::default();
        let result = parse_csv_line("1,Alice", &options, 0, 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_csv_value_null() {
        let options = ParquetExportOptions::default();
        let result = parse_csv_value("", &options);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_csv_value_explicit_null() {
        let options = ParquetExportOptions {
            null_value: Some("\\N".to_string()),
            ..Default::default()
        };
        let result = parse_csv_value("\\N", &options);
        assert!(result.is_none());
    }

    #[test]
    fn test_parse_csv_value_regular() {
        let options = ParquetExportOptions::default();
        let result = parse_csv_value("hello", &options);
        assert_eq!(result, Some("hello".to_string()));
    }

    // ==========================================================================
    // Tests for CSV to RecordBatch conversion
    // ==========================================================================

    #[test]
    fn test_csv_to_record_batches_empty() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
        let options = ParquetExportOptions::default();
        let batches = csv_to_record_batches(b"", &schema, &options).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_csv_to_record_batches_header_only() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
        let options = ParquetExportOptions::default();
        let batches = csv_to_record_batches(b"id", &schema, &options).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 0);
    }

    #[test]
    fn test_csv_to_record_batches_simple() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let options = ParquetExportOptions::default();
        let csv_data = b"id,name\n1,Alice\n2,Bob";
        let batches = csv_to_record_batches(csv_data, &schema, &options).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_csv_to_record_batches_multiple_batches() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
        let options = ParquetExportOptions {
            batch_size: 2,
            ..Default::default()
        };
        let csv_data = b"id\n1\n2\n3\n4\n5";
        let batches = csv_to_record_batches(csv_data, &schema, &options).unwrap();
        assert_eq!(batches.len(), 3); // 2 + 2 + 1
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 2);
        assert_eq!(batches[2].num_rows(), 1);
    }

    // ==========================================================================
    // Tests for array building
    // ==========================================================================

    #[test]
    fn test_build_boolean_array_from_csv() {
        let values = vec![Some("true"), Some("false"), None, Some("1"), Some("0")];
        let array = build_boolean_array_from_csv(&values, 0).unwrap();
        assert_eq!(array.len(), 5);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_boolean_array_from_csv_invalid() {
        let values = vec![Some("invalid")];
        let result = build_boolean_array_from_csv(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_string_array_from_csv() {
        let values = vec![Some("hello"), Some("world"), None];
        let array = build_string_array_from_csv(&values, 0).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_float64_array_from_csv() {
        let values = vec![Some("1.5"), Some("2.7"), None];
        let array = build_float64_array_from_csv(&values, 0).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_float64_array_from_csv_invalid() {
        let values = vec![Some("not_a_number")];
        let result = build_float64_array_from_csv(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_decimal128_array_from_csv() {
        let values = vec![Some("123.45"), Some("678.90"), None];
        let array = build_decimal128_array_from_csv(&values, 10, 2, 0).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_date32_array_from_csv() {
        let values = vec![Some("2024-01-15"), Some("2023-06-20"), None];
        let array = build_date32_array_from_csv(&values, 0).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_timestamp_array_from_csv() {
        let values = vec![
            Some("2024-01-15 10:30:00"),
            Some("2023-06-20 14:45:30.123456"),
            None,
        ];
        let array = build_timestamp_array_from_csv(&values, None, 0).unwrap();
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_int64_array_from_csv() {
        let values = vec![Some("1"), Some("2"), None, Some("-100")];
        let array = build_int64_array_from_csv(&values, 0).unwrap();
        assert_eq!(array.len(), 4);
        assert_eq!(array.null_count(), 1);
    }

    #[test]
    fn test_build_int64_array_from_csv_invalid() {
        let values = vec![Some("not_a_number")];
        let result = build_int64_array_from_csv(&values, 0);
        assert!(result.is_err());
    }

    // ==========================================================================
    // Tests for date parsing
    // ==========================================================================

    #[test]
    fn test_parse_date_to_days_epoch() {
        let days = parse_date_to_days("1970-01-01", 0, 0).unwrap();
        assert_eq!(days, 0);
    }

    #[test]
    fn test_parse_date_to_days_after_epoch() {
        let days = parse_date_to_days("1970-01-02", 0, 0).unwrap();
        assert_eq!(days, 1);
    }

    #[test]
    fn test_parse_date_to_days_before_epoch() {
        let days = parse_date_to_days("1969-12-31", 0, 0).unwrap();
        assert_eq!(days, -1);
    }

    #[test]
    fn test_parse_date_to_days_invalid_format() {
        let result = parse_date_to_days("2024/01/15", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_invalid_month() {
        let result = parse_date_to_days("2024-13-01", 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_date_to_days_leap_year() {
        // 2024 is a leap year
        let march_1_2024 = parse_date_to_days("2024-03-01", 0, 0).unwrap();
        let feb_28_2024 = parse_date_to_days("2024-02-28", 0, 0).unwrap();
        assert_eq!(march_1_2024 - feb_28_2024, 2); // Feb 29 exists
    }

    // ==========================================================================
    // Tests for timestamp parsing
    // ==========================================================================

    #[test]
    fn test_parse_timestamp_to_micros_epoch() {
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00", 0, 0).unwrap();
        assert_eq!(micros, 0);
    }

    #[test]
    fn test_parse_timestamp_to_micros_one_second() {
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:01", 0, 0).unwrap();
        assert_eq!(micros, 1_000_000);
    }

    #[test]
    fn test_parse_timestamp_to_micros_with_fraction() {
        let micros = parse_timestamp_to_micros("1970-01-01 00:00:00.123456", 0, 0).unwrap();
        assert_eq!(micros, 123_456);
    }

    #[test]
    fn test_parse_timestamp_to_micros_date_only() {
        let micros = parse_timestamp_to_micros("1970-01-02", 0, 0).unwrap();
        assert_eq!(micros, 86400 * 1_000_000);
    }

    // ==========================================================================
    // Tests for decimal parsing
    // ==========================================================================

    #[test]
    fn test_parse_decimal_to_i128_integer() {
        let result = parse_decimal_to_i128("123", 2, 0, 0).unwrap();
        assert_eq!(result, 12300);
    }

    #[test]
    fn test_parse_decimal_to_i128_with_fraction() {
        let result = parse_decimal_to_i128("123.45", 2, 0, 0).unwrap();
        assert_eq!(result, 12345);
    }

    #[test]
    fn test_parse_decimal_to_i128_negative() {
        let result = parse_decimal_to_i128("-123.45", 2, 0, 0).unwrap();
        assert_eq!(result, -12345);
    }

    #[test]
    fn test_parse_decimal_to_i128_invalid_format() {
        let result = parse_decimal_to_i128("1.2.3", 2, 0, 0);
        assert!(result.is_err());
    }

    // ==========================================================================
    // Tests for schema conversion
    // ==========================================================================

    #[test]
    fn test_exasol_types_to_arrow_schema() {
        let names = vec!["id".to_string(), "name".to_string(), "active".to_string()];
        let types = vec![
            ExasolType::Decimal {
                precision: 18,
                scale: 0,
            },
            ExasolType::Varchar { size: 100 },
            ExasolType::Boolean,
        ];
        let schema = exasol_types_to_arrow_schema(&names, &types).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(2).name(), "active");
    }

    #[test]
    fn test_exasol_types_to_arrow_schema_mismatched_lengths() {
        let names = vec!["id".to_string()];
        let types = vec![ExasolType::Boolean, ExasolType::Double];
        let result = exasol_types_to_arrow_schema(&names, &types);
        assert!(result.is_err());
    }

    #[test]
    fn test_exasol_type_to_arrow_boolean() {
        let result = exasol_type_to_arrow(&ExasolType::Boolean).unwrap();
        assert_eq!(result, DataType::Boolean);
    }

    #[test]
    fn test_exasol_type_to_arrow_varchar() {
        let result = exasol_type_to_arrow(&ExasolType::Varchar { size: 100 }).unwrap();
        assert_eq!(result, DataType::Utf8);
    }

    #[test]
    fn test_exasol_type_to_arrow_decimal() {
        let result = exasol_type_to_arrow(&ExasolType::Decimal {
            precision: 18,
            scale: 2,
        })
        .unwrap();
        assert_eq!(result, DataType::Decimal128(18, 2));
    }

    #[test]
    fn test_exasol_type_to_arrow_double() {
        let result = exasol_type_to_arrow(&ExasolType::Double).unwrap();
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn test_exasol_type_to_arrow_date() {
        let result = exasol_type_to_arrow(&ExasolType::Date).unwrap();
        assert_eq!(result, DataType::Date32);
    }

    #[test]
    fn test_exasol_type_to_arrow_timestamp_without_tz() {
        let result = exasol_type_to_arrow(&ExasolType::Timestamp {
            with_local_time_zone: false,
        })
        .unwrap();
        assert_eq!(result, DataType::Timestamp(TimeUnit::Microsecond, None));
    }

    #[test]
    fn test_exasol_type_to_arrow_timestamp_with_tz() {
        let result = exasol_type_to_arrow(&ExasolType::Timestamp {
            with_local_time_zone: true,
        })
        .unwrap();
        assert!(matches!(
            result,
            DataType::Timestamp(TimeUnit::Microsecond, Some(_))
        ));
    }

    // ==========================================================================
    // Integration tests for Parquet export
    // ==========================================================================

    #[tokio::test]
    async fn test_export_to_parquet_stream_simple() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let options = ParquetExportOptions::default();
        let csv_data = b"id,name\n1,Alice\n2,Bob\n3,Charlie";

        let mut buffer = Vec::new();
        let rows = export_to_parquet_stream(csv_data, schema, &mut buffer, options)
            .await
            .unwrap();

        assert_eq!(rows, 3);
        assert!(!buffer.is_empty());
        // Verify it's a valid Parquet file (magic bytes: PAR1)
        assert_eq!(&buffer[0..4], b"PAR1");
    }

    #[tokio::test]
    async fn test_export_to_parquet_stream_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let options = ParquetExportOptions::default();
        let csv_data = b"id,name\n1,Alice\n2,\n3,Charlie";

        let mut buffer = Vec::new();
        let rows = export_to_parquet_stream(csv_data, schema, &mut buffer, options)
            .await
            .unwrap();

        assert_eq!(rows, 3);
        assert!(!buffer.is_empty());
    }

    #[tokio::test]
    async fn test_export_to_parquet_stream_empty() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let options = ParquetExportOptions::default();
        let csv_data = b"id";

        let mut buffer = Vec::new();
        let rows = export_to_parquet_stream(csv_data, schema, &mut buffer, options)
            .await
            .unwrap();

        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn test_export_to_parquet_stream_all_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("int_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float64, true),
            Field::new("str_col", DataType::Utf8, true),
            Field::new("date_col", DataType::Date32, true),
            Field::new(
                "ts_col",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let options = ParquetExportOptions::default();
        let csv_data = b"bool_col,int_col,float_col,str_col,date_col,ts_col\ntrue,1,1.5,hello,2024-01-15,2024-01-15 10:30:00\nfalse,2,2.5,world,2024-06-20,2024-06-20 14:45:30.123456";

        let mut buffer = Vec::new();
        let rows = export_to_parquet_stream(csv_data, schema, &mut buffer, options)
            .await
            .unwrap();

        assert_eq!(rows, 2);
        assert!(!buffer.is_empty());
    }

    #[tokio::test]
    async fn test_export_to_parquet_stream_with_compression() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));

        // Test all compression types
        for compression in [
            ParquetCompression::None,
            ParquetCompression::Snappy,
            ParquetCompression::Gzip,
            ParquetCompression::Lz4,
            ParquetCompression::Zstd,
        ] {
            let options = ParquetExportOptions {
                compression,
                ..Default::default()
            };
            let csv_data = b"id\n1\n2\n3";

            let mut buffer = Vec::new();
            let rows = export_to_parquet_stream(csv_data, schema.clone(), &mut buffer, options)
                .await
                .unwrap();

            assert_eq!(rows, 3);
            assert!(!buffer.is_empty());
            // Verify it's a valid Parquet file
            assert_eq!(&buffer[0..4], b"PAR1");
        }
    }

    #[tokio::test]
    async fn test_export_to_parquet_file() {
        use std::fs;
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let options = ParquetExportOptions::default();
        let csv_data = b"id,name\n1,Alice\n2,Bob";

        let rows = export_to_parquet(csv_data, schema, &file_path, options)
            .await
            .unwrap();

        assert_eq!(rows, 2);
        assert!(file_path.exists());

        // Verify file content starts with PAR1
        let content = fs::read(&file_path).unwrap();
        assert_eq!(&content[0..4], b"PAR1");
    }

    // ==========================================================================
    // Tests for AsyncWrite stream export
    // ==========================================================================

    #[tokio::test]
    async fn test_export_to_parquet_async_stream_simple() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let options = ParquetExportOptions::default();
        let csv_data = b"id,name\n1,Alice\n2,Bob\n3,Charlie";

        let mut buffer = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut buffer);

        // Use tokio's async write wrapper
        let rows = export_to_parquet_async_stream(csv_data, schema, &mut cursor, options)
            .await
            .unwrap();

        assert_eq!(rows, 3);
        assert!(!buffer.is_empty());
        // Verify it's a valid Parquet file (magic bytes: PAR1)
        assert_eq!(&buffer[0..4], b"PAR1");
    }

    #[tokio::test]
    async fn test_export_to_parquet_async_stream_with_compression() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));

        // Test with Gzip compression
        let options = ParquetExportOptions {
            compression: ParquetCompression::Gzip,
            ..Default::default()
        };
        let csv_data = b"id\n1\n2\n3";

        let mut buffer = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut buffer);

        let rows = export_to_parquet_async_stream(csv_data, schema.clone(), &mut cursor, options)
            .await
            .unwrap();

        assert_eq!(rows, 3);
        assert!(!buffer.is_empty());
        assert_eq!(&buffer[0..4], b"PAR1");
    }

    #[tokio::test]
    async fn test_export_to_parquet_async_stream_schema_preservation() {
        // Test that complex schema types are preserved through Parquet export
        let schema = Arc::new(Schema::new(vec![
            Field::new("bool_col", DataType::Boolean, true),
            Field::new("int_col", DataType::Int64, true),
            Field::new("float_col", DataType::Float64, true),
            Field::new("str_col", DataType::Utf8, true),
            Field::new("decimal_col", DataType::Decimal128(18, 2), true),
            Field::new("date_col", DataType::Date32, true),
        ]));
        let options = ParquetExportOptions::default();
        let csv_data = b"bool_col,int_col,float_col,str_col,decimal_col,date_col\ntrue,42,3.14,hello,123.45,2024-01-15";

        let mut buffer = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut buffer);

        let rows = export_to_parquet_async_stream(csv_data, schema.clone(), &mut cursor, options)
            .await
            .unwrap();

        assert_eq!(rows, 1);
        assert!(!buffer.is_empty());

        // Read the Parquet file back and verify schema
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let bytes = bytes::Bytes::from(buffer);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let parquet_schema = builder.schema();

        // Verify schema field names match
        assert_eq!(parquet_schema.fields().len(), 6);
        assert_eq!(parquet_schema.field(0).name(), "bool_col");
        assert_eq!(parquet_schema.field(1).name(), "int_col");
        assert_eq!(parquet_schema.field(2).name(), "float_col");
        assert_eq!(parquet_schema.field(3).name(), "str_col");
        assert_eq!(parquet_schema.field(4).name(), "decimal_col");
        assert_eq!(parquet_schema.field(5).name(), "date_col");
    }

    #[tokio::test]
    async fn test_export_to_parquet_roundtrip_data_integrity() {
        // Test that data values are correctly preserved through Parquet export and read back
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]));
        let options = ParquetExportOptions::default();
        let csv_data = b"id,name,active\n1,Alice,true\n2,Bob,false\n3,,true";

        let mut buffer = Vec::new();
        let mut cursor = std::io::Cursor::new(&mut buffer);

        let rows = export_to_parquet_async_stream(csv_data, schema, &mut cursor, options)
            .await
            .unwrap();

        assert_eq!(rows, 3);

        // Read the Parquet file back and verify data
        use arrow::array::{Array, BooleanArray, Int64Array, StringArray};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let bytes = bytes::Bytes::from(buffer);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let mut reader = builder.build().unwrap();

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 3);

        // Verify column values
        let id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);

        let name_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert!(name_col.is_null(2)); // Third row has NULL name

        let active_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(active_col.value(0));
        assert!(!active_col.value(1));
        assert!(active_col.value(2));
    }
}
