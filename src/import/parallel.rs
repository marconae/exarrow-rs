//! Parallel import infrastructure for multi-file operations.
//!
//! This module provides the `ParallelTransportPool` for managing multiple HTTP transport
//! connections for parallel file imports, and utilities for streaming multiple files
//! concurrently.

use std::path::PathBuf;

use tokio::task::JoinHandle;

use crate::query::import::Compression;
use crate::transport::HttpTransportClient;

use super::ImportError;

/// Default chunk size for HTTP chunked transfer encoding (64KB).
const CHUNK_SIZE: usize = 64 * 1024;

/// Entry describing a file for parallel import.
///
/// Each entry contains the address and file name needed to build
/// the multi-FILE IMPORT SQL statement.
#[derive(Debug, Clone)]
pub struct ImportFileEntry {
    /// Internal address from EXA handshake (format: "host:port")
    pub address: String,
    /// File name for this entry (e.g., "001.csv", "002.csv")
    pub file_name: String,
    /// Optional public key fingerprint for TLS
    pub public_key: Option<String>,
}

impl ImportFileEntry {
    /// Create a new import file entry.
    pub fn new(address: String, file_name: String, public_key: Option<String>) -> Self {
        Self {
            address,
            file_name,
            public_key,
        }
    }
}

/// Pool of parallel HTTP transport connections for multi-file import.
///
/// This struct manages multiple HTTP connections, each performing the EXA
/// tunneling handshake to obtain unique internal addresses for the IMPORT SQL.
pub struct ParallelTransportPool {
    /// HTTP transport clients (one per file)
    connections: Vec<HttpTransportClient>,
    /// File entries for SQL query building
    entries: Vec<ImportFileEntry>,
}

impl std::fmt::Debug for ParallelTransportPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelTransportPool")
            .field("connection_count", &self.connections.len())
            .field("entries", &self.entries)
            .finish()
    }
}

impl ParallelTransportPool {
    /// Establishes N parallel HTTP connections with EXA handshake.
    ///
    /// This method creates `file_count` HTTP transport connections in parallel,
    /// each performing the EXA tunneling handshake to obtain unique internal addresses.
    ///
    /// # Arguments
    ///
    /// * `host` - The Exasol host to connect to
    /// * `port` - The port to connect to
    /// * `use_tls` - Whether to use TLS encryption
    /// * `file_count` - Number of parallel connections to establish
    ///
    /// # Returns
    ///
    /// A `ParallelTransportPool` with established connections.
    ///
    /// # Errors
    ///
    /// Returns `ImportError::ParallelConnectionError` if any connection fails.
    /// Uses fail-fast semantics - aborts all remaining connections on first failure.
    pub async fn connect(
        host: &str,
        port: u16,
        use_tls: bool,
        file_count: usize,
    ) -> Result<Self, ImportError> {
        if file_count == 0 {
            return Err(ImportError::InvalidConfig(
                "file_count must be at least 1".to_string(),
            ));
        }

        // Spawn connection tasks in parallel
        let mut connect_handles: Vec<JoinHandle<Result<HttpTransportClient, ImportError>>> =
            Vec::with_capacity(file_count);

        for _ in 0..file_count {
            let host = host.to_string();
            let handle = tokio::spawn(async move {
                HttpTransportClient::connect(&host, port, use_tls)
                    .await
                    .map_err(|e| {
                        ImportError::HttpTransportError(format!("Failed to connect to Exasol: {e}"))
                    })
            });
            connect_handles.push(handle);
        }

        // Collect results with fail-fast semantics
        let mut connections = Vec::with_capacity(file_count);
        let mut entries = Vec::with_capacity(file_count);

        for (idx, handle) in connect_handles.into_iter().enumerate() {
            let client = handle
                .await
                .map_err(|e| {
                    ImportError::ParallelImportError(format!(
                        "Connection task {} panicked: {e}",
                        idx
                    ))
                })?
                .map_err(|e| {
                    ImportError::ParallelImportError(format!("Connection {} failed: {e}", idx))
                })?;

            // Generate file entry with unique file name
            let file_name = format!("{:03}.csv", idx + 1);
            let entry = ImportFileEntry::new(
                client.internal_address().to_string(),
                file_name,
                client.public_key_fingerprint().map(String::from),
            );

            connections.push(client);
            entries.push(entry);
        }

        Ok(Self {
            connections,
            entries,
        })
    }

    /// Returns file entries for SQL query building.
    ///
    /// These entries contain the internal addresses and file names
    /// needed to construct the multi-FILE IMPORT SQL statement.
    #[must_use]
    pub fn file_entries(&self) -> &[ImportFileEntry] {
        &self.entries
    }

    /// Returns the number of connections in the pool.
    #[must_use]
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Returns true if the pool has no connections.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// Consumes pool and returns connections for streaming.
    ///
    /// This method takes ownership of the pool and returns the
    /// individual connections for use in parallel streaming.
    #[must_use]
    pub fn into_connections(self) -> Vec<HttpTransportClient> {
        self.connections
    }
}

/// Streams multiple files through HTTP connections in parallel.
///
/// This function takes ownership of the connections and file data,
/// streaming each file through its corresponding connection concurrently.
///
/// # Arguments
///
/// * `connections` - HTTP transport clients (one per file)
/// * `file_data` - File data to stream (one Vec<u8> per file)
/// * `compression` - Compression to apply (already applied to data)
///
/// # Returns
///
/// Ok(()) on success.
///
/// # Errors
///
/// Returns `ImportError` on first failure (fail-fast).
pub async fn stream_files_parallel(
    connections: Vec<HttpTransportClient>,
    file_data: Vec<Vec<u8>>,
    _compression: Compression,
) -> Result<(), ImportError> {
    if connections.len() != file_data.len() {
        return Err(ImportError::InvalidConfig(format!(
            "Connection count ({}) != file data count ({})",
            connections.len(),
            file_data.len()
        )));
    }

    // Spawn streaming tasks for each connection/file pair
    let mut stream_handles: Vec<JoinHandle<Result<(), ImportError>>> =
        Vec::with_capacity(connections.len());

    for (idx, (mut client, data)) in connections.into_iter().zip(file_data).enumerate() {
        let handle = tokio::spawn(async move {
            // Wait for HTTP GET from Exasol
            client.handle_import_request().await.map_err(|e| {
                ImportError::ParallelImportError(format!(
                    "File {} failed to handle import request: {e}",
                    idx
                ))
            })?;

            // Stream data in chunks
            for chunk in data.chunks(CHUNK_SIZE) {
                client.write_chunked_body(chunk).await.map_err(|e| {
                    ImportError::ParallelImportError(format!("File {} streaming failed: {e}", idx))
                })?;
            }

            // Send final chunk
            client.write_final_chunk().await.map_err(|e| {
                ImportError::ParallelImportError(format!(
                    "File {} failed to send final chunk: {e}",
                    idx
                ))
            })?;

            Ok(())
        });

        stream_handles.push(handle);
    }

    // Wait for all streams to complete with fail-fast
    for (idx, handle) in stream_handles.into_iter().enumerate() {
        handle
            .await
            .map_err(|e| {
                ImportError::ParallelImportError(format!("Stream task {} panicked: {e}", idx))
            })?
            .map_err(|e| ImportError::ParallelImportError(format!("Stream {} failed: {e}", idx)))?;
    }

    Ok(())
}

/// Streams multiple Parquet files through HTTP connections in parallel using
/// the native Parquet HEAD/GET-Range protocol.
///
/// Each file is read into memory and served by `handle_parquet_import_requests`
/// which responds to repeated HEAD and ranged GET requests issued by the
/// server. Tasks run concurrently and use fail-fast semantics.
///
/// # Arguments
///
/// * `connections` - HTTP transport clients (one per file)
/// * `file_paths` - File paths to stream (one path per connection)
///
/// # Returns
///
/// Ok(()) on success.
///
/// # Errors
///
/// Returns `ImportError::InvalidConfig` when the connection count does not
/// match the file count, or `ImportError::ParallelImportError` on the first
/// streaming or task failure.
pub async fn stream_parquet_files_parallel(
    connections: Vec<HttpTransportClient>,
    file_paths: Vec<PathBuf>,
) -> Result<(), ImportError> {
    if connections.len() != file_paths.len() {
        return Err(ImportError::InvalidConfig(format!(
            "Connection count ({}) != file count ({})",
            connections.len(),
            file_paths.len()
        )));
    }

    let mut stream_handles: Vec<JoinHandle<Result<(), ImportError>>> =
        Vec::with_capacity(connections.len());

    for (idx, (mut client, path)) in connections.into_iter().zip(file_paths).enumerate() {
        let handle = tokio::spawn(async move {
            let file_bytes = tokio::fs::read(&path).await.map_err(|e| {
                ImportError::ParallelImportError(format!(
                    "File {}: failed to read '{}': {e}",
                    idx,
                    path.display()
                ))
            })?;

            client
                .handle_parquet_import_requests(&file_bytes)
                .await
                .map_err(|e| {
                    ImportError::ParallelImportError(format!(
                        "File {}: parquet range-request handler failed: {e}",
                        idx
                    ))
                })?;

            Ok(())
        });

        stream_handles.push(handle);
    }

    for (idx, handle) in stream_handles.into_iter().enumerate() {
        handle
            .await
            .map_err(|e| {
                ImportError::ParallelImportError(format!("Stream task {} panicked: {e}", idx))
            })?
            .map_err(|e| ImportError::ParallelImportError(format!("Stream {} failed: {e}", idx)))?;
    }

    Ok(())
}

/// Converts multiple Parquet files to CSV format in parallel.
///
/// This function spawns blocking tasks to convert each Parquet file
/// to CSV format concurrently.
///
/// # Arguments
///
/// * `paths` - Paths to Parquet files
/// * `batch_size` - Batch size for Parquet reader
/// * `null_value` - String representation of NULL values
/// * `column_separator` - CSV column separator
/// * `column_delimiter` - CSV column delimiter
///
/// # Returns
///
/// Vector of CSV data (one Vec<u8> per file).
///
/// # Errors
///
/// Returns `ImportError` on first conversion failure (fail-fast).
pub async fn convert_parquet_files_to_csv(
    paths: Vec<PathBuf>,
    batch_size: usize,
    null_value: String,
    column_separator: char,
    column_delimiter: char,
) -> Result<Vec<Vec<u8>>, ImportError> {
    use crate::import::parquet::{record_batch_to_csv, ParquetImportOptions};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    // Spawn blocking conversion tasks in parallel
    let mut conversion_handles: Vec<JoinHandle<Result<Vec<u8>, ImportError>>> =
        Vec::with_capacity(paths.len());

    for (idx, path) in paths.into_iter().enumerate() {
        let null_value = null_value.clone();
        let handle = tokio::task::spawn_blocking(move || {
            // Read Parquet file
            let file = std::fs::File::open(&path).map_err(|e| {
                ImportError::ParallelImportError(format!(
                    "Failed to open Parquet file {}: {e}",
                    path.display()
                ))
            })?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
                ImportError::ParallelImportError(format!(
                    "Failed to read Parquet file {}: {e}",
                    path.display()
                ))
            })?;

            let reader = builder.with_batch_size(batch_size).build().map_err(|e| {
                ImportError::ParallelImportError(format!(
                    "Failed to build Parquet reader for {}: {e}",
                    path.display()
                ))
            })?;

            // Create options for CSV conversion
            let options = ParquetImportOptions::default()
                .with_null_value(&null_value)
                .with_column_separator(column_separator)
                .with_column_delimiter(column_delimiter);

            // Convert all batches to CSV
            let mut csv_data = Vec::new();
            for batch_result in reader {
                let batch = batch_result.map_err(|e| {
                    ImportError::ParallelImportError(format!(
                        "Failed to read batch from {}: {e}",
                        path.display()
                    ))
                })?;

                let csv_rows = record_batch_to_csv(&batch, &options).map_err(|e| {
                    ImportError::ParallelImportError(format!(
                        "Failed to convert batch to CSV from {}: {e}",
                        path.display()
                    ))
                })?;

                for row in csv_rows {
                    csv_data.extend_from_slice(row.as_bytes());
                    csv_data.push(b'\n');
                }
            }

            Ok(csv_data)
        });

        // Store handle with index for error messages
        let handle = tokio::spawn(async move {
            handle.await.map_err(|e| {
                ImportError::ParallelImportError(format!(
                    "Parquet conversion task {} panicked: {e}",
                    idx
                ))
            })?
        });

        conversion_handles.push(handle);
    }

    // Collect results with fail-fast semantics
    let mut results = Vec::with_capacity(conversion_handles.len());
    for (idx, handle) in conversion_handles.into_iter().enumerate() {
        let csv_data = handle
            .await
            .map_err(|e| {
                ImportError::ParallelImportError(format!("Conversion task {} panicked: {e}", idx))
            })?
            .map_err(|e| {
                ImportError::ParallelImportError(format!("Conversion {} failed: {e}", idx))
            })?;
        results.push(csv_data);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_import_file_entry_new() {
        let entry = ImportFileEntry::new(
            "10.0.0.5:8563".to_string(),
            "001.csv".to_string(),
            Some("sha256//abc123".to_string()),
        );

        assert_eq!(entry.address, "10.0.0.5:8563");
        assert_eq!(entry.file_name, "001.csv");
        assert_eq!(entry.public_key, Some("sha256//abc123".to_string()));
    }

    #[test]
    fn test_import_file_entry_no_tls() {
        let entry = ImportFileEntry::new("10.0.0.5:8563".to_string(), "002.csv".to_string(), None);

        assert_eq!(entry.address, "10.0.0.5:8563");
        assert_eq!(entry.file_name, "002.csv");
        assert!(entry.public_key.is_none());
    }

    #[tokio::test]
    async fn test_parallel_transport_pool_zero_count_error() {
        let result = ParallelTransportPool::connect("localhost", 8563, false, 0).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ImportError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn test_stream_files_parallel_mismatched_counts() {
        let connections = vec![];
        let file_data = vec![vec![1, 2, 3]];

        let result = stream_files_parallel(connections, file_data, Compression::None).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ImportError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn test_stream_files_parallel_empty() {
        let connections = vec![];
        let file_data = vec![];

        let result = stream_files_parallel(connections, file_data, Compression::None).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_stream_parquet_files_parallel_mismatched_counts() {
        let connections = vec![];
        let file_paths = vec![PathBuf::from("/tmp/does-not-matter.parquet")];

        let result = stream_parquet_files_parallel(connections, file_paths).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ImportError::InvalidConfig(_)));
    }

    #[tokio::test]
    async fn test_stream_parquet_files_parallel_empty() {
        let connections = vec![];
        let file_paths: Vec<PathBuf> = vec![];

        let result = stream_parquet_files_parallel(connections, file_paths).await;
        assert!(result.is_ok());
    }
}
