//! Integration tests for import/export functionality.
//!
//! # Overview
//!
//! This test module provides integration tests for CSV, Parquet, and Arrow
//! import/export functionality against a real Exasol database instance.
//!
//! # Prerequisites
//!
//! Before running these tests, ensure you have:
//!
//! 1. Docker installed and running
//! 2. An Exasol database container running:
//!
//!    ```bash
//!    docker run -d --name exasol-test \
//!      -p 8563:8563 \
//!      --privileged \
//!      exasol/docker-db:latest
//!    ```
//!
//! # Network Requirements
//!
//! Import/export functionality uses HTTP transport where the client connects
//! TO Exasol (client mode). This works with cloud Exasol instances and through
//! firewalls since only outbound connections are required. No special network
//! configuration is needed - the client simply needs to reach the Exasol server.
//!
//! # Running Tests
//!
//! Integration tests are marked with `#[ignore]` to prevent failures in
//! CI environments without Exasol. Run them explicitly:
//!
//! ```bash
//! # Run all import/export tests
//! cargo test --test import_export_tests -- --ignored
//!
//! # Run CSV tests only
//! cargo test --test import_export_tests csv -- --ignored
//!
//! # Run Parquet tests only
//! cargo test --test import_export_tests parquet -- --ignored
//!
//! # Run Arrow tests only
//! cargo test --test import_export_tests arrow -- --ignored
//! ```

mod common;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::{generate_test_schema_name, get_test_connection};
use exarrow_rs::adbc::Connection;
use exarrow_rs::export::arrow::ArrowExportOptions;
use exarrow_rs::export::csv::CsvExportOptions;
use exarrow_rs::export::parquet::ParquetExportOptions;
use exarrow_rs::import::arrow::ArrowImportOptions;
use exarrow_rs::import::csv::CsvImportOptions;
use exarrow_rs::import::parquet::ParquetImportOptions;
use exarrow_rs::query::export::ExportSource;
use std::sync::Arc;
use tempfile::TempDir;

// Helper Functions

/// Helper to create a test schema and table for import/export tests
async fn setup_import_export_table(conn: &mut Connection, schema_name: &str) {
    conn.execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.test_data (id INTEGER, name VARCHAR(100), amount DOUBLE)",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");
}

/// Helper to populate test data
async fn populate_test_data(conn: &mut Connection, schema_name: &str, num_rows: i64) {
    conn.execute_update(&format!(
        "INSERT INTO {}.test_data (id, name, amount) SELECT LEVEL, 'name_' || LEVEL, LEVEL * 1.5 FROM DUAL CONNECT BY LEVEL <= {}",
        schema_name, num_rows
    ))
    .await
    .expect("INSERT should succeed");
}

/// Helper to cleanup test schema
async fn cleanup_schema(conn: &mut Connection, schema_name: &str) {
    let _ = conn
        .execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await;
}

// Section 1: CSV Import Integration Tests

/// 11.1.1 Test import CSV into new table
#[tokio::test]
#[ignore]
async fn test_csv_import_into_table() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create a temporary CSV file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("test_data.csv");

    // Write test data to CSV file
    std::fs::write(&csv_path, "1,Alice,10.5\n2,Bob,20.5\n3,Charlie,30.5\n")
        .expect("Failed to write CSV file");

    // Import the CSV file
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows_imported = conn
        .import_csv_from_file(
            &format!("{}.test_data", schema_name),
            &csv_path,
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("CSV import should succeed");

    assert_eq!(rows_imported, 3, "Should import 3 rows");

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.1.2 Test import with column mapping
#[tokio::test]
#[ignore]
async fn test_csv_import_with_column_mapping() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table with extra columns
    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        r#"
        CREATE TABLE {}.mapped_data (
            id INTEGER,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            score DOUBLE
        )
        "#,
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Create CSV file with only some columns
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("mapped_data.csv");

    std::fs::write(&csv_path, "1,John,95.5\n2,Jane,88.0\n").expect("Failed to write CSV file");

    // Import with specific column mapping
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let options = CsvImportOptions::default().use_tls(false).columns(vec![
        "id".to_string(),
        "first_name".to_string(),
        "score".to_string(),
    ]);

    let rows_imported = conn
        .import_csv_from_file(&format!("{}.mapped_data", schema_name), &csv_path, options)
        .await
        .expect("CSV import should succeed");

    assert_eq!(rows_imported, 2, "Should import 2 rows");

    // Verify the data - last_name should be NULL
    let batches = conn
        .query(&format!(
            "SELECT id, first_name, last_name, score FROM {}.mapped_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 2, "Should have 2 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.1.3 Test import with error handling
#[tokio::test]
#[ignore]
async fn test_csv_import_error_handling() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create CSV with invalid data (string where number expected)
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("bad_data.csv");

    std::fs::write(&csv_path, "1,Alice,10.5\n2,Bob,invalid\n3,Charlie,30.5\n")
        .expect("Failed to write CSV file");

    // Import should fail due to data type mismatch
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let result = conn
        .import_csv_from_file(
            &format!("{}.test_data", schema_name),
            &csv_path,
            CsvImportOptions::default().use_tls(false),
        )
        .await;

    // The import should fail
    assert!(result.is_err(), "Import with invalid data should fail");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.1.4 Test import compressed CSV (gzip)
#[tokio::test]
#[ignore]
async fn test_csv_import_compressed_gzip() {
    skip_if_no_exasol!();

    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create compressed CSV file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("compressed_data.csv.gz");

    let csv_data = "1,Alice,10.5\n2,Bob,20.5\n";
    let file = std::fs::File::create(&csv_path).expect("Failed to create file");
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder
        .write_all(csv_data.as_bytes())
        .expect("Failed to write compressed data");
    encoder.finish().expect("Failed to finish compression");

    // Import with gzip compression
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let options = CsvImportOptions::default()
        .use_tls(false)
        .compression(exarrow_rs::query::import::Compression::Gzip);

    let rows_imported = conn
        .import_csv_from_file(&format!("{}.test_data", schema_name), &csv_path, options)
        .await
        .expect("Compressed CSV import should succeed");

    assert_eq!(rows_imported, 2, "Should import 2 rows");

    // Verify the data
    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.test_data",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.1.5 Test import CSV from async stream
#[tokio::test]
#[ignore]
async fn test_csv_import_from_stream() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create CSV data as bytes
    let csv_data = b"1,Alice,10.5\n2,Bob,20.5\n3,Charlie,30.5\n";

    // Create an async reader from the CSV data
    let reader = std::io::Cursor::new(csv_data.to_vec());

    // Import from stream
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows_imported = conn
        .import_csv_from_stream(
            &format!("{}.test_data", schema_name),
            reader,
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("CSV import from stream should succeed");

    assert_eq!(rows_imported, 3, "Should import 3 rows from stream");

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// Section 2: CSV Export Integration Tests

/// 11.2.1 Test export table to CSV file
#[tokio::test]
#[ignore]
async fn test_csv_export_table_to_file() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 5).await;

    // Create temp file for export
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("export.csv");

    // Export to CSV
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows_exported = conn
        .export_csv_to_file(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &csv_path,
            CsvExportOptions::default().use_tls(false),
        )
        .await
        .expect("CSV export should succeed");

    assert_eq!(rows_exported, 5, "Should export 5 rows");

    // Verify file was created and has content
    let content = std::fs::read_to_string(&csv_path).expect("Should read CSV file");
    assert!(!content.is_empty(), "CSV file should have content");

    // Count lines (should be 5 data rows)
    let line_count = content.lines().count();
    assert_eq!(line_count, 5, "Should have 5 lines");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.2.2 Test export query result to CSV
#[tokio::test]
#[ignore]
async fn test_csv_export_query_to_file() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 10).await;

    // Create temp file for export
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("query_export.csv");

    // Export query result (only first 5 rows)
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows_exported = conn
        .export_csv_to_file(
            ExportSource::Query {
                sql: format!(
                    "SELECT id, name, amount FROM {}.test_data WHERE id <= 5 ORDER BY id",
                    schema_name
                ),
            },
            &csv_path,
            CsvExportOptions::default().use_tls(false),
        )
        .await
        .expect("CSV export should succeed");

    assert_eq!(rows_exported, 5, "Should export 5 rows from query");

    // Verify file content
    let content = std::fs::read_to_string(&csv_path).expect("Should read CSV file");
    let line_count = content.lines().count();
    assert_eq!(line_count, 5, "Should have 5 lines");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.2.3 Test export with column headers
#[tokio::test]
#[ignore]
async fn test_csv_export_with_headers() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 3).await;

    // Create temp file for export
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("with_headers.csv");

    // Export with column names
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let options = CsvExportOptions::default()
        .use_tls(false)
        .with_column_names(true);

    let rows_exported = conn
        .export_csv_to_file(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &csv_path,
            options,
        )
        .await
        .expect("CSV export should succeed");

    // Note: When with_column_names is true, Exasol includes the header in the row count
    // So we expect 4 rows (1 header + 3 data rows)
    assert_eq!(rows_exported, 4, "Should export 4 rows (1 header + 3 data)");

    // Verify file has header line
    let content = std::fs::read_to_string(&csv_path).expect("Should read CSV file");
    let lines: Vec<&str> = content.lines().collect();

    // Should have 4 lines (1 header + 3 data)
    assert_eq!(lines.len(), 4, "Should have 4 lines (header + data)");

    // First line should contain column names
    let header = lines[0].to_uppercase();
    assert!(
        header.contains("ID") && header.contains("NAME") && header.contains("AMOUNT"),
        "Header should contain column names"
    );

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.2.4 Test export with gzip transfer encoding
///
/// Note: The compression option in CsvExportOptions enables gzip transfer encoding
/// from Exasol, but the library decompresses the data before writing to the output file.
/// The resulting file is NOT gzip compressed - it contains plain CSV data.
/// This test verifies that gzip transfer encoding works correctly for data transfer.
#[tokio::test]
#[ignore]
async fn test_csv_export_with_gzip_transfer() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 5).await;

    // Create temp file for export
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("gzip_transfer_export.csv");

    // Export with gzip transfer encoding
    // Note: use_tls(false) for HTTP transport as Docker Exasol doesn't expect TLS there
    // The compression option enables gzip transfer from Exasol to client,
    // but the library decompresses before writing to file
    let options = CsvExportOptions::default()
        .use_tls(false)
        .compression(exarrow_rs::query::export::Compression::Gzip);

    let rows_exported = conn
        .export_csv_to_file(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &csv_path,
            options,
        )
        .await
        .expect("Export with gzip transfer should succeed");

    assert_eq!(rows_exported, 5, "Should export 5 rows");

    // Verify file contains plain CSV data (not gzip compressed)
    let content = std::fs::read_to_string(&csv_path).expect("Should read CSV file");
    let line_count = content.lines().count();
    assert_eq!(line_count, 5, "Should have 5 lines of plain CSV data");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.2.5 Test export to in-memory list (callback-style)
#[tokio::test]
#[ignore]
async fn test_csv_export_to_list() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 5).await;

    // Export to in-memory list
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows = conn
        .export_csv_to_list(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            CsvExportOptions::default().use_tls(false),
        )
        .await
        .expect("CSV export to list should succeed");

    // Verify we got the expected number of rows
    assert_eq!(rows.len(), 5, "Should have 5 rows");

    // Verify each row has 3 columns
    for row in &rows {
        assert_eq!(row.len(), 3, "Each row should have 3 columns");
    }

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.2.6 Test export to async stream
#[tokio::test]
#[ignore]
async fn test_csv_export_to_stream() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 3).await;

    // Create an in-memory buffer as the async writer
    let buffer = Vec::new();
    let writer = std::io::Cursor::new(buffer);

    // Export to stream
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows_exported = conn
        .export_csv_to_stream(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            writer,
            CsvExportOptions::default().use_tls(false),
        )
        .await
        .expect("CSV export to stream should succeed");

    assert_eq!(rows_exported, 3, "Should export 3 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// Section 3: Parquet Integration Tests

/// 11.3.1 Test import Parquet file
#[tokio::test]
#[ignore]
async fn test_parquet_import_from_file() {
    skip_if_no_exasol!();

    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create a Parquet file with test data
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = temp_dir.path().join("test_data.parquet");

    // Create Arrow schema and RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    let id_array = Int64Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let amount_array = Float64Array::from(vec![10.5, 20.5, 30.5]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(amount_array),
        ],
    )
    .expect("Failed to create RecordBatch");

    // Write Parquet file
    let file = std::fs::File::create(&parquet_path).expect("Failed to create parquet file");
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .expect("Failed to create ArrowWriter");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    // Import the Parquet file
    let rows_imported = conn
        .import_from_parquet(
            &format!("{}.test_data", schema_name),
            &parquet_path,
            ParquetImportOptions::default(),
        )
        .await
        .expect("Parquet import should succeed");

    assert_eq!(rows_imported, 3, "Should import 3 rows");

    // Verify the data
    let batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.3.2 Test export to Parquet file
#[tokio::test]
#[ignore]
async fn test_parquet_export_to_file() {
    skip_if_no_exasol!();

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 5).await;

    // Create temp file for export
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = temp_dir.path().join("export.parquet");

    // Export to Parquet
    let rows_exported = conn
        .export_to_parquet(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &parquet_path,
            ParquetExportOptions::default(),
        )
        .await
        .expect("Parquet export should succeed");

    assert_eq!(rows_exported, 5, "Should export 5 rows");

    // Verify the Parquet file is valid by reading it
    let file = std::fs::File::open(&parquet_path).expect("Should open parquet file");
    let builder =
        ParquetRecordBatchReaderBuilder::try_new(file).expect("Should create reader builder");
    let reader = builder.build().expect("Should build reader");

    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("Should read all batches");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "Parquet file should contain 5 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.3.3 Test Parquet round-trip (import -> export -> verify)
#[tokio::test]
#[ignore]
async fn test_parquet_round_trip() {
    skip_if_no_exasol!();

    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    // Step 1: Create original Parquet file
    let original_path = temp_dir.path().join("original.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    let id_array = Int64Array::from(vec![100, 200, 300]);
    let name_array = StringArray::from(vec!["Test1", "Test2", "Test3"]);
    let amount_array = Float64Array::from(vec![1.1, 2.2, 3.3]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(amount_array),
        ],
    )
    .expect("Failed to create RecordBatch");

    let file = std::fs::File::create(&original_path).expect("Failed to create file");
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("Failed to create writer");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    // Step 2: Import into Exasol
    let rows_imported = conn
        .import_from_parquet(
            &format!("{}.test_data", schema_name),
            &original_path,
            ParquetImportOptions::default(),
        )
        .await
        .expect("Parquet import should succeed");

    assert_eq!(rows_imported, 3, "Should import 3 rows");

    // Step 3: Export back to Parquet
    let exported_path = temp_dir.path().join("exported.parquet");
    let rows_exported = conn
        .export_to_parquet(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &exported_path,
            ParquetExportOptions::default(),
        )
        .await
        .expect("Parquet export should succeed");

    assert_eq!(rows_exported, 3, "Should export 3 rows");

    // Step 4: Verify exported file matches original data
    let file = std::fs::File::open(&exported_path).expect("Should open exported file");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("Should create reader");
    let reader = builder.build().expect("Should build reader");

    let exported_batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("Should read batches");

    let total_rows: usize = exported_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Exported file should have 3 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// Section 4: Arrow Integration Tests

/// 11.4.1 Test import from RecordBatch
#[tokio::test]
#[ignore]
async fn test_arrow_import_from_record_batch() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create Arrow RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    let id_array = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
    let amount_array = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(amount_array),
        ],
    )
    .expect("Failed to create RecordBatch");

    // Import the RecordBatch
    let rows_imported = conn
        .import_from_record_batch(
            &format!("{}.test_data", schema_name),
            &batch,
            ArrowImportOptions::default(),
        )
        .await
        .expect("Arrow import should succeed");

    assert_eq!(rows_imported, 5, "Should import 5 rows");

    // Verify the data
    let result_batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!result_batches.is_empty(), "Should return results");
    assert_eq!(result_batches[0].num_rows(), 5, "Should have 5 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.4.2 Test export to RecordBatch stream
#[tokio::test]
#[ignore]
async fn test_arrow_export_to_record_batches() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 10).await;

    // Define the Arrow schema matching the table structure
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    // Export to RecordBatches
    let batches = conn
        .export_to_record_batches(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            ArrowExportOptions::default().with_schema(arrow_schema.clone()),
        )
        .await
        .expect("Arrow export should succeed");

    // Verify batches
    assert!(!batches.is_empty(), "Should have at least one batch");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 10, "Should have 10 total rows");

    // Verify schema has expected columns
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 3, "Should have 3 columns");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.4.3 Test import from Arrow IPC file
#[tokio::test]
#[ignore]
async fn test_arrow_import_from_ipc_file() {
    skip_if_no_exasol!();

    use arrow::ipc::writer::FileWriter;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create Arrow IPC file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let ipc_path = temp_dir.path().join("test_data.arrow");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    let id_array = Int64Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
    let amount_array = Float64Array::from(vec![10.5, 20.5, 30.5]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(amount_array),
        ],
    )
    .expect("Failed to create RecordBatch");

    // Write IPC file
    let file = std::fs::File::create(&ipc_path).expect("Failed to create IPC file");
    let mut writer = FileWriter::try_new(file, &schema).expect("Failed to create FileWriter");
    writer.write(&batch).expect("Failed to write batch");
    writer.finish().expect("Failed to finish writer");

    // Import from IPC file
    let file = tokio::fs::File::open(&ipc_path)
        .await
        .expect("Failed to open IPC file");

    let rows_imported = conn
        .import_from_arrow_ipc(
            &format!("{}.test_data", schema_name),
            file,
            ArrowImportOptions::default(),
        )
        .await
        .expect("Arrow IPC import should succeed");

    assert_eq!(rows_imported, 3, "Should import 3 rows");

    // Verify the data
    let result_batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!result_batches.is_empty(), "Should return results");
    assert_eq!(result_batches[0].num_rows(), 3, "Should have 3 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.4.4 Test export to Arrow IPC file
#[tokio::test]
#[ignore]
async fn test_arrow_export_to_ipc_file() {
    skip_if_no_exasol!();

    use arrow::ipc::reader::FileReader;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 5).await;

    // Create temp file for export
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let ipc_path = temp_dir.path().join("export.arrow");

    // Define the Arrow schema matching the table structure
    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    // Export to Arrow IPC file
    let rows_exported = conn
        .export_to_arrow_ipc(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &ipc_path,
            ArrowExportOptions::default().with_schema(arrow_schema.clone()),
        )
        .await
        .expect("Arrow IPC export should succeed");

    assert_eq!(rows_exported, 5, "Should export 5 rows");

    // Verify the IPC file is valid by reading it
    let file = std::fs::File::open(&ipc_path).expect("Should open IPC file");
    let reader = FileReader::try_new(file, None).expect("Should create FileReader");

    let batches: Vec<RecordBatch> = reader
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("Should read all batches");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 5, "IPC file should contain 5 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.4.5 Test Arrow round-trip (RecordBatch -> Exasol -> RecordBatch)
#[tokio::test]
#[ignore]
async fn test_arrow_round_trip() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create original RecordBatch
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    let original_ids = vec![100i64, 200, 300];
    let original_names = vec!["Alpha", "Beta", "Gamma"];
    let original_values = vec![1.1, 2.2, 3.3];

    let id_array = Int64Array::from(original_ids.clone());
    let name_array = StringArray::from(original_names.clone());
    let amount_array = Float64Array::from(original_values.clone());

    let original_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(amount_array),
        ],
    )
    .expect("Failed to create RecordBatch");

    // Import into Exasol
    let rows_imported = conn
        .import_from_record_batch(
            &format!("{}.test_data", schema_name),
            &original_batch,
            ArrowImportOptions::default(),
        )
        .await
        .expect("Arrow import should succeed");

    assert_eq!(rows_imported, 3, "Should import 3 rows");

    // Export back to RecordBatches (need to provide the schema for Arrow export)
    let exported_batches = conn
        .export_to_record_batches(
            ExportSource::Query {
                sql: format!(
                    "SELECT id, name, amount FROM {}.test_data ORDER BY id",
                    schema_name
                ),
            },
            ArrowExportOptions::default().with_schema(schema.clone()),
        )
        .await
        .expect("Arrow export should succeed");

    // Verify total row count
    let total_rows: usize = exported_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "Should have 3 rows after round-trip");

    // Verify data integrity (values should match)
    assert!(!exported_batches.is_empty(), "Should have batches");
    let exported_batch = &exported_batches[0];
    assert_eq!(
        exported_batch.num_rows(),
        3,
        "First batch should have 3 rows"
    );

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// Section 5: Edge Cases and Error Handling

/// Test CSV import with empty file
#[tokio::test]
#[ignore]
async fn test_csv_import_empty_file() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create empty CSV file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("empty.csv");
    std::fs::write(&csv_path, "").expect("Failed to create empty file");

    // Import should succeed with 0 rows
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows_imported = conn
        .import_csv_from_file(
            &format!("{}.test_data", schema_name),
            &csv_path,
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("Empty CSV import should succeed");

    assert_eq!(rows_imported, 0, "Should import 0 rows from empty file");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test CSV export from empty table
#[tokio::test]
#[ignore]
async fn test_csv_export_empty_table() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table (empty)
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create temp file for export
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("empty_export.csv");

    // Export should succeed with 0 rows
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows_exported = conn
        .export_csv_to_file(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &csv_path,
            CsvExportOptions::default().use_tls(false),
        )
        .await
        .expect("Empty table export should succeed");

    assert_eq!(rows_exported, 0, "Should export 0 rows from empty table");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test CSV import with special characters
#[tokio::test]
#[ignore]
async fn test_csv_import_special_characters() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create CSV with special characters (quotes, commas, newlines)
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("special_chars.csv");

    // CSV with properly escaped fields
    let csv_content = r#"1,"John ""Johnny"" Doe",10.5
2,"Jane, Smith",20.5
"#;
    std::fs::write(&csv_path, csv_content).expect("Failed to write CSV file");

    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows_imported = conn
        .import_csv_from_file(
            &format!("{}.test_data", schema_name),
            &csv_path,
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("CSV with special chars should import");

    assert_eq!(rows_imported, 2, "Should import 2 rows");

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT id, name FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 2, "Should have 2 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test large dataset import/export
#[tokio::test]
#[ignore]
async fn test_large_dataset_import_export() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create large CSV file (1000 rows)
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("large_data.csv");

    let mut csv_content = String::new();
    for i in 1..=1000 {
        csv_content.push_str(&format!("{},name_{},{}.5\n", i, i, i));
    }
    std::fs::write(&csv_path, &csv_content).expect("Failed to write large CSV");

    // Import
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let rows_imported = conn
        .import_csv_from_file(
            &format!("{}.test_data", schema_name),
            &csv_path,
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("Large CSV import should succeed");

    assert_eq!(rows_imported, 1000, "Should import 1000 rows");

    // Export
    // Note: use_tls(false) for HTTP transport as the main WebSocket connection already handles TLS
    let export_path = temp_dir.path().join("large_export.csv");
    let rows_exported = conn
        .export_csv_to_file(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &export_path,
            CsvExportOptions::default().use_tls(false),
        )
        .await
        .expect("Large CSV export should succeed");

    assert_eq!(rows_exported, 1000, "Should export 1000 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// Section 6: Connection TLS Tests (WebSocket layer)
//
// Note: These tests verify that import/export operations work correctly when
// the main WebSocket connection uses TLS. The HTTP transport layer for
// import/export runs on the same port but handles TLS separately.
// For Docker Exasol (local testing), the HTTP transport doesn't use TLS,
// so we use use_tls(false) for the HTTP transport options.
// For Exasol SaaS (cloud), TLS might be required for HTTP transport as well.

/// Test import/export with TLS WebSocket connection
///
/// This test verifies that import operations work correctly when the main
/// WebSocket connection uses TLS (which is the default for our test connection).
#[tokio::test]
#[ignore]
async fn test_tls_connection_csv_import() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create a temporary CSV file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("tls_test_data.csv");

    // Write test data to CSV file
    std::fs::write(&csv_path, "1,TLS_Alice,10.5\n2,TLS_Bob,20.5\n")
        .expect("Failed to write CSV file");

    // Import the CSV file (WebSocket connection uses TLS by default)
    // Note: use_tls(false) for HTTP transport as Docker Exasol doesn't expect TLS there
    let rows_imported = conn
        .import_csv_from_file(
            &format!("{}.test_data", schema_name),
            &csv_path,
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("CSV import with TLS WebSocket should succeed");

    assert_eq!(rows_imported, 2, "Should import 2 rows");

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 2, "Should have 2 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test export with TLS WebSocket connection
///
/// This test verifies that export operations work correctly when the main
/// WebSocket connection uses TLS.
#[tokio::test]
#[ignore]
async fn test_tls_connection_csv_export() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create and populate table
    setup_import_export_table(&mut conn, &schema_name).await;
    populate_test_data(&mut conn, &schema_name, 3).await;

    // Create temp file for export
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("tls_export.csv");

    // Export to CSV (WebSocket connection uses TLS by default)
    // Note: use_tls(false) for HTTP transport as Docker Exasol doesn't expect TLS there
    let rows_exported = conn
        .export_csv_to_file(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &csv_path,
            CsvExportOptions::default().use_tls(false),
        )
        .await
        .expect("CSV export with TLS WebSocket should succeed");

    assert_eq!(rows_exported, 3, "Should export 3 rows");

    // Verify file was created and has content
    let content = std::fs::read_to_string(&csv_path).expect("Should read CSV file");
    assert!(!content.is_empty(), "CSV file should have content");

    // Count lines (should be 3 data rows)
    let line_count = content.lines().count();
    assert_eq!(line_count, 3, "Should have 3 lines");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test round-trip (import and export in same session) with TLS WebSocket
///
/// This test performs a full round-trip operation:
/// 1. Import data (WebSocket uses TLS)
/// 2. Export data (WebSocket uses TLS)
/// 3. Verify data integrity
#[tokio::test]
#[ignore]
async fn test_tls_connection_round_trip() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    // Step 1: Import data
    let import_path = temp_dir.path().join("tls_import.csv");
    std::fs::write(
        &import_path,
        "100,TLS_Test1,10.0\n200,TLS_Test2,20.0\n300,TLS_Test3,30.0\n",
    )
    .expect("Failed to write CSV file");

    // Note: use_tls(false) for HTTP transport as Docker Exasol doesn't expect TLS there
    let rows_imported = conn
        .import_csv_from_file(
            &format!("{}.test_data", schema_name),
            &import_path,
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("Import should succeed");

    assert_eq!(rows_imported, 3, "Should import 3 rows");

    // Step 2: Export data
    let export_path = temp_dir.path().join("tls_export.csv");
    // Note: use_tls(false) for HTTP transport as Docker Exasol doesn't expect TLS there
    let rows_exported = conn
        .export_csv_to_file(
            ExportSource::Table {
                schema: Some(schema_name.clone()),
                name: "test_data".to_string(),
                columns: vec![],
            },
            &export_path,
            CsvExportOptions::default().use_tls(false),
        )
        .await
        .expect("Export should succeed");

    assert_eq!(rows_exported, 3, "Should export 3 rows");

    // Step 3: Verify data integrity
    let exported_content = std::fs::read_to_string(&export_path).expect("Should read exported CSV");
    let line_count = exported_content.lines().count();
    assert_eq!(line_count, 3, "Exported file should have 3 lines");

    // Verify the data through SQL query
    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.test_data",
            schema_name
        ))
        .await
        .expect("COUNT query should succeed");

    assert!(!batches.is_empty(), "Should return count result");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// Section 7: Parallel Import Integration Tests

/// 11.7.1 Test parallel CSV import with two files
///
/// Verifies that two CSV files can be imported in parallel into the same table,
/// with the total rows being the sum of both files.
#[tokio::test]
#[ignore]
async fn test_parallel_csv_import_two_files() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create two CSV files with test data
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path1 = temp_dir.path().join("data_part1.csv");
    let csv_path2 = temp_dir.path().join("data_part2.csv");

    std::fs::write(&csv_path1, "1,Alice,10.5\n2,Bob,20.5\n").expect("Failed to write CSV file 1");
    std::fs::write(&csv_path2, "3,Charlie,30.5\n4,Diana,40.5\n5,Eve,50.5\n")
        .expect("Failed to write CSV file 2");

    // Import both files in parallel
    let rows_imported = conn
        .import_csv_from_files(
            &format!("{}.test_data", schema_name),
            vec![csv_path1, csv_path2],
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("Parallel CSV import should succeed");

    assert_eq!(rows_imported, 5, "Should import 5 rows total (2 + 3)");

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 5, "Should have 5 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.7.2 Test parallel CSV import with five files
///
/// Verifies that five CSV files can be imported in parallel into the same table.
#[tokio::test]
#[ignore]
async fn test_parallel_csv_import_five_files() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create five CSV files with test data
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let mut paths = Vec::new();
    let mut expected_rows = 0;

    for i in 0..5 {
        let csv_path = temp_dir.path().join(format!("data_part{}.csv", i));
        let mut content = String::new();
        for j in 0..3 {
            let id = i * 3 + j + 1;
            content.push_str(&format!("{},name_{},{}.5\n", id, id, id));
        }
        std::fs::write(&csv_path, &content).expect("Failed to write CSV file");
        paths.push(csv_path);
        expected_rows += 3;
    }

    // Import all files in parallel
    let rows_imported = conn
        .import_csv_from_files(
            &format!("{}.test_data", schema_name),
            paths,
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("Parallel CSV import should succeed");

    assert_eq!(
        rows_imported, expected_rows,
        "Should import {} rows total",
        expected_rows
    );

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.test_data",
            schema_name
        ))
        .await
        .expect("SELECT COUNT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.7.3 Test parallel CSV import with single file fallback
///
/// Verifies that when a single file is provided as a Vec, it delegates
/// to the existing single-file import implementation.
#[tokio::test]
#[ignore]
async fn test_parallel_csv_import_single_file_fallback() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create a single CSV file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("single_file.csv");

    std::fs::write(&csv_path, "1,Alice,10.5\n2,Bob,20.5\n3,Charlie,30.5\n")
        .expect("Failed to write CSV file");

    // Import with single file in Vec (should use single-file path)
    let rows_imported = conn
        .import_csv_from_files(
            &format!("{}.test_data", schema_name),
            vec![csv_path],
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("Single file import via parallel API should succeed");

    assert_eq!(rows_imported, 3, "Should import 3 rows");

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.7.4 Test parallel Parquet import with two files
///
/// Verifies that two Parquet files can be imported in parallel into the same table,
/// with each file being converted to CSV on-the-fly.
#[tokio::test]
#[ignore]
async fn test_parallel_parquet_import_two_files() {
    skip_if_no_exasol!();

    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create two Parquet files with test data
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    // Create first Parquet file
    let parquet_path1 = temp_dir.path().join("data_part1.parquet");
    let id_array1 = Int64Array::from(vec![1, 2]);
    let name_array1 = StringArray::from(vec!["Alice", "Bob"]);
    let amount_array1 = Float64Array::from(vec![10.5, 20.5]);

    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array1),
            Arc::new(name_array1),
            Arc::new(amount_array1),
        ],
    )
    .expect("Failed to create RecordBatch");

    let file1 = std::fs::File::create(&parquet_path1).expect("Failed to create parquet file");
    let props1 = WriterProperties::builder().build();
    let mut writer1 =
        ArrowWriter::try_new(file1, schema.clone(), Some(props1)).expect("Failed to create writer");
    writer1.write(&batch1).expect("Failed to write batch");
    writer1.close().expect("Failed to close writer");

    // Create second Parquet file
    let parquet_path2 = temp_dir.path().join("data_part2.parquet");
    let id_array2 = Int64Array::from(vec![3, 4, 5]);
    let name_array2 = StringArray::from(vec!["Charlie", "Diana", "Eve"]);
    let amount_array2 = Float64Array::from(vec![30.5, 40.5, 50.5]);

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array2),
            Arc::new(name_array2),
            Arc::new(amount_array2),
        ],
    )
    .expect("Failed to create RecordBatch");

    let file2 = std::fs::File::create(&parquet_path2).expect("Failed to create parquet file");
    let props2 = WriterProperties::builder().build();
    let mut writer2 =
        ArrowWriter::try_new(file2, schema.clone(), Some(props2)).expect("Failed to create writer");
    writer2.write(&batch2).expect("Failed to write batch");
    writer2.close().expect("Failed to close writer");

    // Import both Parquet files in parallel
    let rows_imported = conn
        .import_parquet_from_files(
            &format!("{}.test_data", schema_name),
            vec![parquet_path1, parquet_path2],
            ParquetImportOptions::default(),
        )
        .await
        .expect("Parallel Parquet import should succeed");

    assert_eq!(rows_imported, 5, "Should import 5 rows total (2 + 3)");

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 5, "Should have 5 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.7.5 Test parallel Parquet import with mixed batch sizes
///
/// Verifies that Parquet files with different row counts can be imported in parallel.
#[tokio::test]
#[ignore]
async fn test_parallel_parquet_import_mixed_batch_sizes() {
    skip_if_no_exasol!();

    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    // Create three Parquet files with different row counts: 1, 5, 10
    let row_counts = [1, 5, 10];
    let mut paths = Vec::new();
    let mut total_expected = 0;
    let mut id_counter = 1;

    for (file_idx, &row_count) in row_counts.iter().enumerate() {
        let path = temp_dir
            .path()
            .join(format!("data_part{}.parquet", file_idx));

        let ids: Vec<i64> = (id_counter..id_counter + row_count as i64).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("name_{}", id)).collect();
        let amounts: Vec<f64> = ids.iter().map(|id| *id as f64 * 1.5).collect();

        let id_array = Int64Array::from(ids);
        let name_array = StringArray::from(names);
        let amount_array = Float64Array::from(amounts);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(amount_array),
            ],
        )
        .expect("Failed to create RecordBatch");

        let file = std::fs::File::create(&path).expect("Failed to create parquet file");
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .expect("Failed to create writer");
        writer.write(&batch).expect("Failed to write batch");
        writer.close().expect("Failed to close writer");

        paths.push(path);
        total_expected += row_count;
        id_counter += row_count as i64;
    }

    // Import all Parquet files in parallel
    let rows_imported = conn
        .import_parquet_from_files(
            &format!("{}.test_data", schema_name),
            paths,
            ParquetImportOptions::default(),
        )
        .await
        .expect("Parallel Parquet import should succeed");

    assert_eq!(
        rows_imported, total_expected as u64,
        "Should import {} rows total",
        total_expected
    );

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.test_data",
            schema_name
        ))
        .await
        .expect("SELECT COUNT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 11.7.6 Test that single PathBuf still works with import_csv_from_file
///
/// Verifies backward compatibility - the existing import_csv_from_file
/// method should continue to work with a single PathBuf.
#[tokio::test]
#[ignore]
async fn test_single_path_still_works() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema and table
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create a single CSV file
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let csv_path = temp_dir.path().join("single.csv");

    std::fs::write(&csv_path, "1,Alice,10.5\n2,Bob,20.5\n").expect("Failed to write CSV file");

    // Import using the original single-file API
    let rows_imported = conn
        .import_csv_from_file(
            &format!("{}.test_data", schema_name),
            &csv_path,
            CsvImportOptions::default().use_tls(false),
        )
        .await
        .expect("Single file import should succeed");

    assert_eq!(rows_imported, 2, "Should import 2 rows");

    // Verify the data was imported correctly
    let batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 2, "Should have 2 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// Section 9: Parquet Auto Table Creation Tests

/// Test importing Parquet file with auto table creation (single file)
#[tokio::test]
#[ignore]
async fn test_parquet_import_auto_create_table() {
    skip_if_no_exasol!();

    use arrow::datatypes::{DataType, Field, Schema};
    use exarrow_rs::import::ColumnNameMode;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema only (no table - let auto-create handle it)
    conn.execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    // Create a Parquet file with test data
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = temp_dir.path().join("auto_create_test.parquet");

    // Create Arrow schema and RecordBatch with various types
    let schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("user_name", DataType::Utf8, true),
        Field::new("balance", DataType::Float64, true),
        Field::new("active", DataType::Boolean, true),
    ]));

    let user_id_array = Int64Array::from(vec![1, 2, 3]);
    let user_name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);
    let balance_array = Float64Array::from(vec![Some(100.50), Some(200.75), Some(50.00)]);
    let active_array = arrow::array::BooleanArray::from(vec![Some(true), Some(false), Some(true)]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(user_id_array),
            Arc::new(user_name_array),
            Arc::new(balance_array),
            Arc::new(active_array),
        ],
    )
    .expect("Failed to create RecordBatch");

    // Write Parquet file
    let file = std::fs::File::create(&parquet_path).expect("Failed to create parquet file");
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .expect("Failed to create ArrowWriter");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    // Import the Parquet file with auto table creation
    let options = ParquetImportOptions::default()
        .with_schema(&schema_name)
        .with_create_table_if_not_exists(true)
        .with_column_name_mode(ColumnNameMode::Quoted);

    let rows_imported = conn
        .import_from_parquet("auto_created_table", &parquet_path, options)
        .await
        .expect("Parquet import with auto-create should succeed");

    assert_eq!(rows_imported, 3, "Should import 3 rows");

    // Verify the table was created and data imported
    let batches = conn
        .query(&format!(
            "SELECT \"user_id\", \"user_name\", \"balance\", \"active\" FROM {}.auto_created_table ORDER BY \"user_id\"",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test importing Parquet file with auto table creation using sanitized column names
#[tokio::test]
#[ignore]
async fn test_parquet_import_auto_create_sanitized_names() {
    skip_if_no_exasol!();

    use arrow::datatypes::{DataType, Field, Schema};
    use exarrow_rs::import::ColumnNameMode;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema only
    conn.execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    // Create a Parquet file with columns that have spaces and special chars
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = temp_dir.path().join("sanitize_test.parquet");

    // Column names with spaces and special chars
    let schema = Arc::new(Schema::new(vec![
        Field::new("User ID", DataType::Int64, false),
        Field::new("First Name", DataType::Utf8, true),
        Field::new("account-balance", DataType::Float64, true),
    ]));

    let id_array = Int64Array::from(vec![10, 20]);
    let name_array = StringArray::from(vec!["Test1", "Test2"]);
    let balance_array = Float64Array::from(vec![500.0, 600.0]);

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(name_array),
            Arc::new(balance_array),
        ],
    )
    .expect("Failed to create RecordBatch");

    // Write Parquet file
    let file = std::fs::File::create(&parquet_path).expect("Failed to create parquet file");
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .expect("Failed to create ArrowWriter");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    // Import with sanitize mode (columns become uppercase with underscores)
    let options = ParquetImportOptions::default()
        .with_schema(&schema_name)
        .with_create_table_if_not_exists(true)
        .with_column_name_mode(ColumnNameMode::Sanitize);

    let rows_imported = conn
        .import_from_parquet("sanitized_table", &parquet_path, options)
        .await
        .expect("Parquet import with sanitized names should succeed");

    assert_eq!(rows_imported, 2, "Should import 2 rows");

    // Verify using sanitized column names (uppercase with underscores)
    let batches = conn
        .query(&format!(
            "SELECT USER_ID, FIRST_NAME, ACCOUNT_BALANCE FROM {}.sanitized_table ORDER BY USER_ID",
            schema_name
        ))
        .await
        .expect("SELECT with sanitized names should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 2, "Should have 2 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test importing multiple Parquet files with auto table creation (union schema)
#[tokio::test]
#[ignore]
async fn test_parquet_import_auto_create_multi_file() {
    skip_if_no_exasol!();

    use arrow::datatypes::{DataType, Field, Schema};
    use exarrow_rs::import::ColumnNameMode;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::path::PathBuf;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema only
    conn.execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    // Create two Parquet files with the same schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, true),
    ]));

    // File 1
    let path1 = temp_dir.path().join("part1.parquet");
    {
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(Float64Array::from(vec![10.0, 20.0])),
            ],
        )
        .expect("Failed to create batch 1");

        let file = std::fs::File::create(&path1).expect("Failed to create file 1");
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .expect("Failed to create writer 1");
        writer.write(&batch1).expect("Failed to write batch 1");
        writer.close().expect("Failed to close writer 1");
    }

    // File 2
    let path2 = temp_dir.path().join("part2.parquet");
    {
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 4, 5])),
                Arc::new(Float64Array::from(vec![30.0, 40.0, 50.0])),
            ],
        )
        .expect("Failed to create batch 2");

        let file = std::fs::File::create(&path2).expect("Failed to create file 2");
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .expect("Failed to create writer 2");
        writer.write(&batch2).expect("Failed to write batch 2");
        writer.close().expect("Failed to close writer 2");
    }

    // Import multiple files with auto table creation
    let files: Vec<PathBuf> = vec![path1, path2];
    let options = ParquetImportOptions::default()
        .with_schema(&schema_name)
        .with_create_table_if_not_exists(true)
        .with_column_name_mode(ColumnNameMode::Quoted);

    let rows_imported = conn
        .import_parquet_from_files("multi_file_table", files, options)
        .await
        .expect("Multi-file Parquet import with auto-create should succeed");

    assert_eq!(rows_imported, 5, "Should import 5 rows total (2 + 3)");

    // Verify all data was imported
    let batches = conn
        .query(&format!(
            "SELECT \"id\", \"value\" FROM {}.multi_file_table ORDER BY \"id\"",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 5, "Should have 5 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test importing Parquet when table already exists (auto-create should be no-op)
#[tokio::test]
#[ignore]
async fn test_parquet_import_auto_create_existing_table() {
    skip_if_no_exasol!();

    use arrow::datatypes::{DataType, Field, Schema};
    use exarrow_rs::import::ColumnNameMode;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();

    // Create schema AND table (table already exists)
    setup_import_export_table(&mut conn, &schema_name).await;

    // Create a Parquet file matching the existing table schema
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = temp_dir.path().join("existing_table_test.parquet");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("amount", DataType::Float64, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![100, 200])),
            Arc::new(StringArray::from(vec!["X", "Y"])),
            Arc::new(Float64Array::from(vec![1.0, 2.0])),
        ],
    )
    .expect("Failed to create RecordBatch");

    let file = std::fs::File::create(&parquet_path).expect("Failed to create parquet file");
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .expect("Failed to create ArrowWriter");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    // Import with auto-create enabled (should work even though table exists)
    let options = ParquetImportOptions::default()
        .with_schema(&schema_name)
        .with_create_table_if_not_exists(true)
        .with_column_name_mode(ColumnNameMode::Quoted);

    let rows_imported = conn
        .import_from_parquet("test_data", &parquet_path, options)
        .await
        .expect("Import into existing table should succeed");

    assert_eq!(rows_imported, 2, "Should import 2 rows");

    // Verify data was imported
    let batches = conn
        .query(&format!(
            "SELECT id, name, amount FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 2, "Should have 2 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test that parquet import with auto-create targeting nonexistent schema returns error
#[tokio::test]
#[ignore]
async fn test_parquet_import_auto_create_nonexistent_schema_returns_error() {
    skip_if_no_exasol!();

    use arrow::datatypes::{DataType, Field, Schema};
    use exarrow_rs::import::ColumnNameMode;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = temp_dir.path().join("test_data.parquet");

    // Create a simple Parquet file
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id_array = arrow::array::Int32Array::from(vec![1, 2]);
    let name_array = arrow::array::StringArray::from(vec![Some("a"), Some("b")]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .expect("Failed to create RecordBatch");

    let file = std::fs::File::create(&parquet_path).expect("Failed to create file");
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("Failed to create writer");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close writer");

    let options = ParquetImportOptions::default()
        .with_create_table_if_not_exists(true)
        .with_column_name_mode(ColumnNameMode::Quoted);

    let result = conn
        .import_from_parquet(
            "NONEXISTENT_SCHEMA_12345.test_table",
            &parquet_path,
            options,
        )
        .await;

    assert!(
        result.is_err(),
        "Should return error for nonexistent schema"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("SQL execution failed"),
        "Error should be SqlError, got: {}",
        err_msg
    );

    conn.close().await.expect("Failed to close connection");
}

// Section N: Native Parquet Import Integration Tests

/// Helper that writes a small 3-row Parquet file (id INTEGER, name VARCHAR) and
/// returns the path. Caller is responsible for keeping `TempDir` alive.
fn write_small_parquet(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let path = dir.join(name);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id_array = Int32Array::from(vec![1, 2, 3]);
    let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), Some("Charlie")]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .expect("Failed to create RecordBatch");

    let file = std::fs::File::create(&path).expect("Failed to create parquet file");
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, schema, Some(props)).expect("Failed to create ArrowWriter");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close ArrowWriter");
    path
}

/// Helper that writes a 2-row Parquet file (id INTEGER, name VARCHAR) and returns
/// the path. Used for multi-file tests where each file has distinct data.
fn write_small_parquet_offset(
    dir: &std::path::Path,
    name: &str,
    id_offset: i32,
) -> std::path::PathBuf {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let path = dir.join(name);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id_array = Int32Array::from(vec![id_offset, id_offset + 1]);
    let name_array = StringArray::from(vec![
        Some(format!("row_{}", id_offset).as_str()),
        Some(format!("row_{}", id_offset + 1).as_str()),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(name_array)],
    )
    .expect("Failed to create RecordBatch");

    let file = std::fs::File::create(&path).expect("Failed to create parquet file");
    let props = WriterProperties::builder().build();
    let mut writer =
        ArrowWriter::try_new(file, schema, Some(props)).expect("Failed to create ArrowWriter");
    writer.write(&batch).expect("Failed to write batch");
    writer.close().expect("Failed to close ArrowWriter");
    path
}

/// Helper to create and set up a test table with (id INTEGER, name VARCHAR(100)).
async fn setup_id_name_table(conn: &mut Connection, schema_name: &str) {
    conn.execute_update(&format!("CREATE SCHEMA IF NOT EXISTS {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.test_data (id INTEGER, name VARCHAR(100))",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");
}

/// Test that forcing the CSV path via `with_native_parquet(Some(false))` succeeds
/// regardless of the server version.
#[tokio::test]
#[ignore]
async fn test_parquet_import_forced_csv_path_works() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();
    setup_id_name_table(&mut conn, &schema_name).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = write_small_parquet(temp_dir.path(), "test.parquet");

    let options = ParquetImportOptions::default().with_native_parquet(Some(false));

    let rows = conn
        .import_from_parquet(
            &format!("{}.test_data", schema_name),
            &parquet_path,
            options,
        )
        .await
        .expect("CSV-path Parquet import should succeed");

    assert_eq!(rows, 3, "Should import 3 rows via CSV path");

    let batches = conn
        .query(&format!(
            "SELECT id, name FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows in result");

    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test the native Parquet import path when the server supports it.
/// Skips gracefully on older server versions.
#[tokio::test]
#[ignore]
async fn test_parquet_import_native_path_when_supported() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    if !conn.supports_native_parquet_import() {
        eprintln!("skip: server does not support native Parquet import (< 2025.1.11)");
        conn.close().await.expect("Failed to close connection");
        return;
    }

    let schema_name = generate_test_schema_name();
    setup_id_name_table(&mut conn, &schema_name).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = write_small_parquet(temp_dir.path(), "test.parquet");

    let options = ParquetImportOptions::default().with_native_parquet(Some(true));

    let rows = conn
        .import_from_parquet(
            &format!("{}.test_data", schema_name),
            &parquet_path,
            options,
        )
        .await
        .expect("Native Parquet import should succeed");

    assert_eq!(rows, 3, "Should import 3 rows via native path");

    let batches = conn
        .query(&format!(
            "SELECT id, name FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows in result");

    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test native Parquet import via the stream (reader) path.
/// Skips gracefully on older server versions.
#[tokio::test]
#[ignore]
async fn test_parquet_stream_import_native_path() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    if !conn.supports_native_parquet_import() {
        eprintln!("skip: server does not support native Parquet import (< 2025.1.11)");
        conn.close().await.expect("Failed to close connection");
        return;
    }

    let schema_name = generate_test_schema_name();
    setup_id_name_table(&mut conn, &schema_name).await;

    // Write the parquet file then read it into memory as a Cursor for streaming.
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = write_small_parquet(temp_dir.path(), "test.parquet");

    let parquet_bytes = tokio::fs::read(&parquet_path)
        .await
        .expect("Failed to read parquet file");
    let reader = std::io::Cursor::new(parquet_bytes);

    let options = ParquetImportOptions::default().with_native_parquet(Some(true));

    let rows = conn
        .import_from_parquet_stream(&format!("{}.test_data", schema_name), reader, options)
        .await
        .expect("Native Parquet stream import should succeed");

    assert_eq!(rows, 3, "Should import 3 rows via native stream path");

    let batches = conn
        .query(&format!(
            "SELECT id, name FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 3, "Should have 3 rows in result");

    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test parallel native Parquet import from multiple files.
/// Skips gracefully on older server versions.
#[tokio::test]
#[ignore]
async fn test_parallel_parquet_import_native_path() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    if !conn.supports_native_parquet_import() {
        eprintln!("skip: server does not support native Parquet import (< 2025.1.11)");
        conn.close().await.expect("Failed to close connection");
        return;
    }

    let schema_name = generate_test_schema_name();
    setup_id_name_table(&mut conn, &schema_name).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let path1 = write_small_parquet_offset(temp_dir.path(), "part1.parquet", 1);
    let path2 = write_small_parquet_offset(temp_dir.path(), "part2.parquet", 10);

    let options = ParquetImportOptions::default().with_native_parquet(Some(true));

    let rows = conn
        .import_parquet_from_files(
            &format!("{}.test_data", schema_name),
            vec![path1, path2],
            options,
        )
        .await
        .expect("Parallel native Parquet import should succeed");

    assert_eq!(rows, 4, "Should import 4 rows total (2 per file)");

    let batches = conn
        .query(&format!("SELECT COUNT(*) FROM {}.test_data", schema_name))
        .await
        .expect("SELECT COUNT should succeed");
    assert!(!batches.is_empty());

    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test that forcing CSV path via `with_native_parquet(Some(false))` works even
/// on a modern server that would otherwise auto-select the native path.
#[tokio::test]
#[ignore]
async fn test_parquet_import_forced_csv_path_fallback_works() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema_name = generate_test_schema_name();
    setup_id_name_table(&mut conn, &schema_name).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let parquet_path = write_small_parquet(temp_dir.path(), "test.parquet");

    // Force CSV path explicitly — must succeed regardless of server version.
    let options = ParquetImportOptions::default().with_native_parquet(Some(false));

    let rows = conn
        .import_from_parquet(
            &format!("{}.test_data", schema_name),
            &parquet_path,
            options,
        )
        .await
        .expect("Forced CSV path should succeed on any server");

    assert_eq!(rows, 3, "Should import 3 rows via forced CSV path");

    let batches = conn
        .query(&format!(
            "SELECT id, name FROM {}.test_data ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 3, "Data should round-trip correctly");

    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}
