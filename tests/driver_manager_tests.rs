//! ADBC Driver Manager Integration Tests for exarrow-rs.
//!
//! These tests validate that the exarrow-rs driver can be loaded and used via the
//! ADBC driver manager, which simulates how external applications (Python, R, etc.)
//! would interact with the driver.
//!
//! # Prerequisites
//!
//! These tests require:
//!
//! 1. The FFI library to be built:
//!    ```bash
//!    cargo build --release --features ffi
//!    ```
//!
//! 2. An Exasol database container running:
//!    ```bash
//!    docker run -d --name exasol-test \
//!      -p 8563:8563 \
//!      --privileged \
//!      exasol/docker-db:latest
//!    ```
//!
//! # Running These Tests
//!
//! ```bash
//! # First, build the FFI library
//! cargo build --release --features ffi
//!
//! # Then run the driver manager tests
//! cargo test --test driver_manager_tests -- --ignored
//!
//! # Run with verbose output
//! cargo test --test driver_manager_tests -- --ignored --nocapture
//! ```

mod common;

use adbc_core::options::{
    AdbcVersion, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
};
use adbc_core::{Connection as AdbcConnection, Database, Driver, Optionable, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow::array::{
    Array, Int32Array, ListArray, RecordBatch, RecordBatchReader, StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use common::{get_host, get_password, get_port, get_user, is_exasol_available};
use std::path::Path;
use std::sync::Arc;

// Helper Functions

/// Get the path to the built shared library.
///
/// Returns the appropriate library path based on the operating system:
/// - macOS: `target/release/libexarrow_rs.dylib`
/// - Linux: `target/release/libexarrow_rs.so`
fn get_library_path() -> &'static str {
    if cfg!(target_os = "macos") {
        "target/release/libexarrow_rs.dylib"
    } else {
        "target/release/libexarrow_rs.so"
    }
}

/// Check if the FFI library has been built.
fn is_library_available() -> bool {
    Path::new(get_library_path()).exists()
}

/// Build the connection URI for tests.
/// TLS is required by Exasol and certificate validation is disabled
/// for integration tests since Exasol Docker uses self-signed certificates.
fn get_test_uri() -> String {
    format!(
        "exasol://{}:{}@{}:{}?tls=true&validateservercertificate=0",
        get_user(),
        get_password(),
        get_host(),
        get_port()
    )
}

/// Skip test if Exasol is not available.
macro_rules! skip_if_no_exasol {
    () => {
        if !is_exasol_available() {
            eprintln!(
                "Skipping test: Exasol not available at {}:{}",
                get_host(),
                get_port()
            );
            return;
        }
    };
}

/// Skip test if the FFI library is not built.
macro_rules! skip_if_no_library {
    () => {
        if !is_library_available() {
            eprintln!(
                "Skipping test: FFI library not built. Run: cargo build --release --features ffi"
            );
            return;
        }
    };
}

// Section 10.2 & 10.3: Driver Loading Tests

/// Test that the driver can be loaded via the driver manager.
///
/// This is the foundational test - if this fails, all other driver manager
/// tests will fail.
#[test]

fn test_driver_manager_loads_driver() {
    skip_if_no_library!();

    let lib_path = get_library_path();

    // Load the driver using ManagedDriver
    let driver_result = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    );

    match driver_result {
        Ok(_driver) => {
            // Driver loaded successfully
            println!("Driver loaded successfully from: {}", lib_path);
        }
        Err(e) => {
            panic!("Failed to load driver from {}: {:?}", lib_path, e);
        }
    }
}

// Section 10.4: Database Creation Tests

/// Test creating a database via the driver manager.
#[test]

fn test_driver_manager_creates_database() {
    skip_if_no_library!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    // Create a database
    let db_result = driver.new_database();
    assert!(db_result.is_ok(), "Database creation should succeed");

    let _db = db_result.unwrap();
    println!("Database created successfully via driver manager");
}

/// Test creating a database with options via the driver manager.
#[test]

fn test_driver_manager_creates_database_with_options() {
    skip_if_no_library!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];

    let db_result = driver.new_database_with_opts(opts);
    assert!(
        db_result.is_ok(),
        "Database creation with options should succeed: {:?}",
        db_result.err()
    );

    println!("Database created with URI option via driver manager");
}

// Section 10.5: Connection Establishment Tests

/// Test establishing a connection via the driver manager.
#[test]

fn test_driver_manager_establishes_connection() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    // Create database with URI
    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri.clone()))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    // Create connection
    let conn_result = db.new_connection();
    assert!(
        conn_result.is_ok(),
        "Connection creation should succeed: {:?}",
        conn_result.err()
    );

    println!(
        "Connection established successfully via driver manager to {}",
        uri
    );
}

// Section 10.6: Query Execution Tests

/// Test executing a simple SELECT query via the driver manager.
#[test]

fn test_driver_manager_executes_select_from_dual() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    // Create database with URI
    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    // Create connection
    let mut conn = db.new_connection().expect("Failed to create connection");

    // Create statement
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    // Set SQL query - Exasol supports SELECT without FROM for literals
    stmt.set_sql_query("SELECT 42 AS answer")
        .expect("Failed to set SQL query");

    // Execute query
    let reader_result = stmt.execute();
    assert!(
        reader_result.is_ok(),
        "Query execution should succeed: {:?}",
        reader_result.err()
    );

    println!("SELECT from DUAL executed successfully via driver manager");
}

/// Test executing arithmetic expressions via the driver manager.
#[test]

fn test_driver_manager_executes_arithmetic() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    stmt.set_sql_query("SELECT 1+1 AS sum, 10*5 AS product, 100/4 AS quotient")
        .expect("Failed to set SQL query");

    let reader_result = stmt.execute();
    assert!(
        reader_result.is_ok(),
        "Arithmetic query should succeed: {:?}",
        reader_result.err()
    );

    println!("Arithmetic expressions executed successfully via driver manager");
}

// Section 10.7: Result Retrieval Tests

/// Test retrieving results as Arrow RecordBatch via the driver manager.
#[test]

fn test_driver_manager_retrieves_arrow_recordbatch() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    stmt.set_sql_query("SELECT 42 AS answer, 'hello' AS greeting")
        .expect("Failed to set SQL query");

    let mut reader = stmt.execute().expect("Failed to execute query");

    // Get schema
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 2, "Schema should have 2 fields");

    // Verify field names (Exasol uppercases identifiers)
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"ANSWER"),
        "Schema should contain ANSWER field"
    );
    assert!(
        field_names.contains(&"GREETING"),
        "Schema should contain GREETING field"
    );

    // Collect all batches
    let mut total_rows = 0;
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Failed to read batch");
        total_rows += batch.num_rows();
        assert_eq!(batch.num_columns(), 2, "Each batch should have 2 columns");
    }

    assert_eq!(total_rows, 1, "Should have exactly 1 row");
    println!("Arrow RecordBatch retrieved successfully via driver manager");
}

/// Test retrieving multiple rows as Arrow RecordBatch.
#[test]

fn test_driver_manager_retrieves_multiple_rows() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    // Use a query that generates multiple rows
    stmt.set_sql_query(
        "SELECT LEVEL AS id, 'Row ' || LEVEL AS label FROM DUAL CONNECT BY LEVEL <= 10",
    )
    .expect("Failed to set SQL query");

    let mut reader = stmt.execute().expect("Failed to execute query");

    // Count total rows
    let mut total_rows = 0;
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Failed to read batch");
        total_rows += batch.num_rows();
    }

    assert_eq!(total_rows, 10, "Should have exactly 10 rows");
    println!("Multiple rows retrieved successfully via driver manager");
}

/// Test schema validation of retrieved Arrow data.
#[test]

fn test_driver_manager_validates_arrow_schema() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    stmt.set_sql_query(
        "SELECT 123 AS int_val, 'text' AS str_val, TRUE AS bool_val, 3.14 AS float_val",
    )
    .expect("Failed to set SQL query");

    let reader = stmt.execute().expect("Failed to execute query");

    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 4, "Schema should have 4 fields");

    // Verify each field has an appropriate Arrow type
    for field in schema.fields() {
        let name = field.name().as_str();
        let dtype = field.data_type();

        match name {
            "INT_VAL" => {
                assert!(
                    matches!(
                        dtype,
                        DataType::Int64
                            | DataType::Int32
                            | DataType::Decimal128(_, _)
                            | DataType::Float64
                    ),
                    "INT_VAL should be numeric, got {:?}",
                    dtype
                );
            }
            "STR_VAL" => {
                assert!(
                    matches!(dtype, DataType::Utf8 | DataType::LargeUtf8),
                    "STR_VAL should be string, got {:?}",
                    dtype
                );
            }
            "BOOL_VAL" => {
                assert_eq!(
                    dtype,
                    &DataType::Boolean,
                    "BOOL_VAL should be Boolean, got {:?}",
                    dtype
                );
            }
            "FLOAT_VAL" => {
                assert!(
                    matches!(dtype, DataType::Float64 | DataType::Decimal128(_, _)),
                    "FLOAT_VAL should be Float64 or Decimal128, got {:?}",
                    dtype
                );
            }
            _ => {}
        }
    }

    println!("Arrow schema validation passed via driver manager");
}

// Section 10.8: Full Workflow Comparison Tests

/// Test that driver manager results match direct API results.
///
/// This is the key verification test - it ensures that using the driver
/// through the FFI/driver manager produces the same results as using
/// the direct Rust API.
#[test]

fn test_driver_manager_matches_direct_api() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    // Run the direct API part in an explicit runtime (not #[tokio::test] to avoid nested runtime)
    let direct_batches = {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let mut direct_conn = common::get_test_connection()
                .await
                .expect("Failed to connect via direct API");

            let direct_batches = direct_conn
                .query("SELECT 42 AS answer, 'test' AS label")
                .await
                .expect("Failed to query via direct API");

            direct_conn
                .close()
                .await
                .expect("Failed to close direct connection");

            direct_batches
        })
    };

    // Now, get results via driver manager (synchronous - uses FFI runtime internally)
    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");
    let mut stmt = conn.new_statement().expect("Failed to create statement");

    stmt.set_sql_query("SELECT 42 AS answer, 'test' AS label")
        .expect("Failed to set SQL query");

    let mut reader = stmt.execute().expect("Failed to execute query");

    // Collect driver manager batches
    let mut dm_batches = Vec::new();
    for batch_result in reader.by_ref() {
        dm_batches.push(batch_result.expect("Failed to read batch"));
    }

    // Compare schemas
    assert!(
        !direct_batches.is_empty(),
        "Direct API should return batches"
    );
    assert!(
        !dm_batches.is_empty(),
        "Driver manager should return batches"
    );

    let direct_schema = direct_batches[0].schema();
    let dm_schema = dm_batches[0].schema();

    assert_eq!(
        direct_schema.fields().len(),
        dm_schema.fields().len(),
        "Schema field counts should match"
    );

    // Compare field names
    for (direct_field, dm_field) in direct_schema.fields().iter().zip(dm_schema.fields().iter()) {
        assert_eq!(
            direct_field.name(),
            dm_field.name(),
            "Field names should match"
        );
    }

    // Compare row counts
    let direct_rows: usize = direct_batches.iter().map(|b| b.num_rows()).sum();
    let dm_rows: usize = dm_batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(direct_rows, dm_rows, "Row counts should match");

    println!(
        "Driver manager results match direct API: {} rows, {} fields",
        dm_rows,
        dm_schema.fields().len()
    );
}

/// Test execute_update via driver manager.
#[test]

fn test_driver_manager_execute_update() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Create a unique schema name for this test
    let schema_name = format!(
        "TEST_DM_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .expect("Failed to set SQL query");
        let result = stmt.execute_update();
        assert!(
            result.is_ok(),
            "CREATE SCHEMA should succeed: {:?}",
            result.err()
        );
    }

    // Create table
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "CREATE TABLE {}.test_table (id INTEGER, name VARCHAR(100))",
            schema_name
        ))
        .expect("Failed to set SQL query");
        let result = stmt.execute_update();
        assert!(
            result.is_ok(),
            "CREATE TABLE should succeed: {:?}",
            result.err()
        );
    }

    // Insert row
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "INSERT INTO {}.test_table (id, name) VALUES (1, 'test')",
            schema_name
        ))
        .expect("Failed to set SQL query");
        let result = stmt.execute_update();
        assert!(result.is_ok(), "INSERT should succeed: {:?}", result.err());
        if let Ok(Some(count)) = result {
            assert_eq!(count, 1, "INSERT should affect 1 row");
        }
    }

    // Verify data
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "SELECT COUNT(*) AS cnt FROM {}.test_table",
            schema_name
        ))
        .expect("Failed to set SQL query");
        let mut reader = stmt.execute().expect("Failed to execute query");

        let mut total_rows = 0;
        for batch_result in reader.by_ref() {
            let batch = batch_result.expect("Failed to read batch");
            total_rows += batch.num_rows();
        }
        assert_eq!(total_rows, 1, "COUNT query should return 1 row");
    }

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .expect("Failed to set SQL query");
        let _ = stmt.execute_update();
    }

    println!("execute_update operations successful via driver manager");
}

/// Test that multiple statements from the same connection reuse the same Exasol session.
///
/// This validates the "Connection Session Identity" requirement: each statement
/// should execute on the parent connection's existing WebSocket session rather
/// than opening a new one.
#[test]

fn test_driver_manager_reuses_session_across_statements() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Statement 1: get session ID (cast to VARCHAR so Arrow returns Utf8)
    let session_id_1 = {
        let mut stmt = conn.new_statement().expect("Failed to create statement 1");
        stmt.set_sql_query("SELECT CAST(CURRENT_SESSION AS VARCHAR(40)) AS SID")
            .expect("Failed to set SQL query");
        let mut reader = stmt.execute().expect("Failed to execute statement 1");
        let batch = reader.next().unwrap().expect("Failed to read batch");
        let col = batch.column(0);
        let string_array = col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected string array for CURRENT_SESSION");
        string_array.value(0).to_string()
    };

    // Statement 2: get session ID
    let session_id_2 = {
        let mut stmt = conn.new_statement().expect("Failed to create statement 2");
        stmt.set_sql_query("SELECT CAST(CURRENT_SESSION AS VARCHAR(40)) AS SID")
            .expect("Failed to set SQL query");
        let mut reader = stmt.execute().expect("Failed to execute statement 2");
        let batch = reader.next().unwrap().expect("Failed to read batch");
        let col = batch.column(0);
        let string_array = col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("Expected string array for CURRENT_SESSION");
        string_array.value(0).to_string()
    };

    assert_eq!(
        session_id_1, session_id_2,
        "Expected same session but got {} vs {} — statements are creating new connections",
        session_id_1, session_id_2
    );

    println!(
        "Session reuse verified: both statements used session {}",
        session_id_1
    );
}

/// Test get_info via driver manager.
#[test]

fn test_driver_manager_get_info() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Ensure connection is established by creating a statement
    {
        let _stmt = conn.new_statement().expect("Failed to create statement");
    }

    // Get driver info
    let info_result = conn.get_info(None);
    assert!(
        info_result.is_ok(),
        "get_info should succeed: {:?}",
        info_result.err()
    );

    let mut reader = info_result.unwrap();
    let schema = reader.schema();

    // Verify info schema has expected fields
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"info_name"),
        "Info schema should have info_name field"
    );
    assert!(
        field_names.contains(&"info_value"),
        "Info schema should have info_value field"
    );

    // Read info batches
    let mut has_data = false;
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Failed to read info batch");
        if batch.num_rows() > 0 {
            has_data = true;
        }
    }

    assert!(has_data, "get_info should return driver information");
    println!("get_info returned driver information via driver manager");
}

/// Test get_table_types via driver manager.
#[test]

fn test_driver_manager_get_table_types() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();

    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Ensure connection is established
    {
        let _stmt = conn.new_statement().expect("Failed to create statement");
    }

    // Get table types
    let types_result = conn.get_table_types();
    assert!(
        types_result.is_ok(),
        "get_table_types should succeed: {:?}",
        types_result.err()
    );

    let mut reader = types_result.unwrap();

    // Collect table types
    let mut table_types = Vec::new();
    for batch_result in reader.by_ref() {
        let batch = batch_result.expect("Failed to read table types batch");
        if batch.num_rows() > 0 {
            let type_col = batch.column(0);
            if let Some(string_array) = type_col
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
            {
                for i in 0..string_array.len() {
                    if !string_array.is_null(i) {
                        table_types.push(string_array.value(i).to_string());
                    }
                }
            }
        }
    }

    // Verify expected table types
    assert!(!table_types.is_empty(), "Should return table types");
    assert!(
        table_types.contains(&"TABLE".to_string()),
        "Should include TABLE type"
    );
    assert!(
        table_types.contains(&"VIEW".to_string()),
        "Should include VIEW type"
    );

    println!(
        "get_table_types returned: {:?} via driver manager",
        table_types
    );
}

// Bulk Ingestion Tests

fn make_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
        ],
    )
    .unwrap()
}

#[test]

fn test_bulk_ingest_create() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let schema_name = format!(
        "TEST_INGEST_CREATE_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }

    // Bulk ingest with Create mode
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String(format!("{}.INGEST_TABLE", schema_name)),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.mode".to_string()),
            OptionValue::String("adbc.ingest.mode.create".to_string()),
        )
        .unwrap();

        let batch = make_test_batch();
        stmt.bind(batch).unwrap();
        let result = stmt.execute_update();
        assert!(
            result.is_ok(),
            "Bulk ingest create should succeed: {:?}",
            result.err()
        );
    }

    // Verify data
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "SELECT COUNT(*) AS cnt FROM {}.INGEST_TABLE",
            schema_name
        ))
        .unwrap();
        let mut reader = stmt.execute().expect("Failed to execute query");
        let batch = reader.next().unwrap().expect("Failed to read batch");
        let col = batch.column(0);
        assert!(batch.num_rows() > 0);
        println!(
            "Bulk ingest create: verified data in table, count column type: {:?}",
            col.data_type()
        );
    }

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}

#[test]

fn test_bulk_ingest_append() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let schema_name = format!(
        "TEST_INGEST_APPEND_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema and table
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "CREATE TABLE {}.APPEND_TABLE (\"id\" DECIMAL(18,0) NOT NULL, \"name\" VARCHAR(2000000))",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // Bulk ingest with Append mode (default)
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String(format!("{}.APPEND_TABLE", schema_name)),
        )
        .unwrap();

        let batch = make_test_batch();
        stmt.bind(batch).unwrap();
        let result = stmt.execute_update();
        assert!(
            result.is_ok(),
            "Bulk ingest append should succeed: {:?}",
            result.err()
        );
    }

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}

#[test]

fn test_bulk_ingest_replace() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let schema_name = format!(
        "TEST_INGEST_REPLACE_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema and initial table with data
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }

    // Create and ingest first
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String(format!("{}.REPLACE_TABLE", schema_name)),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.mode".to_string()),
            OptionValue::String("adbc.ingest.mode.create".to_string()),
        )
        .unwrap();
        stmt.bind(make_test_batch()).unwrap();
        stmt.execute_update().unwrap();
    }

    // Replace with new data
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String(format!("{}.REPLACE_TABLE", schema_name)),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.mode".to_string()),
            OptionValue::String("adbc.ingest.mode.replace".to_string()),
        )
        .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![10])),
                Arc::new(StringArray::from(vec!["replaced"])),
            ],
        )
        .unwrap();
        stmt.bind(batch).unwrap();
        let result = stmt.execute_update();
        assert!(
            result.is_ok(),
            "Bulk ingest replace should succeed: {:?}",
            result.err()
        );
    }

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}

#[test]

fn test_bulk_ingest_create_append() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let schema_name = format!(
        "TEST_INGEST_CRAPPEND_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }

    // First create_append: creates table
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String(format!("{}.CA_TABLE", schema_name)),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.mode".to_string()),
            OptionValue::String("adbc.ingest.mode.create_append".to_string()),
        )
        .unwrap();
        stmt.bind(make_test_batch()).unwrap();
        stmt.execute_update().unwrap();
    }

    // Second create_append: appends to existing table
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.target_table".to_string()),
            OptionValue::String(format!("{}.CA_TABLE", schema_name)),
        )
        .unwrap();
        stmt.set_option(
            OptionStatement::Other("adbc.ingest.mode".to_string()),
            OptionValue::String("adbc.ingest.mode.create_append".to_string()),
        )
        .unwrap();
        stmt.bind(make_test_batch()).unwrap();
        let result = stmt.execute_update();
        assert!(
            result.is_ok(),
            "Second create_append should succeed: {:?}",
            result.err()
        );
    }

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}

// Transaction Tests

#[test]

fn test_transaction_commit() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let schema_name = format!(
        "TEST_TXN_COMMIT_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema and table with autocommit on (default)
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "CREATE TABLE {}.TXN_TABLE (id INTEGER, name VARCHAR(100))",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // Disable autocommit
    conn.set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();

    // Insert data
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "INSERT INTO {}.TXN_TABLE VALUES (1, 'committed')",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // Commit
    conn.commit().unwrap();

    // Re-enable autocommit
    conn.set_option(OptionConnection::AutoCommit, "true".into())
        .unwrap();

    // Verify data persisted
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "SELECT COUNT(*) AS cnt FROM {}.TXN_TABLE",
            schema_name
        ))
        .unwrap();
        let mut reader = stmt.execute().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert!(batch.num_rows() > 0, "Committed data should be visible");
    }

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}

#[test]

fn test_transaction_rollback() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let schema_name = format!(
        "TEST_TXN_ROLLBACK_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema and table with autocommit on
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "CREATE TABLE {}.TXN_TABLE (id INTEGER, name VARCHAR(100))",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // Disable autocommit
    conn.set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();

    // Insert data
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "INSERT INTO {}.TXN_TABLE VALUES (1, 'will_rollback')",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // Rollback
    conn.rollback().unwrap();

    // Re-enable autocommit
    conn.set_option(OptionConnection::AutoCommit, "true".into())
        .unwrap();

    // Verify data was rolled back
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "SELECT COUNT(*) AS cnt FROM {}.TXN_TABLE WHERE name = 'will_rollback'",
            schema_name
        ))
        .unwrap();
        let mut reader = stmt.execute().unwrap();
        let batch = reader.next().unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>();
        if let Some(arr) = col {
            assert_eq!(arr.value(0), 0, "Rolled back data should not be visible");
        }
    }

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}

#[test]
fn test_commit_after_rollback_succeeds() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Disable autocommit
    conn.set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();

    // Execute a SELECT to start a transaction
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query("SELECT 1 FROM DUAL").unwrap();
        let mut reader = stmt.execute().unwrap();
        let _batch = reader.next().unwrap().unwrap();
    }

    // Rollback
    conn.rollback().unwrap();

    // Commit after rollback should succeed (no active transaction is a no-op)
    conn.commit().unwrap();

    // Re-enable autocommit
    conn.set_option(OptionConnection::AutoCommit, "true".into())
        .unwrap();
}

#[test]
fn test_rollback_without_transaction_succeeds() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Disable autocommit
    conn.set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();

    // Rollback without any prior statement should succeed
    conn.rollback().unwrap();

    // Re-enable autocommit
    conn.set_option(OptionConnection::AutoCommit, "true".into())
        .unwrap();
}

#[test]

fn test_autocommit_default() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let conn = db.new_connection().expect("Failed to create connection");

    let auto_commit = conn
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(auto_commit, "true", "AutoCommit should default to true");
}

#[test]

fn test_autocommit_toggle() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    // Ensure connection is established
    {
        let _stmt = conn.new_statement().expect("Failed to create statement");
    }

    // Default should be true
    let auto_commit = conn
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(auto_commit, "true");

    // Toggle to false
    conn.set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();
    let auto_commit = conn
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(auto_commit, "false");

    // Toggle back to true
    conn.set_option(OptionConnection::AutoCommit, "true".into())
        .unwrap();
    let auto_commit = conn
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(auto_commit, "true");
}

// GetObjects Tests

/// Helper macro to set up a driver manager connection.
macro_rules! setup_driver_manager_conn {
    ($driver:ident, $db:ident, $conn:ident) => {
        let lib_path = get_library_path();
        let mut $driver = ManagedDriver::load_dynamic_from_filename(
            lib_path,
            Some(b"ExarrowDriverInit"),
            AdbcVersion::V110,
        )
        .expect("Failed to load driver");

        let uri = get_test_uri();
        let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
        let $db = $driver
            .new_database_with_opts(opts)
            .expect("Failed to create database");

        let mut $conn = $db.new_connection().expect("Failed to create connection");

        // Ensure connection is established
        {
            let _stmt = $conn.new_statement().expect("Failed to create statement");
        }
    };
}

/// Combined test for get_objects at all depth levels via driver manager.
///
/// Creates a dedicated test schema with a table so that the test does not
/// depend on system schemas (which may not appear in EXA_ALL_SCHEMAS on a
/// fresh CI container).
#[test]
fn test_driver_manager_get_objects() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    setup_driver_manager_conn!(_driver, _db, conn);

    let schema_name = format!(
        "TEST_GET_OBJECTS_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create test schema and table
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "CREATE TABLE {}.TEST_TABLE (ID INT, NAME VARCHAR(100))",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // --- Test Catalogs depth ---
    {
        let mut reader = conn
            .get_objects(ObjectDepth::Catalogs, None, None, None, None, None)
            .expect("get_objects(Catalogs) should succeed");

        let mut total_rows = 0;
        let mut catalog_names = Vec::new();
        let mut schemas_all_null = true;

        for batch_result in reader.by_ref() {
            let batch: RecordBatch = batch_result.expect("Failed to read batch");
            total_rows += batch.num_rows();

            let catalog_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("catalog_name should be StringArray");

            for i in 0..catalog_col.len() {
                if !catalog_col.is_null(i) {
                    catalog_names.push(catalog_col.value(i).to_string());
                }
            }

            let schemas_col = batch.column(1);
            for i in 0..batch.num_rows() {
                if !schemas_col.is_null(i) {
                    schemas_all_null = false;
                }
            }
        }

        assert_eq!(total_rows, 1, "Should return exactly 1 catalog row");
        assert_eq!(catalog_names, vec!["EXA"], "Catalog name should be 'EXA'");
        assert!(
            schemas_all_null,
            "catalog_db_schemas should be null at Catalogs depth"
        );
    }

    // --- Test Schemas depth ---
    {
        let mut reader = conn
            .get_objects(ObjectDepth::Schemas, None, None, None, None, None)
            .expect("get_objects(Schemas) should succeed");

        let mut found_catalog = false;
        let mut schema_names = Vec::new();

        for batch_result in reader.by_ref() {
            let batch: RecordBatch = batch_result.expect("Failed to read batch");

            let catalog_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("catalog_name should be StringArray");

            let schemas_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("catalog_db_schemas should be ListArray");

            for i in 0..batch.num_rows() {
                if !catalog_col.is_null(i) && catalog_col.value(i) == "EXA" {
                    found_catalog = true;

                    let schema_list = schemas_col.value(i);
                    let schema_structs = schema_list
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("Schema list values should be StructArray");

                    let name_col = schema_structs
                        .column_by_name("db_schema_name")
                        .expect("Schema struct should have db_schema_name")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("db_schema_name should be StringArray");

                    for j in 0..schema_structs.len() {
                        if !name_col.is_null(j) {
                            schema_names.push(name_col.value(j).to_string());
                        }

                        if let Some(tables_col) = schema_structs.column_by_name("db_schema_tables")
                        {
                            assert!(
                                tables_col.is_null(j),
                                "db_schema_tables should be null at Schemas depth"
                            );
                        }
                    }
                }
            }
        }

        assert!(found_catalog, "Should find catalog 'EXA'");
        assert!(
            !schema_names.is_empty(),
            "Should return at least one schema"
        );
        assert!(
            schema_names.contains(&schema_name),
            "Should contain test schema '{}', got: {:?}",
            schema_name,
            schema_names
        );
    }

    // --- Test Tables depth ---
    {
        let mut reader = conn
            .get_objects(
                ObjectDepth::Tables,
                None,
                Some(schema_name.as_str()),
                None,
                None,
                None,
            )
            .expect("get_objects(Tables) should succeed");

        let mut table_names = Vec::new();
        let mut table_types = Vec::new();

        for batch_result in reader.by_ref() {
            let batch: RecordBatch = batch_result.expect("Failed to read batch");

            let schemas_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("catalog_db_schemas should be ListArray");

            for i in 0..batch.num_rows() {
                if schemas_col.is_null(i) {
                    continue;
                }

                let schema_list = schemas_col.value(i);
                let schema_structs = schema_list
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Schema list values should be StructArray");

                let tables_list_col = schema_structs
                    .column_by_name("db_schema_tables")
                    .expect("Schema struct should have db_schema_tables");

                let tables_list = tables_list_col
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("db_schema_tables should be ListArray");

                for j in 0..schema_structs.len() {
                    if tables_list.is_null(j) {
                        continue;
                    }

                    let table_array = tables_list.value(j);
                    let table_structs = table_array
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("Table list values should be StructArray");

                    let name_col = table_structs
                        .column_by_name("table_name")
                        .expect("Table struct should have table_name")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("table_name should be StringArray");

                    let type_col = table_structs
                        .column_by_name("table_type")
                        .expect("Table struct should have table_type")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("table_type should be StringArray");

                    for k in 0..table_structs.len() {
                        if !name_col.is_null(k) {
                            table_names.push(name_col.value(k).to_string());
                        }
                        if !type_col.is_null(k) {
                            let tt = type_col.value(k).to_string();
                            if !table_types.contains(&tt) {
                                table_types.push(tt);
                            }
                        }
                    }
                }
            }
        }

        assert!(
            table_names.contains(&"TEST_TABLE".to_string()),
            "Should find TEST_TABLE, got: {:?}",
            table_names
        );
        assert!(
            table_types.contains(&"TABLE".to_string()),
            "Should have TABLE type, got: {:?}",
            table_types
        );
    }

    // --- Test All depth ---
    {
        let mut reader = conn
            .get_objects(
                ObjectDepth::All,
                None,
                Some(schema_name.as_str()),
                None,
                None,
                None,
            )
            .expect("get_objects(All) should succeed");

        let mut found_columns = false;
        let mut column_names = Vec::new();

        for batch_result in reader.by_ref() {
            let batch: RecordBatch = batch_result.expect("Failed to read batch");

            let schemas_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("catalog_db_schemas should be ListArray");

            for i in 0..batch.num_rows() {
                if schemas_col.is_null(i) {
                    continue;
                }

                let schema_list = schemas_col.value(i);
                let schema_structs = schema_list
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Schema list values should be StructArray");

                let tables_list_col = schema_structs
                    .column_by_name("db_schema_tables")
                    .expect("Schema struct should have db_schema_tables");

                let tables_list = tables_list_col
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("db_schema_tables should be ListArray");

                for j in 0..schema_structs.len() {
                    if tables_list.is_null(j) {
                        continue;
                    }

                    let table_array = tables_list.value(j);
                    let table_structs = table_array
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("Table list values should be StructArray");

                    let columns_list_col = table_structs
                        .column_by_name("table_columns")
                        .expect("Table struct should have table_columns");

                    let columns_list = columns_list_col
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .expect("table_columns should be ListArray");

                    for k in 0..table_structs.len() {
                        if columns_list.is_null(k) {
                            continue;
                        }

                        let col_array = columns_list.value(k);
                        let col_structs = col_array
                            .as_any()
                            .downcast_ref::<StructArray>()
                            .expect("Column list values should be StructArray");

                        if col_structs.len() > 0 {
                            found_columns = true;

                            let col_name = col_structs
                                .column_by_name("column_name")
                                .expect("Column struct should have column_name")
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .expect("column_name should be StringArray");

                            for idx in 0..col_name.len() {
                                if !col_name.is_null(idx) {
                                    column_names.push(col_name.value(idx).to_string());
                                }
                            }

                            let ordinal = col_structs
                                .column_by_name("ordinal_position")
                                .expect("Column struct should have ordinal_position");
                            assert!(!ordinal.is_empty(), "ordinal_position should not be empty");
                        }
                    }
                }
            }
        }

        assert!(
            found_columns,
            "Should find columns at All depth for test schema"
        );
        assert!(
            column_names.contains(&"ID".to_string()),
            "Should find column ID, got: {:?}",
            column_names
        );
        assert!(
            column_names.contains(&"NAME".to_string()),
            "Should find column NAME, got: {:?}",
            column_names
        );
    }

    // --- Test schema filter ---
    {
        let mut reader = conn
            .get_objects(
                ObjectDepth::Schemas,
                None,
                Some(schema_name.as_str()),
                None,
                None,
                None,
            )
            .expect("get_objects with schema filter should succeed");

        let mut schema_names = Vec::new();

        for batch_result in reader.by_ref() {
            let batch: RecordBatch = batch_result.expect("Failed to read batch");

            let schemas_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("catalog_db_schemas should be ListArray");

            for i in 0..batch.num_rows() {
                if schemas_col.is_null(i) {
                    continue;
                }

                let schema_list = schemas_col.value(i);
                let schema_structs = schema_list
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Schema list values should be StructArray");

                let name_col = schema_structs
                    .column_by_name("db_schema_name")
                    .expect("Schema struct should have db_schema_name")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("db_schema_name should be StringArray");

                for j in 0..schema_structs.len() {
                    if !name_col.is_null(j) {
                        schema_names.push(name_col.value(j).to_string());
                    }
                }
            }
        }

        assert!(
            !schema_names.is_empty(),
            "Should return at least one schema matching filter"
        );

        for name in &schema_names {
            assert_eq!(
                name, &schema_name,
                "All returned schemas should match the filter '{}', got: {}",
                schema_name, name
            );
        }
    }

    // --- Test table_type filter (VIEW only) ---
    {
        let mut reader = conn
            .get_objects(
                ObjectDepth::Tables,
                None,
                Some(schema_name.as_str()),
                None,
                Some(vec!["VIEW"]),
                None,
            )
            .expect("get_objects with table_type filter should succeed");

        for batch_result in reader.by_ref() {
            let batch: RecordBatch = batch_result.expect("Failed to read batch");

            let schemas_col = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("catalog_db_schemas should be ListArray");

            for i in 0..batch.num_rows() {
                if schemas_col.is_null(i) {
                    continue;
                }

                let schema_list = schemas_col.value(i);
                let schema_structs = schema_list
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("Schema list values should be StructArray");

                let tables_list_col = schema_structs
                    .column_by_name("db_schema_tables")
                    .expect("Schema struct should have db_schema_tables");

                let tables_list = tables_list_col
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .expect("db_schema_tables should be ListArray");

                for j in 0..schema_structs.len() {
                    if tables_list.is_null(j) {
                        continue;
                    }

                    let table_array = tables_list.value(j);
                    let table_structs = table_array
                        .as_any()
                        .downcast_ref::<StructArray>()
                        .expect("Table list values should be StructArray");

                    let type_col = table_structs
                        .column_by_name("table_type")
                        .expect("Table struct should have table_type")
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("table_type should be StringArray");

                    for k in 0..table_structs.len() {
                        if !type_col.is_null(k) {
                            let tt = type_col.value(k).to_string();
                            assert_eq!(
                                tt, "VIEW",
                                "All returned tables should be VIEW when filtered, got: {}",
                                tt
                            );
                        }
                    }
                }
            }
        }
    }

    // Cleanup: drop test schema
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}

#[test]

fn test_get_table_schema_timestamp_with_precision() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let schema_name = format!(
        "TEST_TS_PREC_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema and table with TIMESTAMP(3) column
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "CREATE TABLE {}.TS_TABLE (id INTEGER, ts3 TIMESTAMP(3), ts6 TIMESTAMP(6))",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // Call get_table_schema and verify it succeeds
    let table_schema = conn
        .get_table_schema(None, Some(&schema_name), "TS_TABLE")
        .expect("get_table_schema should not fail for TIMESTAMP(3) columns");

    assert_eq!(
        table_schema.fields().len(),
        3,
        "Expected 3 fields in schema"
    );

    // Verify the timestamp fields have the correct Arrow type
    let ts3_field = table_schema.field(1);
    assert_eq!(ts3_field.name(), "TS3");
    assert!(
        matches!(ts3_field.data_type(), DataType::Timestamp(_, _)),
        "Expected Timestamp type for TS3, got: {:?}",
        ts3_field.data_type()
    );

    let ts6_field = table_schema.field(2);
    assert_eq!(ts6_field.name(), "TS6");
    assert!(
        matches!(ts6_field.data_type(), DataType::Timestamp(_, _)),
        "Expected Timestamp type for TS6, got: {:?}",
        ts6_field.data_type()
    );

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}

/// Test that get_table_schema succeeds for a table containing all Exasol data types.
///
/// This verifies that the type parser handles every Exasol type without
/// producing "Unknown Exasol type" errors.
#[test]

fn test_get_table_schema_all_types() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let schema_name = format!(
        "TEST_ALL_TYPES_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }

    // Create table with all Exasol data types
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "CREATE TABLE {}.ALL_TYPES (\
                col_boolean BOOLEAN, \
                col_char CHAR(10), \
                col_varchar VARCHAR(100), \
                col_decimal DECIMAL(18,0), \
                col_double DOUBLE PRECISION, \
                col_date DATE, \
                col_timestamp TIMESTAMP, \
                col_timestamp_tz TIMESTAMP WITH LOCAL TIME ZONE, \
                col_interval_ym INTERVAL YEAR TO MONTH, \
                col_interval_ds INTERVAL DAY TO SECOND, \
                col_geometry GEOMETRY, \
                col_hashtype HASHTYPE\
            )",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // Call get_table_schema and verify it succeeds for all types
    let table_schema = conn
        .get_table_schema(None, Some(&schema_name), "ALL_TYPES")
        .expect("get_table_schema should not fail for any Exasol type");

    assert_eq!(
        table_schema.fields().len(),
        12,
        "Expected 12 fields in schema"
    );

    // Verify all column names are present
    let expected_names = [
        "COL_BOOLEAN",
        "COL_CHAR",
        "COL_VARCHAR",
        "COL_DECIMAL",
        "COL_DOUBLE",
        "COL_DATE",
        "COL_TIMESTAMP",
        "COL_TIMESTAMP_TZ",
        "COL_INTERVAL_YM",
        "COL_INTERVAL_DS",
        "COL_GEOMETRY",
        "COL_HASHTYPE",
    ];
    for (i, expected_name) in expected_names.iter().enumerate() {
        assert_eq!(
            table_schema.field(i).name(),
            *expected_name,
            "Column {} should be named {}",
            i,
            expected_name
        );
    }

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}

/// Test that querying a TIMESTAMP WITH LOCAL TIME ZONE column returns data
/// without "Unsupported Exasol type" errors.
#[test]

fn test_query_timestamp_with_local_time_zone() {
    skip_if_no_library!();
    skip_if_no_exasol!();

    let lib_path = get_library_path();
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        lib_path,
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )
    .expect("Failed to load driver");

    let uri = get_test_uri();
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri))];
    let db = driver
        .new_database_with_opts(opts)
        .expect("Failed to create database");

    let mut conn = db.new_connection().expect("Failed to create connection");

    let schema_name = format!(
        "TEST_TS_TZ_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );

    // Create schema
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("CREATE SCHEMA {}", schema_name))
            .unwrap();
        stmt.execute_update().unwrap();
    }

    // Create table with TIMESTAMP WITH LOCAL TIME ZONE column
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "CREATE TABLE {}.TS_TZ_TABLE (id INTEGER, ts_tz TIMESTAMP WITH LOCAL TIME ZONE)",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // Insert a value
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!(
            "INSERT INTO {}.TS_TZ_TABLE VALUES (1, TIMESTAMP '2024-01-15 10:30:00')",
            schema_name
        ))
        .unwrap();
        stmt.execute_update().unwrap();
    }

    // Query the data back
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("SELECT id, ts_tz FROM {}.TS_TZ_TABLE", schema_name))
            .unwrap();

        let mut reader = stmt
            .execute()
            .expect("Query with TIMESTAMP WITH LOCAL TIME ZONE should succeed");

        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 1, "Expected 1 row");
        assert_eq!(batch.num_columns(), 2, "Expected 2 columns");
    }

    // Cleanup
    {
        let mut stmt = conn.new_statement().expect("Failed to create statement");
        stmt.set_sql_query(format!("DROP SCHEMA {} CASCADE", schema_name))
            .unwrap();
        let _ = stmt.execute_update();
    }
}
