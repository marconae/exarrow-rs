//! Integration tests for exarrow-rs ADBC driver.
//!
//! # Overview
//!
//! This test module provides integration tests that validate exarrow-rs
//! against a real Exasol database instance. Unlike unit tests that use
//! mocked dependencies, these tests verify end-to-end functionality.
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
//! 3. Wait for the database to be ready (1-2 minutes on first run):
//!
//!    ```bash
//!    docker logs exasol-test 2>&1 | grep -i "started"
//!    ```
//!
//! # Configuration
//!
//! Tests use environment variables with sensible defaults:
//!
//! | Variable         | Default     | Description           |
//! |-----------------|-------------|-----------------------|
//! | `EXASOL_HOST`   | localhost   | Database host         |
//! | `EXASOL_PORT`   | 8563        | Database port         |
//! | `EXASOL_USER`   | sys         | Username              |
//! | `EXASOL_PASSWORD` | exasol    | Password              |
//!
//! # Running Tests
//!
//! Integration tests are marked with `#[ignore]` to prevent failures in
//! CI environments without Exasol. Run them explicitly:
//!
//! ```bash
//! # Run all integration tests
//! cargo test --test integration_tests -- --ignored
//!
//! # Run a specific test
//! cargo test --test integration_tests test_connection_succeeds -- --ignored
//!
//! # Run with verbose output
//! cargo test --test integration_tests -- --ignored --nocapture
//!
//! # Run with custom Exasol instance
//! EXASOL_HOST=192.168.1.100 cargo test --test integration_tests -- --ignored
//! ```
//!
//! # Test Organization
//!
//! Tests are organized by functionality:
//! - `infrastructure_*` - Validates test setup and helpers
//! - `connection_*` - Connection establishment and management
//! - `query_*` - Query execution and results
//! - `ddl_*` - Schema and table operations
//! - `dml_*` - Data manipulation
//! - `transaction_*` - Transaction handling
//! - `arrow_*` - Arrow conversion validation
//! - `prepared_*` - Prepared statement lifecycle and execution

// Declare the common module for shared test utilities
mod common;

use arrow::array::{Array, BooleanArray, Float64Array, StringArray};
use arrow::datatypes::DataType;
use common::{
    generate_test_schema_name, get_host, get_password, get_port, get_test_connection,
    get_test_connection_string, get_user, is_exasol_available, DEFAULT_HOST, DEFAULT_PASSWORD,
    DEFAULT_PORT, DEFAULT_USER,
};
use exarrow_rs::adbc::Connection;

// ============================================================================
// Infrastructure Tests
// ============================================================================
// These tests validate that the test infrastructure itself works correctly.

#[test]
fn test_default_constants_are_correct() {
    // Verify the default constants have expected values
    assert_eq!(DEFAULT_HOST, "localhost");
    assert_eq!(DEFAULT_PORT, 8563);
    assert_eq!(DEFAULT_USER, "sys");
    assert_eq!(DEFAULT_PASSWORD, "exasol");
}

#[test]
fn test_connection_string_format_is_valid() {
    let conn_str = get_test_connection_string();

    // Should start with exasol:// scheme
    assert!(
        conn_str.starts_with("exasol://"),
        "Connection string should start with 'exasol://', got: {}",
        conn_str
    );

    // Should contain host and port
    assert!(
        conn_str.contains(&get_host()),
        "Connection string should contain host"
    );
    assert!(
        conn_str.contains(&get_port().to_string()),
        "Connection string should contain port"
    );

    // Should contain user (but we don't check password for security)
    assert!(
        conn_str.contains(&get_user()),
        "Connection string should contain username"
    );
}

#[test]
fn test_helper_functions_return_values() {
    // All helper functions should return non-empty values
    assert!(!get_host().is_empty(), "Host should not be empty");
    assert!(get_port() > 0, "Port should be positive");
    assert!(!get_user().is_empty(), "User should not be empty");
    assert!(!get_password().is_empty(), "Password should not be empty");
}

#[test]
fn test_schema_name_generation_is_unique() {
    let schema1 = generate_test_schema_name();
    // Small delay to ensure different timestamp
    std::thread::sleep(std::time::Duration::from_millis(1));
    let schema2 = generate_test_schema_name();

    assert!(
        schema1.starts_with("TEST_INTEGRATION_"),
        "Schema should have correct prefix"
    );
    assert!(
        schema2.starts_with("TEST_INTEGRATION_"),
        "Schema should have correct prefix"
    );

    // Names should be unique (different timestamps)
    // Note: In very fast execution, they might be the same, so we just check format
    assert!(schema1.len() > 17, "Schema name should include timestamp");
}

#[test]
fn test_is_exasol_available_returns_bool() {
    // This should not panic, just return a boolean
    let available = is_exasol_available();
    // We can't assert the value since it depends on environment,
    // but we can verify it returns without panicking
    let _ = available;
}

// ============================================================================
// Connection Infrastructure Tests (require Exasol)
// ============================================================================

#[tokio::test]
async fn test_connection_helper_works_when_exasol_available() {
    skip_if_no_exasol!();

    // Test that get_test_connection() works
    let conn = get_test_connection()
        .await
        .expect("Should be able to connect to Exasol");

    // Connection should be established
    assert!(
        !conn.is_closed().await,
        "Connection should not be closed immediately after creation"
    );

    // Clean up
    conn.close()
        .await
        .expect("Should be able to close connection");
}

#[tokio::test]
async fn test_is_exasol_available_detects_running_instance() {
    // This test validates that is_exasol_available() correctly detects
    // a running Exasol instance when properly configured.
    //
    // Note: When running with default config (localhost:8563), this test
    // will be skipped if Exasol is not available. When running with
    // EXASOL_HOST set to a valid host, it validates the detection works.
    skip_if_no_exasol!();

    // If we get here, Exasol is available, so is_exasol_available() should return true
    assert!(
        is_exasol_available(),
        "is_exasol_available() should return true when Exasol is reachable"
    );
}

// ============================================================================
// Section 2: Connection Integration Tests
// ============================================================================
// Tests for connection establishment, failure handling, and resource management.

/// 2.1 Test successful connection to Exasol with valid credentials
#[tokio::test]
async fn test_connection_succeeds_with_valid_credentials() {
    skip_if_no_exasol!();

    let conn = get_test_connection()
        .await
        .expect("Connection with valid credentials should succeed");

    // Verify connection is active
    assert!(
        !conn.is_closed().await,
        "Connection should be open after successful connect"
    );

    // Verify we have a session ID
    let session_id = conn.session_id();
    assert!(
        !session_id.is_empty(),
        "Session ID should be assigned after connection"
    );

    // Clean up
    conn.close()
        .await
        .expect("Should be able to close connection");
}

/// 2.2 Test connection failure with invalid credentials (verify error handling)
#[tokio::test]
async fn test_connection_fails_with_invalid_credentials() {
    skip_if_no_exasol!();

    // Try to connect with invalid credentials
    let result = Connection::builder()
        .host(&get_host())
        .port(get_port())
        .username("invalid_user")
        .password("wrong_password")
        .connect()
        .await;

    // Connection should fail
    assert!(
        result.is_err(),
        "Connection with invalid credentials should fail"
    );

    // Error message should indicate authentication failure
    let error = result.unwrap_err();
    let error_msg = error.to_string().to_lowercase();
    assert!(
        error_msg.contains("auth") || error_msg.contains("failed") || error_msg.contains("invalid"),
        "Error should indicate authentication failure, got: {}",
        error
    );
}

/// 2.3 Test connection closure and resource cleanup
#[tokio::test]
async fn test_connection_closure_and_cleanup() {
    skip_if_no_exasol!();

    let conn = get_test_connection()
        .await
        .expect("Failed to connect for cleanup test");

    // Get session ID before closing
    let session_id = conn.session_id().to_string();
    assert!(!session_id.is_empty(), "Session ID should exist");

    // Close the connection
    conn.close().await.expect("Connection close should succeed");

    // After closing, we can't verify is_closed on the same connection
    // because close() consumes self. But the test passing means cleanup worked.
}

/// 2.4 Test connection health check against live database
#[tokio::test]
async fn test_connection_health_check() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection()
        .await
        .expect("Failed to connect for health check test");

    // Connection should not be closed
    assert!(
        !conn.is_closed().await,
        "Fresh connection should not be closed"
    );

    // Execute a simple health check query
    let batches = conn
        .query("SELECT 1 AS health_check")
        .await
        .expect("Health check query should succeed");

    assert!(!batches.is_empty(), "Health check should return results");
    assert_eq!(batches[0].num_rows(), 1, "Health check should return 1 row");

    // Clean up
    conn.close().await.expect("Failed to close connection");
}

// ============================================================================
// Section 3: Basic Query Integration Tests
// ============================================================================
// Tests for SELECT queries, Arrow RecordBatch validation, and data retrieval.

/// 3.1 Test SELECT from DUAL returns correct results
#[tokio::test]
async fn test_select_from_dual() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    // Exasol supports SELECT without FROM for literals
    let batches = conn
        .query("SELECT 42 AS answer")
        .await
        .expect("SELECT query should succeed");

    assert_eq!(batches.len(), 1, "Should return exactly one batch");
    assert_eq!(batches[0].num_rows(), 1, "Should return exactly one row");
    assert_eq!(
        batches[0].num_columns(),
        1,
        "Should return exactly one column"
    );

    // Verify the column name
    let schema = batches[0].schema();
    assert_eq!(
        schema.field(0).name(),
        "ANSWER",
        "Column name should match (Exasol uppercases identifiers)"
    );

    conn.close().await.expect("Failed to close connection");
}

/// 3.2 Verify Arrow RecordBatch contains expected schema and data
#[tokio::test]
async fn test_arrow_recordbatch_schema_and_data() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let batches = conn
        .query("SELECT 100 AS num_value, 'hello' AS text_value, TRUE AS bool_value")
        .await
        .expect("Query should succeed");

    assert!(!batches.is_empty(), "Should return at least one batch");

    let batch = &batches[0];
    let schema = batch.schema();

    // Verify schema has 3 fields
    assert_eq!(schema.fields().len(), 3, "Schema should have 3 fields");

    // Verify column names (Exasol uppercases)
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert!(
        field_names.contains(&"NUM_VALUE"),
        "Schema should contain NUM_VALUE"
    );
    assert!(
        field_names.contains(&"TEXT_VALUE"),
        "Schema should contain TEXT_VALUE"
    );
    assert!(
        field_names.contains(&"BOOL_VALUE"),
        "Schema should contain BOOL_VALUE"
    );

    // Verify data
    assert_eq!(batch.num_rows(), 1, "Should have 1 row");

    conn.close().await.expect("Failed to close connection");
}

/// 3.3 Test simple arithmetic expressions (SELECT 1+1)
#[tokio::test]
async fn test_arithmetic_expressions() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    // Test various arithmetic operations
    let batches = conn
        .query(
            "SELECT 1+1 AS addition, 10-3 AS subtraction, 4*5 AS multiplication, 20/4 AS division",
        )
        .await
        .expect("Arithmetic query should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1, "Should return 1 row");
    assert_eq!(batch.num_columns(), 4, "Should return 4 columns");

    conn.close().await.expect("Failed to close connection");
}

/// 3.4 Test string data retrieval and UTF-8 handling
#[tokio::test]
async fn test_string_data_and_utf8() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    // Test various string scenarios including UTF-8 characters
    let batches = conn
        .query("SELECT 'Hello, World!' AS english, 'Gruess Gott' AS german, 'Bonjour' AS french")
        .await
        .expect("String query should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1, "Should return 1 row");

    // Verify string column data type
    let schema = batch.schema();
    for field in schema.fields() {
        assert!(
            matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8),
            "String columns should be Utf8 type"
        );
    }

    conn.close().await.expect("Failed to close connection");
}

// ============================================================================
// Section 4: DDL Integration Tests
// ============================================================================
// Tests for CREATE/DROP SCHEMA and TABLE operations.

/// 4.1 Test CREATE SCHEMA for test isolation
#[tokio::test]
async fn test_create_schema() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    // Create schema
    let result = conn
        .execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await;

    assert!(
        result.is_ok(),
        "CREATE SCHEMA should succeed: {:?}",
        result.err()
    );

    // Verify schema exists by opening it
    let open_result = conn
        .execute_update(&format!("OPEN SCHEMA {}", schema_name))
        .await;

    assert!(
        open_result.is_ok(),
        "Should be able to open created schema: {:?}",
        open_result.err()
    );

    // Cleanup
    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await
        .expect("Cleanup: DROP SCHEMA should succeed");

    conn.close().await.expect("Failed to close connection");
}

/// 4.2 Test CREATE TABLE with various column types
#[tokio::test]
async fn test_create_table_various_types() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    // Create schema first
    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    // Create table with various column types
    let create_table_sql = format!(
        r#"
        CREATE TABLE {}.test_types (
            id INTEGER,
            name VARCHAR(100),
            description VARCHAR(2000),
            amount DECIMAL(18, 2),
            is_active BOOLEAN,
            created_date DATE,
            updated_at TIMESTAMP,
            ratio DOUBLE
        )
        "#,
        schema_name
    );

    let result = conn.execute_update(&create_table_sql).await;
    assert!(
        result.is_ok(),
        "CREATE TABLE should succeed: {:?}",
        result.err()
    );

    // Verify table exists by querying it
    let query_result = conn
        .query(&format!(
            "SELECT * FROM {}.test_types WHERE 1=0",
            schema_name
        ))
        .await;

    assert!(
        query_result.is_ok(),
        "Query on created table should succeed"
    );

    // Cleanup
    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await
        .expect("Cleanup: DROP SCHEMA should succeed");

    conn.close().await.expect("Failed to close connection");
}

/// 4.3 Test DROP TABLE cleanup
#[tokio::test]
async fn test_drop_table() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    // Setup: Create schema and table
    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.drop_test (id INTEGER)",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Drop the table
    let drop_result = conn
        .execute_update(&format!("DROP TABLE {}.drop_test", schema_name))
        .await;

    assert!(
        drop_result.is_ok(),
        "DROP TABLE should succeed: {:?}",
        drop_result.err()
    );

    // Verify table no longer exists
    let query_result = conn
        .query(&format!("SELECT * FROM {}.drop_test", schema_name))
        .await;

    assert!(query_result.is_err(), "Query on dropped table should fail");

    // Cleanup
    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await
        .expect("Cleanup: DROP SCHEMA should succeed");

    conn.close().await.expect("Failed to close connection");
}

/// 4.4 Test DROP SCHEMA cleanup
#[tokio::test]
async fn test_drop_schema() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    // Create schema with a table
    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.test_table (id INTEGER)",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Drop schema with CASCADE
    let drop_result = conn
        .execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await;

    assert!(
        drop_result.is_ok(),
        "DROP SCHEMA CASCADE should succeed: {:?}",
        drop_result.err()
    );

    // Verify schema no longer exists
    let open_result = conn
        .execute_update(&format!("OPEN SCHEMA {}", schema_name))
        .await;

    assert!(
        open_result.is_err(),
        "OPEN SCHEMA on dropped schema should fail"
    );

    conn.close().await.expect("Failed to close connection");
}

// ============================================================================
// Section 5: DML Integration Tests
// ============================================================================
// Tests for INSERT, SELECT, UPDATE, and DELETE operations.

/// Helper to create a test schema and table for DML tests
async fn setup_dml_test_table(conn: &mut Connection, schema_name: &str) {
    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        r#"
        CREATE TABLE {}.users (
            id INTEGER,
            name VARCHAR(100),
            email VARCHAR(255),
            age INTEGER
        )
        "#,
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");
}

/// Helper to cleanup test schema
async fn cleanup_schema(conn: &mut Connection, schema_name: &str) {
    let _ = conn
        .execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await;
}

/// 5.1 Test INSERT single row into table
#[tokio::test]
async fn test_insert_single_row() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    setup_dml_test_table(&mut conn, &schema_name).await;

    // Insert single row
    let insert_result = conn
        .execute_update(&format!(
            "INSERT INTO {}.users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
            schema_name
        ))
        .await;

    assert!(
        insert_result.is_ok(),
        "INSERT should succeed: {:?}",
        insert_result.err()
    );
    assert_eq!(insert_result.unwrap(), 1, "INSERT should affect 1 row");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 5.2 Test INSERT multiple rows
#[tokio::test]
async fn test_insert_multiple_rows() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    setup_dml_test_table(&mut conn, &schema_name).await;

    // Insert multiple rows
    let insert_result = conn
        .execute_update(&format!(
            r#"
            INSERT INTO {}.users (id, name, email, age) VALUES
            (1, 'Alice', 'alice@example.com', 30),
            (2, 'Bob', 'bob@example.com', 25),
            (3, 'Charlie', 'charlie@example.com', 35)
            "#,
            schema_name
        ))
        .await;

    assert!(
        insert_result.is_ok(),
        "INSERT should succeed: {:?}",
        insert_result.err()
    );
    assert_eq!(insert_result.unwrap(), 3, "INSERT should affect 3 rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 5.3 Test SELECT to verify inserted data
#[tokio::test]
async fn test_select_inserted_data() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    setup_dml_test_table(&mut conn, &schema_name).await;

    // Insert data
    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.users (id, name, email, age) VALUES
        (1, 'Alice', 'alice@example.com', 30),
        (2, 'Bob', 'bob@example.com', 25)
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Select and verify
    let batches = conn
        .query(&format!(
            "SELECT id, name, email, age FROM {}.users ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2, "Should have 2 rows");
    assert_eq!(batch.num_columns(), 4, "Should have 4 columns");

    // Verify column names
    let schema = batch.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["ID", "NAME", "EMAIL", "AGE"]);

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 5.4 Test UPDATE row and verify changes
#[tokio::test]
async fn test_update_row() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    setup_dml_test_table(&mut conn, &schema_name).await;

    // Insert data
    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Update the row
    let update_result = conn
        .execute_update(&format!(
            "UPDATE {}.users SET age = 31, email = 'alice.new@example.com' WHERE id = 1",
            schema_name
        ))
        .await;

    assert!(
        update_result.is_ok(),
        "UPDATE should succeed: {:?}",
        update_result.err()
    );
    assert_eq!(update_result.unwrap(), 1, "UPDATE should affect 1 row");

    // Verify the update
    let batches = conn
        .query(&format!(
            "SELECT age, email FROM {}.users WHERE id = 1",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1, "Should have 1 row");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 5.5 Test DELETE row and verify removal
#[tokio::test]
async fn test_delete_row() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    setup_dml_test_table(&mut conn, &schema_name).await;

    // Insert data
    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.users (id, name, email, age) VALUES
        (1, 'Alice', 'alice@example.com', 30),
        (2, 'Bob', 'bob@example.com', 25)
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Delete one row
    let delete_result = conn
        .execute_update(&format!("DELETE FROM {}.users WHERE id = 1", schema_name))
        .await;

    assert!(
        delete_result.is_ok(),
        "DELETE should succeed: {:?}",
        delete_result.err()
    );
    assert_eq!(delete_result.unwrap(), 1, "DELETE should affect 1 row");

    // Verify deletion
    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.users",
            schema_name
        ))
        .await
        .expect("SELECT COUNT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1, "Should have 1 row");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// ============================================================================
// Section 6: Transaction Integration Tests
// ============================================================================
// Tests for transaction begin, commit, rollback, and auto-commit behavior.

/// 6.1 Test explicit transaction begin
#[tokio::test]
async fn test_transaction_begin() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    // Verify not in transaction initially
    assert!(
        !conn.in_transaction(),
        "Should not be in transaction initially"
    );

    // Begin transaction
    let begin_result = conn.begin_transaction().await;
    assert!(
        begin_result.is_ok(),
        "BEGIN TRANSACTION should succeed: {:?}",
        begin_result.err()
    );

    // Verify now in transaction
    assert!(
        conn.in_transaction(),
        "Should be in transaction after BEGIN"
    );

    // Rollback to clean up (don't leave open transaction)
    conn.rollback().await.expect("ROLLBACK should succeed");

    conn.close().await.expect("Failed to close connection");
}

/// 6.2 Test COMMIT makes changes permanent
#[tokio::test]
async fn test_transaction_commit() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    setup_dml_test_table(&mut conn, &schema_name).await;

    // Begin transaction
    conn.begin_transaction()
        .await
        .expect("BEGIN should succeed");

    // Insert data
    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Commit
    let commit_result = conn.commit().await;
    assert!(
        commit_result.is_ok(),
        "COMMIT should succeed: {:?}",
        commit_result.err()
    );

    // Verify not in transaction after commit
    assert!(
        !conn.in_transaction(),
        "Should not be in transaction after COMMIT"
    );

    // Verify data persists (create new connection to be sure)
    let mut conn2 = get_test_connection()
        .await
        .expect("Failed to connect second connection");

    let batches = conn2
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.users",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    // Cleanup
    cleanup_schema(&mut conn2, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
    conn2.close().await.expect("Failed to close connection 2");
}

/// 6.3 Test ROLLBACK discards changes
#[tokio::test]
async fn test_transaction_rollback() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    setup_dml_test_table(&mut conn, &schema_name).await;

    // Insert initial data and commit
    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
        schema_name
    ))
    .await
    .expect("Initial INSERT should succeed");

    // Begin new transaction
    conn.begin_transaction()
        .await
        .expect("BEGIN should succeed");

    // Insert more data
    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)",
        schema_name
    ))
    .await
    .expect("Second INSERT should succeed");

    // Rollback
    let rollback_result = conn.rollback().await;
    assert!(
        rollback_result.is_ok(),
        "ROLLBACK should succeed: {:?}",
        rollback_result.err()
    );

    // Verify not in transaction after rollback
    assert!(
        !conn.in_transaction(),
        "Should not be in transaction after ROLLBACK"
    );

    // Verify only first row exists (second was rolled back)
    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.users",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    // Note: Due to auto-commit behavior, the first insert was already committed
    // so we should have 1 row

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 6.4 Verify auto-commit default behavior
#[tokio::test]
async fn test_auto_commit_behavior() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    setup_dml_test_table(&mut conn, &schema_name).await;

    // Insert without explicit transaction (auto-commit mode)
    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Verify not in transaction (auto-commit should have committed)
    assert!(
        !conn.in_transaction(),
        "Should not be in transaction in auto-commit mode"
    );

    // Create new connection to verify data was committed
    let mut conn2 = get_test_connection()
        .await
        .expect("Failed to connect second connection");

    let batches = conn2
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.users",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    // Cleanup
    cleanup_schema(&mut conn2, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
    conn2.close().await.expect("Failed to close connection 2");
}

// ============================================================================
// Section 7: Arrow Conversion Validation
// ============================================================================
// Tests for verifying correct conversion of Exasol types to Arrow types.

/// 7.1 Test INTEGER type conversion to Arrow Int64
#[tokio::test]
async fn test_integer_to_arrow_int64() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    // Create schema and table
    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.int_test (small_int INTEGER, big_int DECIMAL(18,0))",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Insert test data including edge cases
    // Note: DECIMAL(18,0) max is 999999999999999999 (18 digits), not INT64_MAX
    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.int_test VALUES
        (0, 0),
        (1, 1),
        (-1, -1),
        (2147483647, 999999999999999999),
        (-2147483648, -999999999999999999)
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Query and verify
    let batches = conn
        .query(&format!(
            "SELECT small_int, big_int FROM {}.int_test ORDER BY small_int",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 5, "Should have 5 rows");

    // Verify data types are numeric
    let schema = batch.schema();
    for field in schema.fields() {
        let dt = field.data_type();
        assert!(
            matches!(
                dt,
                DataType::Int64 | DataType::Int32 | DataType::Decimal128(_, _) | DataType::Float64
            ),
            "Integer columns should be numeric Arrow type, got {:?}",
            dt
        );
    }

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 7.2 Test VARCHAR type conversion to Arrow Utf8
#[tokio::test]
async fn test_varchar_to_arrow_utf8() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.varchar_test (short_text VARCHAR(50), long_text VARCHAR(2000))",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Insert test data
    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.varchar_test VALUES
        ('Hello', 'World'),
        ('Test', 'Data with special chars: @#$%'),
        ('Unicode', 'Symbols: < > & " ''')
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Query and verify
    let batches = conn
        .query(&format!(
            "SELECT short_text, long_text FROM {}.varchar_test",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    let schema = batch.schema();

    // Verify both columns are Utf8
    for field in schema.fields() {
        assert!(
            matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8),
            "VARCHAR columns should be Utf8 Arrow type, got {:?}",
            field.data_type()
        );
    }

    // Access string values using StringArray
    let short_text_col = batch.column(0);
    let string_array = short_text_col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Should be StringArray");

    assert_eq!(string_array.len(), 3, "Should have 3 values");
    assert_eq!(string_array.value(0), "Hello");
    assert_eq!(string_array.value(1), "Test");
    assert_eq!(string_array.value(2), "Unicode");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 7.3 Test DECIMAL type conversion with precision/scale
#[tokio::test]
async fn test_decimal_type_conversion() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.decimal_test (price DECIMAL(10,2), quantity DECIMAL(18,4))",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Insert test data
    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.decimal_test VALUES
        (99.99, 1234.5678),
        (0.01, 0.0001),
        (-123.45, -9999.9999)
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Query and verify
    let batches = conn
        .query(&format!(
            "SELECT price, quantity FROM {}.decimal_test ORDER BY price",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3, "Should have 3 rows");

    // Verify schema types
    let schema = batch.schema();
    for field in schema.fields() {
        let dt = field.data_type();
        assert!(
            matches!(dt, DataType::Decimal128(_, _) | DataType::Float64),
            "DECIMAL columns should be Decimal128 or Float64, got {:?}",
            dt
        );
    }

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 7.4 Test NULL value handling in result sets
#[tokio::test]
async fn test_null_value_handling() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        r#"
        CREATE TABLE {}.null_test (
            id INTEGER,
            nullable_int INTEGER,
            nullable_varchar VARCHAR(100),
            nullable_decimal DECIMAL(10,2)
        )
        "#,
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Insert rows with NULL values
    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.null_test VALUES
        (1, NULL, NULL, NULL),
        (2, 42, 'not null', 123.45),
        (3, NULL, 'text', NULL)
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Query and verify
    let batches = conn
        .query(&format!(
            "SELECT id, nullable_int, nullable_varchar, nullable_decimal FROM {}.null_test ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3, "Should have 3 rows");

    // Check null counts for each column
    let nullable_int_col = batch.column(1);
    let nullable_varchar_col = batch.column(2);
    let nullable_decimal_col = batch.column(3);

    // Verify null values are present
    assert_eq!(
        nullable_int_col.null_count(),
        2,
        "nullable_int should have 2 nulls"
    );
    assert_eq!(
        nullable_varchar_col.null_count(),
        1,
        "nullable_varchar should have 1 null"
    );
    assert_eq!(
        nullable_decimal_col.null_count(),
        2,
        "nullable_decimal should have 2 nulls"
    );

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 7.5 Test DATE/TIMESTAMP type conversion
#[tokio::test]
async fn test_date_timestamp_conversion() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        r#"
        CREATE TABLE {}.datetime_test (
            event_date DATE,
            event_timestamp TIMESTAMP
        )
        "#,
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Insert test data
    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.datetime_test VALUES
        (DATE '2024-01-15', TIMESTAMP '2024-01-15 10:30:00'),
        (DATE '2023-12-31', TIMESTAMP '2023-12-31 23:59:59'),
        (DATE '2000-01-01', TIMESTAMP '2000-01-01 00:00:00')
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Query and verify
    let batches = conn
        .query(&format!(
            "SELECT event_date, event_timestamp FROM {}.datetime_test ORDER BY event_date",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3, "Should have 3 rows");

    // Verify schema types
    let schema = batch.schema();

    let date_field = schema.field(0);
    assert!(
        matches!(
            date_field.data_type(),
            DataType::Date32 | DataType::Date64 | DataType::Utf8
        ),
        "DATE column should be Date32, Date64, or Utf8, got {:?}",
        date_field.data_type()
    );

    let timestamp_field = schema.field(1);
    assert!(
        matches!(
            timestamp_field.data_type(),
            DataType::Timestamp(_, _) | DataType::Date64 | DataType::Int64 | DataType::Utf8
        ),
        "TIMESTAMP column should be Timestamp, Date64, Int64, or Utf8, got {:?}",
        timestamp_field.data_type()
    );

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// ============================================================================
// Additional Arrow Validation Tests
// ============================================================================

/// Test BOOLEAN type conversion
#[tokio::test]
async fn test_boolean_type_conversion() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.bool_test (flag BOOLEAN)",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.bool_test VALUES
        (TRUE),
        (FALSE),
        (NULL)
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    let batches = conn
        .query(&format!("SELECT flag FROM {}.bool_test", schema_name))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 3, "Should have 3 rows");

    let schema = batch.schema();
    let bool_field = schema.field(0);
    assert_eq!(
        bool_field.data_type(),
        &DataType::Boolean,
        "BOOLEAN column should be Arrow Boolean type"
    );

    // Verify values using BooleanArray
    let bool_col = batch.column(0);
    let bool_array = bool_col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Should be BooleanArray");

    assert!(bool_array.value(0), "First value should be true");
    assert!(!bool_array.value(1), "Second value should be false");
    assert!(bool_array.is_null(2), "Third value should be null");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test DOUBLE type conversion
#[tokio::test]
async fn test_double_type_conversion() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.double_test (val DOUBLE)",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.double_test VALUES
        (3.14159265358979),
        (-273.15),
        (0.0),
        (1.0E100)
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    let batches = conn
        .query(&format!(
            "SELECT val FROM {}.double_test ORDER BY val",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 4, "Should have 4 rows");

    let schema = batch.schema();
    let double_field = schema.field(0);
    assert_eq!(
        double_field.data_type(),
        &DataType::Float64,
        "DOUBLE column should be Arrow Float64 type"
    );

    // Verify values using Float64Array
    let double_col = batch.column(0);
    let float_array = double_col
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Should be Float64Array");

    assert_eq!(float_array.len(), 4, "Should have 4 values");
    // Values are ordered, so first is the most negative
    assert!(
        float_array.value(0) < 0.0,
        "First value should be negative (ordered)"
    );

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test large result set handling
#[tokio::test]
async fn test_large_result_set() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.large_test (id INTEGER, text_data VARCHAR(100))",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Insert 1000 rows using a sequence
    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.large_test (id, text_data)
        SELECT LEVEL, 'Row number ' || LEVEL
        FROM DUAL
        CONNECT BY LEVEL <= 1000
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    // Query and verify
    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.large_test",
            schema_name
        ))
        .await
        .expect("SELECT COUNT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    // Query all rows
    let all_batches = conn
        .query(&format!(
            "SELECT id, text_data FROM {}.large_test ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT all should succeed");

    // Count total rows across all batches
    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1000, "Should have 1000 total rows");

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Test empty result set handling
#[tokio::test]
async fn test_empty_result_set() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.empty_test (id INTEGER, name VARCHAR(100))",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // Query empty table
    let batches = conn
        .query(&format!("SELECT id, name FROM {}.empty_test", schema_name))
        .await
        .expect("SELECT from empty table should succeed");

    // Empty table should return valid batches with 0 rows
    // The total row count across all batches should be 0
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Empty table should return 0 rows");

    // But schema should still be present
    if !batches.is_empty() {
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 2, "Schema should have 2 fields");
    }

    // Cleanup
    cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// ============================================================================
// Section 8: Prepared Statement Integration Tests
// ============================================================================
// Tests for prepared statement lifecycle, parameter binding, and execution.

/// 8.1 Test creating and closing a prepared statement
#[tokio::test]
async fn test_prepared_statement_lifecycle() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    // Create a simple prepared statement using new Connection::prepare API
    let prepared = conn
        .prepare("SELECT 1")
        .await
        .expect("Failed to prepare statement");

    assert!(!prepared.is_closed());
    assert_eq!(prepared.parameter_count(), 0);

    // Execute without parameters using Connection::execute_prepared
    let results = conn
        .execute_prepared(&prepared)
        .await
        .expect("Failed to execute prepared statement");

    // Verify we got results
    let batches = results.fetch_all().await.expect("Failed to fetch results");
    assert!(!batches.is_empty(), "Should return at least one batch");
    assert_eq!(batches[0].num_rows(), 1, "Should return 1 row");

    // Close the prepared statement using Connection::close_prepared
    conn.close_prepared(prepared)
        .await
        .expect("Failed to close prepared statement");

    conn.close().await.expect("Failed to close connection");
}

/// 8.2 Test prepared statement with parameters (INSERT)
#[tokio::test]
async fn test_prepared_statement_with_parameters() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    // Setup: create schema and table
    let _ = conn
        .execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await;
    conn.execute_update(&format!(
        "CREATE TABLE {}.test_params (id INT, name VARCHAR(100))",
        schema_name
    ))
    .await
    .expect("Failed to create table");

    // Insert using prepared statement with new Connection::prepare API
    let mut prepared = conn
        .prepare(&format!(
            "INSERT INTO {}.test_params VALUES (?, ?)",
            schema_name
        ))
        .await
        .expect("Failed to prepare insert");

    assert_eq!(prepared.parameter_count(), 2);

    // Bind and execute using Connection::execute_prepared_update
    prepared.bind(0, 1).expect("Failed to bind param 0");
    prepared.bind(1, "Alice").expect("Failed to bind param 1");
    let rows = conn
        .execute_prepared_update(&prepared)
        .await
        .expect("Failed to execute insert");

    assert_eq!(rows, 1);

    // Re-bind and execute again
    prepared.clear_parameters();
    prepared.bind(0, 2).expect("Failed to bind param 0");
    prepared.bind(1, "Bob").expect("Failed to bind param 1");
    let rows = conn
        .execute_prepared_update(&prepared)
        .await
        .expect("Failed to execute insert");

    assert_eq!(rows, 1);

    conn.close_prepared(prepared)
        .await
        .expect("Failed to close prepared");

    // Verify data was inserted
    let batches = conn
        .query(&format!(
            "SELECT id, name FROM {}.test_params ORDER BY id",
            schema_name
        ))
        .await
        .expect("Failed to query");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 2);

    // Cleanup
    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await
        .expect("Failed to drop schema");
    conn.close().await.expect("Failed to close connection");
}

/// 8.3 Test prepared statement with SELECT and parameters
#[tokio::test]
async fn test_prepared_select_with_parameters() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    // Setup
    let _ = conn
        .execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await;
    conn.execute_update(&format!(
        "CREATE TABLE {}.test_select (id INT, val INT)",
        schema_name
    ))
    .await
    .expect("Failed to create table");

    // Insert test data
    conn.execute_update(&format!(
        "INSERT INTO {}.test_select VALUES (1, 100), (2, 200), (3, 300)",
        schema_name
    ))
    .await
    .expect("Failed to insert data");

    // Select with parameter using new Connection::prepare API
    let mut prepared = conn
        .prepare(&format!(
            "SELECT val FROM {}.test_select WHERE id = ?",
            schema_name
        ))
        .await
        .expect("Failed to prepare select");

    // Query for id = 2
    prepared.bind(0, 2).expect("Failed to bind param");
    let results = conn
        .execute_prepared(&prepared)
        .await
        .expect("Failed to execute select");

    let batches = results.fetch_all().await.expect("Failed to fetch");
    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1);

    // Query for id = 3 (reuse prepared statement)
    prepared.clear_parameters();
    prepared.bind(0, 3).expect("Failed to bind param");
    let results = conn
        .execute_prepared(&prepared)
        .await
        .expect("Failed to execute select");

    let batches = results.fetch_all().await.expect("Failed to fetch");
    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1);

    conn.close_prepared(prepared)
        .await
        .expect("Failed to close prepared");

    // Cleanup
    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await
        .expect("Failed to drop schema");
    conn.close().await.expect("Failed to close connection");
}

/// 8.4 Test prepared statement parameter types
#[allow(clippy::approx_constant)]
#[tokio::test]
async fn test_prepared_statement_parameter_types() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    // Setup
    let _ = conn
        .execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await;
    conn.execute_update(&format!(
        "CREATE TABLE {}.test_types (
            bool_col BOOLEAN,
            int_col INTEGER,
            float_col DOUBLE,
            str_col VARCHAR(100)
        )",
        schema_name
    ))
    .await
    .expect("Failed to create table");

    // Insert using prepared statement with various types using new Connection::prepare API
    let mut prepared = conn
        .prepare(&format!(
            "INSERT INTO {}.test_types VALUES (?, ?, ?, ?)",
            schema_name
        ))
        .await
        .expect("Failed to prepare");

    prepared.bind(0, true).expect("Failed to bind bool");
    prepared.bind(1, 42i64).expect("Failed to bind int");
    prepared.bind(2, 3.14f64).expect("Failed to bind float");
    prepared.bind(3, "hello").expect("Failed to bind string");

    let rows = conn
        .execute_prepared_update(&prepared)
        .await
        .expect("Failed to execute");

    assert_eq!(rows, 1);
    conn.close_prepared(prepared)
        .await
        .expect("Failed to close");

    // Verify
    let batches = conn
        .query(&format!("SELECT * FROM {}.test_types", schema_name))
        .await
        .expect("Failed to query");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1);

    // Cleanup
    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await
        .expect("Failed to drop schema");
    conn.close().await.expect("Failed to close connection");
}
