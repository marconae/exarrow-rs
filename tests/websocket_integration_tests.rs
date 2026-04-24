//! WebSocket transport integration tests for exarrow-rs ADBC driver.
//!
//! Mirrors all functional tests from `integration_tests.rs` but forces
//! `transport=websocket` in the connection string. Gated behind the
//! `websocket` feature flag.
//!
//! # Running
//!
//! ```bash
//! cargo test --no-default-features --features websocket --test websocket_integration_tests
//! ```

#![cfg(feature = "websocket")]

mod common;

use arrow::array::{Array, BooleanArray, Float64Array, StringArray};
use arrow::datatypes::DataType;
use common::{generate_test_schema_name, get_host, get_password, get_port, get_user};
use exarrow_rs::adbc::{Connection, Driver};

// Helper: build a connection string that forces the WebSocket transport.
fn ws_connection_string() -> String {
    format!(
        "exasol://{}:{}@{}:{}?tls=true&validateservercertificate=0&transport=websocket",
        get_user(),
        get_password(),
        get_host(),
        get_port()
    )
}

// Helper: open a connection over WebSocket (with retry).
async fn get_ws_connection() -> Result<Connection, exarrow_rs::error::ExasolError> {
    use std::time::Duration;
    let driver = Driver::new();
    let conn_string = ws_connection_string();

    let mut last_error = None;
    for attempt in 1..=5u32 {
        let database = driver.open(&conn_string)?;
        match database.connect().await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                eprintln!("WS connection attempt {}/5 failed: {}", attempt, e);
                last_error = Some(e);
                if attempt < 5 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    Err(exarrow_rs::error::ExasolError::Connection(
        last_error.unwrap(),
    ))
}

// Helper: set up a DML test table inside `schema_name` (schema must already exist).
async fn ws_setup_dml_test_table(conn: &mut Connection, schema_name: &str) {
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

// Helper: drop a schema.
async fn ws_cleanup_schema(conn: &mut Connection, schema_name: &str) {
    let _ = conn
        .execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await;
}

// ── Section: Transport selection ──────────────────────────────────────────────

/// Verify that the WebSocket transport is actually selected when `transport=websocket`
/// is present in the connection string.
#[tokio::test]
async fn test_ws_transport_selected() {
    skip_if_no_exasol!();

    let conn = get_ws_connection()
        .await
        .expect("WebSocket connection should succeed");

    assert!(!conn.is_closed().await, "Connection should be open");

    conn.close().await.expect("Failed to close connection");
}

// ── Section 2: Connection ─────────────────────────────────────────────────────

/// 2.1 Connection succeeds with valid credentials over WebSocket.
#[tokio::test]
async fn test_ws_connection_succeeds_with_valid_credentials() {
    skip_if_no_exasol!();

    let conn = get_ws_connection()
        .await
        .expect("WebSocket connection with valid credentials should succeed");

    assert!(
        !conn.is_closed().await,
        "Connection should be open after successful connect"
    );

    let session_id = conn.session_id();
    assert!(
        !session_id.is_empty(),
        "Session ID should be assigned after connection"
    );

    conn.close()
        .await
        .expect("Should be able to close connection");
}

/// 2.2 Connection fails with invalid credentials over WebSocket.
#[tokio::test]
async fn test_ws_connection_fails_with_invalid_credentials() {
    skip_if_no_exasol!();

    let conn_str = format!(
        "exasol://invalid_user:wrong_password@{}:{}?tls=true&validateservercertificate=0&transport=websocket",
        get_host(),
        get_port()
    );

    let driver = Driver::new();
    let database = driver.open(&conn_str).expect("open should succeed");
    let result = database.connect().await;

    assert!(
        result.is_err(),
        "Connection with invalid credentials should fail"
    );

    let error = result.unwrap_err();
    let error_msg = error.to_string().to_lowercase();
    assert!(
        error_msg.contains("auth") || error_msg.contains("failed") || error_msg.contains("invalid"),
        "Error should indicate authentication failure, got: {}",
        error
    );
}

/// 2.3 Connection closure and cleanup over WebSocket.
#[tokio::test]
async fn test_ws_connection_closure_and_cleanup() {
    skip_if_no_exasol!();

    let conn = get_ws_connection()
        .await
        .expect("Failed to connect for cleanup test");

    let session_id = conn.session_id().to_string();
    assert!(!session_id.is_empty(), "Session ID should exist");

    conn.close().await.expect("Connection close should succeed");
}

/// 2.4 Connection health check over WebSocket.
#[tokio::test]
async fn test_ws_connection_health_check() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection()
        .await
        .expect("Failed to connect for health check test");

    assert!(
        !conn.is_closed().await,
        "Fresh connection should not be closed"
    );

    let batches = conn
        .query("SELECT 1 AS health_check")
        .await
        .expect("Health check query should succeed");

    assert!(!batches.is_empty(), "Health check should return results");
    assert_eq!(batches[0].num_rows(), 1, "Health check should return 1 row");

    conn.close().await.expect("Failed to close connection");
}

// ── Section 3: Basic Queries ──────────────────────────────────────────────────

/// 3.1 SELECT from DUAL over WebSocket.
#[tokio::test]
async fn test_ws_select_from_dual() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

    let schema = batches[0].schema();
    assert_eq!(
        schema.field(0).name(),
        "ANSWER",
        "Column name should match (Exasol uppercases identifiers)"
    );

    conn.close().await.expect("Failed to close connection");
}

/// 3.2 Arrow RecordBatch schema and data over WebSocket.
#[tokio::test]
async fn test_ws_arrow_recordbatch_schema_and_data() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let batches = conn
        .query("SELECT 100 AS num_value, 'hello' AS text_value, TRUE AS bool_value")
        .await
        .expect("Query should succeed");

    assert!(!batches.is_empty(), "Should return at least one batch");

    let batch = &batches[0];
    let schema = batch.schema();

    assert_eq!(schema.fields().len(), 3, "Schema should have 3 fields");

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

    assert_eq!(batch.num_rows(), 1, "Should have 1 row");

    conn.close().await.expect("Failed to close connection");
}

/// 3.3 Arithmetic expressions over WebSocket.
#[tokio::test]
async fn test_ws_arithmetic_expressions() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

/// 3.4 String data and UTF-8 over WebSocket.
#[tokio::test]
async fn test_ws_string_data_and_utf8() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let batches = conn
        .query("SELECT 'Hello, World!' AS english, 'Gruess Gott' AS german, 'Bonjour' AS french")
        .await
        .expect("String query should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1, "Should return 1 row");

    let schema = batch.schema();
    for field in schema.fields() {
        assert!(
            matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8),
            "String columns should be Utf8 type"
        );
    }

    conn.close().await.expect("Failed to close connection");
}

// ── Section 4: DDL ────────────────────────────────────────────────────────────

/// 4.1 CREATE SCHEMA over WebSocket.
#[tokio::test]
async fn test_ws_create_schema() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    let result = conn
        .execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await;

    assert!(
        result.is_ok(),
        "CREATE SCHEMA should succeed: {:?}",
        result.err()
    );

    let open_result = conn
        .execute_update(&format!("OPEN SCHEMA {}", schema_name))
        .await;

    ws_cleanup_schema(&mut conn, &schema_name).await;

    assert!(
        open_result.is_ok(),
        "Should be able to open created schema: {:?}",
        open_result.err()
    );

    conn.close().await.expect("Failed to close connection");
}

/// 4.2 CREATE TABLE with various column types over WebSocket.
#[tokio::test]
async fn test_ws_create_table_various_types() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

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

    let query_result = conn
        .query(&format!(
            "SELECT * FROM {}.test_types WHERE 1=0",
            schema_name
        ))
        .await;

    ws_cleanup_schema(&mut conn, &schema_name).await;

    assert!(
        query_result.is_ok(),
        "Query on created table should succeed"
    );

    conn.close().await.expect("Failed to close connection");
}

/// 4.2b DDL followed by DML and SELECT over WebSocket.
#[tokio::test]
async fn test_ws_ddl_then_insert_select_same_connection() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.ddl_then_dml (id INTEGER, name VARCHAR(32))",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    let insert_result = conn
        .execute_update(&format!(
            "INSERT INTO {}.ddl_then_dml (id, name) VALUES (1, 'Alice'), (2, 'Bob')",
            schema_name
        ))
        .await;

    let query_result = conn
        .query(&format!(
            "SELECT id, name FROM {}.ddl_then_dml ORDER BY id",
            schema_name
        ))
        .await;

    ws_cleanup_schema(&mut conn, &schema_name).await;

    assert!(
        insert_result.is_ok(),
        "INSERT after DDL should succeed: {:?}",
        insert_result.err()
    );
    assert_eq!(insert_result.unwrap(), 2);
    assert!(query_result.is_ok(), "SELECT after DDL should succeed");

    let batches = query_result.unwrap();
    let total_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(total_rows, 2, "Should read both inserted rows");

    conn.close().await.expect("Failed to close connection");
}

/// 4.3 DROP TABLE over WebSocket.
#[tokio::test]
async fn test_ws_drop_table() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.drop_test (id INTEGER)",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    let drop_result = conn
        .execute_update(&format!("DROP TABLE {}.drop_test", schema_name))
        .await;

    assert!(
        drop_result.is_ok(),
        "DROP TABLE should succeed: {:?}",
        drop_result.err()
    );

    let query_result = conn
        .query(&format!("SELECT * FROM {}.drop_test", schema_name))
        .await;

    ws_cleanup_schema(&mut conn, &schema_name).await;

    assert!(query_result.is_err(), "Query on dropped table should fail");

    conn.close().await.expect("Failed to close connection");
}

/// 4.4 DROP SCHEMA over WebSocket.
#[tokio::test]
async fn test_ws_drop_schema() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.test_table (id INTEGER)",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    let drop_result = conn
        .execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await;

    assert!(
        drop_result.is_ok(),
        "DROP SCHEMA CASCADE should succeed: {:?}",
        drop_result.err()
    );

    let open_result = conn
        .execute_update(&format!("OPEN SCHEMA {}", schema_name))
        .await;

    assert!(
        open_result.is_err(),
        "OPEN SCHEMA on dropped schema should fail"
    );

    conn.close().await.expect("Failed to close connection");
}

// ── Section 5: DML ────────────────────────────────────────────────────────────

/// 5.1 INSERT single row over WebSocket.
#[tokio::test]
async fn test_ws_insert_single_row() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    ws_setup_dml_test_table(&mut conn, &schema_name).await;

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

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 5.2 INSERT multiple rows over WebSocket.
#[tokio::test]
async fn test_ws_insert_multiple_rows() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    ws_setup_dml_test_table(&mut conn, &schema_name).await;

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

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 5.3 SELECT to verify inserted data over WebSocket.
#[tokio::test]
async fn test_ws_select_inserted_data() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    ws_setup_dml_test_table(&mut conn, &schema_name).await;

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

    let schema = batch.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(field_names, vec!["ID", "NAME", "EMAIL", "AGE"]);

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 5.4 UPDATE row over WebSocket.
#[tokio::test]
async fn test_ws_update_row() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    ws_setup_dml_test_table(&mut conn, &schema_name).await;

    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

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

    let batches = conn
        .query(&format!(
            "SELECT age, email FROM {}.users WHERE id = 1",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1, "Should have 1 row");

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 5.5 DELETE row over WebSocket.
#[tokio::test]
async fn test_ws_delete_row() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    ws_setup_dml_test_table(&mut conn, &schema_name).await;

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

    let delete_result = conn
        .execute_update(&format!("DELETE FROM {}.users WHERE id = 1", schema_name))
        .await;

    assert!(
        delete_result.is_ok(),
        "DELETE should succeed: {:?}",
        delete_result.err()
    );
    assert_eq!(delete_result.unwrap(), 1, "DELETE should affect 1 row");

    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.users",
            schema_name
        ))
        .await
        .expect("SELECT COUNT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1, "Should have 1 row");

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// ── Section 6: Transactions ───────────────────────────────────────────────────

/// 6.1 Transaction begin over WebSocket.
#[tokio::test]
async fn test_ws_transaction_begin() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    assert!(
        !conn.in_transaction(),
        "Should not be in transaction initially"
    );

    let begin_result = conn.begin_transaction().await;
    assert!(
        begin_result.is_ok(),
        "BEGIN TRANSACTION should succeed: {:?}",
        begin_result.err()
    );

    assert!(
        conn.in_transaction(),
        "Should be in transaction after BEGIN"
    );

    conn.rollback().await.expect("ROLLBACK should succeed");

    conn.close().await.expect("Failed to close connection");
}

/// 6.2 COMMIT makes changes permanent over WebSocket.
#[tokio::test]
async fn test_ws_transaction_commit() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    ws_setup_dml_test_table(&mut conn, &schema_name).await;

    conn.begin_transaction()
        .await
        .expect("BEGIN should succeed");

    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    let commit_result = conn.commit().await;
    assert!(
        commit_result.is_ok(),
        "COMMIT should succeed: {:?}",
        commit_result.err()
    );

    assert!(
        !conn.in_transaction(),
        "Should not be in transaction after COMMIT"
    );

    let mut conn2 = get_ws_connection()
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

    ws_cleanup_schema(&mut conn2, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
    conn2.close().await.expect("Failed to close connection 2");
}

/// 6.3 ROLLBACK discards changes over WebSocket.
#[tokio::test]
async fn test_ws_transaction_rollback() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    ws_setup_dml_test_table(&mut conn, &schema_name).await;

    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
        schema_name
    ))
    .await
    .expect("Initial INSERT should succeed");

    conn.begin_transaction()
        .await
        .expect("BEGIN should succeed");

    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 25)",
        schema_name
    ))
    .await
    .expect("Second INSERT should succeed");

    let rollback_result = conn.rollback().await;
    assert!(
        rollback_result.is_ok(),
        "ROLLBACK should succeed: {:?}",
        rollback_result.err()
    );

    assert!(
        !conn.in_transaction(),
        "Should not be in transaction after ROLLBACK"
    );

    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.users",
            schema_name
        ))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 6.4 Auto-commit default behavior over WebSocket.
#[tokio::test]
async fn test_ws_auto_commit_behavior() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();
    ws_setup_dml_test_table(&mut conn, &schema_name).await;

    conn.execute_update(&format!(
        "INSERT INTO {}.users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)",
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    assert!(
        !conn.in_transaction(),
        "Should not be in transaction in auto-commit mode"
    );

    let mut conn2 = get_ws_connection()
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

    ws_cleanup_schema(&mut conn2, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
    conn2.close().await.expect("Failed to close connection 2");
}

// ── Section 7: Arrow Conversion ───────────────────────────────────────────────

/// 7.1 INTEGER to Arrow Int64 over WebSocket.
#[tokio::test]
async fn test_ws_integer_to_arrow_int64() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.int_test (small_int INTEGER, big_int DECIMAL(18,0))",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

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

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 7.2 VARCHAR to Arrow Utf8 over WebSocket.
#[tokio::test]
async fn test_ws_varchar_to_arrow_utf8() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

    for field in schema.fields() {
        assert!(
            matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8),
            "VARCHAR columns should be Utf8 Arrow type, got {:?}",
            field.data_type()
        );
    }

    let short_text_col = batch.column(0);
    let string_array = short_text_col
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("Should be StringArray");

    assert_eq!(string_array.len(), 3, "Should have 3 values");
    assert_eq!(string_array.value(0), "Hello");
    assert_eq!(string_array.value(1), "Test");
    assert_eq!(string_array.value(2), "Unicode");

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 7.3 DECIMAL type conversion over WebSocket.
#[tokio::test]
async fn test_ws_decimal_type_conversion() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

    let schema = batch.schema();
    for field in schema.fields() {
        let dt = field.data_type();
        assert!(
            matches!(dt, DataType::Decimal128(_, _) | DataType::Float64),
            "DECIMAL columns should be Decimal128 or Float64, got {:?}",
            dt
        );
    }

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 7.4 NULL value handling over WebSocket.
#[tokio::test]
async fn test_ws_null_value_handling() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

    let nullable_int_col = batch.column(1);
    let nullable_varchar_col = batch.column(2);
    let nullable_decimal_col = batch.column(3);

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

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// 7.5 DATE/TIMESTAMP type conversion over WebSocket.
#[tokio::test]
async fn test_ws_date_timestamp_conversion() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// BOOLEAN type conversion over WebSocket.
#[tokio::test]
async fn test_ws_boolean_type_conversion() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

    let bool_col = batch.column(0);
    let bool_array = bool_col
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("Should be BooleanArray");

    assert!(bool_array.value(0), "First value should be true");
    assert!(!bool_array.value(1), "Second value should be false");
    assert!(bool_array.is_null(2), "Third value should be null");

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// DOUBLE type conversion over WebSocket.
#[tokio::test]
async fn test_ws_double_type_conversion() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

    let double_col = batch.column(0);
    let float_array = double_col
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Should be Float64Array");

    assert_eq!(float_array.len(), 4, "Should have 4 values");
    assert!(
        float_array.value(0) < 0.0,
        "First value should be negative (ordered)"
    );

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Large result set (1000 rows) over WebSocket.
#[tokio::test]
async fn test_ws_large_result_set() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

    let batches = conn
        .query(&format!(
            "SELECT COUNT(*) AS cnt FROM {}.large_test",
            schema_name
        ))
        .await
        .expect("SELECT COUNT should succeed");

    assert!(!batches.is_empty(), "Should return results");

    let all_batches = conn
        .query(&format!(
            "SELECT id, text_data FROM {}.large_test ORDER BY id",
            schema_name
        ))
        .await
        .expect("SELECT all should succeed");

    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1000, "Should have 1000 total rows");

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

/// Empty result set over WebSocket.
#[tokio::test]
async fn test_ws_empty_result_set() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

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

    let batches = conn
        .query(&format!("SELECT id, name FROM {}.empty_test", schema_name))
        .await
        .expect("SELECT from empty table should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0, "Empty table should return 0 rows");

    if !batches.is_empty() {
        let schema = batches[0].schema();
        assert_eq!(schema.fields().len(), 2, "Schema should have 2 fields");
    }

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// ── Section 8: Prepared Statements ───────────────────────────────────────────

/// 8.1 Prepared statement lifecycle over WebSocket.
#[tokio::test]
async fn test_ws_prepared_statement_lifecycle() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let prepared = conn
        .prepare("SELECT 1")
        .await
        .expect("Failed to prepare statement");

    assert!(!prepared.is_closed());
    assert_eq!(prepared.parameter_count(), 0);

    let results = conn
        .execute_prepared(&prepared)
        .await
        .expect("Failed to execute prepared statement");

    let batches = results.fetch_all().await.expect("Failed to fetch results");
    assert!(!batches.is_empty(), "Should return at least one batch");
    assert_eq!(batches[0].num_rows(), 1, "Should return 1 row");

    conn.close_prepared(prepared)
        .await
        .expect("Failed to close prepared statement");

    conn.close().await.expect("Failed to close connection");
}

/// 8.2 Prepared statement with parameters (INSERT) over WebSocket.
#[tokio::test]
async fn test_ws_prepared_statement_with_parameters() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    let _ = conn
        .execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await;
    conn.execute_update(&format!(
        "CREATE TABLE {}.test_params (id INT, name VARCHAR(100))",
        schema_name
    ))
    .await
    .expect("Failed to create table");

    let mut prepared = conn
        .prepare(&format!(
            "INSERT INTO {}.test_params VALUES (?, ?)",
            schema_name
        ))
        .await
        .expect("Failed to prepare insert");

    assert_eq!(prepared.parameter_count(), 2);

    prepared.bind(0, 1).expect("Failed to bind param 0");
    prepared.bind(1, "Alice").expect("Failed to bind param 1");
    let rows = conn
        .execute_prepared_update(&prepared)
        .await
        .expect("Failed to execute insert");

    assert_eq!(rows, 1);

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

    let batches = conn
        .query(&format!(
            "SELECT id, name FROM {}.test_params ORDER BY id",
            schema_name
        ))
        .await
        .expect("Failed to query");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 2);

    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await
        .expect("Failed to drop schema");
    conn.close().await.expect("Failed to close connection");
}

/// 8.3 Prepared SELECT with parameters over WebSocket.
#[tokio::test]
async fn test_ws_prepared_select_with_parameters() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    let _ = conn
        .execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await;
    conn.execute_update(&format!(
        "CREATE TABLE {}.test_select (id INT, val INT)",
        schema_name
    ))
    .await
    .expect("Failed to create table");

    conn.execute_update(&format!(
        "INSERT INTO {}.test_select VALUES (1, 100), (2, 200), (3, 300)",
        schema_name
    ))
    .await
    .expect("Failed to insert data");

    let mut prepared = conn
        .prepare(&format!(
            "SELECT val FROM {}.test_select WHERE id = ?",
            schema_name
        ))
        .await
        .expect("Failed to prepare select");

    prepared.bind(0, 2).expect("Failed to bind param");
    let results = conn
        .execute_prepared(&prepared)
        .await
        .expect("Failed to execute select");

    let batches = results.fetch_all().await.expect("Failed to fetch");
    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1);

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

    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await
        .expect("Failed to drop schema");
    conn.close().await.expect("Failed to close connection");
}

/// 8.4 Prepared statement parameter types over WebSocket.
#[allow(clippy::approx_constant)]
#[tokio::test]
async fn test_ws_prepared_statement_parameter_types() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

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

    let batches = conn
        .query(&format!("SELECT * FROM {}.test_types", schema_name))
        .await
        .expect("Failed to query");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1);

    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema_name))
        .await
        .expect("Failed to drop schema");
    conn.close().await.expect("Failed to close connection");
}

// ── Section 9: WebSocket Regression ──────────────────────────────────────────

/// Large result set exceeding default 16 MiB WebSocket frame limit, over WebSocket.
#[tokio::test]
async fn test_ws_large_result_set_exceeds_default_frame_limit() {
    skip_if_no_exasol!();

    let mut conn = get_ws_connection().await.expect("Failed to connect");

    let schema_name = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema_name))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.wide_test (
            id INTEGER,
            col1 VARCHAR(1000),
            col2 VARCHAR(1000),
            col3 VARCHAR(1000),
            col4 VARCHAR(1000),
            col5 VARCHAR(1000)
        )",
        schema_name
    ))
    .await
    .expect("CREATE TABLE should succeed");

    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.wide_test (id, col1, col2, col3, col4, col5)
        SELECT
            LEVEL,
            LPAD('A', 200, 'A'),
            LPAD('B', 200, 'B'),
            LPAD('C', 200, 'C'),
            LPAD('D', 200, 'D'),
            LPAD('E', 200, 'E')
        FROM DUAL
        CONNECT BY LEVEL <= 20000
        "#,
        schema_name
    ))
    .await
    .expect("INSERT should succeed");

    let all_batches = conn
        .query(&format!(
            "SELECT id, col1, col2, col3, col4, col5 FROM {}.wide_test",
            schema_name
        ))
        .await
        .expect("SELECT all rows should succeed (must handle frames > 16 MiB)");

    let total_rows: usize = all_batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        total_rows, 20000,
        "Should have all 20,000 rows returned despite large frame size"
    );

    ws_cleanup_schema(&mut conn, &schema_name).await;
    conn.close().await.expect("Failed to close connection");
}

// ── Certificate fingerprint tests ────────────────────────────────────────────

/// Connect with wrong fingerprint over WebSocket — error should expose the actual fingerprint.
#[tokio::test]
async fn test_ws_connect_with_wrong_fingerprint_fails() {
    skip_if_no_exasol!();

    let conn_str = format!(
        "exasol://{}:{}@{}:{}?tls=true&certificate_fingerprint=0000000000000000000000000000000000000000000000000000000000000000&transport=websocket",
        get_user(),
        get_password(),
        get_host(),
        get_port(),
    );

    let driver = Driver::new();
    let database = driver.open(&conn_str).expect("open should succeed");
    let result = database.connect().await;

    assert!(
        result.is_err(),
        "Connection with wrong fingerprint should fail"
    );

    let err_msg = result.unwrap_err().to_string();
    let hex_chars: String = err_msg.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    assert!(
        hex_chars.len() >= 64,
        "Error message should contain a 64-char SHA-256 hex fingerprint, got: {}",
        err_msg
    );
}

/// Connect with a discovered certificate fingerprint over WebSocket.
#[tokio::test]
async fn test_ws_connect_with_certificate_fingerprint() {
    skip_if_no_exasol!();

    let conn_str_wrong = format!(
        "exasol://{}:{}@{}:{}?tls=true&certificate_fingerprint=placeholder&transport=websocket",
        get_user(),
        get_password(),
        get_host(),
        get_port(),
    );

    let driver = Driver::new();
    let database = driver.open(&conn_str_wrong).expect("open should succeed");
    let result = database.connect().await;

    assert!(
        result.is_err(),
        "Connection with placeholder fingerprint should fail"
    );

    let err_msg = result.unwrap_err().to_string();

    let actual_fingerprint = err_msg
        .split("got ")
        .nth(1)
        .map(|s| s.trim().to_string())
        .expect("Error message should contain 'got <fingerprint>'");

    assert_eq!(
        actual_fingerprint.len(),
        64,
        "Actual fingerprint should be 64 hex chars, got: '{}'",
        actual_fingerprint
    );

    let conn_str_pinned = format!(
        "exasol://{}:{}@{}:{}?tls=true&certificate_fingerprint={}&transport=websocket",
        get_user(),
        get_password(),
        get_host(),
        get_port(),
        actual_fingerprint,
    );

    let database2 = driver.open(&conn_str_pinned).expect("open should succeed");
    let conn = database2
        .connect()
        .await
        .expect("Connection with correct fingerprint should succeed");

    assert!(
        !conn.is_closed().await,
        "Connection should be open after fingerprint-pinned connect"
    );

    conn.close().await.expect("Failed to close connection");
}
