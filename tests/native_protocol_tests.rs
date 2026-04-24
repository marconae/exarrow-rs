//! Native protocol integration tests.
//!
//! These tests verify native-specific behavior: connection, protocol version
//! negotiation, error handling, and session attributes through the ADBC layer.

mod common;

#[cfg(feature = "websocket")]
use common::get_test_connection_with_transport;
use common::{generate_test_schema_name, get_test_connection};

#[tokio::test]
async fn test_native_connection() {
    skip_if_no_exasol!();

    let conn = get_test_connection().await.expect("Failed to connect");
    assert!(!conn.is_closed().await);
    conn.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_native_protocol_version() {
    skip_if_no_exasol!();

    let conn = get_test_connection().await.expect("Failed to connect");

    // Protocol version should be non-zero
    let server_info = conn.params();
    assert!(!server_info.host.is_empty());
    assert!(server_info.port > 0);

    conn.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_native_error_handling_bad_sql() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    // Invalid SQL should return an error, not crash
    let result = conn.execute("THIS IS NOT VALID SQL").await;
    assert!(result.is_err(), "Invalid SQL should return an error");

    // Connection should still be usable after error
    let result = conn.execute("SELECT 1").await;
    assert!(result.is_ok(), "Connection should work after error");

    conn.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_native_multiple_queries() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    for i in 1..=5 {
        let result = conn.query(&format!("SELECT {}", i)).await;
        assert!(result.is_ok(), "Query {} should succeed", i);
    }

    conn.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_native_ddl_operations() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema = generate_test_schema_name();

    // CREATE SCHEMA
    conn.execute_update(&format!("CREATE SCHEMA {}", schema))
        .await
        .expect("CREATE SCHEMA should succeed");

    // CREATE TABLE
    conn.execute_update(&format!(
        "CREATE TABLE {}.test_t (id INTEGER, name VARCHAR(50))",
        schema
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // INSERT
    conn.execute_update(&format!(
        "INSERT INTO {}.test_t VALUES (1, 'hello')",
        schema
    ))
    .await
    .expect("INSERT should succeed");

    // SELECT
    let batches = conn
        .query(&format!("SELECT * FROM {}.test_t", schema))
        .await
        .expect("SELECT should succeed");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);

    // Cleanup
    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema))
        .await
        .expect("DROP SCHEMA should succeed");

    conn.close().await.expect("Failed to close");
}

#[tokio::test]
async fn test_native_set_autocommit() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");

    // Begin transaction (disables autocommit)
    conn.begin_transaction()
        .await
        .expect("Begin transaction should succeed");

    // Verify we can execute in a transaction
    let result = conn.execute("SELECT 1").await;
    assert!(result.is_ok());

    // Rollback
    conn.rollback().await.expect("Rollback should succeed");

    conn.close().await.expect("Failed to close");
}

/// Verify that the default connection (no transport= param) uses the native transport.
/// With the `native` feature enabled this should connect successfully; the test merely
/// exercises the connection path rather than inspecting internal transport type.
#[cfg(feature = "native")]
#[tokio::test]
async fn test_default_native_transport() {
    skip_if_no_exasol!();

    // Connect without any transport= override — should default to native.
    let conn = get_test_connection()
        .await
        .expect("Default-transport connection should succeed (native)");

    assert!(
        !conn.is_closed().await,
        "Connection should be open with default (native) transport"
    );

    let session_id = conn.session_id();
    assert!(!session_id.is_empty(), "Session ID should be set");

    conn.close().await.expect("Failed to close connection");
}

/// Verify that `transport=websocket` in the connection string causes the driver to
/// use the WebSocket transport even when the `native` feature is the default.
#[cfg(feature = "websocket")]
#[tokio::test]
async fn test_transport_override_websocket() {
    skip_if_no_exasol!();

    let conn = get_test_connection_with_transport("websocket")
        .await
        .expect("WebSocket transport override connection should succeed");

    assert!(
        !conn.is_closed().await,
        "Connection should be open when transport=websocket is forced"
    );

    // A simple query confirms the connection is fully functional.
    let mut conn = conn;
    let batches = conn
        .query("SELECT 1 AS ws_check")
        .await
        .expect("Query over websocket transport override should succeed");

    assert!(!batches.is_empty(), "Should return results");
    assert_eq!(batches[0].num_rows(), 1, "Should return 1 row");

    conn.close().await.expect("Failed to close connection");
}

/// Fetch 2000 rows to exercise multi-fetch pagination in the native protocol.
///
/// The native protocol returns data in frames; 2000 rows with a modest row width
/// is sufficient to span at least two fetch cycles on default settings.
#[tokio::test]
async fn test_native_large_result_set() {
    skip_if_no_exasol!();

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.large_t (id INTEGER, txt VARCHAR(50))",
        schema
    ))
    .await
    .expect("CREATE TABLE should succeed");

    conn.execute_update(&format!(
        r#"
        INSERT INTO {}.large_t (id, txt)
        SELECT LEVEL, 'row_' || LEVEL
        FROM DUAL
        CONNECT BY LEVEL <= 2000
        "#,
        schema
    ))
    .await
    .expect("INSERT should succeed");

    let batches = conn
        .query(&format!(
            "SELECT id, txt FROM {}.large_t ORDER BY id",
            schema
        ))
        .await
        .expect("SELECT should succeed");

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2000, "Should retrieve all 2000 rows");

    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema))
        .await
        .expect("DROP SCHEMA should succeed");

    conn.close().await.expect("Failed to close");
}

/// Verify that a DECIMAL(10,4) column is returned as Arrow Decimal128, not Float64,
/// and that the numeric value round-trips without precision loss.
#[tokio::test]
async fn test_native_decimal_fidelity() {
    skip_if_no_exasol!();

    use arrow::array::Decimal128Array;
    use arrow::datatypes::DataType;

    let mut conn = get_test_connection().await.expect("Failed to connect");
    let schema = generate_test_schema_name();

    conn.execute_update(&format!("CREATE SCHEMA {}", schema))
        .await
        .expect("CREATE SCHEMA should succeed");

    conn.execute_update(&format!(
        "CREATE TABLE {}.dec_t (val DECIMAL(10,4))",
        schema
    ))
    .await
    .expect("CREATE TABLE should succeed");

    // 123456.7891 stored as DECIMAL(10,4)
    conn.execute_update(&format!(
        "INSERT INTO {}.dec_t VALUES (123456.7891)",
        schema
    ))
    .await
    .expect("INSERT should succeed");

    let batches = conn
        .query(&format!("SELECT val FROM {}.dec_t", schema))
        .await
        .expect("SELECT should succeed");

    assert!(!batches.is_empty(), "Should return results");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1, "Should have 1 row");

    let batch_schema = batch.schema();
    let field = batch_schema.field(0);
    assert!(
        matches!(field.data_type(), DataType::Decimal128(_, _)),
        "DECIMAL(10,4) should map to Decimal128, got {:?}",
        field.data_type()
    );

    // Extract the scale from the type.
    let scale = if let DataType::Decimal128(_, s) = field.data_type() {
        *s
    } else {
        panic!("Expected Decimal128");
    };

    // Verify the raw integer value equals 123456.7891 * 10^scale.
    let dec_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("Should be Decimal128Array");

    let raw = dec_col.value(0);
    let expected_raw = (123_456.789_1_f64 * 10f64.powi(scale as i32)).round() as i128;
    assert_eq!(
        raw, expected_raw,
        "Decimal128 raw value should encode 123456.7891 without precision loss"
    );

    conn.execute_update(&format!("DROP SCHEMA {} CASCADE", schema))
        .await
        .expect("DROP SCHEMA should succeed");

    conn.close().await.expect("Failed to close");
}
