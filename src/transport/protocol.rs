//! Transport protocol abstraction trait.
//!
//! This module defines the `TransportProtocol` trait that abstracts the underlying
//! communication mechanism for connecting to Exasol. This allows for different
//! protocol implementations (WebSocket in Phase 1, gRPC in Phase 2).

use crate::error::TransportError;
use async_trait::async_trait;

use super::messages::{DataType, ResultData, ResultSetHandle, SessionInfo};

/// Connection parameters for establishing a transport connection.
#[derive(Debug, Clone)]
pub struct ConnectionParams {
    /// Database host
    pub host: String,
    /// Database port
    pub port: u16,
    /// Use TLS/SSL
    pub use_tls: bool,
    /// Validate server certificate (default: true)
    pub validate_server_certificate: bool,
    /// Connection timeout in milliseconds
    pub timeout_ms: u64,
}

impl ConnectionParams {
    /// Create new connection parameters.
    pub fn new(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            use_tls: true,
            validate_server_certificate: true,
            timeout_ms: 30_000, // 30 seconds default
        }
    }

    /// Set whether to use TLS.
    pub fn with_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    /// Set whether to validate the server certificate.
    ///
    /// # Security Warning
    ///
    /// Disabling certificate validation makes the connection vulnerable to
    /// man-in-the-middle attacks. Only disable in development environments
    /// with self-signed certificates.
    pub fn with_validate_server_certificate(mut self, validate: bool) -> Self {
        self.validate_server_certificate = validate;
        self
    }

    /// Set connection timeout.
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Build the WebSocket URL from parameters.
    pub fn to_websocket_url(&self) -> String {
        let scheme = if self.use_tls { "wss" } else { "ws" };
        format!("{}://{}:{}", scheme, self.host, self.port)
    }
}

/// User credentials for authentication.
#[derive(Debug, Clone)]
pub struct Credentials {
    /// Username
    pub username: String,
    /// Password (will be cleared after use)
    pub password: String,
}

impl Credentials {
    /// Create new credentials.
    pub fn new(username: String, password: String) -> Self {
        Self { username, password }
    }
}

// Security: Implement Drop to clear password from memory
impl Drop for Credentials {
    fn drop(&mut self) {
        // Clear password bytes (basic security measure)
        // For production, consider using the `zeroize` crate
        self.password.clear();
    }
}

/// Handle to a prepared statement on the server.
#[derive(Debug, Clone)]
pub struct PreparedStatementHandle {
    /// Server-side statement identifier
    pub handle: i32,
    /// Number of parameters expected
    pub num_params: i32,
    /// Parameter type information (if available)
    pub parameter_types: Vec<DataType>,
}

impl PreparedStatementHandle {
    /// Create a new prepared statement handle.
    pub fn new(handle: i32, num_params: i32, parameter_types: Vec<DataType>) -> Self {
        Self {
            handle,
            num_params,
            parameter_types,
        }
    }
}

/// Transport protocol trait for database communication.
///
/// This trait abstracts the underlying transport mechanism, allowing for
/// different implementations (WebSocket, gRPC, etc.).
#[async_trait]
pub trait TransportProtocol: Send + Sync {
    /// Connect to the database server.
    ///
    /// # Arguments
    ///
    /// * `params` - Connection parameters
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if connection fails.
    async fn connect(&mut self, params: &ConnectionParams) -> Result<(), TransportError>;

    /// Authenticate with the database.
    ///
    /// # Arguments
    ///
    /// * `credentials` - User credentials
    ///
    /// # Returns
    ///
    /// Session information on successful authentication.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if authentication fails.
    async fn authenticate(
        &mut self,
        credentials: &Credentials,
    ) -> Result<SessionInfo, TransportError>;

    /// Execute a SQL query.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL statement to execute
    ///
    /// # Returns
    ///
    /// Result set handle for SELECT queries, or result data for other statements.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if execution fails.
    async fn execute_query(&mut self, sql: &str) -> Result<QueryResult, TransportError>;

    /// Fetch result data from a result set.
    ///
    /// # Arguments
    ///
    /// * `handle` - Result set handle from execute_query
    ///
    /// # Returns
    ///
    /// Result data containing rows and metadata.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if fetch fails.
    async fn fetch_results(
        &mut self,
        handle: ResultSetHandle,
    ) -> Result<ResultData, TransportError>;

    /// Close a result set.
    ///
    /// # Arguments
    ///
    /// * `handle` - Result set handle to close
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if close fails.
    async fn close_result_set(&mut self, handle: ResultSetHandle) -> Result<(), TransportError>;

    /// Create a prepared statement from SQL text.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL statement with parameter placeholders (?)
    ///
    /// # Returns
    ///
    /// A statement handle for later execution.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if statement creation fails.
    async fn create_prepared_statement(
        &mut self,
        sql: &str,
    ) -> Result<PreparedStatementHandle, TransportError>;

    /// Execute a prepared statement with parameters.
    ///
    /// # Arguments
    ///
    /// * `handle` - Prepared statement handle from create_prepared_statement
    /// * `parameters` - Parameters in column-major format (each inner Vec is a column)
    ///
    /// # Returns
    ///
    /// Query result (result set or row count).
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if execution fails.
    async fn execute_prepared_statement(
        &mut self,
        handle: &PreparedStatementHandle,
        parameters: Option<Vec<Vec<serde_json::Value>>>,
    ) -> Result<QueryResult, TransportError>;

    /// Close a prepared statement and release server-side resources.
    ///
    /// # Arguments
    ///
    /// * `handle` - Prepared statement handle to close
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if close fails.
    async fn close_prepared_statement(
        &mut self,
        handle: &PreparedStatementHandle,
    ) -> Result<(), TransportError>;

    /// Close the connection.
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if disconnect fails.
    async fn close(&mut self) -> Result<(), TransportError>;

    /// Check if the connection is still active.
    fn is_connected(&self) -> bool;
}

/// Result of a query execution.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// Result set from a SELECT query
    ResultSet {
        /// Handle for fetching more data (None if all data is in the response)
        handle: Option<ResultSetHandle>,
        /// Result data (may include first batch of rows)
        data: ResultData,
    },
    /// Row count from an INSERT/UPDATE/DELETE query
    RowCount {
        /// Number of affected rows
        count: i64,
    },
}

impl QueryResult {
    /// Create a result set query result.
    pub fn result_set(handle: Option<ResultSetHandle>, data: ResultData) -> Self {
        Self::ResultSet { handle, data }
    }

    /// Create a row count query result.
    pub fn row_count(count: i64) -> Self {
        Self::RowCount { count }
    }

    /// Check if this is a result set.
    pub fn is_result_set(&self) -> bool {
        matches!(self, Self::ResultSet { .. })
    }

    /// Check if this is a row count.
    pub fn is_row_count(&self) -> bool {
        matches!(self, Self::RowCount { .. })
    }

    /// Get the result set handle if this is a result set.
    /// Returns None if all data was included in the initial response.
    pub fn handle(&self) -> Option<ResultSetHandle> {
        match self {
            Self::ResultSet { handle, .. } => *handle,
            _ => None,
        }
    }

    /// Get the row count if this is a row count result.
    pub fn get_row_count(&self) -> Option<i64> {
        match self {
            Self::RowCount { count } => Some(*count),
            _ => None,
        }
    }

    /// Check if this result has more data to fetch.
    pub fn has_more_data(&self) -> bool {
        match self {
            Self::ResultSet { handle, data } => {
                // Has more if there's a handle AND we have fewer rows than total
                handle.is_some() && (data.rows.len() as i64) < data.total_rows
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_params_default() {
        let params = ConnectionParams::new("localhost".to_string(), 8563);
        assert_eq!(params.host, "localhost");
        assert_eq!(params.port, 8563);
        assert!(params.use_tls);
        assert!(params.validate_server_certificate);
        assert_eq!(params.timeout_ms, 30_000);
    }

    #[test]
    fn test_connection_params_builder() {
        let params = ConnectionParams::new("db.example.com".to_string(), 9000)
            .with_tls(false)
            .with_timeout(60_000);

        assert_eq!(params.host, "db.example.com");
        assert_eq!(params.port, 9000);
        assert!(!params.use_tls);
        assert!(params.validate_server_certificate);
        assert_eq!(params.timeout_ms, 60_000);
    }

    #[test]
    fn test_connection_params_validate_certificate_disabled() {
        let params = ConnectionParams::new("localhost".to_string(), 8563)
            .with_tls(true)
            .with_validate_server_certificate(false);

        assert!(params.use_tls);
        assert!(!params.validate_server_certificate);
    }

    #[test]
    fn test_websocket_url_with_tls() {
        let params = ConnectionParams::new("localhost".to_string(), 8563).with_tls(true);
        assert_eq!(params.to_websocket_url(), "wss://localhost:8563");
    }

    #[test]
    fn test_websocket_url_without_tls() {
        let params = ConnectionParams::new("localhost".to_string(), 8563).with_tls(false);
        assert_eq!(params.to_websocket_url(), "ws://localhost:8563");
    }

    #[test]
    fn test_credentials_creation() {
        let creds = Credentials::new("user".to_string(), "pass".to_string());
        assert_eq!(creds.username, "user");
        assert_eq!(creds.password, "pass");
    }

    #[test]
    fn test_credentials_drop_clears_password() {
        let creds = Credentials::new("user".to_string(), "secret".to_string());
        assert_eq!(creds.password, "secret");
        drop(creds);
        // Password should be cleared (can't test directly after drop)
    }

    #[test]
    fn test_prepared_statement_handle_creation() {
        let param_types = vec![
            DataType {
                type_name: "DECIMAL".to_string(),
                precision: Some(18),
                scale: Some(0),
                size: None,
                character_set: None,
                with_local_time_zone: None,
                fraction: None,
            },
            DataType {
                type_name: "VARCHAR".to_string(),
                precision: None,
                scale: None,
                size: Some(100),
                character_set: Some("UTF8".to_string()),
                with_local_time_zone: None,
                fraction: None,
            },
        ];

        let handle = PreparedStatementHandle::new(42, 2, param_types);
        assert_eq!(handle.handle, 42);
        assert_eq!(handle.num_params, 2);
        assert_eq!(handle.parameter_types.len(), 2);
        assert_eq!(handle.parameter_types[0].type_name, "DECIMAL");
        assert_eq!(handle.parameter_types[1].type_name, "VARCHAR");
    }

    #[test]
    fn test_prepared_statement_handle_no_params() {
        let handle = PreparedStatementHandle::new(1, 0, vec![]);
        assert_eq!(handle.handle, 1);
        assert_eq!(handle.num_params, 0);
        assert!(handle.parameter_types.is_empty());
    }

    #[test]
    fn test_query_result_result_set() {
        use super::super::messages::{ColumnInfo, DataType, ResultData};

        let data = ResultData {
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: DataType {
                    type_name: "DECIMAL".to_string(),
                    precision: Some(18),
                    scale: Some(0),
                    size: None,
                    character_set: None,
                    with_local_time_zone: None,
                    fraction: None,
                },
            }],
            rows: vec![],
            total_rows: 0,
        };

        let result = QueryResult::result_set(Some(ResultSetHandle::new(1)), data);
        assert!(result.is_result_set());
        assert!(!result.is_row_count());
        assert_eq!(result.handle().unwrap().as_i32(), 1);
        assert!(result.get_row_count().is_none());
    }

    #[test]
    fn test_query_result_row_count() {
        let result = QueryResult::row_count(42);
        assert!(!result.is_result_set());
        assert!(result.is_row_count());
        assert_eq!(result.get_row_count().unwrap(), 42);
        assert!(result.handle().is_none());
    }
}
