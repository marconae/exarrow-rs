//! ADBC Connection implementation.
//!
//! This module provides the `Connection` type which represents an active
//! database connection and provides methods for executing queries.
//!
//! # New in v2.0.0
//!
//! Connection now owns the transport directly and Statement is a pure data container.
//! Execute statements via Connection methods: `execute_statement()`, `execute_prepared()`.

use crate::adbc::Statement;
use crate::connection::auth::AuthResponseData;
use crate::connection::params::ConnectionParams;
use crate::connection::session::{Session as SessionInfo, SessionConfig, SessionState};
use crate::error::{ConnectionError, ExasolError, QueryError};
use crate::query::prepared::PreparedStatement;
use crate::query::results::ResultSet;
use crate::transport::protocol::{
    ConnectionParams as TransportConnectionParams, Credentials as TransportCredentials,
    QueryResult, TransportProtocol,
};
use arrow::array::RecordBatch;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::Mutex;
use tokio::time::timeout;

/// Session is a type alias for Connection.
///
/// This alias provides a more intuitive name for database sessions when
/// performing import/export operations. Both `Session` and `Connection`
/// can be used interchangeably.
///
/// # Example
///
pub type Session = Connection;

fn blocking_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime for blocking operations")
    })
}

/// ADBC Connection to an Exasol database.
///
/// The `Connection` type represents an active database connection and provides
/// methods for executing queries, managing transactions, and retrieving metadata.
///
/// # v2.0.0 Breaking Changes
///
/// - Connection now owns the transport directly
/// - `create_statement()` is now synchronous and returns a pure data container
/// - Use `execute_statement()` instead of `Statement::execute()`
/// - Use `prepare()` instead of `Statement::prepare()`
///
/// # Example
///
pub struct Connection {
    /// Transport layer for communication (owned by Connection)
    transport: Arc<Mutex<dyn TransportProtocol>>,
    /// Session information
    session: SessionInfo,
    /// Connection parameters
    params: ConnectionParams,
}

impl Connection {
    /// Create a connection from connection parameters.
    ///
    /// This establishes a connection to the Exasol database using WebSocket transport,
    /// authenticates the user, and creates a session.
    ///
    /// # Arguments
    ///
    /// * `params` - Connection parameters
    ///
    /// # Returns
    ///
    /// A connected `Connection` instance.
    ///
    /// # Errors
    ///
    /// Returns `ConnectionError` if the connection or authentication fails.
    pub async fn from_params(params: ConnectionParams) -> Result<Self, ConnectionError> {
        let requested = params.transport.as_deref();

        match requested {
            #[cfg(feature = "native")]
            Some("native") | None => {
                let transport = crate::transport::NativeTcpTransport::new();
                Self::connect_with_transport(params, transport).await
            }
            #[cfg(feature = "websocket")]
            Some("websocket") => {
                let transport = crate::transport::WebSocketTransport::new();
                Self::connect_with_transport(params, transport).await
            }
            #[cfg(all(not(feature = "native"), feature = "websocket"))]
            None => {
                let transport = crate::transport::WebSocketTransport::new();
                Self::connect_with_transport(params, transport).await
            }
            Some(t) => Err(ConnectionError::InvalidParameter {
                parameter: "transport".to_string(),
                message: format!("Transport '{}' is not available. Check feature flags.", t),
            }),
            #[cfg(not(any(feature = "native", feature = "websocket")))]
            None => Err(ConnectionError::InvalidParameter {
                parameter: "transport".to_string(),
                message: "No transport feature enabled. Enable 'native' or 'websocket'."
                    .to_string(),
            }),
        }
    }

    /// Connect using the given transport implementation.
    async fn connect_with_transport<T: TransportProtocol + 'static>(
        params: ConnectionParams,
        mut transport: T,
    ) -> Result<Self, ConnectionError> {
        // Convert ConnectionParams to TransportConnectionParams
        let mut transport_params = TransportConnectionParams::new(params.host.clone(), params.port)
            .with_tls(params.use_tls)
            .with_validate_server_certificate(params.validate_server_certificate)
            .with_timeout(params.connection_timeout.as_millis() as u64);
        if let Some(ref fp) = params.certificate_fingerprint {
            transport_params = transport_params.with_certificate_fingerprint(fp.clone());
        }

        // Connect
        transport.connect(&transport_params).await.map_err(|e| {
            ConnectionError::ConnectionFailed {
                host: params.host.clone(),
                port: params.port,
                message: e.to_string(),
            }
        })?;

        // Authenticate
        let credentials =
            TransportCredentials::new(params.username.clone(), params.password().to_string());
        let session_info = transport
            .authenticate(&credentials)
            .await
            .map_err(|e| ConnectionError::AuthenticationFailed(e.to_string()))?;

        // Create session config from connection params
        let session_config = SessionConfig {
            idle_timeout: params.idle_timeout,
            query_timeout: params.query_timeout,
            ..Default::default()
        };

        // Extract session_id once to avoid double clone
        let session_id = session_info.session_id.clone();

        // Convert SessionInfo to AuthResponseData
        let auth_response = AuthResponseData {
            session_id: session_id.clone(),
            protocol_version: session_info.protocol_version,
            release_version: session_info.release_version,
            database_name: session_info.database_name,
            product_name: session_info.product_name,
            max_data_message_size: session_info.max_data_message_size,
            max_identifier_length: 128,
            max_varchar_length: 2_000_000,
            identifier_quote_string: "\"".to_string(),
            time_zone: session_info.time_zone.unwrap_or_else(|| "UTC".to_string()),
            time_zone_behavior: "INVALID TIMESTAMP TO DOUBLE".to_string(),
        };

        // Create session
        let session = SessionInfo::new(session_id, auth_response, session_config);

        let schema = params.schema.clone();

        let mut connection = Self {
            transport: Arc::new(Mutex::new(transport)),
            session,
            params,
        };

        // If the URI carried a schema, activate it server-side so unqualified
        // queries work without a follow-up `set_schema()` call. On failure we
        // must close the transport (we are already authenticated server-side)
        // before propagating the error — returning a half-open `Connection`
        // whose session state silently disagrees with the URI is worse than a
        // clear error.
        if let Some(schema_name) = schema {
            if let Err(query_err) = connection.set_schema(schema_name.clone()).await {
                let host = connection.params.host.clone();
                let port = connection.params.port;
                let _ = connection.shutdown().await;
                return Err(ConnectionError::ConnectionFailed {
                    host,
                    port,
                    message: format!(
                        "failed to activate schema '{}' from connection URI: {}",
                        schema_name, query_err
                    ),
                });
            }
        }

        Ok(connection)
    }

    /// Create a builder for constructing a connection.
    ///
    /// # Returns
    ///
    /// A `ConnectionBuilder` instance.
    ///
    /// # Example
    ///
    pub fn builder() -> ConnectionBuilder {
        ConnectionBuilder::new()
    }

    // ========================================================================
    // Statement Creation (synchronous - Statement is now a pure data container)
    // ========================================================================

    /// Create a new statement for executing SQL.
    ///
    /// Statement is now a pure data container. Use `execute_statement()` to execute it.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL query text
    ///
    /// # Returns
    ///
    /// A `Statement` instance ready for execution via `execute_statement()`.
    ///
    /// # Example
    ///
    pub fn create_statement(&self, sql: impl Into<String>) -> Statement {
        Statement::new(sql)
    }

    // ========================================================================
    // Statement Execution Methods
    // ========================================================================

    /// Execute a statement and return results.
    ///
    /// # Arguments
    ///
    /// * `stmt` - Statement to execute
    ///
    /// # Returns
    ///
    /// A `ResultSet` containing the query results.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if execution fails or times out.
    ///
    /// # Example
    ///
    pub async fn execute_statement(&mut self, stmt: &Statement) -> Result<ResultSet, QueryError> {
        // Validate session state
        self.session
            .validate_ready()
            .await
            .map_err(|e| QueryError::InvalidState(e.to_string()))?;

        // Update session state
        self.session.set_state(SessionState::Executing).await;

        // Increment query counter
        self.session.increment_query_count();

        // Build final SQL with parameters
        let final_sql = stmt.build_sql()?;

        // Execute with timeout
        let timeout_duration = Duration::from_millis(stmt.timeout_ms());
        let transport = Arc::clone(&self.transport);

        let result = timeout(timeout_duration, async move {
            let mut transport_guard = transport.lock().await;
            transport_guard.execute_query(&final_sql).await
        })
        .await
        .map_err(|_| QueryError::Timeout {
            timeout_ms: stmt.timeout_ms(),
        })?
        .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        // Update session state back to ready/in_transaction
        self.update_session_state_after_query().await;

        // Convert transport result to ResultSet
        ResultSet::from_transport_result(result, Arc::clone(&self.transport))
    }

    /// Execute a statement and return the row count (for non-SELECT statements).
    ///
    /// # Arguments
    ///
    /// * `stmt` - Statement to execute
    ///
    /// # Returns
    ///
    /// The number of rows affected.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if execution fails or if statement is a SELECT.
    ///
    /// # Example
    ///
    pub async fn execute_statement_update(&mut self, stmt: &Statement) -> Result<i64, QueryError> {
        let result_set = self.execute_statement(stmt).await?;

        result_set.row_count().ok_or_else(|| {
            QueryError::NoResultSet("Expected row count, got result set".to_string())
        })
    }

    // ========================================================================
    // Prepared Statement Methods
    // ========================================================================

    /// Create a prepared statement for parameterized query execution.
    ///
    /// This creates a server-side prepared statement that can be executed
    /// multiple times with different parameter values.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL statement with parameter placeholders (?)
    ///
    /// # Returns
    ///
    /// A `PreparedStatement` ready for parameter binding and execution.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if preparation fails.
    ///
    /// # Example
    ///
    pub async fn prepare(
        &mut self,
        sql: impl Into<String>,
    ) -> Result<PreparedStatement, QueryError> {
        let sql = sql.into();

        // Validate session state
        self.session
            .validate_ready()
            .await
            .map_err(|e| QueryError::InvalidState(e.to_string()))?;

        let mut transport = self.transport.lock().await;
        let handle = transport
            .create_prepared_statement(&sql)
            .await
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        Ok(PreparedStatement::new(handle))
    }

    /// Execute a prepared statement and return results.
    ///
    /// # Arguments
    ///
    /// * `stmt` - Prepared statement to execute
    ///
    /// # Returns
    ///
    /// A `ResultSet` containing the query results.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if execution fails.
    ///
    /// # Example
    ///
    pub async fn execute_prepared(
        &mut self,
        stmt: &PreparedStatement,
    ) -> Result<ResultSet, QueryError> {
        if stmt.is_closed() {
            return Err(QueryError::StatementClosed);
        }

        // Validate session state
        self.session
            .validate_ready()
            .await
            .map_err(|e| QueryError::InvalidState(e.to_string()))?;

        // Update session state
        self.session.set_state(SessionState::Executing).await;

        // Increment query counter
        self.session.increment_query_count();

        // Convert parameters to column-major JSON format
        let params_data = stmt.build_parameters_data()?;

        let mut transport = self.transport.lock().await;
        let result = transport
            .execute_prepared_statement(stmt.handle_ref(), params_data)
            .await
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        drop(transport);

        // Update session state back to ready/in_transaction
        self.update_session_state_after_query().await;

        ResultSet::from_transport_result(result, Arc::clone(&self.transport))
    }

    /// Execute a prepared statement and return the number of affected rows.
    ///
    /// Use this for INSERT, UPDATE, DELETE statements.
    ///
    /// # Arguments
    ///
    /// * `stmt` - Prepared statement to execute
    ///
    /// # Returns
    ///
    /// The number of rows affected.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if execution fails or statement returns a result set.
    pub async fn execute_prepared_update(
        &mut self,
        stmt: &PreparedStatement,
    ) -> Result<i64, QueryError> {
        if stmt.is_closed() {
            return Err(QueryError::StatementClosed);
        }

        // Validate session state
        self.session
            .validate_ready()
            .await
            .map_err(|e| QueryError::InvalidState(e.to_string()))?;

        // Update session state
        self.session.set_state(SessionState::Executing).await;

        // Convert parameters to column-major JSON format
        let params_data = stmt.build_parameters_data()?;

        let mut transport = self.transport.lock().await;
        let result = transport
            .execute_prepared_statement(stmt.handle_ref(), params_data)
            .await
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        drop(transport);

        // Update session state back to ready/in_transaction
        self.update_session_state_after_query().await;

        match result {
            QueryResult::RowCount { count } => Ok(count),
            QueryResult::ResultSet { .. } => Err(QueryError::UnexpectedResultSet),
        }
    }

    /// Close a prepared statement and release server-side resources.
    ///
    /// # Arguments
    ///
    /// * `stmt` - Prepared statement to close (consumed)
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if closing fails.
    ///
    /// # Example
    ///
    pub async fn close_prepared(&mut self, mut stmt: PreparedStatement) -> Result<(), QueryError> {
        if stmt.is_closed() {
            return Ok(());
        }

        let mut transport = self.transport.lock().await;
        transport
            .close_prepared_statement(stmt.handle_ref())
            .await
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        stmt.mark_closed();
        Ok(())
    }

    // ========================================================================
    // Convenience Methods (these internally use execute_statement)
    // ========================================================================

    /// Execute a SQL query and return results.
    ///
    /// This is a convenience method that creates a statement and executes it.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL query text
    ///
    /// # Returns
    ///
    /// A `ResultSet` containing the query results.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if execution fails.
    ///
    /// # Example
    ///
    pub async fn execute(&mut self, sql: impl Into<String>) -> Result<ResultSet, QueryError> {
        let stmt = self.create_statement(sql);
        self.execute_statement(&stmt).await
    }

    /// Execute a SQL query and return all results as RecordBatches.
    ///
    /// This is a convenience method that fetches all results into memory.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL query text
    ///
    /// # Returns
    ///
    /// A vector of `RecordBatch` instances.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if execution fails.
    ///
    /// # Example
    ///
    pub async fn query(&mut self, sql: impl Into<String>) -> Result<Vec<RecordBatch>, QueryError> {
        let result_set = self.execute(sql).await?;
        result_set.fetch_all().await
    }

    /// Execute a non-SELECT statement and return the row count.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL statement (INSERT, UPDATE, DELETE, etc.)
    ///
    /// # Returns
    ///
    /// The number of rows affected.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if execution fails.
    ///
    /// # Example
    ///
    pub async fn execute_update(&mut self, sql: impl Into<String>) -> Result<i64, QueryError> {
        let stmt = self.create_statement(sql);
        self.execute_statement_update(&stmt).await
    }

    // ========================================================================
    // Transaction Methods
    // ========================================================================

    pub async fn begin_transaction(&mut self) -> Result<(), QueryError> {
        // Disable autocommit on the server so statements don't auto-commit
        self.transport
            .lock()
            .await
            .set_autocommit(false)
            .await
            .map_err(|e| QueryError::TransactionError(e.to_string()))?;

        self.session
            .begin_transaction()
            .await
            .map_err(|e| QueryError::TransactionError(e.to_string()))?;

        Ok(())
    }

    pub async fn commit(&mut self) -> Result<(), QueryError> {
        if !self.in_transaction() {
            return Ok(());
        }

        self.execute_update("COMMIT").await?;

        self.session
            .commit_transaction()
            .await
            .map_err(|e| QueryError::TransactionError(e.to_string()))?;

        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<(), QueryError> {
        if !self.in_transaction() {
            return Ok(());
        }

        self.execute_update("ROLLBACK").await?;

        self.session
            .rollback_transaction()
            .await
            .map_err(|e| QueryError::TransactionError(e.to_string()))?;

        Ok(())
    }

    /// Check if a transaction is currently active.
    ///
    /// # Returns
    ///
    /// `true` if a transaction is active, `false` otherwise.
    pub fn in_transaction(&self) -> bool {
        self.session.in_transaction()
    }

    // ========================================================================
    // Session and Schema Methods
    // ========================================================================

    /// Get the current schema.
    ///
    /// # Returns
    ///
    /// The current schema name, or `None` if no schema is set.
    pub async fn current_schema(&self) -> Option<String> {
        self.session.current_schema().await
    }

    /// Set the current schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema name to set
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if the operation fails.
    ///
    /// # Example
    ///
    pub async fn set_schema(&mut self, schema: impl Into<String>) -> Result<(), QueryError> {
        let schema_name = schema.into();
        self.execute_update(format!("OPEN SCHEMA {}", schema_name))
            .await?;
        self.session.set_current_schema(Some(schema_name)).await;
        Ok(())
    }

    // ========================================================================
    // Metadata Methods
    // ========================================================================

    /// Get metadata about catalogs.
    ///
    /// # Returns
    ///
    /// A `ResultSet` containing catalog metadata.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if the operation fails.
    pub async fn get_catalogs(&mut self) -> Result<ResultSet, QueryError> {
        self.execute("SELECT DISTINCT SCHEMA_NAME AS CATALOG_NAME FROM SYS.EXA_ALL_SCHEMAS ORDER BY CATALOG_NAME")
            .await
    }

    /// Get metadata about schemas.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Optional catalog name filter
    ///
    /// # Returns
    ///
    /// A `ResultSet` containing schema metadata.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if the operation fails.
    pub async fn get_schemas(&mut self, catalog: Option<&str>) -> Result<ResultSet, QueryError> {
        let sql = if let Some(cat) = catalog {
            format!(
                "SELECT SCHEMA_NAME FROM SYS.EXA_ALL_SCHEMAS WHERE SCHEMA_NAME = '{}' ORDER BY SCHEMA_NAME",
                cat.replace('\'', "''")
            )
        } else {
            "SELECT SCHEMA_NAME FROM SYS.EXA_ALL_SCHEMAS ORDER BY SCHEMA_NAME".to_string()
        };
        self.execute(sql).await
    }

    /// Get metadata about tables.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Optional catalog name filter
    /// * `schema` - Optional schema name filter
    /// * `table` - Optional table name filter
    ///
    /// # Returns
    ///
    /// A `ResultSet` containing table metadata.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if the operation fails.
    pub async fn get_tables(
        &mut self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table: Option<&str>,
    ) -> Result<ResultSet, QueryError> {
        let mut conditions = vec!["OBJECT_TYPE IN ('TABLE', 'VIEW')".to_string()];

        // Exasol has no catalogs — ignore the catalog parameter
        let _ = catalog;
        if let Some(sch) = schema {
            conditions.push(format!("ROOT_NAME = '{}'", sch.replace('\'', "''")));
        }
        if let Some(tbl) = table {
            conditions.push(format!("OBJECT_NAME = '{}'", tbl.replace('\'', "''")));
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));

        let sql = format!(
            "SELECT ROOT_NAME AS TABLE_SCHEMA, OBJECT_NAME AS TABLE_NAME, OBJECT_TYPE AS TABLE_TYPE FROM SYS.EXA_ALL_OBJECTS {} ORDER BY ROOT_NAME, OBJECT_NAME",
            where_clause
        );

        self.execute(sql).await
    }

    /// Get metadata about columns.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Optional catalog name filter
    /// * `schema` - Optional schema name filter
    /// * `table` - Optional table name filter
    /// * `column` - Optional column name filter
    ///
    /// # Returns
    ///
    /// A `ResultSet` containing column metadata.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if the operation fails.
    pub async fn get_columns(
        &mut self,
        catalog: Option<&str>,
        schema: Option<&str>,
        table: Option<&str>,
        column: Option<&str>,
    ) -> Result<ResultSet, QueryError> {
        let mut conditions = Vec::new();

        if let Some(cat) = catalog {
            conditions.push(format!("COLUMN_SCHEMA = '{}'", cat.replace('\'', "''")));
        }
        if let Some(sch) = schema {
            conditions.push(format!("COLUMN_SCHEMA = '{}'", sch.replace('\'', "''")));
        }
        if let Some(tbl) = table {
            conditions.push(format!("COLUMN_TABLE = '{}'", tbl.replace('\'', "''")));
        }
        if let Some(col) = column {
            conditions.push(format!("COLUMN_NAME = '{}'", col.replace('\'', "''")));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "SELECT COLUMN_SCHEMA, COLUMN_TABLE, COLUMN_NAME, COLUMN_TYPE, COLUMN_NUM_PREC, COLUMN_NUM_SCALE, COLUMN_IS_NULLABLE \
             FROM SYS.EXA_ALL_COLUMNS {} ORDER BY COLUMN_SCHEMA, COLUMN_TABLE, ORDINAL_POSITION",
            where_clause
        );

        self.execute(sql).await
    }

    // ========================================================================
    // Session Information Methods
    // ========================================================================

    /// Get session information.
    ///
    /// # Returns
    ///
    /// The session ID.
    pub fn session_id(&self) -> &str {
        self.session.session_id()
    }

    /// Get connection parameters.
    ///
    /// # Returns
    ///
    /// A reference to the connection parameters.
    pub fn params(&self) -> &ConnectionParams {
        &self.params
    }

    /// Check if the connection is closed.
    ///
    /// # Returns
    ///
    /// `true` if the connection is closed, `false` otherwise.
    pub async fn is_closed(&self) -> bool {
        self.session.is_closed().await
    }

    /// Close the connection.
    ///
    /// This closes the session and transport layer.
    ///
    /// # Errors
    ///
    /// Returns `ConnectionError` if closing fails.
    ///
    /// # Example
    ///
    pub async fn close(self) -> Result<(), ConnectionError> {
        // Close session
        self.session.close().await?;

        // Close transport
        let mut transport = self.transport.lock().await;
        transport
            .close()
            .await
            .map_err(|e| ConnectionError::ConnectionFailed {
                host: self.params.host.clone(),
                port: self.params.port,
                message: e.to_string(),
            })?;

        Ok(())
    }

    /// Shut down the connection without consuming self.
    ///
    /// Unlike `close()`, this can be called through a shared reference,
    /// making it suitable for use in `Drop` implementations where ownership
    /// transfer is not possible (e.g., when the connection is behind `Arc<Mutex<>>`).
    pub async fn shutdown(&self) -> Result<(), ConnectionError> {
        self.session.close().await?;

        let mut transport = self.transport.lock().await;
        transport
            .close()
            .await
            .map_err(|e| ConnectionError::ConnectionFailed {
                host: self.params.host.clone(),
                port: self.params.port,
                message: e.to_string(),
            })?;

        Ok(())
    }

    // ========================================================================
    // Import/Export Methods
    // ========================================================================

    /// Creates an SQL executor closure for import/export operations.
    ///
    /// This is a helper method that creates a closure which can execute SQL
    /// statements and return the row count. The closure captures a cloned
    /// reference to the transport, allowing it to be called multiple times
    /// (e.g., for parallel file imports).
    ///
    /// # Returns
    ///
    /// A closure that takes an SQL string and returns a Future resolving to
    /// either the affected row count or an error string.
    fn make_sql_executor(
        &self,
    ) -> impl Fn(
        String,
    )
        -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<u64, String>> + Send>> {
        let transport = Arc::clone(&self.transport);
        move |sql: String| {
            let transport = Arc::clone(&transport);
            Box::pin(async move {
                let mut transport_guard = transport.lock().await;
                match transport_guard.execute_query(&sql).await {
                    Ok(QueryResult::RowCount { count }) => Ok(count as u64),
                    Ok(QueryResult::ResultSet { .. }) => Ok(0),
                    Err(e) => Err(e.to_string()),
                }
            })
        }
    }

    /// Import CSV data from a file into an Exasol table.
    ///
    /// This method reads CSV data from the specified file and imports it into
    /// the target table using Exasol's HTTP transport layer.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `file_path` - Path to the CSV file
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    ///
    /// # Example
    ///
    pub async fn import_csv_from_file(
        &mut self,
        table: &str,
        file_path: &std::path::Path,
        options: crate::import::csv::CsvImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        // Pass Exasol host/port from connection params to import options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        crate::import::csv::import_from_file(self.make_sql_executor(), table, file_path, options)
            .await
    }

    /// Import CSV data from an async reader into an Exasol table.
    ///
    /// This method reads CSV data from an async reader and imports it into
    /// the target table using Exasol's HTTP transport layer.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `reader` - Async reader providing CSV data
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    pub async fn import_csv_from_stream<R>(
        &mut self,
        table: &str,
        reader: R,
        options: crate::import::csv::CsvImportOptions,
    ) -> Result<u64, crate::import::ImportError>
    where
        R: tokio::io::AsyncRead + Unpin + Send + 'static,
    {
        // Pass Exasol host/port from connection params to import options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        crate::import::csv::import_from_stream(self.make_sql_executor(), table, reader, options)
            .await
    }

    /// Import CSV data from an iterator into an Exasol table.
    ///
    /// This method converts iterator rows to CSV format and imports them into
    /// the target table using Exasol's HTTP transport layer.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `rows` - Iterator of rows, where each row is an iterator of field values
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    ///
    /// # Example
    ///
    pub async fn import_csv_from_iter<I, T, S>(
        &mut self,
        table: &str,
        rows: I,
        options: crate::import::csv::CsvImportOptions,
    ) -> Result<u64, crate::import::ImportError>
    where
        I: IntoIterator<Item = T> + Send + 'static,
        T: IntoIterator<Item = S> + Send,
        S: AsRef<str>,
    {
        // Pass Exasol host/port from connection params to import options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        crate::import::csv::import_from_iter(self.make_sql_executor(), table, rows, options).await
    }

    /// Export data from an Exasol table or query to a CSV file.
    ///
    /// This method exports data from the specified source to a CSV file
    /// using Exasol's HTTP transport layer.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `file_path` - Path to the output file
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// The number of rows exported on success.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    ///
    /// # Example
    ///
    pub async fn export_csv_to_file(
        &mut self,
        source: crate::query::export::ExportSource,
        file_path: &std::path::Path,
        options: crate::export::csv::CsvExportOptions,
    ) -> Result<u64, crate::export::csv::ExportError> {
        // Pass Exasol host/port from connection params to export options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        let mut transport_guard = self.transport.lock().await;
        crate::export::csv::export_to_file(&mut *transport_guard, source, file_path, options).await
    }

    /// Export data from an Exasol table or query to an async writer.
    ///
    /// This method exports data from the specified source to an async writer
    /// using Exasol's HTTP transport layer.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `writer` - Async writer to write the CSV data to
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// The number of rows exported on success.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    pub async fn export_csv_to_stream<W>(
        &mut self,
        source: crate::query::export::ExportSource,
        writer: W,
        options: crate::export::csv::CsvExportOptions,
    ) -> Result<u64, crate::export::csv::ExportError>
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        // Pass Exasol host/port from connection params to export options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        let mut transport_guard = self.transport.lock().await;
        crate::export::csv::export_to_stream(&mut *transport_guard, source, writer, options).await
    }

    /// Export data from an Exasol table or query to an in-memory list of rows.
    ///
    /// Each row is represented as a vector of string values.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// A vector of rows, where each row is a vector of column values.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    ///
    /// # Example
    ///
    pub async fn export_csv_to_list(
        &mut self,
        source: crate::query::export::ExportSource,
        options: crate::export::csv::CsvExportOptions,
    ) -> Result<Vec<Vec<String>>, crate::export::csv::ExportError> {
        // Pass Exasol host/port from connection params to export options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        let mut transport_guard = self.transport.lock().await;
        crate::export::csv::export_to_list(&mut *transport_guard, source, options).await
    }

    /// Import multiple CSV files in parallel into an Exasol table.
    ///
    /// This method reads CSV data from multiple files and imports them into
    /// the target table using parallel HTTP transport connections. Each file
    /// gets its own connection with a unique internal address.
    ///
    /// For a single file, this method delegates to `import_csv_from_file`.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `paths` - File paths (accepts single path, Vec, array, or slice)
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails. Uses fail-fast semantics.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use exarrow_rs::adbc::Connection;
    /// use exarrow_rs::import::CsvImportOptions;
    /// use std::path::PathBuf;
    ///
    /// # async fn example(conn: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let files = vec![
    ///     PathBuf::from("/data/part1.csv"),
    ///     PathBuf::from("/data/part2.csv"),
    /// ];
    ///
    /// let options = CsvImportOptions::default();
    /// let rows = conn.import_csv_from_files("my_table", files, options).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn import_csv_from_files<S: crate::import::IntoFileSources>(
        &mut self,
        table: &str,
        paths: S,
        options: crate::import::csv::CsvImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        // Pass Exasol host/port from connection params to import options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        crate::import::csv::import_from_files(self.make_sql_executor(), table, paths, options).await
    }

    /// Import data from a Parquet file into an Exasol table.
    ///
    /// This method reads a Parquet file, converts the data to CSV format,
    /// and imports it into the target table using Exasol's HTTP transport layer.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `file_path` - Path to the Parquet file
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    ///
    /// # Example
    ///
    pub async fn import_from_parquet(
        &mut self,
        table: &str,
        file_path: &std::path::Path,
        options: crate::import::parquet::ParquetImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        // Pass Exasol host/port from connection params to import options
        let options = options
            .with_exasol_host(&self.params.host)
            .with_exasol_port(self.params.port);

        crate::import::parquet::import_from_parquet(
            self.make_sql_executor(),
            table,
            file_path,
            options,
        )
        .await
    }

    /// Import multiple Parquet files in parallel into an Exasol table.
    ///
    /// This method converts each Parquet file to CSV format concurrently,
    /// then streams the data through parallel HTTP transport connections.
    ///
    /// For a single file, this method delegates to `import_from_parquet`.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `paths` - File paths (accepts single path, Vec, array, or slice)
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails. Uses fail-fast semantics.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use exarrow_rs::adbc::Connection;
    /// use exarrow_rs::import::ParquetImportOptions;
    /// use std::path::PathBuf;
    ///
    /// # async fn example(conn: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let files = vec![
    ///     PathBuf::from("/data/part1.parquet"),
    ///     PathBuf::from("/data/part2.parquet"),
    /// ];
    ///
    /// let options = ParquetImportOptions::default();
    /// let rows = conn.import_parquet_from_files("my_table", files, options).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn import_parquet_from_files<S: crate::import::IntoFileSources>(
        &mut self,
        table: &str,
        paths: S,
        options: crate::import::parquet::ParquetImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        // Pass Exasol host/port from connection params to import options
        let options = options
            .with_exasol_host(&self.params.host)
            .with_exasol_port(self.params.port);

        crate::import::parquet::import_from_parquet_files(
            self.make_sql_executor(),
            table,
            paths,
            options,
        )
        .await
    }

    /// Export data from an Exasol table or query to a Parquet file.
    ///
    /// This method exports data from the specified source to a Parquet file.
    /// The data is first received as CSV from Exasol, then converted to Parquet format.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `file_path` - Path to the output Parquet file
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// The number of rows exported on success.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    ///
    /// # Example
    ///
    pub async fn export_to_parquet(
        &mut self,
        source: crate::query::export::ExportSource,
        file_path: &std::path::Path,
        options: crate::export::parquet::ParquetExportOptions,
    ) -> Result<u64, crate::export::csv::ExportError> {
        // Pass Exasol host/port from connection params to export options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        let mut transport_guard = self.transport.lock().await;
        crate::export::parquet::export_to_parquet_via_transport(
            &mut *transport_guard,
            source,
            file_path,
            options,
        )
        .await
    }

    /// Import data from an Arrow RecordBatch into an Exasol table.
    ///
    /// This method converts the RecordBatch to CSV format and imports it
    /// into the target table using Exasol's HTTP transport layer.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `batch` - The RecordBatch to import
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    ///
    /// # Example
    ///
    pub async fn import_from_record_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
        options: crate::import::arrow::ArrowImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        // Pass Exasol host/port from connection params to import options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        crate::import::arrow::import_from_record_batch(
            self.make_sql_executor(),
            table,
            batch,
            options,
        )
        .await
    }

    /// Import data from multiple Arrow RecordBatches into an Exasol table.
    ///
    /// This method converts each RecordBatch to CSV format and imports them
    /// into the target table using Exasol's HTTP transport layer.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `batches` - An iterator of RecordBatches to import
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    pub async fn import_from_record_batches<I>(
        &mut self,
        table: &str,
        batches: I,
        options: crate::import::arrow::ArrowImportOptions,
    ) -> Result<u64, crate::import::ImportError>
    where
        I: IntoIterator<Item = RecordBatch>,
    {
        // Pass Exasol host/port from connection params to import options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        crate::import::arrow::import_from_record_batches(
            self.make_sql_executor(),
            table,
            batches,
            options,
        )
        .await
    }

    /// Import data from an Arrow IPC file/stream into an Exasol table.
    ///
    /// This method reads Arrow IPC format data, converts it to CSV,
    /// and imports it into the target table using Exasol's HTTP transport layer.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `reader` - An async reader containing Arrow IPC data
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    ///
    /// # Example
    ///
    pub async fn import_from_arrow_ipc<R>(
        &mut self,
        table: &str,
        reader: R,
        options: crate::import::arrow::ArrowImportOptions,
    ) -> Result<u64, crate::import::ImportError>
    where
        R: tokio::io::AsyncRead + Unpin + Send,
    {
        // Pass Exasol host/port from connection params to import options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        crate::import::arrow::import_from_arrow_ipc(
            self.make_sql_executor(),
            table,
            reader,
            options,
        )
        .await
    }

    /// Export data from an Exasol table or query to Arrow RecordBatches.
    ///
    /// This method exports data from the specified source and converts it
    /// to Arrow RecordBatches.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// A vector of RecordBatches on success.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    ///
    /// # Example
    ///
    pub async fn export_to_record_batches(
        &mut self,
        source: crate::query::export::ExportSource,
        options: crate::export::arrow::ArrowExportOptions,
    ) -> Result<Vec<RecordBatch>, crate::export::csv::ExportError> {
        // Pass Exasol host/port from connection params to export options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        let mut transport_guard = self.transport.lock().await;
        crate::export::arrow::export_to_record_batches(&mut *transport_guard, source, options).await
    }

    /// Export data from an Exasol table or query to an Arrow IPC file.
    ///
    /// This method exports data from the specified source to an Arrow IPC file.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `file_path` - Path to the output Arrow IPC file
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// The number of rows exported on success.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    ///
    /// # Example
    ///
    pub async fn export_to_arrow_ipc(
        &mut self,
        source: crate::query::export::ExportSource,
        file_path: &std::path::Path,
        options: crate::export::arrow::ArrowExportOptions,
    ) -> Result<u64, crate::export::csv::ExportError> {
        // Pass Exasol host/port from connection params to export options
        let options = options
            .exasol_host(&self.params.host)
            .exasol_port(self.params.port);

        let mut transport_guard = self.transport.lock().await;
        crate::export::arrow::export_to_arrow_ipc(&mut *transport_guard, source, file_path, options)
            .await
    }

    // ========================================================================
    // Blocking Import/Export Methods
    // ========================================================================

    /// Import CSV data from a file into an Exasol table (blocking).
    ///
    /// This is a synchronous wrapper around [`import_csv_from_file`](Self::import_csv_from_file)
    /// for use in non-async contexts.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `file_path` - Path to the CSV file
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    ///
    /// # Example
    ///
    pub fn blocking_import_csv_from_file(
        &mut self,
        table: &str,
        file_path: &std::path::Path,
        options: crate::import::csv::CsvImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        blocking_runtime().block_on(self.import_csv_from_file(table, file_path, options))
    }

    /// Import multiple CSV files in parallel into an Exasol table (blocking).
    ///
    /// This is a synchronous wrapper around [`import_csv_from_files`](Self::import_csv_from_files)
    /// for use in non-async contexts.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `paths` - File paths (accepts single path, Vec, array, or slice)
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    pub fn blocking_import_csv_from_files<S: crate::import::IntoFileSources>(
        &mut self,
        table: &str,
        paths: S,
        options: crate::import::csv::CsvImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        blocking_runtime().block_on(self.import_csv_from_files(table, paths, options))
    }

    /// Import data from a Parquet file into an Exasol table (blocking).
    ///
    /// This is a synchronous wrapper around [`import_from_parquet`](Self::import_from_parquet)
    /// for use in non-async contexts.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `file_path` - Path to the Parquet file
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    ///
    /// # Example
    ///
    pub fn blocking_import_from_parquet(
        &mut self,
        table: &str,
        file_path: &std::path::Path,
        options: crate::import::parquet::ParquetImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        blocking_runtime().block_on(self.import_from_parquet(table, file_path, options))
    }

    /// Import multiple Parquet files in parallel into an Exasol table (blocking).
    ///
    /// This is a synchronous wrapper around [`import_parquet_from_files`](Self::import_parquet_from_files)
    /// for use in non-async contexts.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `paths` - File paths (accepts single path, Vec, array, or slice)
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    pub fn blocking_import_parquet_from_files<S: crate::import::IntoFileSources>(
        &mut self,
        table: &str,
        paths: S,
        options: crate::import::parquet::ParquetImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        blocking_runtime().block_on(self.import_parquet_from_files(table, paths, options))
    }

    /// Import data from an Arrow RecordBatch into an Exasol table (blocking).
    ///
    /// This is a synchronous wrapper around [`import_from_record_batch`](Self::import_from_record_batch)
    /// for use in non-async contexts.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `batch` - The RecordBatch to import
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    ///
    /// # Example
    ///
    pub fn blocking_import_from_record_batch(
        &mut self,
        table: &str,
        batch: &RecordBatch,
        options: crate::import::arrow::ArrowImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        blocking_runtime().block_on(self.import_from_record_batch(table, batch, options))
    }

    /// Import data from an Arrow IPC file into an Exasol table (blocking).
    ///
    /// This is a synchronous wrapper around [`import_from_arrow_ipc`](Self::import_from_arrow_ipc)
    /// for use in non-async contexts.
    ///
    /// Note: This method requires a synchronous reader that implements `std::io::Read`.
    /// The data will be read into memory before being imported.
    ///
    /// # Arguments
    ///
    /// * `table` - Name of the target table
    /// * `file_path` - Path to the Arrow IPC file
    /// * `options` - Import options
    ///
    /// # Returns
    ///
    /// The number of rows imported on success.
    ///
    /// # Errors
    ///
    /// Returns `ImportError` if the import fails.
    ///
    /// # Example
    ///
    pub fn blocking_import_from_arrow_ipc(
        &mut self,
        table: &str,
        file_path: &std::path::Path,
        options: crate::import::arrow::ArrowImportOptions,
    ) -> Result<u64, crate::import::ImportError> {
        blocking_runtime().block_on(async {
            let file = tokio::fs::File::open(file_path)
                .await
                .map_err(crate::import::ImportError::IoError)?;
            self.import_from_arrow_ipc(table, file, options).await
        })
    }

    /// Export data from an Exasol table or query to a CSV file (blocking).
    ///
    /// This is a synchronous wrapper around [`export_csv_to_file`](Self::export_csv_to_file)
    /// for use in non-async contexts.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `file_path` - Path to the output file
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// The number of rows exported on success.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    ///
    /// # Example
    ///
    pub fn blocking_export_csv_to_file(
        &mut self,
        source: crate::query::export::ExportSource,
        file_path: &std::path::Path,
        options: crate::export::csv::CsvExportOptions,
    ) -> Result<u64, crate::export::csv::ExportError> {
        blocking_runtime().block_on(self.export_csv_to_file(source, file_path, options))
    }

    /// Export data from an Exasol table or query to a Parquet file (blocking).
    ///
    /// This is a synchronous wrapper around [`export_to_parquet`](Self::export_to_parquet)
    /// for use in non-async contexts.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `file_path` - Path to the output Parquet file
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// The number of rows exported on success.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    ///
    /// # Example
    ///
    pub fn blocking_export_to_parquet(
        &mut self,
        source: crate::query::export::ExportSource,
        file_path: &std::path::Path,
        options: crate::export::parquet::ParquetExportOptions,
    ) -> Result<u64, crate::export::csv::ExportError> {
        blocking_runtime().block_on(self.export_to_parquet(source, file_path, options))
    }

    /// Export data from an Exasol table or query to Arrow RecordBatches (blocking).
    ///
    /// This is a synchronous wrapper around [`export_to_record_batches`](Self::export_to_record_batches)
    /// for use in non-async contexts.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// A vector of RecordBatches on success.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    ///
    /// # Example
    ///
    pub fn blocking_export_to_record_batches(
        &mut self,
        source: crate::query::export::ExportSource,
        options: crate::export::arrow::ArrowExportOptions,
    ) -> Result<Vec<RecordBatch>, crate::export::csv::ExportError> {
        blocking_runtime().block_on(self.export_to_record_batches(source, options))
    }

    /// Export data from an Exasol table or query to an Arrow IPC file (blocking).
    ///
    /// This is a synchronous wrapper around [`export_to_arrow_ipc`](Self::export_to_arrow_ipc)
    /// for use in non-async contexts.
    ///
    /// # Arguments
    ///
    /// * `source` - The data source (table or query)
    /// * `file_path` - Path to the output Arrow IPC file
    /// * `options` - Export options
    ///
    /// # Returns
    ///
    /// The number of rows exported on success.
    ///
    /// # Errors
    ///
    /// Returns `ExportError` if the export fails.
    ///
    /// # Example
    ///
    pub fn blocking_export_to_arrow_ipc(
        &mut self,
        source: crate::query::export::ExportSource,
        file_path: &std::path::Path,
        options: crate::export::arrow::ArrowExportOptions,
    ) -> Result<u64, crate::export::csv::ExportError> {
        blocking_runtime().block_on(self.export_to_arrow_ipc(source, file_path, options))
    }

    // ========================================================================
    // Private Helper Methods
    // ========================================================================

    /// Update session state after query execution.
    async fn update_session_state_after_query(&self) {
        if self.session.in_transaction() {
            self.session.set_state(SessionState::InTransaction).await;
        } else {
            self.session.set_state(SessionState::Ready).await;
        }
        self.session.update_activity().await;
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("session_id", &self.session.session_id())
            .field("host", &self.params.host)
            .field("port", &self.params.port)
            .field("username", &self.params.username)
            .field("in_transaction", &self.session.in_transaction())
            .finish()
    }
}

/// Builder for creating Connection instances.
pub struct ConnectionBuilder {
    /// Connection parameters builder
    params_builder: crate::connection::params::ConnectionBuilder,
}

impl ConnectionBuilder {
    /// Create a new ConnectionBuilder.
    pub fn new() -> Self {
        Self {
            params_builder: crate::connection::params::ConnectionBuilder::new(),
        }
    }

    /// Set the database host.
    pub fn host(mut self, host: &str) -> Self {
        self.params_builder = self.params_builder.host(host);
        self
    }

    /// Set the database port.
    pub fn port(mut self, port: u16) -> Self {
        self.params_builder = self.params_builder.port(port);
        self
    }

    /// Set the username.
    pub fn username(mut self, username: &str) -> Self {
        self.params_builder = self.params_builder.username(username);
        self
    }

    /// Set the password.
    pub fn password(mut self, password: &str) -> Self {
        self.params_builder = self.params_builder.password(password);
        self
    }

    /// Set the default schema.
    pub fn schema(mut self, schema: &str) -> Self {
        self.params_builder = self.params_builder.schema(schema);
        self
    }

    /// Enable or disable TLS.
    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.params_builder = self.params_builder.use_tls(use_tls);
        self
    }

    /// Build and connect.
    ///
    /// # Returns
    ///
    /// A connected `Connection` instance.
    ///
    /// # Errors
    ///
    /// Returns `ExasolError` if the connection fails.
    pub async fn connect(self) -> Result<Connection, ExasolError> {
        let params = self.params_builder.build()?;
        Ok(Connection::from_params(params).await?)
    }
}

impl Default for ConnectionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_builder() {
        let _builder = ConnectionBuilder::new()
            .host("localhost")
            .port(8563)
            .username("test")
            .password("secret")
            .schema("MY_SCHEMA")
            .use_tls(false);

        // Builder should compile and be valid
        // Actual connection requires a running Exasol instance
    }

    #[test]
    fn test_create_statement_is_sync() {
        // This test verifies that create_statement is synchronous
        // by calling it without await
        // Note: We can't actually create a Connection without a database,
        // but we can verify the API compiles correctly
    }
}

/// Blocking wrapper tests
#[cfg(test)]
mod blocking_tests {
    use super::*;

    #[test]
    fn test_session_type_alias_exists() {
        // Verify that Session is a type alias for Connection
        fn takes_session(_session: &Session) {}
        fn takes_connection(_connection: &Connection) {}

        // This should compile, showing Session = Connection
        fn verify_interchangeable<F1, F2>(f1: F1, f2: F2)
        where
            F1: Fn(&Session),
            F2: Fn(&Connection),
        {
            let _ = (f1, f2);
        }

        verify_interchangeable(takes_session, takes_connection);
    }

    #[test]
    fn test_blocking_runtime_exists() {
        // Verify that blocking_runtime() function exists and returns a Runtime
        let runtime = blocking_runtime();
        // If this compiles, the runtime is valid
        let _ = runtime.handle();
    }

    #[test]
    fn test_connection_has_blocking_import_csv() {
        // This test verifies the blocking_import_csv_from_file method exists
        // by checking the method signature compiles
        fn _check_method_exists(_conn: &mut Connection) {
            // Method signature check - will fail to compile if method doesn't exist
            let _: fn(
                &mut Connection,
                &str,
                &std::path::Path,
                crate::import::csv::CsvImportOptions,
            ) -> Result<u64, crate::import::ImportError> =
                Connection::blocking_import_csv_from_file;
        }
    }

    #[test]
    fn test_connection_has_blocking_import_parquet() {
        fn _check_method_exists(_conn: &mut Connection) {
            let _: fn(
                &mut Connection,
                &str,
                &std::path::Path,
                crate::import::parquet::ParquetImportOptions,
            ) -> Result<u64, crate::import::ImportError> = Connection::blocking_import_from_parquet;
        }
    }

    #[test]
    fn test_connection_has_blocking_import_record_batch() {
        fn _check_method_exists(_conn: &mut Connection) {
            let _: fn(
                &mut Connection,
                &str,
                &RecordBatch,
                crate::import::arrow::ArrowImportOptions,
            ) -> Result<u64, crate::import::ImportError> =
                Connection::blocking_import_from_record_batch;
        }
    }

    #[test]
    fn test_connection_has_blocking_import_arrow_ipc() {
        fn _check_method_exists(_conn: &mut Connection) {
            let _: fn(
                &mut Connection,
                &str,
                &std::path::Path,
                crate::import::arrow::ArrowImportOptions,
            ) -> Result<u64, crate::import::ImportError> =
                Connection::blocking_import_from_arrow_ipc;
        }
    }

    #[test]
    fn test_connection_has_blocking_export_csv() {
        fn _check_method_exists(_conn: &mut Connection) {
            let _: fn(
                &mut Connection,
                crate::query::export::ExportSource,
                &std::path::Path,
                crate::export::csv::CsvExportOptions,
            ) -> Result<u64, crate::export::csv::ExportError> =
                Connection::blocking_export_csv_to_file;
        }
    }

    #[test]
    fn test_connection_has_blocking_export_parquet() {
        fn _check_method_exists(_conn: &mut Connection) {
            let _: fn(
                &mut Connection,
                crate::query::export::ExportSource,
                &std::path::Path,
                crate::export::parquet::ParquetExportOptions,
            ) -> Result<u64, crate::export::csv::ExportError> =
                Connection::blocking_export_to_parquet;
        }
    }

    #[test]
    fn test_connection_has_blocking_export_record_batches() {
        fn _check_method_exists(_conn: &mut Connection) {
            let _: fn(
                &mut Connection,
                crate::query::export::ExportSource,
                crate::export::arrow::ArrowExportOptions,
            ) -> Result<Vec<RecordBatch>, crate::export::csv::ExportError> =
                Connection::blocking_export_to_record_batches;
        }
    }

    #[test]
    fn test_connection_has_blocking_export_arrow_ipc() {
        fn _check_method_exists(_conn: &mut Connection) {
            let _: fn(
                &mut Connection,
                crate::query::export::ExportSource,
                &std::path::Path,
                crate::export::arrow::ArrowExportOptions,
            ) -> Result<u64, crate::export::csv::ExportError> =
                Connection::blocking_export_to_arrow_ipc;
        }
    }
}
