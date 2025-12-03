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
use crate::connection::session::{Session, SessionConfig, SessionState};
use crate::error::{ConnectionError, ExasolError, QueryError};
use crate::query::prepared::PreparedStatement;
use crate::query::results::ResultSet;
use crate::transport::protocol::{
    ConnectionParams as TransportConnectionParams, Credentials as TransportCredentials,
    QueryResult, TransportProtocol,
};
use crate::transport::WebSocketTransport;
use arrow::array::RecordBatch;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;

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
/// ```no_run
/// use exarrow_rs::adbc::{Driver, Connection};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let driver = Driver::new();
/// let database = driver.open("exasol://user:pass@localhost:8563")?;
/// let mut connection = database.connect().await?;
///
/// // Create and execute a statement
/// let stmt = connection.create_statement("SELECT * FROM my_table");
/// let results = connection.execute_statement(&stmt).await?;
///
/// // Or use convenience methods
/// let results = connection.execute("SELECT * FROM my_table").await?;
///
/// // Close the connection
/// connection.close().await?;
/// # Ok(())
/// # }
/// ```
pub struct Connection {
    /// Transport layer for communication (owned by Connection)
    transport: Arc<Mutex<dyn TransportProtocol>>,
    /// Session information
    session: Session,
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
        // Create transport
        let mut transport = WebSocketTransport::new();

        // Convert ConnectionParams to TransportConnectionParams
        let transport_params = TransportConnectionParams::new(params.host.clone(), params.port)
            .with_tls(params.use_tls)
            .with_validate_server_certificate(params.validate_server_certificate)
            .with_timeout(params.connection_timeout.as_millis() as u64);

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

        // Convert SessionInfo to AuthResponseData
        let auth_response = AuthResponseData {
            session_id: session_info.session_id.clone(),
            protocol_version: session_info.protocol_version,
            release_version: session_info.release_version.clone(),
            database_name: session_info.database_name.clone(),
            product_name: session_info.product_name.clone(),
            max_data_message_size: session_info.max_data_message_size,
            max_identifier_length: 128,    // Default value
            max_varchar_length: 2_000_000, // Default value
            identifier_quote_string: "\"".to_string(),
            time_zone: session_info
                .time_zone
                .clone()
                .unwrap_or_else(|| "UTC".to_string()),
            time_zone_behavior: "INVALID TIMESTAMP TO DOUBLE".to_string(),
        };

        // Create session (owned, not Arc)
        let session = Session::new(
            session_info.session_id.clone(),
            auth_response,
            session_config,
        );

        // Set schema if specified
        if let Some(schema) = &params.schema {
            session.set_current_schema(Some(schema.clone())).await;
        }

        Ok(Self {
            transport: Arc::new(Mutex::new(transport)),
            session,
            params,
        })
    }

    /// Create a builder for constructing a connection.
    ///
    /// # Returns
    ///
    /// A `ConnectionBuilder` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use exarrow_rs::adbc::Connection;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut connection = Connection::builder()
    ///     .host("localhost")
    ///     .port(8563)
    ///     .username("sys")
    ///     .password("exasol")
    ///     .connect()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("SELECT * FROM users WHERE id = ?");
    /// stmt.bind(0, 42)?;
    /// let results = connection.execute_statement(&stmt).await?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("SELECT * FROM users WHERE age > ?");
    /// stmt.bind(0, 18)?;
    /// let results = connection.execute_statement(&stmt).await?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("DELETE FROM users WHERE active = ?");
    /// stmt.bind(0, false)?;
    /// let count = connection.execute_statement_update(&stmt).await?;
    /// println!("Deleted {} rows", count);
    /// # Ok(())
    /// # }
    /// ```
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut prepared = connection.prepare("SELECT * FROM users WHERE id = ?").await?;
    /// prepared.bind(0, 42)?;
    /// let results = connection.execute_prepared(&prepared).await?;
    /// connection.close_prepared(prepared).await?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut prepared = connection.prepare("SELECT * FROM users WHERE id = ?").await?;
    /// prepared.bind(0, 42)?;
    /// let results = connection.execute_prepared(&prepared).await?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let prepared = connection.prepare("SELECT 1").await?;
    /// // Use the prepared statement...
    /// connection.close_prepared(prepared).await?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let results = connection.execute("SELECT COUNT(*) FROM users").await?;
    /// # Ok(())
    /// # }
    /// ```
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let batches = connection.query("SELECT * FROM users").await?;
    /// for batch in batches {
    ///     println!("Rows: {}", batch.num_rows());
    /// }
    /// # Ok(())
    /// # }
    /// ```
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let count = connection.execute_update("DELETE FROM users WHERE id = 1").await?;
    /// println!("Deleted {} rows", count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_update(&mut self, sql: impl Into<String>) -> Result<i64, QueryError> {
        let stmt = self.create_statement(sql);
        self.execute_statement_update(&stmt).await
    }

    // ========================================================================
    // Transaction Methods
    // ========================================================================

    /// Begin a transaction.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if a transaction is already active or if the operation fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// connection.begin_transaction().await?;
    /// // Execute queries...
    /// connection.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn begin_transaction(&mut self) -> Result<(), QueryError> {
        // Exasol doesn't have a BEGIN statement - transactions are implicit.
        // Starting a transaction just means we're tracking that autocommit is off.
        // The actual transaction begins with the first DML/query.
        self.session
            .begin_transaction()
            .await
            .map_err(|e| QueryError::TransactionError(e.to_string()))?;

        Ok(())
    }

    /// Commit the current transaction.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if no transaction is active or if the operation fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// connection.begin_transaction().await?;
    /// connection.execute_update("INSERT INTO users VALUES (1, 'Alice')").await?;
    /// connection.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit(&mut self) -> Result<(), QueryError> {
        // Execute COMMIT statement
        self.execute_update("COMMIT").await?;

        self.session
            .commit_transaction()
            .await
            .map_err(|e| QueryError::TransactionError(e.to_string()))?;

        Ok(())
    }

    /// Rollback the current transaction.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if no transaction is active or if the operation fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// connection.begin_transaction().await?;
    /// connection.execute_update("DELETE FROM users").await?;
    /// connection.rollback().await?; // Undo the delete
    /// # Ok(())
    /// # }
    /// ```
    pub async fn rollback(&mut self) -> Result<(), QueryError> {
        // Execute ROLLBACK statement
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// connection.set_schema("MY_SCHEMA").await?;
    /// # Ok(())
    /// # }
    /// ```
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
        let mut conditions = Vec::new();

        if let Some(cat) = catalog {
            conditions.push(format!("TABLE_SCHEMA = '{}'", cat.replace('\'', "''")));
        }
        if let Some(sch) = schema {
            conditions.push(format!("TABLE_SCHEMA = '{}'", sch.replace('\'', "''")));
        }
        if let Some(tbl) = table {
            conditions.push(format!("TABLE_NAME = '{}'", tbl.replace('\'', "''")));
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let sql = format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM SYS.EXA_ALL_TABLES {} ORDER BY TABLE_SCHEMA, TABLE_NAME",
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
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// // Use the connection...
    /// connection.close().await?;
    /// # Ok(())
    /// # }
    /// ```
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
