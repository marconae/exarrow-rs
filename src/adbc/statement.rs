//! ADBC Statement implementation.
//!
//! This module provides the `Statement` type which wraps the query execution
//! module and provides an ADBC-compatible API.

use crate::connection::session::Session;
use crate::error::QueryError;
use crate::query::results::ResultSet;
use crate::query::statement::{Parameter, Statement as QueryStatement};
use crate::transport::TransportProtocol;
use std::sync::Arc;
use tokio::sync::Mutex;

/// ADBC Statement for executing SQL queries.
///
/// The `Statement` type represents a SQL statement that can be executed
/// against the database. It supports parameter binding, timeout configuration,
/// and returns results as Arrow RecordBatches.
///
/// # Example
///
/// ```no_run
/// use exarrow_rs::adbc::Connection;
///
/// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
/// // Create a statement
/// let mut stmt = connection.create_statement("SELECT * FROM users WHERE age > ?").await?;
///
/// // Bind parameters
/// stmt.bind(0, 18)?;
///
/// // Execute
/// let results = stmt.execute().await?;
/// # Ok(())
/// # }
/// ```
pub struct Statement {
    /// Underlying query statement
    inner: QueryStatement,
    /// Session reference for state tracking
    session: Arc<Session>,
}

impl Statement {
    /// Create a new statement.
    ///
    /// # Arguments
    ///
    /// * `transport` - Transport layer reference
    /// * `sql` - SQL query text
    /// * `session` - Session reference
    ///
    /// # Returns
    ///
    /// A new `Statement` instance.
    pub(crate) fn new(
        transport: Arc<Mutex<dyn TransportProtocol>>,
        sql: String,
        session: Arc<Session>,
    ) -> Self {
        Self {
            inner: QueryStatement::new(transport, sql),
            session,
        }
    }

    /// Get the SQL text.
    ///
    /// # Returns
    ///
    /// The SQL query string.
    pub fn sql(&self) -> &str {
        self.inner.sql()
    }

    /// Get the statement type.
    ///
    /// # Returns
    ///
    /// The type of SQL statement (SELECT, INSERT, etc.).
    pub fn statement_type(&self) -> crate::query::statement::StatementType {
        self.inner.statement_type()
    }

    /// Set the query timeout in milliseconds.
    ///
    /// # Arguments
    ///
    /// * `timeout_ms` - Timeout in milliseconds
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("SELECT * FROM large_table").await?;
    /// stmt.set_timeout(60_000); // 60 seconds
    /// let results = stmt.execute().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn set_timeout(&mut self, timeout_ms: u64) {
        self.inner.set_timeout(timeout_ms);
    }

    /// Bind a parameter at the given index.
    ///
    /// Parameters are 0-indexed and correspond to `?` placeholders in the SQL.
    ///
    /// # Arguments
    ///
    /// * `index` - Parameter index (0-based)
    /// * `value` - Parameter value
    ///
    /// # Errors
    ///
    /// Returns `QueryError::ParameterBindingError` if binding fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("SELECT * FROM users WHERE id = ?").await?;
    /// stmt.bind(0, 42)?;
    /// let results = stmt.execute().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind<T: Into<Parameter>>(&mut self, index: usize, value: T) -> Result<(), QueryError> {
        self.inner.bind(index, value)
    }

    /// Bind multiple parameters.
    ///
    /// # Arguments
    ///
    /// * `params` - Slice of parameter values
    ///
    /// # Errors
    ///
    /// Returns `QueryError::ParameterBindingError` if binding fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("SELECT * FROM users WHERE age > ? AND active = ?").await?;
    /// stmt.bind_all(&[18, 1])?;
    /// let results = stmt.execute().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn bind_all<T: Into<Parameter> + Clone>(&mut self, params: &[T]) -> Result<(), QueryError> {
        self.inner.bind_all(params)
    }

    /// Clear all bound parameters.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("SELECT * FROM users WHERE id = ?").await?;
    /// stmt.bind(0, 42)?;
    /// stmt.clear_parameters();
    /// stmt.bind(0, 99)?;
    /// let results = stmt.execute().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn clear_parameters(&mut self) {
        self.inner.clear_parameters();
    }

    /// Execute the statement.
    ///
    /// Returns a `ResultSet` for SELECT queries or row count for DML statements.
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
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("SELECT * FROM users").await?;
    /// let result_set = stmt.execute().await?;
    ///
    /// // For SELECT queries, iterate over results
    /// if result_set.is_stream() {
    ///     for batch in result_set.into_iterator()? {
    ///         let batch = batch?;
    ///         println!("Rows: {}", batch.num_rows());
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(&mut self) -> Result<ResultSet, QueryError> {
        // Validate session state
        self.session
            .validate_ready()
            .await
            .map_err(|e| QueryError::InvalidState(e.to_string()))?;

        // Update session state
        self.session
            .set_state(crate::connection::session::SessionState::Executing)
            .await;

        // Increment query counter
        self.session.increment_query_count();

        // Execute the query
        let result = self.inner.execute().await;

        // Update session state back to ready/in_transaction
        if self.session.in_transaction() {
            self.session
                .set_state(crate::connection::session::SessionState::InTransaction)
                .await;
        } else {
            self.session
                .set_state(crate::connection::session::SessionState::Ready)
                .await;
        }

        // Update activity timestamp
        self.session.update_activity().await;

        result
    }

    /// Execute and return the row count (for non-SELECT statements).
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
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("DELETE FROM users WHERE active = ?").await?;
    /// stmt.bind(0, false)?;
    /// let count = stmt.execute_update().await?;
    /// println!("Deleted {} rows", count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_update(&mut self) -> Result<i64, QueryError> {
        let result_set = self.execute().await?;

        result_set.row_count().ok_or_else(|| {
            QueryError::NoResultSet("Expected row count, got result set".to_string())
        })
    }

    /// Reset the statement for re-execution.
    ///
    /// This clears the executed flag but preserves bound parameters.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("INSERT INTO logs VALUES (?)").await?;
    ///
    /// // Execute multiple times with different parameters
    /// for i in 0..10 {
    ///     stmt.bind(0, i)?;
    ///     stmt.execute_update().await?;
    ///     stmt.reset();
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn reset(&mut self) {
        self.inner.reset();
    }

    /// Check if the statement returns a result set.
    ///
    /// # Returns
    ///
    /// `true` if this is a SELECT statement, `false` otherwise.
    pub fn returns_result_set(&self) -> bool {
        self.inner.statement_type().returns_result_set()
    }

    /// Check if the statement returns a row count.
    ///
    /// # Returns
    ///
    /// `true` if this is an INSERT/UPDATE/DELETE statement, `false` otherwise.
    pub fn returns_row_count(&self) -> bool {
        self.inner.statement_type().returns_row_count()
    }
}

impl std::fmt::Debug for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statement")
            .field("sql", &self.sql())
            .field("statement_type", &self.statement_type())
            .finish()
    }
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Statement({})", self.sql())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::auth::{AuthResponseData, Credentials};
    use crate::connection::session::SessionConfig;
    use crate::transport::messages::{ColumnInfo, DataType, ResultData, ResultSetHandle};
    use crate::transport::protocol::QueryResult as TransportQueryResult;
    use async_trait::async_trait;
    use mockall::mock;

    // Mock transport for testing
    mock! {
        pub Transport {}

        #[async_trait]
        impl TransportProtocol for Transport {
            async fn connect(&mut self, params: &crate::transport::protocol::ConnectionParams) -> Result<(), crate::error::TransportError>;
            async fn authenticate(&mut self, credentials: &crate::transport::protocol::Credentials) -> Result<crate::transport::messages::SessionInfo, crate::error::TransportError>;
            async fn execute_query(&mut self, sql: &str) -> Result<TransportQueryResult, crate::error::TransportError>;
            async fn fetch_results(&mut self, handle: ResultSetHandle) -> Result<ResultData, crate::error::TransportError>;
            async fn close_result_set(&mut self, handle: ResultSetHandle) -> Result<(), crate::error::TransportError>;
            async fn close(&mut self) -> Result<(), crate::error::TransportError>;
            fn is_connected(&self) -> bool;
        }
    }

    fn mock_session() -> Arc<Session> {
        let server_info = AuthResponseData {
            session_id: "test_session".to_string(),
            protocol_version: 3,
            release_version: "7.1.0".to_string(),
            database_name: "EXA".to_string(),
            product_name: "Exasol".to_string(),
            max_data_message_size: 4_194_304,
            max_identifier_length: 128,
            max_varchar_length: 2_000_000,
            identifier_quote_string: "\"".to_string(),
            time_zone: "UTC".to_string(),
            time_zone_behavior: "INVALID TIMESTAMP TO DOUBLE".to_string(),
        };

        Arc::new(Session::new(
            "test_session".to_string(),
            server_info,
            SessionConfig::default(),
        ))
    }

    #[tokio::test]
    async fn test_statement_creation() {
        let mut mock_transport = MockTransport::new();
        mock_transport.expect_is_connected().returning(|| true);

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let session = mock_session();

        let stmt = Statement::new(transport, "SELECT * FROM users".to_string(), session);

        assert_eq!(stmt.sql(), "SELECT * FROM users");
        assert!(stmt.returns_result_set());
        assert!(!stmt.returns_row_count());
    }

    #[tokio::test]
    async fn test_statement_parameter_binding() {
        let mut mock_transport = MockTransport::new();
        mock_transport.expect_is_connected().returning(|| true);

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let session = mock_session();

        let mut stmt = Statement::new(
            transport,
            "SELECT * FROM users WHERE id = ?".to_string(),
            session,
        );

        stmt.bind(0, 42).unwrap();
    }

    #[tokio::test]
    async fn test_statement_execute_update() {
        let mut mock_transport = MockTransport::new();

        mock_transport
            .expect_execute_query()
            .times(1)
            .returning(|_sql| Ok(TransportQueryResult::RowCount { count: 5 }));

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let session = mock_session();

        let mut stmt = Statement::new(
            transport,
            "DELETE FROM users WHERE active = false".to_string(),
            session,
        );

        let count = stmt.execute_update().await.unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_statement_type_detection() {
        let mut mock_transport = MockTransport::new();
        mock_transport.expect_is_connected().returning(|| true);

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let session = mock_session();

        let select_stmt = Statement::new(
            Arc::clone(&transport),
            "SELECT * FROM users".to_string(),
            Arc::clone(&session),
        );
        assert!(select_stmt.returns_result_set());

        let insert_stmt = Statement::new(
            Arc::clone(&transport),
            "INSERT INTO users VALUES (1)".to_string(),
            Arc::clone(&session),
        );
        assert!(insert_stmt.returns_row_count());

        let update_stmt = Statement::new(
            Arc::clone(&transport),
            "UPDATE users SET active = true".to_string(),
            Arc::clone(&session),
        );
        assert!(update_stmt.returns_row_count());
    }

    #[tokio::test]
    async fn test_statement_clear_parameters() {
        let mut mock_transport = MockTransport::new();
        mock_transport.expect_is_connected().returning(|| true);

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let session = mock_session();

        let mut stmt = Statement::new(
            transport,
            "SELECT * FROM users WHERE id = ?".to_string(),
            session,
        );

        stmt.bind(0, 42).unwrap();
        stmt.clear_parameters();
        stmt.bind(0, 99).unwrap();
    }

    #[test]
    fn test_statement_display() {
        let mut mock_transport = MockTransport::new();
        mock_transport.expect_is_connected().returning(|| true);

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let session = mock_session();

        let stmt = Statement::new(transport, "SELECT 1".to_string(), session);

        let display = format!("{}", stmt);
        assert!(display.contains("SELECT 1"));
    }
}
