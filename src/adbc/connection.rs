//! ADBC Connection implementation.
//!
//! This module provides the `Connection` type which represents an active
//! database connection and provides methods for executing queries.

use crate::adbc::Statement;
use crate::connection::auth::AuthResponseData;
use crate::connection::params::ConnectionParams;
use crate::connection::session::{Session, SessionConfig};
use crate::error::{ConnectionError, ExasolError, QueryError};
use crate::query::results::ResultSet;
use crate::transport::protocol::{
    ConnectionParams as TransportConnectionParams, Credentials as TransportCredentials,
    TransportProtocol,
};
use crate::transport::WebSocketTransport;
use arrow::array::RecordBatch;
use std::sync::Arc;
use tokio::sync::Mutex;

/// ADBC Connection to an Exasol database.
///
/// The `Connection` type represents an active database connection and provides
/// methods for executing queries, managing transactions, and retrieving metadata.
///
/// # Example
///
/// ```no_run
/// use exarrow_rs::adbc::{Driver, Connection};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let driver = Driver::new();
/// let database = driver.open("exasol://user:pass@localhost:8563")?;
/// let connection = database.connect().await?;
///
/// // Execute a query
/// let mut stmt = connection.create_statement("SELECT * FROM my_table").await?;
/// let results = stmt.execute().await?;
///
/// // Close the connection
/// connection.close().await?;
/// # Ok(())
/// # }
/// ```
pub struct Connection {
    /// Transport layer for communication
    transport: Arc<Mutex<dyn TransportProtocol>>,
    /// Session information
    session: Arc<Session>,
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

        // Create session
        let session = Arc::new(Session::new(
            session_info.session_id.clone(),
            auth_response,
            session_config,
        ));

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
    /// let connection = Connection::builder()
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

    /// Create a new statement for executing SQL.
    ///
    /// # Arguments
    ///
    /// * `sql` - SQL query text
    ///
    /// # Returns
    ///
    /// A `Statement` instance ready for execution.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if statement creation fails.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use exarrow_rs::adbc::Connection;
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let mut stmt = connection.create_statement("SELECT * FROM users").await?;
    /// let results = stmt.execute().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_statement(&self, sql: impl Into<String>) -> Result<Statement, QueryError> {
        self.session
            .validate_ready()
            .await
            .map_err(|e| QueryError::InvalidState(e.to_string()))?;

        Ok(Statement::new(
            Arc::clone(&self.transport),
            sql.into(),
            Arc::clone(&self.session),
        ))
    }

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
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let results = connection.execute("SELECT COUNT(*) FROM users").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(&self, sql: impl Into<String>) -> Result<ResultSet, QueryError> {
        let mut stmt = self.create_statement(sql).await?;
        stmt.execute().await
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
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let batches = connection.query("SELECT * FROM users").await?;
    /// for batch in batches {
    ///     println!("Rows: {}", batch.num_rows());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query(&self, sql: impl Into<String>) -> Result<Vec<RecordBatch>, QueryError> {
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
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// let count = connection.execute_update("DELETE FROM users WHERE id = 1").await?;
    /// println!("Deleted {} rows", count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute_update(&self, sql: impl Into<String>) -> Result<i64, QueryError> {
        let mut stmt = self.create_statement(sql).await?;
        stmt.execute_update().await
    }

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
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// connection.begin_transaction().await?;
    /// // Execute queries...
    /// connection.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn begin_transaction(&self) -> Result<(), QueryError> {
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
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// connection.begin_transaction().await?;
    /// connection.execute_update("INSERT INTO users VALUES (1, 'Alice')").await?;
    /// connection.commit().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit(&self) -> Result<(), QueryError> {
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
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// connection.begin_transaction().await?;
    /// connection.execute_update("DELETE FROM users").await?;
    /// connection.rollback().await?; // Undo the delete
    /// # Ok(())
    /// # }
    /// ```
    pub async fn rollback(&self) -> Result<(), QueryError> {
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
    /// # async fn example(connection: &Connection) -> Result<(), Box<dyn std::error::Error>> {
    /// connection.set_schema("MY_SCHEMA").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_schema(&self, schema: impl Into<String>) -> Result<(), QueryError> {
        let schema_name = schema.into();
        self.execute_update(format!("OPEN SCHEMA {}", schema_name))
            .await?;
        self.session.set_current_schema(Some(schema_name)).await;
        Ok(())
    }

    /// Get metadata about catalogs.
    ///
    /// # Returns
    ///
    /// A `ResultSet` containing catalog metadata.
    ///
    /// # Errors
    ///
    /// Returns `QueryError` if the operation fails.
    pub async fn get_catalogs(&self) -> Result<ResultSet, QueryError> {
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
    pub async fn get_schemas(&self, catalog: Option<&str>) -> Result<ResultSet, QueryError> {
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
        &self,
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
        &self,
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
        let builder = ConnectionBuilder::new()
            .host("localhost")
            .port(8563)
            .username("test")
            .password("secret")
            .schema("MY_SCHEMA")
            .use_tls(false);

        // Builder should compile and be valid
        // Actual connection requires a running Exasol instance
    }
}
