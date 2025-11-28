//! Prepared statement handling for parameterized queries.
//!
//! This module provides the `PreparedStatement` type for executing
//! parameterized queries using Exasol's native prepared statement protocol.

use crate::error::QueryError;
use crate::query::results::ResultSet;
use crate::query::statement::Parameter;
use crate::transport::messages::DataType;
use crate::transport::protocol::{PreparedStatementHandle, QueryResult};
use crate::transport::TransportProtocol;
use std::sync::Arc;
use tokio::sync::Mutex;

/// A prepared statement for parameterized query execution.
///
/// PreparedStatement uses Exasol's native prepared statement protocol
/// for secure, efficient parameterized queries. Parameters are sent
/// separately from SQL text, eliminating SQL injection risks.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // PreparedStatement is typically created via Statement::prepare()
/// // let prepared = statement.prepare().await?;
/// // prepared.bind(0, 42)?;
/// // let results = prepared.execute().await?;
/// # Ok(())
/// # }
/// ```
pub struct PreparedStatement {
    /// Transport layer for communication
    transport: Arc<Mutex<dyn TransportProtocol>>,
    /// Server-side prepared statement handle
    handle: PreparedStatementHandle,
    /// Bound parameter values (column-major format for batch execution)
    parameters: Vec<Option<Parameter>>,
    /// Whether this prepared statement has been closed
    closed: bool,
}

impl PreparedStatement {
    /// Create a new PreparedStatement from a handle.
    pub(crate) fn new(
        transport: Arc<Mutex<dyn TransportProtocol>>,
        handle: PreparedStatementHandle,
    ) -> Self {
        let num_params = handle.num_params as usize;
        Self {
            transport,
            handle,
            parameters: vec![None; num_params],
            closed: false,
        }
    }

    /// Get the number of parameters in this prepared statement.
    pub fn parameter_count(&self) -> usize {
        self.handle.num_params as usize
    }

    /// Get the parameter types (if available from server).
    pub fn parameter_types(&self) -> &[DataType] {
        &self.handle.parameter_types
    }

    /// Get the statement handle.
    pub fn handle(&self) -> i32 {
        self.handle.handle
    }

    /// Check if the prepared statement has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Bind a parameter value at the given index.
    ///
    /// # Arguments
    /// * `index` - Zero-based parameter index
    /// * `value` - Value to bind (must implement Into<Parameter>)
    ///
    /// # Errors
    /// Returns `QueryError::ParameterBindingError` if index is out of bounds.
    pub fn bind(&mut self, index: usize, value: impl Into<Parameter>) -> Result<(), QueryError> {
        if index >= self.parameters.len() {
            return Err(QueryError::ParameterBindingError {
                index,
                message: format!(
                    "Parameter index {} out of bounds (statement has {} parameters)",
                    index,
                    self.parameters.len()
                ),
            });
        }
        self.parameters[index] = Some(value.into());
        Ok(())
    }

    /// Clear all bound parameters.
    pub fn clear_parameters(&mut self) {
        for param in &mut self.parameters {
            *param = None;
        }
    }

    /// Execute the prepared statement and return results.
    ///
    /// # Errors
    /// Returns an error if:
    /// - Not all parameters are bound
    /// - The statement has been closed
    /// - Execution fails on the server
    pub async fn execute(&mut self) -> Result<ResultSet, QueryError> {
        if self.closed {
            return Err(QueryError::StatementClosed);
        }

        // Convert parameters to column-major JSON format
        let params_data = self.build_parameters_data()?;

        let mut transport = self.transport.lock().await;
        let result = transport
            .execute_prepared_statement(&self.handle, params_data)
            .await
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        drop(transport);

        ResultSet::from_transport_result(result, Arc::clone(&self.transport))
    }

    /// Execute the prepared statement and return the number of affected rows.
    ///
    /// Use this for INSERT, UPDATE, DELETE statements.
    pub async fn execute_update(&mut self) -> Result<i64, QueryError> {
        if self.closed {
            return Err(QueryError::StatementClosed);
        }

        let params_data = self.build_parameters_data()?;

        let mut transport = self.transport.lock().await;
        let result = transport
            .execute_prepared_statement(&self.handle, params_data)
            .await
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        match result {
            QueryResult::RowCount { count } => Ok(count),
            QueryResult::ResultSet { .. } => Err(QueryError::UnexpectedResultSet),
        }
    }

    /// Close the prepared statement and release server-side resources.
    pub async fn close(&mut self) -> Result<(), QueryError> {
        if self.closed {
            return Ok(());
        }

        let mut transport = self.transport.lock().await;
        transport
            .close_prepared_statement(&self.handle)
            .await
            .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        self.closed = true;
        Ok(())
    }

    /// Build parameters data in column-major format for the protocol.
    fn build_parameters_data(&self) -> Result<Option<Vec<Vec<serde_json::Value>>>, QueryError> {
        if self.parameters.is_empty() {
            return Ok(None);
        }

        // Check all parameters are bound
        for (i, param) in self.parameters.iter().enumerate() {
            if param.is_none() {
                return Err(QueryError::ParameterBindingError {
                    index: i,
                    message: format!("Parameter {} is not bound", i),
                });
            }
        }

        // Convert to column-major format (each column has one value for single-row execution)
        let columns: Vec<Vec<serde_json::Value>> = self
            .parameters
            .iter()
            .map(|p| vec![parameter_to_json(p.as_ref().unwrap())])
            .collect();

        Ok(Some(columns))
    }
}

/// Convert a Parameter to JSON value for the wire protocol.
fn parameter_to_json(param: &Parameter) -> serde_json::Value {
    match param {
        Parameter::Null => serde_json::Value::Null,
        Parameter::Boolean(b) => serde_json::Value::Bool(*b),
        Parameter::Integer(i) => serde_json::json!(*i),
        Parameter::Float(f) => serde_json::json!(*f),
        Parameter::String(s) => serde_json::Value::String(s.clone()),
        Parameter::Binary(b) => serde_json::Value::String(hex::encode(b)),
    }
}

impl Drop for PreparedStatement {
    fn drop(&mut self) {
        // Note: We can't do async cleanup in Drop.
        // Users should call close() explicitly for proper cleanup.
        // The server will eventually clean up orphaned statements.
        if !self.closed {
            // Log a warning in debug builds
            #[cfg(debug_assertions)]
            eprintln!(
                "Warning: PreparedStatement {} dropped without calling close()",
                self.handle.handle
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;
    use crate::transport::messages::{ColumnInfo, ResultData, ResultSetHandle};
    use crate::transport::protocol::{ConnectionParams, Credentials, QueryResult};
    use async_trait::async_trait;
    use mockall::mock;

    // Mock transport for testing
    mock! {
        pub Transport {}

        #[async_trait]
        impl TransportProtocol for Transport {
            async fn connect(&mut self, params: &ConnectionParams) -> Result<(), crate::error::TransportError>;
            async fn authenticate(&mut self, credentials: &Credentials) -> Result<crate::transport::messages::SessionInfo, crate::error::TransportError>;
            async fn execute_query(&mut self, sql: &str) -> Result<QueryResult, crate::error::TransportError>;
            async fn fetch_results(&mut self, handle: ResultSetHandle) -> Result<ResultData, crate::error::TransportError>;
            async fn close_result_set(&mut self, handle: ResultSetHandle) -> Result<(), crate::error::TransportError>;
            async fn create_prepared_statement(&mut self, sql: &str) -> Result<PreparedStatementHandle, crate::error::TransportError>;
            async fn execute_prepared_statement(&mut self, handle: &PreparedStatementHandle, parameters: Option<Vec<Vec<serde_json::Value>>>) -> Result<QueryResult, crate::error::TransportError>;
            async fn close_prepared_statement(&mut self, handle: &PreparedStatementHandle) -> Result<(), crate::error::TransportError>;
            async fn close(&mut self) -> Result<(), crate::error::TransportError>;
            fn is_connected(&self) -> bool;
        }
    }

    // Note: Full tests require mock transport which is tested in integration tests

    #[test]
    fn test_parameter_to_json() {
        assert_eq!(parameter_to_json(&Parameter::Null), serde_json::Value::Null);
        assert_eq!(
            parameter_to_json(&Parameter::Boolean(true)),
            serde_json::json!(true)
        );
        assert_eq!(
            parameter_to_json(&Parameter::Integer(42)),
            serde_json::json!(42)
        );
        assert_eq!(
            parameter_to_json(&Parameter::Float(3.14)),
            serde_json::json!(3.14)
        );
        assert_eq!(
            parameter_to_json(&Parameter::String("hello".to_string())),
            serde_json::json!("hello")
        );
        assert_eq!(
            parameter_to_json(&Parameter::Binary(vec![0xDE, 0xAD])),
            serde_json::json!("dead")
        );
    }

    #[tokio::test]
    async fn test_prepared_statement_creation() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(
            42,
            2,
            vec![
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
            ],
        );

        let stmt = PreparedStatement::new(transport, handle);

        assert_eq!(stmt.handle(), 42);
        assert_eq!(stmt.parameter_count(), 2);
        assert_eq!(stmt.parameter_types().len(), 2);
        assert!(!stmt.is_closed());
    }

    #[tokio::test]
    async fn test_prepared_statement_bind_valid() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);

        assert!(stmt.bind(0, 42).is_ok());
        assert!(stmt.bind(1, "test").is_ok());
    }

    #[tokio::test]
    async fn test_prepared_statement_bind_out_of_bounds() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);

        let result = stmt.bind(5, 42);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            QueryError::ParameterBindingError { index: 5, .. }
        ));
    }

    #[tokio::test]
    async fn test_prepared_statement_clear_parameters() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);

        stmt.bind(0, 42).unwrap();
        stmt.bind(1, "test").unwrap();

        stmt.clear_parameters();

        // Build should fail because parameters are cleared
        let result = stmt.build_parameters_data();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_prepared_statement_build_params_unbound() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let stmt = PreparedStatement::new(transport, handle);

        let result = stmt.build_parameters_data();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            QueryError::ParameterBindingError { index: 0, .. }
        ));
    }

    #[tokio::test]
    async fn test_prepared_statement_build_params_success() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);

        stmt.bind(0, 42).unwrap();
        stmt.bind(1, "test").unwrap();

        let result = stmt.build_parameters_data().unwrap();
        assert!(result.is_some());

        let columns = result.unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0], vec![serde_json::json!(42)]);
        assert_eq!(columns[1], vec![serde_json::json!("test")]);
    }

    #[tokio::test]
    async fn test_prepared_statement_execute_closed() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 0, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);
        stmt.closed = true;

        let result = stmt.execute().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::StatementClosed));
    }

    #[tokio::test]
    async fn test_prepared_statement_execute_update_closed() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 0, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);
        stmt.closed = true;

        let result = stmt.execute_update().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::StatementClosed));
    }

    #[tokio::test]
    async fn test_prepared_statement_close_already_closed() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 0, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);
        stmt.closed = true;

        // Should succeed without calling transport
        let result = stmt.close().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_prepared_statement_close_success() {
        let mut mock_transport = MockTransport::new();

        mock_transport
            .expect_close_prepared_statement()
            .times(1)
            .returning(|_| Ok(()));

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 0, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);

        let result = stmt.close().await;
        assert!(result.is_ok());
        assert!(stmt.is_closed());
    }

    #[tokio::test]
    async fn test_prepared_statement_execute_update_success() {
        let mut mock_transport = MockTransport::new();

        mock_transport
            .expect_execute_prepared_statement()
            .times(1)
            .returning(|_, _| Ok(QueryResult::RowCount { count: 5 }));

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 0, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);

        let result = stmt.execute_update().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 5);
    }

    #[tokio::test]
    async fn test_prepared_statement_execute_update_unexpected_result_set() {
        let mut mock_transport = MockTransport::new();

        mock_transport
            .expect_execute_prepared_statement()
            .times(1)
            .returning(|_, _| {
                Ok(QueryResult::ResultSet {
                    handle: None,
                    data: ResultData {
                        columns: vec![],
                        rows: vec![],
                        total_rows: 0,
                    },
                })
            });

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 0, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);

        let result = stmt.execute_update().await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            QueryError::UnexpectedResultSet
        ));
    }

    #[tokio::test]
    async fn test_prepared_statement_execute_with_params() {
        let mut mock_transport = MockTransport::new();

        mock_transport
            .expect_execute_prepared_statement()
            .times(1)
            .returning(|_, _| {
                Ok(QueryResult::ResultSet {
                    handle: None,
                    data: ResultData {
                        columns: vec![ColumnInfo {
                            name: "result".to_string(),
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
                        rows: vec![vec![serde_json::json!(1)]],
                        total_rows: 1,
                    },
                })
            });

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 1, vec![]);
        let mut stmt = PreparedStatement::new(transport, handle);

        stmt.bind(0, 42).unwrap();

        let result = stmt.execute().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_parameter_to_json_all_types() {
        // Test null
        assert_eq!(parameter_to_json(&Parameter::Null), serde_json::Value::Null);

        // Test boolean
        assert_eq!(
            parameter_to_json(&Parameter::Boolean(true)),
            serde_json::Value::Bool(true)
        );
        assert_eq!(
            parameter_to_json(&Parameter::Boolean(false)),
            serde_json::Value::Bool(false)
        );

        // Test integer
        assert_eq!(
            parameter_to_json(&Parameter::Integer(0)),
            serde_json::json!(0)
        );
        assert_eq!(
            parameter_to_json(&Parameter::Integer(-1)),
            serde_json::json!(-1)
        );
        assert_eq!(
            parameter_to_json(&Parameter::Integer(i64::MAX)),
            serde_json::json!(i64::MAX)
        );

        // Test float
        assert_eq!(
            parameter_to_json(&Parameter::Float(0.0)),
            serde_json::json!(0.0)
        );
        assert_eq!(
            parameter_to_json(&Parameter::Float(-1.5)),
            serde_json::json!(-1.5)
        );

        // Test string
        assert_eq!(
            parameter_to_json(&Parameter::String("".to_string())),
            serde_json::json!("")
        );
        assert_eq!(
            parameter_to_json(&Parameter::String("hello world".to_string())),
            serde_json::json!("hello world")
        );

        // Test binary
        assert_eq!(
            parameter_to_json(&Parameter::Binary(vec![])),
            serde_json::json!("")
        );
        assert_eq!(
            parameter_to_json(&Parameter::Binary(vec![0x00, 0xFF])),
            serde_json::json!("00ff")
        );
    }

    #[tokio::test]
    async fn test_prepared_statement_no_params() {
        let mock_transport = MockTransport::new();
        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let handle = PreparedStatementHandle::new(1, 0, vec![]);
        let stmt = PreparedStatement::new(transport, handle);

        assert_eq!(stmt.parameter_count(), 0);
        let result = stmt.build_parameters_data();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}
