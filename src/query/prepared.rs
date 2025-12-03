//! Prepared statement handling for parameterized queries.
//!
//! This module provides the `PreparedStatement` type as a data container for
//! server-side prepared statements. Execution is handled by Connection.

use crate::error::QueryError;
use crate::query::statement::Parameter;
use crate::transport::messages::DataType;
use crate::transport::protocol::PreparedStatementHandle;

/// A prepared statement for parameterized query execution.
///
/// PreparedStatement stores the server-side statement handle and parameter values.
/// Execution is performed by Connection, not by PreparedStatement itself.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // PreparedStatement is created via Connection::prepare()
/// // let mut prepared = connection.prepare("SELECT * FROM users WHERE id = ?").await?;
/// // prepared.bind(0, 42)?;
/// // let results = connection.execute_prepared(&prepared).await?;
/// // connection.close_prepared(prepared).await?;
/// # Ok(())
/// # }
/// ```
pub struct PreparedStatement {
    /// Server-side prepared statement handle
    handle: PreparedStatementHandle,
    /// Bound parameter values (column-major format for batch execution)
    parameters: Vec<Option<Parameter>>,
    /// Whether this prepared statement has been closed
    closed: bool,
}

impl PreparedStatement {
    /// Create a new PreparedStatement from a handle.
    pub(crate) fn new(handle: PreparedStatementHandle) -> Self {
        let num_params = handle.num_params as usize;
        Self {
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

    /// Get the statement handle ID.
    pub fn handle(&self) -> i32 {
        self.handle.handle
    }

    /// Get the full handle (for Connection use).
    pub(crate) fn handle_ref(&self) -> &PreparedStatementHandle {
        &self.handle
    }

    /// Check if the prepared statement has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Mark the prepared statement as closed (called by Connection).
    pub(crate) fn mark_closed(&mut self) {
        self.closed = true;
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

    /// Get bound parameters.
    pub fn parameters(&self) -> &[Option<Parameter>] {
        &self.parameters
    }

    /// Build parameters data in column-major format for the protocol.
    ///
    /// This is used internally by Connection when executing prepared statements.
    pub fn build_parameters_data(&self) -> Result<Option<Vec<Vec<serde_json::Value>>>, QueryError> {
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
pub(crate) fn parameter_to_json(param: &Parameter) -> serde_json::Value {
    match param {
        Parameter::Null => serde_json::Value::Null,
        Parameter::Boolean(b) => serde_json::Value::Bool(*b),
        Parameter::Integer(i) => serde_json::json!(*i),
        Parameter::Float(f) => serde_json::json!(*f),
        Parameter::String(s) => serde_json::Value::String(s.clone()),
        Parameter::Binary(b) => serde_json::Value::String(hex::encode(b)),
    }
}

impl std::fmt::Debug for PreparedStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedStatement")
            .field("handle", &self.handle.handle)
            .field("parameter_count", &self.parameter_count())
            .field("closed", &self.closed)
            .finish()
    }
}

impl Drop for PreparedStatement {
    fn drop(&mut self) {
        // Note: We can't do async cleanup in Drop.
        // Users should call Connection::close_prepared() explicitly for proper cleanup.
        // The server will eventually clean up orphaned statements.
        if !self.closed {
            // Log a warning in debug builds
            #[cfg(debug_assertions)]
            eprintln!(
                "Warning: PreparedStatement {} dropped without calling close_prepared()",
                self.handle.handle
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

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

    #[test]
    fn test_prepared_statement_creation() {
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

        let stmt = PreparedStatement::new(handle);

        assert_eq!(stmt.handle(), 42);
        assert_eq!(stmt.parameter_count(), 2);
        assert_eq!(stmt.parameter_types().len(), 2);
        assert!(!stmt.is_closed());
    }

    #[test]
    fn test_prepared_statement_bind_valid() {
        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let mut stmt = PreparedStatement::new(handle);

        assert!(stmt.bind(0, 42).is_ok());
        assert!(stmt.bind(1, "test").is_ok());
    }

    #[test]
    fn test_prepared_statement_bind_out_of_bounds() {
        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let mut stmt = PreparedStatement::new(handle);

        let result = stmt.bind(5, 42);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            QueryError::ParameterBindingError { index: 5, .. }
        ));
    }

    #[test]
    fn test_prepared_statement_clear_parameters() {
        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let mut stmt = PreparedStatement::new(handle);

        stmt.bind(0, 42).unwrap();
        stmt.bind(1, "test").unwrap();

        stmt.clear_parameters();

        // Build should fail because parameters are cleared
        let result = stmt.build_parameters_data();
        assert!(result.is_err());
    }

    #[test]
    fn test_prepared_statement_build_params_unbound() {
        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let stmt = PreparedStatement::new(handle);

        let result = stmt.build_parameters_data();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            QueryError::ParameterBindingError { index: 0, .. }
        ));
    }

    #[test]
    fn test_prepared_statement_build_params_success() {
        let handle = PreparedStatementHandle::new(1, 2, vec![]);
        let mut stmt = PreparedStatement::new(handle);

        stmt.bind(0, 42).unwrap();
        stmt.bind(1, "test").unwrap();

        let result = stmt.build_parameters_data().unwrap();
        assert!(result.is_some());

        let columns = result.unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0], vec![serde_json::json!(42)]);
        assert_eq!(columns[1], vec![serde_json::json!("test")]);
    }

    #[test]
    fn test_prepared_statement_no_params() {
        let handle = PreparedStatementHandle::new(1, 0, vec![]);
        let stmt = PreparedStatement::new(handle);

        assert_eq!(stmt.parameter_count(), 0);
        let result = stmt.build_parameters_data();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
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
}
