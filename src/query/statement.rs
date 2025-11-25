//! SQL statement handling and execution.
//!
//! This module provides the `Statement` type for executing SQL queries with
//! parameter binding and transaction support.

use crate::error::QueryError;
use crate::query::results::ResultSet;
use crate::transport::TransportProtocol;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::timeout;

/// Type of SQL statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    /// SELECT query
    Select,
    /// INSERT statement
    Insert,
    /// UPDATE statement
    Update,
    /// DELETE statement
    Delete,
    /// DDL statement (CREATE, ALTER, DROP)
    Ddl,
    /// Transaction control (BEGIN, COMMIT, ROLLBACK)
    Transaction,
    /// Unknown or other statement type
    Other,
}

impl StatementType {
    /// Detect statement type from SQL text.
    pub fn from_sql(sql: &str) -> Self {
        let trimmed = sql.trim_start().to_uppercase();

        if trimmed.starts_with("SELECT") || trimmed.starts_with("WITH") {
            Self::Select
        } else if trimmed.starts_with("INSERT") {
            Self::Insert
        } else if trimmed.starts_with("UPDATE") {
            Self::Update
        } else if trimmed.starts_with("DELETE") {
            Self::Delete
        } else if trimmed.starts_with("CREATE")
            || trimmed.starts_with("ALTER")
            || trimmed.starts_with("DROP")
            || trimmed.starts_with("TRUNCATE")
        {
            Self::Ddl
        } else if trimmed.starts_with("BEGIN")
            || trimmed.starts_with("COMMIT")
            || trimmed.starts_with("ROLLBACK")
        {
            Self::Transaction
        } else {
            Self::Other
        }
    }

    /// Check if this statement type returns a result set.
    pub fn returns_result_set(&self) -> bool {
        matches!(self, Self::Select)
    }

    /// Check if this statement type returns a row count.
    pub fn returns_row_count(&self) -> bool {
        matches!(self, Self::Insert | Self::Update | Self::Delete)
    }
}

/// Parameter value for prepared statements.
#[derive(Debug, Clone)]
pub enum Parameter {
    /// NULL value
    Null,
    /// Boolean value
    Boolean(bool),
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// String value
    String(String),
    /// Binary data
    Binary(Vec<u8>),
}

impl Parameter {
    /// Convert parameter to SQL literal string.
    ///
    /// This is a basic implementation for Phase 1.
    /// In production, use proper prepared statement protocol.
    pub fn to_sql_literal(&self) -> Result<String, QueryError> {
        match self {
            Parameter::Null => Ok("NULL".to_string()),
            Parameter::Boolean(b) => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
            Parameter::Integer(i) => Ok(i.to_string()),
            Parameter::Float(f) => {
                if f.is_nan() || f.is_infinite() {
                    Err(QueryError::ParameterBindingError {
                        index: 0,
                        message: "NaN and Infinity are not supported".to_string(),
                    })
                } else {
                    Ok(f.to_string())
                }
            }
            Parameter::String(s) => {
                // Additional check for suspicious patterns (before escaping)
                if Self::contains_sql_injection_pattern(s) {
                    return Err(QueryError::SqlInjectionDetected);
                }

                // Basic SQL injection prevention: escape single quotes
                let escaped = s.replace('\'', "''");

                Ok(format!("'{}'", escaped))
            }
            Parameter::Binary(b) => {
                // Convert binary to hex string
                Ok(format!("'{}'", hex::encode(b)))
            }
        }
    }

    /// Basic SQL injection detection.
    fn contains_sql_injection_pattern(s: &str) -> bool {
        let upper = s.to_uppercase();

        // Check for common SQL injection patterns
        let patterns = [
            "'; DROP",
            "'; DELETE",
            "'; UPDATE",
            "'; INSERT",
            "' OR '1'='1",
            "' OR 1=1",
            "' OR TRUE",
            "UNION SELECT",
            "EXEC(",
            "EXECUTE(",
        ];

        patterns.iter().any(|pattern| upper.contains(pattern))
    }
}

impl From<bool> for Parameter {
    fn from(value: bool) -> Self {
        Parameter::Boolean(value)
    }
}

impl From<i32> for Parameter {
    fn from(value: i32) -> Self {
        Parameter::Integer(value as i64)
    }
}

impl From<i64> for Parameter {
    fn from(value: i64) -> Self {
        Parameter::Integer(value)
    }
}

impl From<f64> for Parameter {
    fn from(value: f64) -> Self {
        Parameter::Float(value)
    }
}

impl From<String> for Parameter {
    fn from(value: String) -> Self {
        Parameter::String(value)
    }
}

impl From<&str> for Parameter {
    fn from(value: &str) -> Self {
        Parameter::String(value.to_string())
    }
}

impl From<Vec<u8>> for Parameter {
    fn from(value: Vec<u8>) -> Self {
        Parameter::Binary(value)
    }
}

/// SQL statement for query execution.
///
/// Supports parameter binding, timeout control, and transaction management.
pub struct Statement {
    /// Reference to the transport layer
    transport: Arc<Mutex<dyn TransportProtocol>>,
    /// SQL text (may contain parameter placeholders)
    sql: String,
    /// Bound parameters (indexed by position)
    parameters: Vec<Option<Parameter>>,
    /// Query timeout in milliseconds
    timeout_ms: u64,
    /// Statement type
    statement_type: StatementType,
    /// Whether the statement has been executed
    executed: bool,
}

impl Statement {
    /// Create a new statement.
    pub fn new(transport: Arc<Mutex<dyn TransportProtocol>>, sql: String) -> Self {
        let statement_type = StatementType::from_sql(&sql);

        Self {
            transport,
            sql,
            parameters: Vec::new(),
            timeout_ms: 120_000, // 2 minutes default
            statement_type,
            executed: false,
        }
    }

    /// Get the SQL text.
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Get the statement type.
    pub fn statement_type(&self) -> StatementType {
        self.statement_type
    }

    /// Set query timeout.
    pub fn set_timeout(&mut self, timeout_ms: u64) {
        self.timeout_ms = timeout_ms;
    }

    /// Bind a parameter at the given index.
    ///
    /// # Arguments
    /// * `index` - Parameter index (0-based)
    /// * `value` - Parameter value
    ///
    /// # Errors
    /// Returns `QueryError::ParameterBindingError` if binding fails.
    pub fn bind<T: Into<Parameter>>(&mut self, index: usize, value: T) -> Result<(), QueryError> {
        // Ensure parameters vector is large enough
        if index >= self.parameters.len() {
            self.parameters.resize(index + 1, None);
        }

        self.parameters[index] = Some(value.into());
        Ok(())
    }

    /// Bind multiple parameters.
    pub fn bind_all<T: Into<Parameter> + Clone>(&mut self, params: &[T]) -> Result<(), QueryError> {
        for (index, param) in params.iter().enumerate() {
            self.bind(index, param.clone())?;
        }
        Ok(())
    }

    /// Clear all bound parameters.
    pub fn clear_parameters(&mut self) {
        self.parameters.clear();
    }

    /// Build the final SQL with parameters substituted.
    fn build_sql(&self) -> Result<String, QueryError> {
        let mut sql = self.sql.clone();
        let mut param_index = 0;

        // Replace '?' placeholders with parameter values
        while let Some(pos) = sql.find('?') {
            if param_index >= self.parameters.len() {
                return Err(QueryError::ParameterBindingError {
                    index: param_index,
                    message: "Not enough parameters bound".to_string(),
                });
            }

            let param = self.parameters[param_index].as_ref().ok_or_else(|| {
                QueryError::ParameterBindingError {
                    index: param_index,
                    message: "Parameter not bound".to_string(),
                }
            })?;

            let literal = param.to_sql_literal()?;
            sql.replace_range(pos..pos + 1, &literal);
            param_index += 1;
        }

        Ok(sql)
    }

    /// Execute the statement.
    ///
    /// Returns a `ResultSet` for SELECT queries or row count for DML statements.
    ///
    /// # Errors
    /// Returns `QueryError` if execution fails or times out.
    pub async fn execute(&mut self) -> Result<ResultSet, QueryError> {
        if self.executed {
            return Err(QueryError::InvalidState(
                "Statement already executed".to_string(),
            ));
        }

        // Build final SQL with parameters
        let final_sql = self.build_sql()?;

        // Execute with timeout
        let timeout_duration = Duration::from_millis(self.timeout_ms);
        let transport = Arc::clone(&self.transport);

        let result = timeout(timeout_duration, async move {
            let mut transport_guard = transport.lock().await;
            transport_guard.execute_query(&final_sql).await
        })
        .await
        .map_err(|_| QueryError::Timeout {
            timeout_ms: self.timeout_ms,
        })?
        .map_err(|e| QueryError::ExecutionFailed(e.to_string()))?;

        self.executed = true;

        // Convert transport result to ResultSet
        ResultSet::from_transport_result(result, Arc::clone(&self.transport))
    }

    /// Execute and return the row count (for non-SELECT statements).
    ///
    /// # Errors
    /// Returns `QueryError` if execution fails or if statement is a SELECT.
    pub async fn execute_update(&mut self) -> Result<i64, QueryError> {
        let result_set = self.execute().await?;

        result_set.row_count().ok_or_else(|| {
            QueryError::NoResultSet("Expected row count, got result set".to_string())
        })
    }

    /// Reset the statement for re-execution.
    ///
    /// Clears the executed flag but preserves parameters.
    pub fn reset(&mut self) {
        self.executed = false;
    }
}

/// Builder for creating `Statement` instances with a fluent API.
pub struct StatementBuilder {
    transport: Arc<Mutex<dyn TransportProtocol>>,
    sql: Option<String>,
    timeout_ms: u64,
}

impl StatementBuilder {
    /// Create a new statement builder.
    pub fn new(transport: Arc<Mutex<dyn TransportProtocol>>) -> Self {
        Self {
            transport,
            sql: None,
            timeout_ms: 120_000, // 2 minutes default
        }
    }

    /// Set the SQL text.
    pub fn sql(mut self, sql: impl Into<String>) -> Self {
        self.sql = Some(sql.into());
        self
    }

    /// Set the query timeout.
    pub fn timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Build the statement.
    ///
    /// # Panics
    /// Panics if SQL text was not set.
    pub fn build(self) -> Statement {
        let sql = self
            .sql
            .expect("SQL text must be set before building statement");

        let mut stmt = Statement::new(self.transport, sql);
        stmt.set_timeout(self.timeout_ms);
        stmt
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    #[test]
    fn test_statement_type_detection() {
        assert_eq!(
            StatementType::from_sql("SELECT * FROM users"),
            StatementType::Select
        );
        assert_eq!(
            StatementType::from_sql("  select * from users"),
            StatementType::Select
        );
        assert_eq!(
            StatementType::from_sql("WITH cte AS (SELECT 1) SELECT * FROM cte"),
            StatementType::Select
        );
        assert_eq!(
            StatementType::from_sql("INSERT INTO users VALUES (1)"),
            StatementType::Insert
        );
        assert_eq!(
            StatementType::from_sql("UPDATE users SET name = 'John'"),
            StatementType::Update
        );
        assert_eq!(
            StatementType::from_sql("DELETE FROM users WHERE id = 1"),
            StatementType::Delete
        );
        assert_eq!(
            StatementType::from_sql("CREATE TABLE test (id INT)"),
            StatementType::Ddl
        );
        assert_eq!(
            StatementType::from_sql("DROP TABLE test"),
            StatementType::Ddl
        );
        assert_eq!(StatementType::from_sql("BEGIN"), StatementType::Transaction);
        assert_eq!(
            StatementType::from_sql("COMMIT"),
            StatementType::Transaction
        );
        assert_eq!(
            StatementType::from_sql("ROLLBACK"),
            StatementType::Transaction
        );
    }

    #[test]
    fn test_statement_type_returns_result_set() {
        assert!(StatementType::Select.returns_result_set());
        assert!(!StatementType::Insert.returns_result_set());
        assert!(!StatementType::Update.returns_result_set());
        assert!(!StatementType::Delete.returns_result_set());
    }

    #[test]
    fn test_parameter_to_sql_literal() {
        assert_eq!(Parameter::Null.to_sql_literal().unwrap(), "NULL");
        assert_eq!(Parameter::Boolean(true).to_sql_literal().unwrap(), "TRUE");
        assert_eq!(Parameter::Boolean(false).to_sql_literal().unwrap(), "FALSE");
        assert_eq!(Parameter::Integer(42).to_sql_literal().unwrap(), "42");
        assert_eq!(Parameter::Float(3.14).to_sql_literal().unwrap(), "3.14");
        assert_eq!(
            Parameter::String("hello".to_string())
                .to_sql_literal()
                .unwrap(),
            "'hello'"
        );
    }

    #[test]
    fn test_parameter_string_escaping() {
        let param = Parameter::String("O'Reilly".to_string());
        assert_eq!(param.to_sql_literal().unwrap(), "'O''Reilly'");
    }

    #[test]
    fn test_parameter_sql_injection_detection() {
        let dangerous = Parameter::String("'; DROP TABLE users; --".to_string());
        assert!(dangerous.to_sql_literal().is_err());

        let malicious = Parameter::String("' OR '1'='1".to_string());
        assert!(malicious.to_sql_literal().is_err());

        let safe = Parameter::String("It's a nice day".to_string());
        assert!(safe.to_sql_literal().is_ok());
    }

    #[test]
    fn test_parameter_conversions() {
        let _p: Parameter = true.into();
        let _p: Parameter = 42i32.into();
        let _p: Parameter = 42i64.into();
        let _p: Parameter = 3.14f64.into();
        let _p: Parameter = "test".into();
        let _p: Parameter = String::from("test").into();
        let _p: Parameter = vec![1u8, 2, 3].into();
    }

    #[tokio::test]
    async fn test_statement_creation() {
        let mut mock_transport = MockTransport::new();
        mock_transport.expect_is_connected().returning(|| true);

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let stmt = Statement::new(transport, "SELECT * FROM users".to_string());

        assert_eq!(stmt.sql(), "SELECT * FROM users");
        assert_eq!(stmt.statement_type(), StatementType::Select);
    }

    #[tokio::test]
    async fn test_statement_parameter_binding() {
        let mut mock_transport = MockTransport::new();
        mock_transport.expect_is_connected().returning(|| true);

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let mut stmt = Statement::new(transport, "SELECT * FROM users WHERE id = ?".to_string());

        stmt.bind(0, 42).unwrap();

        let final_sql = stmt.build_sql().unwrap();
        assert_eq!(final_sql, "SELECT * FROM users WHERE id = 42");
    }

    #[tokio::test]
    async fn test_statement_multiple_parameters() {
        let mut mock_transport = MockTransport::new();
        mock_transport.expect_is_connected().returning(|| true);

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let mut stmt = Statement::new(
            transport,
            "SELECT * FROM users WHERE age > ? AND name = ?".to_string(),
        );

        stmt.bind(0, 18).unwrap();
        stmt.bind(1, "John").unwrap();

        let final_sql = stmt.build_sql().unwrap();
        assert_eq!(
            final_sql,
            "SELECT * FROM users WHERE age > 18 AND name = 'John'"
        );
    }

    #[tokio::test]
    async fn test_statement_builder() {
        let mut mock_transport = MockTransport::new();
        mock_transport.expect_is_connected().returning(|| true);

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));

        let stmt = StatementBuilder::new(transport)
            .sql("SELECT * FROM users")
            .timeout_ms(30_000)
            .build();

        assert_eq!(stmt.sql(), "SELECT * FROM users");
        assert_eq!(stmt.timeout_ms, 30_000);
    }

    #[tokio::test]
    async fn test_statement_execute_row_count() {
        let mut mock_transport = MockTransport::new();

        // Expect execute_query to be called
        mock_transport
            .expect_execute_query()
            .times(1)
            .returning(|_sql| Ok(TransportQueryResult::RowCount { count: 5 }));

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let mut stmt = Statement::new(transport, "INSERT INTO users VALUES (1)".to_string());

        let row_count = stmt.execute_update().await.unwrap();
        assert_eq!(row_count, 5);
    }

    #[tokio::test]
    async fn test_statement_execute_result_set() {
        let mut mock_transport = MockTransport::new();

        // Expect execute_query to be called
        mock_transport
            .expect_execute_query()
            .times(1)
            .returning(|_sql| {
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
                    rows: vec![vec![serde_json::json!(1)]],
                    total_rows: 1,
                };

                Ok(TransportQueryResult::ResultSet {
                    handle: ResultSetHandle::new(1),
                    data,
                })
            });

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let mut stmt = Statement::new(transport, "SELECT * FROM users".to_string());

        let result_set = stmt.execute().await.unwrap();
        assert!(result_set.row_count().is_none());
    }

    #[tokio::test]
    async fn test_statement_double_execution_error() {
        let mut mock_transport = MockTransport::new();

        mock_transport
            .expect_execute_query()
            .times(1)
            .returning(|_sql| Ok(TransportQueryResult::RowCount { count: 1 }));

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let mut stmt = Statement::new(transport, "UPDATE users SET name = 'test'".to_string());

        // First execution should succeed
        let _ = stmt.execute().await.unwrap();

        // Second execution should fail
        let result = stmt.execute().await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), QueryError::InvalidState(_)));
    }

    #[tokio::test]
    async fn test_statement_reset() {
        let mut mock_transport = MockTransport::new();

        mock_transport
            .expect_execute_query()
            .times(2)
            .returning(|_sql| Ok(TransportQueryResult::RowCount { count: 1 }));

        let transport: Arc<Mutex<dyn TransportProtocol>> = Arc::new(Mutex::new(mock_transport));
        let mut stmt = Statement::new(transport, "UPDATE users SET name = 'test'".to_string());

        // First execution
        let _ = stmt.execute().await.unwrap();

        // Reset and execute again
        stmt.reset();
        let result = stmt.execute().await;
        assert!(result.is_ok());
    }
}
