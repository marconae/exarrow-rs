//! SQL statement handling and execution.
//!
//! This module provides the `Statement` type as a pure data container for SQL queries
//! with parameter binding. Statement execution is handled by Connection.

use crate::error::QueryError;

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

/// SQL statement as a pure data container.
///
/// Statement holds SQL text, parameters, timeout, and statement type.
/// Execution is performed by Connection, not by Statement itself.
///
/// # Example
///
/// ```no_run
/// use exarrow_rs::adbc::Connection;
///
/// # async fn example(connection: &mut Connection) -> Result<(), Box<dyn std::error::Error>> {
/// // Create a statement
/// let mut stmt = connection.create_statement("SELECT * FROM users WHERE age > ?");
///
/// // Bind parameters
/// stmt.bind(0, 18)?;
///
/// // Execute via Connection
/// let results = connection.execute_statement(&stmt).await?;
/// # Ok(())
/// # }
/// ```
pub struct Statement {
    /// SQL text (may contain parameter placeholders)
    sql: String,
    /// Bound parameters (indexed by position)
    parameters: Vec<Option<Parameter>>,
    /// Query timeout in milliseconds
    timeout_ms: u64,
    /// Statement type
    statement_type: StatementType,
}

impl Statement {
    /// Create a new statement.
    pub fn new(sql: impl Into<String>) -> Self {
        let sql = sql.into();
        let statement_type = StatementType::from_sql(&sql);

        Self {
            sql,
            parameters: Vec::new(),
            timeout_ms: 120_000, // 2 minutes default
            statement_type,
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

    /// Get the timeout in milliseconds.
    pub fn timeout_ms(&self) -> u64 {
        self.timeout_ms
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

    /// Get bound parameters.
    pub fn parameters(&self) -> &[Option<Parameter>] {
        &self.parameters
    }

    /// Build the final SQL with parameters substituted.
    ///
    /// This is used internally by Connection when executing statements.
    pub fn build_sql(&self) -> Result<String, QueryError> {
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
}

impl std::fmt::Debug for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Statement")
            .field("sql", &self.sql)
            .field("statement_type", &self.statement_type)
            .field("timeout_ms", &self.timeout_ms)
            .finish()
    }
}

impl std::fmt::Display for Statement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Statement({})", self.sql)
    }
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

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

    #[test]
    fn test_statement_creation() {
        let stmt = Statement::new("SELECT * FROM users");

        assert_eq!(stmt.sql(), "SELECT * FROM users");
        assert_eq!(stmt.statement_type(), StatementType::Select);
        assert_eq!(stmt.timeout_ms(), 120_000);
    }

    #[test]
    fn test_statement_parameter_binding() {
        let mut stmt = Statement::new("SELECT * FROM users WHERE id = ?");

        stmt.bind(0, 42).unwrap();

        let final_sql = stmt.build_sql().unwrap();
        assert_eq!(final_sql, "SELECT * FROM users WHERE id = 42");
    }

    #[test]
    fn test_statement_multiple_parameters() {
        let mut stmt = Statement::new("SELECT * FROM users WHERE age > ? AND name = ?");

        stmt.bind(0, 18).unwrap();
        stmt.bind(1, "John").unwrap();

        let final_sql = stmt.build_sql().unwrap();
        assert_eq!(
            final_sql,
            "SELECT * FROM users WHERE age > 18 AND name = 'John'"
        );
    }

    #[test]
    fn test_statement_set_timeout() {
        let mut stmt = Statement::new("SELECT * FROM users");
        stmt.set_timeout(30_000);
        assert_eq!(stmt.timeout_ms(), 30_000);
    }

    #[test]
    fn test_statement_clear_parameters() {
        let mut stmt = Statement::new("SELECT * FROM users WHERE id = ?");
        stmt.bind(0, 42).unwrap();
        stmt.clear_parameters();
        assert!(stmt.parameters().is_empty());
    }

    #[test]
    fn test_statement_display() {
        let stmt = Statement::new("SELECT 1");
        let display = format!("{}", stmt);
        assert!(display.contains("SELECT 1"));
    }
}
