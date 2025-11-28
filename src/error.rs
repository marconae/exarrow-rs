//! Error types for exarrow-rs.
//!
//! This module defines domain-specific error types organized by functional area.

use std::fmt;
use thiserror::Error;

/// Top-level error type encompassing all possible errors.
#[derive(Error, Debug)]
pub enum ExasolError {
    /// Connection-related errors
    #[error(transparent)]
    Connection(#[from] ConnectionError),

    /// Query execution errors
    #[error(transparent)]
    Query(#[from] QueryError),

    /// Data conversion errors
    #[error(transparent)]
    Conversion(#[from] ConversionError),

    /// Transport protocol errors
    #[error(transparent)]
    Transport(#[from] TransportError),
}

/// Errors related to database connections.
#[derive(Error, Debug)]
pub enum ConnectionError {
    /// Failed to establish connection to the database
    #[error("Failed to connect to {host}:{port}: {message}")]
    ConnectionFailed {
        host: String,
        port: u16,
        message: String,
    },

    /// Authentication failure
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    /// Invalid connection parameters
    #[error("Invalid connection parameter '{parameter}': {message}")]
    InvalidParameter { parameter: String, message: String },

    /// Connection string parsing error
    #[error("Failed to parse connection string: {0}")]
    ParseError(String),

    /// Connection timeout
    #[error("Connection timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    /// Connection is closed
    #[error("Connection is closed")]
    ConnectionClosed,

    /// TLS/SSL error
    #[error("TLS error: {0}")]
    TlsError(String),
}

/// Errors related to query execution.
#[derive(Error, Debug)]
pub enum QueryError {
    /// SQL syntax error
    #[error("SQL syntax error at position {position}: {message}")]
    SyntaxError { position: usize, message: String },

    /// Query execution failed
    #[error("Query execution failed: {0}")]
    ExecutionFailed(String),

    /// Query timeout
    #[error("Query timeout after {timeout_ms}ms")]
    Timeout { timeout_ms: u64 },

    /// Invalid query state
    #[error("Invalid query state: {0}")]
    InvalidState(String),

    /// Parameter binding error
    #[error("Parameter binding error for parameter {index}: {message}")]
    ParameterBindingError { index: usize, message: String },

    /// Result set not available
    #[error("Result set not available: {0}")]
    NoResultSet(String),

    /// Transaction error
    #[error("Transaction error: {0}")]
    TransactionError(String),

    /// SQL injection attempt detected
    #[error("Potential SQL injection detected")]
    SqlInjectionDetected,

    /// Prepared statement has been closed
    #[error("Prepared statement has been closed")]
    StatementClosed,

    /// Unexpected result set when row count was expected
    #[error("Expected row count but received result set")]
    UnexpectedResultSet,
}

/// Errors related to data type conversion.
#[derive(Error, Debug)]
pub enum ConversionError {
    /// Unsupported Exasol type
    #[error("Unsupported Exasol type: {exasol_type}")]
    UnsupportedType { exasol_type: String },

    /// Failed to convert value
    #[error("Failed to convert value at row {row}, column {column}: {message}")]
    ValueConversionFailed {
        row: usize,
        column: usize,
        message: String,
    },

    /// Schema mismatch
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),

    /// Invalid data format
    #[error("Invalid data format: {0}")]
    InvalidFormat(String),

    /// Overflow during conversion
    #[error("Numeric overflow at row {row}, column {column}")]
    NumericOverflow { row: usize, column: usize },

    /// Invalid UTF-8 string
    #[error("Invalid UTF-8 string at row {row}, column {column}")]
    InvalidUtf8 { row: usize, column: usize },

    /// Arrow error
    #[error("Arrow error: {0}")]
    ArrowError(String),
}

/// Errors related to transport protocol.
#[derive(Error, Debug)]
pub enum TransportError {
    /// WebSocket connection error
    #[error("WebSocket error: {0}")]
    WebSocketError(String),

    /// Message serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Message deserialization error
    #[error("Deserialization error: {0}")]
    DeserializationError(String),

    /// Protocol error
    #[error("Protocol error: {0}")]
    ProtocolError(String),

    /// Invalid response from server
    #[error("Invalid server response: {0}")]
    InvalidResponse(String),

    /// Network I/O error
    #[error("Network I/O error: {0}")]
    IoError(String),

    /// Message send error
    #[error("Failed to send message: {0}")]
    SendError(String),

    /// Message receive error
    #[error("Failed to receive message: {0}")]
    ReceiveError(String),

    /// TLS/SSL error
    #[error("TLS error: {0}")]
    TlsError(String),
}

/// ADBC-compatible error codes.
///
/// These codes map to the ADBC specification for driver interoperability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdbcErrorCode {
    /// Unknown error
    Unknown = 0,
    /// Connection error
    Connection = 1,
    /// Query error
    Query = 2,
    /// Invalid argument
    InvalidArgument = 3,
    /// Invalid state
    InvalidState = 4,
    /// Not implemented
    NotImplemented = 5,
    /// Timeout
    Timeout = 6,
}

impl fmt::Display for AdbcErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdbcErrorCode::Unknown => write!(f, "UNKNOWN"),
            AdbcErrorCode::Connection => write!(f, "CONNECTION"),
            AdbcErrorCode::Query => write!(f, "QUERY"),
            AdbcErrorCode::InvalidArgument => write!(f, "INVALID_ARGUMENT"),
            AdbcErrorCode::InvalidState => write!(f, "INVALID_STATE"),
            AdbcErrorCode::NotImplemented => write!(f, "NOT_IMPLEMENTED"),
            AdbcErrorCode::Timeout => write!(f, "TIMEOUT"),
        }
    }
}

impl ExasolError {
    /// Map to ADBC error code.
    pub fn to_adbc_code(&self) -> AdbcErrorCode {
        match self {
            ExasolError::Connection(e) => e.to_adbc_code(),
            ExasolError::Query(e) => e.to_adbc_code(),
            ExasolError::Conversion(_) => AdbcErrorCode::Query,
            ExasolError::Transport(_) => AdbcErrorCode::Connection,
        }
    }
}

impl ConnectionError {
    /// Map to ADBC error code.
    pub fn to_adbc_code(&self) -> AdbcErrorCode {
        match self {
            ConnectionError::Timeout { .. } => AdbcErrorCode::Timeout,
            ConnectionError::InvalidParameter { .. } => AdbcErrorCode::InvalidArgument,
            _ => AdbcErrorCode::Connection,
        }
    }
}

impl QueryError {
    /// Map to ADBC error code.
    pub fn to_adbc_code(&self) -> AdbcErrorCode {
        match self {
            QueryError::Timeout { .. } => AdbcErrorCode::Timeout,
            QueryError::InvalidState(_) => AdbcErrorCode::InvalidState,
            QueryError::ParameterBindingError { .. } => AdbcErrorCode::InvalidArgument,
            QueryError::StatementClosed => AdbcErrorCode::InvalidState,
            _ => AdbcErrorCode::Query,
        }
    }
}

// Conversions from external error types
impl From<arrow::error::ArrowError> for ConversionError {
    fn from(err: arrow::error::ArrowError) -> Self {
        ConversionError::ArrowError(err.to_string())
    }
}

impl From<serde_json::Error> for TransportError {
    fn from(err: serde_json::Error) -> Self {
        TransportError::SerializationError(err.to_string())
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for TransportError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        TransportError::WebSocketError(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_error_display() {
        let err = ConnectionError::ConnectionFailed {
            host: "localhost".to_string(),
            port: 8563,
            message: "Connection refused".to_string(),
        };
        assert!(err.to_string().contains("localhost"));
        assert!(err.to_string().contains("8563"));
    }

    #[test]
    fn test_query_error_display() {
        let err = QueryError::SyntaxError {
            position: 10,
            message: "Unexpected token".to_string(),
        };
        assert!(err.to_string().contains("position 10"));
    }

    #[test]
    fn test_conversion_error_display() {
        let err = ConversionError::ValueConversionFailed {
            row: 5,
            column: 2,
            message: "Invalid number format".to_string(),
        };
        assert!(err.to_string().contains("row 5"));
        assert!(err.to_string().contains("column 2"));
    }

    #[test]
    fn test_adbc_error_code_mapping() {
        let err = ExasolError::Connection(ConnectionError::Timeout { timeout_ms: 5000 });
        assert_eq!(err.to_adbc_code(), AdbcErrorCode::Timeout);

        let err = ExasolError::Query(QueryError::InvalidState("Bad state".to_string()));
        assert_eq!(err.to_adbc_code(), AdbcErrorCode::InvalidState);
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(AdbcErrorCode::Connection.to_string(), "CONNECTION");
        assert_eq!(AdbcErrorCode::Timeout.to_string(), "TIMEOUT");
    }

    #[test]
    fn test_transport_tls_error() {
        let err = TransportError::TlsError("Certificate validation failed".to_string());
        assert!(err.to_string().contains("TLS error"));
        assert!(err.to_string().contains("Certificate validation failed"));
    }

    #[test]
    fn test_statement_closed_error() {
        let err = QueryError::StatementClosed;
        assert!(err.to_string().contains("closed"));
        assert_eq!(err.to_adbc_code(), AdbcErrorCode::InvalidState);
    }

    #[test]
    fn test_unexpected_result_set_error() {
        let err = QueryError::UnexpectedResultSet;
        assert!(err.to_string().contains("result set"));
    }
}
