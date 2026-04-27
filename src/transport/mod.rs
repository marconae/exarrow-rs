//! Transport layer for Exasol database communication.
//!
//! This module provides the transport protocol abstraction and implementations
//! for communicating with Exasol databases. Phase 1 implements WebSocket transport,
//! with future support planned for Arrow-native gRPC protocol.
//!
//! # Architecture
//!
//! The transport layer is organized into:
//! - `protocol` - Transport protocol trait definition
//! - `messages` - Protocol message types
//! - `websocket` - WebSocket transport implementation
//!
//! # Example
//!
//! ```no_run
//! use exarrow_rs::transport::{
//!     WebSocketTransport, TransportProtocol, ConnectionParams, Credentials
//! };
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut transport = WebSocketTransport::new();
//!
//! // Connect
//! let params = ConnectionParams::new("localhost".to_string(), 8563);
//! transport.connect(&params).await?;
//!
//! // Authenticate
//! let credentials = Credentials::new("sys".to_string(), "exasol".to_string());
//! let session = transport.authenticate(&credentials).await?;
//! println!("Connected to: {}", session.database_name);
//!
//! // Execute query
//! let result = transport.execute_query("SELECT * FROM my_table").await?;
//!
//! // Close connection
//! transport.close().await?;
//! # Ok(())
//! # }
//! ```

pub mod deserialize;
pub mod http_transport;
pub mod messages;
#[cfg(feature = "native")]
pub mod native;
pub mod protocol;
#[cfg(feature = "websocket")]
pub mod websocket;

// Re-export commonly used types
pub use http_transport::{DataPipe, HttpTransportClient, TlsCertificate};
pub use messages::{ColumnInfo, DataType, ResultData, ResultPayload, ResultSetHandle, SessionInfo};
#[cfg(feature = "native")]
pub use native::NativeTcpTransport;
pub use protocol::{
    ConnectionParams, Credentials, PreparedStatementHandle, QueryResult, TransportProtocol,
};
#[cfg(feature = "websocket")]
pub use websocket::WebSocketTransport;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        let _params = ConnectionParams::new("localhost".to_string(), 8563);
        let _creds = Credentials::new("user".to_string(), "pass".to_string());
        let _handle = ResultSetHandle::new(1);
    }

    #[cfg(feature = "websocket")]
    #[test]
    fn test_websocket_transport_export() {
        let _transport = WebSocketTransport::new();
    }

    #[cfg(feature = "native")]
    #[test]
    fn test_native_transport_export() {
        let _transport = NativeTcpTransport::new();
    }

    #[test]
    fn test_prepared_statement_handle_export() {
        let _handle = PreparedStatementHandle::new(1, 0, vec![], vec![]);
    }
}
