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

pub mod messages;
pub mod protocol;
pub mod websocket;

// Re-export commonly used types
pub use messages::{ColumnInfo, DataType, ResultData, ResultSetHandle, SessionInfo};
pub use protocol::{ConnectionParams, Credentials, QueryResult, TransportProtocol};
pub use websocket::WebSocketTransport;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_exports() {
        // Verify that key types are exported and accessible
        let _params = ConnectionParams::new("localhost".to_string(), 8563);
        let _creds = Credentials::new("user".to_string(), "pass".to_string());
        let _transport = WebSocketTransport::new();
        let _handle = ResultSetHandle::new(1);
    }
}
