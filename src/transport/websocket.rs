//! WebSocket transport implementation for Exasol.
//!
//! This module provides a WebSocket-based transport for communicating with
//! Exasol databases using the Exasol WebSocket protocol.

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::error::TransportError;

use super::messages::{
    CloseResultSetRequest, CloseResultSetResponse, DisconnectRequest, DisconnectResponse,
    ExecuteRequest, ExecuteResponse, FetchRequest, FetchResponse, LoginRequest, LoginResponse,
    ResultData, ResultSetHandle, SessionInfo,
};
use super::protocol::{ConnectionParams, Credentials, QueryResult, TransportProtocol};

/// WebSocket transport implementation.
///
/// This transport uses Exasol's WebSocket API for communication.
pub struct WebSocketTransport {
    /// WebSocket connection (None if not connected)
    ws_stream: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    /// Current session information (None if not authenticated)
    session_info: Option<SessionInfo>,
    /// Connection state
    state: ConnectionState,
}

/// Connection state tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Connected but not authenticated
    Connected,
    /// Connected and authenticated
    Authenticated,
    /// Connection closed
    Closed,
}

impl WebSocketTransport {
    /// Create a new WebSocket transport.
    pub fn new() -> Self {
        Self {
            ws_stream: None,
            session_info: None,
            state: ConnectionState::Disconnected,
        }
    }

    /// Send a message and receive a response.
    async fn send_receive<T, R>(&mut self, request: &T) -> Result<R, TransportError>
    where
        T: serde::Serialize,
        R: serde::de::DeserializeOwned,
    {
        // Serialize request
        let request_json = serde_json::to_string(request)?;

        // Get WebSocket stream
        let ws_stream = self
            .ws_stream
            .as_mut()
            .ok_or_else(|| TransportError::ProtocolError("Not connected".to_string()))?;

        // Send request
        ws_stream
            .send(Message::Text(request_json))
            .await
            .map_err(|e| TransportError::SendError(e.to_string()))?;

        // Receive response
        let response_msg = ws_stream
            .next()
            .await
            .ok_or_else(|| TransportError::ReceiveError("Connection closed".to_string()))?
            .map_err(|e| TransportError::ReceiveError(e.to_string()))?;

        // Parse response
        let response_text = response_msg
            .to_text()
            .map_err(|e| TransportError::ProtocolError(format!("Invalid message format: {}", e)))?;

        let response: R = serde_json::from_str(response_text)?;

        Ok(response)
    }

    /// Check response status and return error if not ok.
    fn check_status(
        &self,
        status: &str,
        exception: &Option<super::messages::ExceptionInfo>,
    ) -> Result<(), TransportError> {
        if status != "ok" {
            let error_msg = exception
                .as_ref()
                .map(|e| {
                    format!(
                        "{} (SQL code: {})",
                        e.text,
                        e.sql_code.as_deref().unwrap_or("unknown")
                    )
                })
                .unwrap_or_else(|| "Unknown error".to_string());
            return Err(TransportError::ProtocolError(error_msg));
        }
        Ok(())
    }
}

impl Default for WebSocketTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransportProtocol for WebSocketTransport {
    async fn connect(&mut self, params: &ConnectionParams) -> Result<(), TransportError> {
        if self.state != ConnectionState::Disconnected {
            return Err(TransportError::ProtocolError(
                "Already connected".to_string(),
            ));
        }

        let url = params.to_websocket_url();

        // Connect with timeout
        let connect_future = connect_async(&url);
        let (ws_stream, _) = tokio::time::timeout(
            tokio::time::Duration::from_millis(params.timeout_ms),
            connect_future,
        )
        .await
        .map_err(|_| {
            TransportError::IoError(format!("Connection timeout after {}ms", params.timeout_ms))
        })?
        .map_err(|e| TransportError::WebSocketError(e.to_string()))?;

        self.ws_stream = Some(ws_stream);
        self.state = ConnectionState::Connected;

        Ok(())
    }

    async fn authenticate(
        &mut self,
        credentials: &Credentials,
    ) -> Result<SessionInfo, TransportError> {
        if self.state != ConnectionState::Connected {
            return Err(TransportError::ProtocolError(
                "Must connect before authenticating".to_string(),
            ));
        }

        // Send login request
        let request = LoginRequest::new(credentials.username.clone(), credentials.password.clone());
        let response: LoginResponse = self.send_receive(&request).await?;

        // Check response status
        self.check_status(&response.status, &response.exception)?;

        // Extract session info
        let session_data = response
            .response_data
            .ok_or_else(|| TransportError::InvalidResponse("Missing response data".to_string()))?;

        let session_info: SessionInfo = session_data.into();

        self.session_info = Some(session_info.clone());
        self.state = ConnectionState::Authenticated;

        Ok(session_info)
    }

    async fn execute_query(&mut self, sql: &str) -> Result<QueryResult, TransportError> {
        if self.state != ConnectionState::Authenticated {
            return Err(TransportError::ProtocolError(
                "Must authenticate before executing queries".to_string(),
            ));
        }

        // Send execute request
        let request = ExecuteRequest::new(sql.to_string());
        let response: ExecuteResponse = self.send_receive(&request).await?;

        // Check response status
        self.check_status(&response.status, &response.exception)?;

        // Extract result data
        let response_data = response
            .response_data
            .ok_or_else(|| TransportError::InvalidResponse("Missing response data".to_string()))?;

        if response_data.results.is_empty() {
            return Err(TransportError::InvalidResponse(
                "No results returned".to_string(),
            ));
        }

        // Process first result (multi-result statements not fully supported yet)
        let result = &response_data.results[0];

        match result.result_type.as_str() {
            "resultSet" => {
                // SELECT query with result set
                let handle = result.result_set_handle.ok_or_else(|| {
                    TransportError::InvalidResponse("Missing result set handle".to_string())
                })?;

                let columns = result.columns.clone().ok_or_else(|| {
                    TransportError::InvalidResponse("Missing columns".to_string())
                })?;

                let rows = result.data.clone().unwrap_or_default();
                let total_rows = result.num_rows.unwrap_or(0);

                let data = ResultData {
                    columns,
                    rows,
                    total_rows,
                };

                Ok(QueryResult::result_set(ResultSetHandle::new(handle), data))
            }
            "rowCount" => {
                // INSERT/UPDATE/DELETE query
                let count = result.row_count.unwrap_or(0);
                Ok(QueryResult::row_count(count))
            }
            other => Err(TransportError::InvalidResponse(format!(
                "Unknown result type: {}",
                other
            ))),
        }
    }

    async fn fetch_results(
        &mut self,
        handle: ResultSetHandle,
    ) -> Result<ResultData, TransportError> {
        if self.state != ConnectionState::Authenticated {
            return Err(TransportError::ProtocolError(
                "Must authenticate before fetching results".to_string(),
            ));
        }

        // Get max data message size from session info
        let max_bytes = self
            .session_info
            .as_ref()
            .map(|s| s.max_data_message_size)
            .unwrap_or(1024 * 1024); // 1MB default

        // Send fetch request
        let request = FetchRequest::new(handle.as_i32(), 0, max_bytes);
        let response: FetchResponse = self.send_receive(&request).await?;

        // Check response status
        self.check_status(&response.status, &response.exception)?;

        // Extract fetch data
        let fetch_data = response
            .response_data
            .ok_or_else(|| TransportError::InvalidResponse("Missing response data".to_string()))?;

        // Note: columns are not included in fetch response,
        // they should be cached from the initial execute response
        Ok(ResultData {
            columns: vec![], // Caller must cache columns from execute
            rows: fetch_data.data,
            total_rows: fetch_data.num_rows,
        })
    }

    async fn close_result_set(&mut self, handle: ResultSetHandle) -> Result<(), TransportError> {
        if self.state != ConnectionState::Authenticated {
            return Err(TransportError::ProtocolError(
                "Must authenticate before closing result sets".to_string(),
            ));
        }

        // Send close result set request
        let request = CloseResultSetRequest::new(vec![handle.as_i32()]);
        let response: CloseResultSetResponse = self.send_receive(&request).await?;

        // Check response status
        self.check_status(&response.status, &response.exception)?;

        Ok(())
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if self.state == ConnectionState::Disconnected || self.state == ConnectionState::Closed {
            return Ok(());
        }

        // Send disconnect request if authenticated
        if self.state == ConnectionState::Authenticated {
            let request = DisconnectRequest::new();
            let response: DisconnectResponse = self.send_receive(&request).await?;
            self.check_status(&response.status, &response.exception)?;
        }

        // Close WebSocket connection
        if let Some(mut ws_stream) = self.ws_stream.take() {
            let _ = ws_stream.close(None).await; // Ignore errors on close
        }

        self.state = ConnectionState::Closed;
        self.session_info = None;

        Ok(())
    }

    fn is_connected(&self) -> bool {
        matches!(
            self.state,
            ConnectionState::Connected | ConnectionState::Authenticated
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_websocket_transport_new() {
        let transport = WebSocketTransport::new();
        assert!(!transport.is_connected());
        assert_eq!(transport.state, ConnectionState::Disconnected);
    }

    #[test]
    fn test_websocket_transport_default() {
        let transport = WebSocketTransport::default();
        assert!(!transport.is_connected());
    }

    #[test]
    fn test_connection_state_transitions() {
        let mut transport = WebSocketTransport::new();
        assert_eq!(transport.state, ConnectionState::Disconnected);

        // Simulate state changes
        transport.state = ConnectionState::Connected;
        assert!(transport.is_connected());

        transport.state = ConnectionState::Authenticated;
        assert!(transport.is_connected());

        transport.state = ConnectionState::Closed;
        assert!(!transport.is_connected());
    }

    // Integration-style tests would require a real or mocked WebSocket server
    // For now, we have unit tests for the basic structure

    #[tokio::test]
    async fn test_connect_requires_disconnected_state() {
        let mut transport = WebSocketTransport::new();
        transport.state = ConnectionState::Connected;

        let params = ConnectionParams::new("localhost".to_string(), 8563);
        let result = transport.connect(&params).await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Already connected"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_authenticate_requires_connected_state() {
        let mut transport = WebSocketTransport::new();
        assert_eq!(transport.state, ConnectionState::Disconnected);

        let credentials = Credentials::new("user".to_string(), "pass".to_string());
        let result = transport.authenticate(&credentials).await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Must connect before authenticating"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_execute_query_requires_authenticated_state() {
        let mut transport = WebSocketTransport::new();

        let result = transport.execute_query("SELECT 1").await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Must authenticate before executing queries"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_close_idempotent() {
        let mut transport = WebSocketTransport::new();

        // Close when disconnected should be ok
        let result = transport.close().await;
        assert!(result.is_ok());
        assert_eq!(transport.state, ConnectionState::Disconnected);

        // Close when already closed should be ok
        transport.state = ConnectionState::Closed;
        let result = transport.close().await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_status_ok() {
        let transport = WebSocketTransport::new();
        let result = transport.check_status("ok", &None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_check_status_error() {
        use super::super::messages::ExceptionInfo;

        let transport = WebSocketTransport::new();
        let exception = Some(ExceptionInfo {
            sql_code: Some("42000".to_string()),
            text: "Syntax error".to_string(),
        });

        let result = transport.check_status("error", &exception);
        assert!(result.is_err());

        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Syntax error"));
            assert!(msg.contains("42000"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    // Mock-based tests would go here with a mocked WebSocket connection
    // For comprehensive testing, consider using mockall or similar
}
