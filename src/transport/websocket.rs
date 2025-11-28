//! WebSocket transport implementation for Exasol.
//!
//! This module provides a WebSocket-based transport for communicating with
//! Exasol databases using the Exasol WebSocket protocol.

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::{Pkcs1v15Encrypt, RsaPublicKey};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async_tls_with_config, tungstenite::Message, Connector, MaybeTlsStream,
    WebSocketStream,
};

use crate::error::TransportError;

use super::messages::{
    AuthRequest, ClosePreparedStatementRequest, ClosePreparedStatementResponse,
    CloseResultSetRequest, CloseResultSetResponse, CreatePreparedStatementRequest,
    CreatePreparedStatementResponse, DisconnectRequest, DisconnectResponse,
    ExecutePreparedStatementRequest, ExecuteRequest, ExecuteResponse, FetchRequest, FetchResponse,
    LoginInitRequest, LoginResponse, PublicKeyResponse, ResultData, ResultSetHandle, SessionInfo,
};
use super::protocol::{
    ConnectionParams, Credentials, PreparedStatementHandle, QueryResult, TransportProtocol,
};

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
    ///
    /// Exasol may send intermediate status messages (like "EXECUTING") before
    /// the actual JSON response. This method skips those messages and returns
    /// only the final JSON response.
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

        // Receive response, skipping intermediate status messages
        loop {
            let response_msg = ws_stream
                .next()
                .await
                .ok_or_else(|| TransportError::ReceiveError("Connection closed".to_string()))?
                .map_err(|e| TransportError::ReceiveError(e.to_string()))?;

            // Parse response
            let response_text = response_msg
                .to_text()
                .map_err(|e| TransportError::ProtocolError(format!("Invalid message format: {}", e)))?;

            // Skip intermediate status messages (e.g., "EXECUTING", "FETCHING")
            // These are plain text, not JSON objects starting with '{'
            if !response_text.starts_with('{') {
                continue;
            }

            let response: R = serde_json::from_str(response_text).map_err(|e| {
                // Include context in error for debugging
                let preview: String = response_text.chars().take(200).collect();
                TransportError::InvalidResponse(format!(
                    "JSON parse error: {}. Response preview: '{}'",
                    e, preview
                ))
            })?;

            return Ok(response);
        }
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

    /// Encrypt password using RSA with PKCS#1 v1.5 padding.
    ///
    /// The password is encrypted using the server's public key and then
    /// base64-encoded for transmission.
    fn encrypt_password(
        password: &str,
        public_key_pem: &str,
    ) -> Result<String, TransportError> {
        // Parse the RSA public key from PEM format
        let public_key = RsaPublicKey::from_pkcs1_pem(public_key_pem).map_err(|e| {
            TransportError::ProtocolError(format!("Failed to parse RSA public key: {}", e))
        })?;

        // Encrypt the password using PKCS#1 v1.5 padding
        let mut rng = rand::thread_rng();
        let encrypted = public_key
            .encrypt(&mut rng, Pkcs1v15Encrypt, password.as_bytes())
            .map_err(|e| {
                TransportError::ProtocolError(format!("Failed to encrypt password: {}", e))
            })?;

        // Base64-encode the encrypted password
        Ok(STANDARD.encode(&encrypted))
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

        // Create TLS connector if needed
        let connector = if params.use_tls {
            let mut tls_builder = native_tls::TlsConnector::builder();
            if !params.validate_server_certificate {
                tls_builder.danger_accept_invalid_certs(true);
                tls_builder.danger_accept_invalid_hostnames(true);
            }
            let tls_connector = tls_builder
                .build()
                .map_err(|e| TransportError::TlsError(e.to_string()))?;
            Some(Connector::NativeTls(tls_connector))
        } else {
            None
        };

        // Connect with optional TLS connector
        let connect_future = connect_async_tls_with_config(
            &url,
            None,      // WebSocket config
            false,     // disable_nagle
            connector, // TLS connector
        );

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

        // Step 1: Send login init request to get the server's public key
        let init_request = LoginInitRequest::new();
        let key_response: PublicKeyResponse = self.send_receive(&init_request).await?;

        // Check response status
        self.check_status(&key_response.status, &key_response.exception)?;

        // Extract public key data
        let key_data = key_response.response_data.ok_or_else(|| {
            TransportError::InvalidResponse("Missing public key data in response".to_string())
        })?;

        // Step 2: Encrypt password using the server's RSA public key
        let encrypted_password =
            Self::encrypt_password(&credentials.password, &key_data.public_key_pem)?;

        // Step 3: Send authentication request with encrypted password
        let auth_request = AuthRequest::new(
            credentials.username.clone(),
            encrypted_password,
            "exarrow-rs".to_string(),
        );
        let login_response: LoginResponse = self.send_receive(&auth_request).await?;

        // Check response status
        self.check_status(&login_response.status, &login_response.exception)?;

        // Step 4: Extract session info from the final response
        let session_data = login_response
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
                // SELECT query with result set - data is nested in result_set field
                let result_set = result.result_set.as_ref().ok_or_else(|| {
                    TransportError::InvalidResponse(format!(
                        "Missing result_set data. Result: {:?}",
                        result
                    ))
                })?;

                let columns = result_set.columns.clone().ok_or_else(|| {
                    TransportError::InvalidResponse("Missing columns".to_string())
                })?;

                let rows = result_set.data.clone().unwrap_or_default();
                let total_rows = result_set.num_rows.unwrap_or(0);

                let data = ResultData {
                    columns,
                    rows,
                    total_rows,
                };

                // Handle may be None when all data fits in one response
                let handle = result_set.result_set_handle.map(ResultSetHandle::new);

                Ok(QueryResult::result_set(handle, data))
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

    async fn create_prepared_statement(
        &mut self,
        sql: &str,
    ) -> Result<PreparedStatementHandle, TransportError> {
        if self.state != ConnectionState::Authenticated {
            return Err(TransportError::ProtocolError(
                "Must authenticate before creating prepared statements".to_string(),
            ));
        }

        // Send create prepared statement request
        let request = CreatePreparedStatementRequest::new(sql);
        let response: CreatePreparedStatementResponse = self.send_receive(&request).await?;

        // Check response status
        self.check_status(&response.status, &response.exception)?;

        // Extract response data
        let response_data = response.response_data.ok_or_else(|| {
            TransportError::InvalidResponse("Missing response data".to_string())
        })?;

        // Extract parameter types from parameter_data if present
        let (num_params, parameter_types) = if let Some(param_data) = response_data.parameter_data {
            let types = param_data
                .columns
                .into_iter()
                .map(|p| p.data_type)
                .collect();
            (param_data.num_columns, types)
        } else {
            (0, vec![])
        };

        Ok(PreparedStatementHandle::new(
            response_data.statement_handle,
            num_params,
            parameter_types,
        ))
    }

    async fn execute_prepared_statement(
        &mut self,
        handle: &PreparedStatementHandle,
        parameters: Option<Vec<Vec<serde_json::Value>>>,
    ) -> Result<QueryResult, TransportError> {
        if self.state != ConnectionState::Authenticated {
            return Err(TransportError::ProtocolError(
                "Must authenticate before executing prepared statements".to_string(),
            ));
        }

        // Build execution request
        let mut request = ExecutePreparedStatementRequest::new(handle.handle);

        // Add parameters if provided
        if let Some(data) = parameters {
            // Build column info from the handle's parameter types
            let columns: Vec<_> = handle
                .parameter_types
                .iter()
                .enumerate()
                .map(|(i, dt)| super::messages::ColumnInfo {
                    name: format!("param{}", i),
                    data_type: dt.clone(),
                })
                .collect();

            request = request.with_data(columns, data);
        }

        // Send execute prepared statement request
        let response: ExecuteResponse = self.send_receive(&request).await?;

        // Check response status
        self.check_status(&response.status, &response.exception)?;

        // Extract result data (same processing as execute_query)
        let response_data = response
            .response_data
            .ok_or_else(|| TransportError::InvalidResponse("Missing response data".to_string()))?;

        if response_data.results.is_empty() {
            return Err(TransportError::InvalidResponse(
                "No results returned".to_string(),
            ));
        }

        // Process first result
        let result = &response_data.results[0];

        match result.result_type.as_str() {
            "resultSet" => {
                let result_set = result.result_set.as_ref().ok_or_else(|| {
                    TransportError::InvalidResponse(format!(
                        "Missing result_set data. Result: {:?}",
                        result
                    ))
                })?;

                let columns = result_set.columns.clone().ok_or_else(|| {
                    TransportError::InvalidResponse("Missing columns".to_string())
                })?;

                let rows = result_set.data.clone().unwrap_or_default();
                let total_rows = result_set.num_rows.unwrap_or(0);

                let data = ResultData {
                    columns,
                    rows,
                    total_rows,
                };

                let handle = result_set.result_set_handle.map(ResultSetHandle::new);

                Ok(QueryResult::result_set(handle, data))
            }
            "rowCount" => {
                let count = result.row_count.unwrap_or(0);
                Ok(QueryResult::row_count(count))
            }
            other => Err(TransportError::InvalidResponse(format!(
                "Unknown result type: {}",
                other
            ))),
        }
    }

    async fn close_prepared_statement(
        &mut self,
        handle: &PreparedStatementHandle,
    ) -> Result<(), TransportError> {
        if self.state != ConnectionState::Authenticated {
            return Err(TransportError::ProtocolError(
                "Must authenticate before closing prepared statements".to_string(),
            ));
        }

        // Send close prepared statement request
        let request = ClosePreparedStatementRequest::new(handle.handle);
        let response: ClosePreparedStatementResponse = self.send_receive(&request).await?;

        // Check response status
        self.check_status(&response.status, &response.exception)?;

        Ok(())
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if self.state == ConnectionState::Disconnected || self.state == ConnectionState::Closed {
            return Ok(());
        }

        // Send disconnect request if authenticated
        // We ignore errors here because:
        // 1. The server may close the connection before responding
        // 2. We're closing anyway, so errors don't matter
        if self.state == ConnectionState::Authenticated {
            let request = DisconnectRequest::new();
            // Try to send disconnect, but don't fail if it doesn't work
            let _ = self.send_receive::<_, DisconnectResponse>(&request).await;
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
    async fn test_create_prepared_statement_requires_authenticated_state() {
        let mut transport = WebSocketTransport::new();

        let result = transport
            .create_prepared_statement("SELECT * FROM test WHERE id = ?")
            .await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Must authenticate before creating prepared statements"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_execute_prepared_statement_requires_authenticated_state() {
        let mut transport = WebSocketTransport::new();
        let handle = PreparedStatementHandle::new(1, 0, vec![]);

        let result = transport
            .execute_prepared_statement(&handle, None)
            .await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Must authenticate before executing prepared statements"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_close_prepared_statement_requires_authenticated_state() {
        let mut transport = WebSocketTransport::new();
        let handle = PreparedStatementHandle::new(1, 0, vec![]);

        let result = transport.close_prepared_statement(&handle).await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Must authenticate before closing prepared statements"));
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

    #[test]
    fn test_encrypt_password_with_valid_key() {
        // This is a test RSA public key (2048-bit) in PKCS#1 PEM format
        // Generated specifically for testing - NOT for production use
        let test_public_key_pem = r#"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAulMKxKfPd02qNEVCU1M6hG/Vc9xz+0u+N47Qqa1Y0E2A5bDiz3XA
aCg2d65C7DyuTL38zwmOtjagvvIAgRj9yDf0v1/v9e1X4l5XE6UiaKKqdcXNy6lJ
QspqkOBUptlz+2h/G8Z12++xUo/4AGAGz9ZkrRRvcTGW1GJhCROizeJhTpGMpc/v
o1G53uy2eTHwnz5S3YgJF7nfX60wjJ99ifQuQ9BhDIYLNqzwHTzExMN63v0UOBIL
vJ+yVUqh0/T2f5e9E1lDNuIqLyXe8VwwUsS72A1EGtg0s77+xUQ7KiGRbHD4bsBo
A74EI7MHQ7163wVPT0VWFRvUmmv+UO7W8wIDAQAB
-----END RSA PUBLIC KEY-----"#;

        let result =
            WebSocketTransport::encrypt_password("test_password", test_public_key_pem);

        assert!(result.is_ok(), "encrypt_password failed: {:?}", result.err());
        let encrypted = result.unwrap();

        // The encrypted result should be base64-encoded
        assert!(!encrypted.is_empty());

        // Verify it's valid base64
        let decoded = STANDARD.decode(&encrypted);
        assert!(decoded.is_ok());

        // RSA 2048-bit encryption produces 256 bytes
        assert_eq!(decoded.unwrap().len(), 256);
    }

    #[test]
    fn test_encrypt_password_with_invalid_key() {
        let invalid_pem = "not a valid PEM key";

        let result = WebSocketTransport::encrypt_password("password", invalid_pem);

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Failed to parse RSA public key"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    // Mock-based tests would go here with a mocked WebSocket connection
    // For comprehensive testing, consider using mockall or similar
}
