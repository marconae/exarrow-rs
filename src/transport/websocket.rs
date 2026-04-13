//! WebSocket transport implementation for Exasol.
//!
//! This module provides a WebSocket-based transport for communicating with
//! Exasol databases using the Exasol WebSocket protocol.

use std::sync::Arc;

use async_trait::async_trait;
use aws_lc_rs::rand::{SecureRandom, SystemRandom};
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use num_bigint::BigUint;
use rustls::pki_types::CertificateDer;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async_tls_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    Connector, MaybeTlsStream, WebSocketStream,
};

use crate::error::TransportError;

use super::messages::{
    AuthRequest, ClosePreparedStatementRequest, ClosePreparedStatementResponse,
    CloseResultSetRequest, CloseResultSetResponse, CreatePreparedStatementRequest,
    CreatePreparedStatementResponse, DisconnectRequest, DisconnectResponse,
    ExecutePreparedStatementRequest, ExecuteRequest, ExecuteResponse, FetchRequest, FetchResponse,
    LoginInitRequest, LoginResponse, PublicKeyResponse, ResultData, ResultSetHandle, SessionInfo,
    SetAttributesRequest, SetAttributesResponse,
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
            .send(Message::Text(request_json.into()))
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
            let response_text = response_msg.to_text().map_err(|e| {
                TransportError::ProtocolError(format!("Invalid message format: {}", e))
            })?;

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
    /// base64-encoded for transmission. Supports RSA key sizes from 1024
    /// to 8192 bits (Exasol servers may emit 1024-bit keys during login).
    fn encrypt_password(password: &str, public_key_pem: &str) -> Result<String, TransportError> {
        let pkcs1_der = Self::pem_to_pkcs1_der(public_key_pem).map_err(|e| {
            TransportError::ProtocolError(format!("Failed to parse RSA public key PEM: {}", e))
        })?;

        let (n, e) = Self::parse_rsa_public_key(&pkcs1_der).map_err(|err| {
            TransportError::ProtocolError(format!("Failed to parse RSA public key: {}", err))
        })?;

        let bit_len = n.bits() as usize;
        if !(1024..=8192).contains(&bit_len) {
            return Err(TransportError::ProtocolError(format!(
                "Unsupported RSA key size: {} bits (expected 1024..=8192)",
                bit_len
            )));
        }

        let k = bit_len.div_ceil(8);

        let em = Self::pkcs1_v15_pad(password.as_bytes(), k).map_err(|err| {
            TransportError::ProtocolError(format!("Failed to pad password: {}", err))
        })?;

        let m = BigUint::from_bytes_be(&em);
        let c = m.modpow(&e, &n);

        let mut ciphertext = vec![0u8; k];
        let c_bytes = c.to_bytes_be();
        ciphertext[k - c_bytes.len()..].copy_from_slice(&c_bytes);

        Ok(STANDARD.encode(&ciphertext))
    }

    /// Strip PEM headers and base64-decode to obtain the raw PKCS#1 DER bytes.
    ///
    /// Exasol sends RSA public keys in PKCS#1 PEM format (`BEGIN RSA PUBLIC KEY`).
    /// Returns the raw PKCS#1 RSAPublicKey DER (SEQUENCE { INTEGER n, INTEGER e }).
    fn pem_to_pkcs1_der(pem: &str) -> Result<Vec<u8>, &'static str> {
        let start_marker = "-----BEGIN RSA PUBLIC KEY-----";
        let end_marker = "-----END RSA PUBLIC KEY-----";

        let start = pem.find(start_marker).ok_or("Missing PEM start marker")? + start_marker.len();
        let end = pem.find(end_marker).ok_or("Missing PEM end marker")?;

        let base64_content: String = pem[start..end]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();

        STANDARD
            .decode(&base64_content)
            .map_err(|_| "Invalid base64 in PEM")
    }

    /// Parse a PKCS#1 RSAPublicKey DER structure into (n, e).
    ///
    /// Reads `SEQUENCE { INTEGER n, INTEGER e }` from the raw DER bytes.
    fn parse_rsa_public_key(pkcs1_der: &[u8]) -> Result<(BigUint, BigUint), &'static str> {
        let mut pos = 0;

        // Outer SEQUENCE
        if pkcs1_der.get(pos) != Some(&0x30) {
            return Err("Invalid DER: expected SEQUENCE");
        }
        pos += 1;

        let (seq_len, len_bytes) = Self::read_der_length(&pkcs1_der[pos..])?;
        pos += len_bytes;

        let seq_end = pos + seq_len;
        if seq_end > pkcs1_der.len() {
            return Err("Invalid DER: truncated");
        }

        // First INTEGER: n (modulus)
        let (n_bytes, consumed) = Self::read_der_integer(&pkcs1_der[pos..seq_end])?;
        pos += consumed;

        // Second INTEGER: e (public exponent)
        let (e_bytes, consumed) = Self::read_der_integer(&pkcs1_der[pos..seq_end])?;
        pos += consumed;

        if pos != seq_end {
            return Err("Invalid DER: trailing bytes after SEQUENCE");
        }

        Ok((
            BigUint::from_bytes_be(n_bytes),
            BigUint::from_bytes_be(e_bytes),
        ))
    }

    /// Read a DER length field, returning (length_value, bytes_consumed).
    fn read_der_length(data: &[u8]) -> Result<(usize, usize), &'static str> {
        if data.is_empty() {
            return Err("Invalid DER: truncated");
        }

        let first = data[0];
        if first < 0x80 {
            Ok((first as usize, 1))
        } else {
            let num_bytes = (first & 0x7F) as usize;
            if num_bytes == 0 || num_bytes > 3 {
                return Err("Invalid DER: truncated");
            }
            if data.len() < 1 + num_bytes {
                return Err("Invalid DER: truncated");
            }
            let mut len: usize = 0;
            for &b in &data[1..1 + num_bytes] {
                len = (len << 8) | (b as usize);
            }
            Ok((len, 1 + num_bytes))
        }
    }

    /// Read a DER INTEGER, returning (value_bytes_without_sign_padding, total_bytes_consumed).
    fn read_der_integer(data: &[u8]) -> Result<(&[u8], usize), &'static str> {
        if data.is_empty() || data[0] != 0x02 {
            return Err("Invalid DER: expected INTEGER");
        }

        let (int_len, len_bytes) = Self::read_der_length(&data[1..])?;
        let header_len = 1 + len_bytes;

        if data.len() < header_len + int_len {
            return Err("Invalid DER: truncated");
        }

        let mut value = &data[header_len..header_len + int_len];

        // Strip leading 0x00 sign-bit padding
        if value.len() > 1 && value[0] == 0x00 {
            value = &value[1..];
        }

        Ok((value, header_len + int_len))
    }

    /// Build a PKCS#1 v1.5 encryption block: 0x00 || 0x02 || PS || 0x00 || message.
    ///
    /// PS consists of random non-zero bytes from `SystemRandom`.
    fn pkcs1_v15_pad(message: &[u8], k: usize) -> Result<Vec<u8>, &'static str> {
        if message.len() > k.saturating_sub(11) {
            return Err("Message too long for RSA modulus");
        }

        let ps_len = k - message.len() - 3;
        let rng = SystemRandom::new();

        let mut em = Vec::with_capacity(k);
        em.push(0x00);
        em.push(0x02);

        // Fill PS with non-zero random bytes
        for _ in 0..ps_len {
            let mut byte = [0u8; 1];
            loop {
                rng.fill(&mut byte)
                    .map_err(|_| "Failed to generate random bytes")?;
                if byte[0] != 0 {
                    break;
                }
            }
            em.push(byte[0]);
        }

        em.push(0x00);
        em.extend_from_slice(message);

        Ok(em)
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
            let tls_connector = if let Some(ref fingerprint) = params.certificate_fingerprint {
                let config = rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(FingerprintVerifier {
                        expected_fingerprint: fingerprint.clone(),
                    }))
                    .with_no_client_auth();
                Connector::Rustls(Arc::new(config))
            } else if params.validate_server_certificate {
                // Use default rustls config with native root certificates
                let mut root_store = rustls::RootCertStore::empty();
                let certs = rustls_native_certs::load_native_certs();
                for cert in certs.certs {
                    // Ignore invalid certificates - some systems have malformed certs
                    let _ = root_store.add(cert);
                }
                let config = rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();
                Connector::Rustls(Arc::new(config))
            } else {
                let config = rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoVerifier))
                    .with_no_client_auth();
                Connector::Rustls(Arc::new(config))
            };
            Some(tls_connector)
        } else {
            None
        };

        // Connect with optional TLS connector
        // Set unlimited frame and message sizes because Exasol sends large frames
        // for result sets that can exceed the tungstenite default of 16 MiB.
        let mut ws_config = WebSocketConfig::default();
        ws_config.max_frame_size = None;
        ws_config.max_message_size = None;
        let connect_future = connect_async_tls_with_config(
            &url,
            Some(ws_config),
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

                // Data is in column-major format from Exasol
                let data_values = result_set.data.clone().unwrap_or_default();
                let total_rows = result_set.num_rows.unwrap_or(0);

                let data = ResultData {
                    columns,
                    data: data_values, // Column-major format
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
        // Data is in column-major format from Exasol
        Ok(ResultData {
            columns: vec![],       // Caller must cache columns from execute
            data: fetch_data.data, // Column-major format
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
        let response_data = response
            .response_data
            .ok_or_else(|| TransportError::InvalidResponse("Missing response data".to_string()))?;

        // Extract parameter types from parameter_data if present
        let (num_params, parameter_types, parameter_names) =
            if let Some(param_data) = response_data.parameter_data {
                let mut types = Vec::with_capacity(param_data.columns.len());
                let mut names = Vec::with_capacity(param_data.columns.len());
                for p in param_data.columns {
                    types.push(p.data_type);
                    names.push(p.name);
                }
                (param_data.num_columns, types, names)
            } else {
                (0, vec![], vec![])
            };

        Ok(PreparedStatementHandle::new(
            response_data.statement_handle,
            num_params,
            parameter_types,
            parameter_names,
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
        if let Some(ref data) = parameters {
            // Build column info from the handle's parameter types, or infer from values
            let columns: Vec<_> = if handle.parameter_types.is_empty() {
                // Infer types from the first row of parameter values
                data.iter()
                    .enumerate()
                    .map(|(i, col_values)| {
                        let data_type = col_values
                            .first()
                            .map(super::messages::DataType::infer_from_json)
                            .unwrap_or_else(|| super::messages::DataType::varchar(2_000_000));
                        super::messages::ColumnInfo {
                            name: format!("param{}", i),
                            data_type,
                        }
                    })
                    .collect()
            } else {
                handle
                    .parameter_types
                    .iter()
                    .enumerate()
                    .map(|(i, dt)| super::messages::ColumnInfo {
                        name: format!("param{}", i),
                        data_type: dt.clone(),
                    })
                    .collect()
            };

            request = request.with_data(columns, data.clone());
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

                // Data is in column-major format from Exasol
                let data_values = result_set.data.clone().unwrap_or_default();
                let total_rows = result_set.num_rows.unwrap_or(0);

                let data = ResultData {
                    columns,
                    data: data_values, // Column-major format
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

        // Close WebSocket connection with proper close handshake
        if let Some(mut ws_stream) = self.ws_stream.take() {
            use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
            use tokio_tungstenite::tungstenite::protocol::CloseFrame;

            // Send close frame with normal closure code
            let close_frame = CloseFrame {
                code: CloseCode::Normal,
                reason: "Client closing connection".into(),
            };
            let _ = ws_stream.close(Some(close_frame)).await;

            // Drain any remaining messages to complete the close handshake
            use futures_util::StreamExt;
            while let Ok(Some(_)) =
                tokio::time::timeout(std::time::Duration::from_millis(100), ws_stream.next()).await
            {
            }
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

    async fn set_autocommit(&mut self, enabled: bool) -> Result<(), TransportError> {
        if self.state != ConnectionState::Authenticated {
            return Err(TransportError::ProtocolError(
                "Must authenticate before setting attributes".to_string(),
            ));
        }

        let request = SetAttributesRequest::autocommit(enabled);
        let response: SetAttributesResponse = self.send_receive(&request).await?;
        self.check_status(&response.status, &response.exception)?;
        Ok(())
    }
}

/// Returns the set of signature schemes supported by the custom certificate verifiers.
fn all_supported_verify_schemes() -> Vec<rustls::SignatureScheme> {
    vec![
        rustls::SignatureScheme::RSA_PKCS1_SHA256,
        rustls::SignatureScheme::RSA_PKCS1_SHA384,
        rustls::SignatureScheme::RSA_PKCS1_SHA512,
        rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
        rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
        rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
        rustls::SignatureScheme::RSA_PSS_SHA256,
        rustls::SignatureScheme::RSA_PSS_SHA384,
        rustls::SignatureScheme::RSA_PSS_SHA512,
        rustls::SignatureScheme::ED25519,
    ]
}

/// A certificate verifier that accepts any certificate.
/// Used when certificate validation is disabled.
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        all_supported_verify_schemes()
    }
}

/// A certificate verifier that validates by SHA-256 fingerprint of the DER-encoded certificate.
/// Bypasses hostname and CA chain validation.
#[derive(Debug)]
struct FingerprintVerifier {
    expected_fingerprint: String,
}

impl rustls::client::danger::ServerCertVerifier for FingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        use aws_lc_rs::digest;
        let fingerprint = digest::digest(&digest::SHA256, end_entity.as_ref());
        let actual: String = fingerprint
            .as_ref()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        if actual == self.expected_fingerprint {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General(format!(
                "Certificate fingerprint mismatch: expected {}, got {}",
                self.expected_fingerprint, actual
            )))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        all_supported_verify_schemes()
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
        let handle = PreparedStatementHandle::new(1, 0, vec![], vec![]);

        let result = transport.execute_prepared_statement(&handle, None).await;

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
        let handle = PreparedStatementHandle::new(1, 0, vec![], vec![]);

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

        let result = WebSocketTransport::encrypt_password("test_password", test_public_key_pem);

        assert!(
            result.is_ok(),
            "encrypt_password failed: {:?}",
            result.err()
        );
        let encrypted = result.unwrap();

        // The encrypted result should be base64-encoded
        assert!(!encrypted.is_empty());

        // Verify it's valid base64
        let decoded = STANDARD.decode(&encrypted);
        assert!(decoded.is_ok());

        // RSA 2048-bit encryption produces 256 bytes
        assert_eq!(decoded.unwrap().len(), 256);

        // Additionally verify the new parser accepts this 2048-bit key
        let der = WebSocketTransport::pem_to_pkcs1_der(test_public_key_pem).unwrap();
        let (n, e) = WebSocketTransport::parse_rsa_public_key(&der).unwrap();
        // 2048-bit modulus may report 2040..=2048 bits due to leading-bit nondeterminism
        assert!(
            n.bits() > 2040 && n.bits() <= 2048,
            "Expected ~2048-bit modulus, got {} bits",
            n.bits()
        );
        assert_eq!(
            e,
            BigUint::from(65537u32),
            "Expected standard RSA exponent 65537"
        );
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

    #[test]
    fn test_encrypt_password_with_1024_bit_key() {
        // Test-only 1024-bit RSA public key (no private key committed). Do not use in production.
        let pem_1024 = r#"-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAMh0gXUltxbJYmwQUIvXPLl9Y8bGaGFN/urgclF3Czd7viHaAMuanebQ
62s1mLV0vXaYSZk8Zsrat2T/i7jbPE0XKpVUgmnlT/CHXv6gPdTpOr3JTpo/lop0
t6J/6xJBNQDp6OrFMtTTq2M3zxSfcomlT4Q759uuGkEdM9crb8A9AgMBAAE=
-----END RSA PUBLIC KEY-----"#;

        let result = WebSocketTransport::encrypt_password("hunter2", pem_1024);
        assert!(
            result.is_ok(),
            "encrypt_password with 1024-bit key failed: {:?}",
            result.err()
        );
        let encrypted = result.unwrap();

        // Base64-decode: length MUST equal 128 bytes (1024 bits / 8)
        let decoded = STANDARD.decode(&encrypted).expect("valid base64");
        assert_eq!(
            decoded.len(),
            128,
            "1024-bit RSA ciphertext must be 128 bytes, got {}",
            decoded.len()
        );

        // Encrypt again with a different password: ciphertexts must differ (random PS padding)
        let result2 = WebSocketTransport::encrypt_password("different_password", pem_1024);
        assert!(result2.is_ok());
        let encrypted2 = result2.unwrap();
        assert_ne!(
            encrypted, encrypted2,
            "Different passwords must produce different ciphertexts"
        );

        // Even the same password should produce different ciphertext due to random padding
        let result3 = WebSocketTransport::encrypt_password("hunter2", pem_1024);
        assert!(result3.is_ok());
        let encrypted3 = result3.unwrap();
        assert_ne!(
            encrypted, encrypted3,
            "Same password with random padding should produce different ciphertexts"
        );
    }

    #[test]
    fn test_encrypt_password_rejects_too_small_key() {
        // Test-only 512-bit RSA public key. Do not use in production.
        let pem_512 = r#"-----BEGIN RSA PUBLIC KEY-----
MEgCQQCrHBlqjk3p2boRBFTAZdqcWNU+g5LjKicoOX5UIyIanBCV5fgbtoRCCvBr
++vdlAaIAcJx5iKBMp1obShMOPwVAgMBAAE=
-----END RSA PUBLIC KEY-----"#;

        let result = WebSocketTransport::encrypt_password("pw", pem_512);
        assert!(result.is_err(), "512-bit key should be rejected");
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(
                msg.contains("Unsupported RSA key size"),
                "Error should mention unsupported key size, got: {}",
                msg
            );
            assert!(
                msg.contains("512"),
                "Error should mention 512 bits, got: {}",
                msg
            );
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[test]
    fn test_parse_rsa_public_key_roundtrip() {
        // Use the 2048-bit key from the existing test
        let test_public_key_pem = r#"-----BEGIN RSA PUBLIC KEY-----
MIIBCgKCAQEAulMKxKfPd02qNEVCU1M6hG/Vc9xz+0u+N47Qqa1Y0E2A5bDiz3XA
aCg2d65C7DyuTL38zwmOtjagvvIAgRj9yDf0v1/v9e1X4l5XE6UiaKKqdcXNy6lJ
QspqkOBUptlz+2h/G8Z12++xUo/4AGAGz9ZkrRRvcTGW1GJhCROizeJhTpGMpc/v
o1G53uy2eTHwnz5S3YgJF7nfX60wjJ99ifQuQ9BhDIYLNqzwHTzExMN63v0UOBIL
vJ+yVUqh0/T2f5e9E1lDNuIqLyXe8VwwUsS72A1EGtg0s77+xUQ7KiGRbHD4bsBo
A74EI7MHQ7163wVPT0VWFRvUmmv+UO7W8wIDAQAB
-----END RSA PUBLIC KEY-----"#;

        let der = WebSocketTransport::pem_to_pkcs1_der(test_public_key_pem).unwrap();
        let (n, e) = WebSocketTransport::parse_rsa_public_key(&der).unwrap();

        // Verify standard RSA exponent
        assert_eq!(
            e,
            BigUint::from(65537u32),
            "Expected standard RSA exponent 65537"
        );

        // Verify modulus size: n.to_bytes_be() should be 256 bytes for a 2048-bit key
        let n_bytes = n.to_bytes_be();
        assert_eq!(
            n_bytes.len(),
            256,
            "2048-bit modulus should be 256 bytes in big-endian, got {}",
            n_bytes.len()
        );
        // The high byte should have the top bit set (modulus is close to 2^2048)
        assert!(
            n_bytes[0] & 0x80 != 0,
            "High byte of 2048-bit modulus should have top bit set, got 0x{:02x}",
            n_bytes[0]
        );

        // Verify bit length
        assert!(
            n.bits() > 2040 && n.bits() <= 2048,
            "Expected ~2048-bit modulus, got {} bits",
            n.bits()
        );
    }

    #[test]
    fn test_encrypt_password_with_1024_bit_key_decrypts_correctly() {
        // Test-only 1024-bit RSA keypair. Do not use in production.
        let pub_pem = r#"-----BEGIN RSA PUBLIC KEY-----
MIGJAoGBAM5D4w7VjlVI8hUmJj8MXx4glxP3dqroATH1hito7CzfGGD/Ss73lp/n
0JLwI4oT6g0wBiMlLkPY6C+hfTI2x/UhJE8gKhz6URld6uQ29d0PAq4OvsZW5Rhz
nuIWmHv9WKvS/5DVcNbBtkiTJ9BDnsSQW/YqA4DQGmzph4ThqEkJAgMBAAE=
-----END RSA PUBLIC KEY-----"#;

        // PKCS#1 private key DER fields: SEQUENCE of 9 INTEGERs
        // [version, n, e, d, p, q, dp, dq, qinv]
        // We only need n and d for decryption.
        let priv_pem = r#"-----BEGIN RSA PRIVATE KEY-----
MIICXQIBAAKBgQDOQ+MO1Y5VSPIVJiY/DF8eIJcT93aq6AEx9YYraOws3xhg/0rO
95af59CS8COKE+oNMAYjJS5D2OgvoX0yNsf1ISRPICoc+lEZXerkNvXdDwKuDr7G
VuUYc57iFph7/Vir0v+Q1XDWwbZIkyfQQ57EkFv2KgOA0Bps6YeE4ahJCQIDAQAB
AoGAVYDAu+J86Q+fAnNZAWPAfj2mQumfMIOSE0KjBpWs6YDlmzfYq+jocIro5DBV
myRcLnFM6f68qfVdcnkv68PXqSA7acyTtAKSIJAgN1xiYELuRVWMk/+UVgGhRpcH
rY4sTIwPM5b9r6JA++6PX13b8qqybPijf/Lz5urEbU3oPu0CQQDmZrbz623uijm4
ifEhk6f+Gq4spF5tUHVwY/GlfdtaDPr/wSTBkeKweZIAd9LJh8iv7Il2tYNKzzot
BTMut3VnAkEA5S6sKNaeLQevYy7N2zKm2SdSKKo1Sh48+oLYd2CZQGG5jDRom6p/
e+4hol1jGDBwvx0sMrFCFsoQXRiP2etYDwJBANSDj2LjF837Xwww5+IhkMVXlKoG
njZUDU6yUPRlZwrjiCyY2S9WQXKnX5zg6OMMRHbIRW7iM4ywIafe8Pu5KicCQQCE
rB8nyQ5qfP9wSGENWuYx4cxzFA2jaZvdXa/Yc8hj9+7FFnXUX8BLSxCXgL5j+27Z
hBbZBbp/nNwaOKTV/6LLAkAjNOwM7VST6Medyd2lNpsCjmYAoF0eUf8Z8ZJPaZeo
El6NrMeFybqeqwjPHPG1oCwg4YIeaT8ZB2qUW143brUB
-----END RSA PRIVATE KEY-----"#;

        let password = "exasol_secret_123";
        let encrypted = WebSocketTransport::encrypt_password(password, pub_pem)
            .expect("encryption should succeed");

        // Decode ciphertext
        let ciphertext = STANDARD.decode(&encrypted).expect("valid base64");
        assert_eq!(
            ciphertext.len(),
            128,
            "1024-bit RSA ciphertext must be 128 bytes"
        );

        // Parse the private key to extract n and d
        let (n, d) = parse_pkcs1_private_key_n_d(priv_pem);

        // RSA decrypt: m = c^d mod n
        let c = BigUint::from_bytes_be(&ciphertext);
        let m = c.modpow(&d, &n);

        // Convert to fixed-width bytes and strip PKCS#1 v1.5 padding
        let k = 128; // 1024 bits / 8
        let mut em = vec![0u8; k];
        let m_bytes = m.to_bytes_be();
        em[k - m_bytes.len()..].copy_from_slice(&m_bytes);

        // EM format: 0x00 || 0x02 || PS (non-zero) || 0x00 || M
        assert_eq!(em[0], 0x00, "EM must start with 0x00");
        assert_eq!(em[1], 0x02, "EM block type must be 0x02");

        // Find the 0x00 separator after PS
        let separator_pos = em[2..]
            .iter()
            .position(|&b| b == 0x00)
            .expect("Must find 0x00 separator after PS padding");
        let message_start = 2 + separator_pos + 1;

        // Verify PS is all non-zero
        for (i, &b) in em[2..2 + separator_pos].iter().enumerate() {
            assert_ne!(b, 0x00, "PS byte at position {} must be non-zero", i);
        }

        // Verify PS is at least 8 bytes (PKCS#1 v1.5 requirement)
        assert!(
            separator_pos >= 8,
            "PS must be at least 8 bytes, got {}",
            separator_pos
        );

        // Extract and verify the decrypted message matches the original password
        let decrypted = &em[message_start..];
        assert_eq!(
            decrypted,
            password.as_bytes(),
            "Decrypted message must match original password"
        );
    }

    /// Parse a PKCS#1 RSA private key PEM to extract (n, d).
    /// Only used in tests. The PKCS#1 private key is:
    /// SEQUENCE { version, n, e, d, p, q, dp, dq, qinv }
    fn parse_pkcs1_private_key_n_d(pem: &str) -> (BigUint, BigUint) {
        // Strip PEM headers
        let start_marker = "-----BEGIN RSA PRIVATE KEY-----";
        let end_marker = "-----END RSA PRIVATE KEY-----";
        let start = pem.find(start_marker).unwrap() + start_marker.len();
        let end = pem.find(end_marker).unwrap();
        let base64_content: String = pem[start..end]
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect();
        let der = STANDARD.decode(&base64_content).unwrap();

        let mut pos = 0;

        // Outer SEQUENCE
        assert_eq!(der[pos], 0x30);
        pos += 1;
        let (_, len_bytes) = read_test_der_length(&der[pos..]);
        pos += len_bytes;

        // version INTEGER (skip)
        let (_, consumed) = read_test_der_integer(&der[pos..]);
        pos += consumed;

        // n INTEGER
        let (n_bytes, consumed) = read_test_der_integer(&der[pos..]);
        pos += consumed;
        let n = BigUint::from_bytes_be(n_bytes);

        // e INTEGER (skip)
        let (_, consumed) = read_test_der_integer(&der[pos..]);
        pos += consumed;

        // d INTEGER
        let (d_bytes, _) = read_test_der_integer(&der[pos..]);
        let d = BigUint::from_bytes_be(d_bytes);

        (n, d)
    }

    fn read_test_der_length(data: &[u8]) -> (usize, usize) {
        let first = data[0];
        if first < 0x80 {
            (first as usize, 1)
        } else {
            let num_bytes = (first & 0x7F) as usize;
            let mut len: usize = 0;
            for &b in &data[1..1 + num_bytes] {
                len = (len << 8) | (b as usize);
            }
            (len, 1 + num_bytes)
        }
    }

    fn read_test_der_integer(data: &[u8]) -> (&[u8], usize) {
        assert_eq!(data[0], 0x02, "Expected INTEGER tag");
        let (int_len, len_bytes) = read_test_der_length(&data[1..]);
        let header_len = 1 + len_bytes;
        let mut value = &data[header_len..header_len + int_len];
        // Strip leading 0x00 sign-bit padding
        if value.len() > 1 && value[0] == 0x00 {
            value = &value[1..];
        }
        (value, header_len + int_len)
    }

    #[test]
    fn test_check_status_error_with_none_exception() {
        let transport = WebSocketTransport::new();

        let result = transport.check_status("error", &None);

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert_eq!(msg, "Unknown error");
        } else {
            panic!("Expected ProtocolError with 'Unknown error' message");
        }
    }

    #[test]
    fn test_check_status_error_with_exception_missing_sql_code() {
        use super::super::messages::ExceptionInfo;

        let transport = WebSocketTransport::new();
        let exception = Some(ExceptionInfo {
            sql_code: None,
            text: "Some database error".to_string(),
        });

        let result = transport.check_status("error", &exception);

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Some database error"));
            assert!(msg.contains("unknown")); // sql_code defaults to "unknown"
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_fetch_results_requires_authenticated_state() {
        let mut transport = WebSocketTransport::new();
        let handle = super::super::messages::ResultSetHandle::new(1);

        let result = transport.fetch_results(handle).await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Must authenticate before fetching results"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_fetch_results_requires_authenticated_state_from_connected() {
        let mut transport = WebSocketTransport::new();
        transport.state = ConnectionState::Connected;
        let handle = super::super::messages::ResultSetHandle::new(1);

        let result = transport.fetch_results(handle).await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Must authenticate before fetching results"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_close_result_set_requires_authenticated_state() {
        let mut transport = WebSocketTransport::new();
        let handle = super::super::messages::ResultSetHandle::new(1);

        let result = transport.close_result_set(handle).await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Must authenticate before closing result sets"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_close_result_set_requires_authenticated_state_from_connected() {
        let mut transport = WebSocketTransport::new();
        transport.state = ConnectionState::Connected;
        let handle = super::super::messages::ResultSetHandle::new(1);

        let result = transport.close_result_set(handle).await;

        assert!(result.is_err());
        if let Err(TransportError::ProtocolError(msg)) = result {
            assert!(msg.contains("Must authenticate before closing result sets"));
        } else {
            panic!("Expected ProtocolError");
        }
    }

    #[tokio::test]
    async fn test_close_from_connected_state_succeeds() {
        let mut transport = WebSocketTransport::new();
        transport.state = ConnectionState::Connected;

        // Close when connected (but not authenticated) should succeed
        // and should NOT try to send disconnect request
        let result = transport.close().await;

        assert!(result.is_ok());
        assert_eq!(transport.state, ConnectionState::Closed);
    }

    #[tokio::test]
    async fn test_close_clears_session_info() {
        use super::super::messages::SessionInfo;

        let mut transport = WebSocketTransport::new();
        transport.state = ConnectionState::Connected;
        transport.session_info = Some(SessionInfo {
            session_id: "12345".to_string(),
            protocol_version: 3,
            release_version: "7.1.0".to_string(),
            database_name: "test_db".to_string(),
            product_name: "EXASolution".to_string(),
            max_data_message_size: 1024 * 1024,
            time_zone: Some("UTC".to_string()),
        });

        let result = transport.close().await;

        assert!(result.is_ok());
        assert!(transport.session_info.is_none());
    }
}
