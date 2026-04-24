pub mod arrow_builder;
pub mod attributes;
pub mod constants;
pub mod encryption;
pub mod framing;
pub mod handshake;
pub mod result_parser;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rustls::pki_types::CertificateDer;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::error::TransportError;

use super::messages::{ColumnInfo, ResultData, ResultPayload, ResultSetHandle, SessionInfo};
use super::protocol::{
    ConnectionParams, Credentials, PreparedStatementHandle, QueryResult, TransportProtocol,
};

use self::attributes::{AttributeSet, AttributeValue};
use self::constants::*;
use self::encryption::ChaCha20Encryptor;
use self::framing::{MessageHeader, SerialCounter};
use self::result_parser::{NativeColumnMeta, NativeResponse, NativeResponseEnvelope};

/// Connection state for the native TCP transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionState {
    Disconnected,
    Connected,
    Authenticated,
    Closed,
}

/// Abstraction over plain TCP or TLS-wrapped TCP stream.
enum NativeStream {
    Plain(TcpStream),
    Tls(Box<TlsStream<TcpStream>>),
}

impl NativeStream {
    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), TransportError> {
        match self {
            NativeStream::Plain(s) => {
                s.read_exact(buf)
                    .await
                    .map_err(|e| TransportError::ReceiveError(e.to_string()))?;
                Ok(())
            }
            NativeStream::Tls(s) => {
                s.read_exact(buf)
                    .await
                    .map_err(|e| TransportError::ReceiveError(e.to_string()))?;
                Ok(())
            }
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        match self {
            NativeStream::Plain(s) => s
                .write_all(buf)
                .await
                .map_err(|e| TransportError::SendError(e.to_string())),
            NativeStream::Tls(s) => s
                .write_all(buf)
                .await
                .map_err(|e| TransportError::SendError(e.to_string())),
        }
    }

    async fn flush(&mut self) -> Result<(), TransportError> {
        match self {
            NativeStream::Plain(s) => s
                .flush()
                .await
                .map_err(|e| TransportError::SendError(e.to_string())),
            NativeStream::Tls(s) => s
                .flush()
                .await
                .map_err(|e| TransportError::SendError(e.to_string())),
        }
    }
}

/// Native binary TCP transport for Exasol.
///
/// Communicates using Exasol's native binary protocol over TCP (optionally TLS-wrapped),
/// with ChaCha20 encryption for message payloads after the handshake.
pub struct NativeTcpTransport {
    stream: Option<NativeStream>,
    state: ConnectionState,
    serial: SerialCounter,
    encryptor: ChaCha20Encryptor,
    session: Option<SessionInfo>,
    tls_active: bool,
    fetch_positions: HashMap<i32, i64>,
    result_columns: HashMap<i32, Vec<NativeColumnMeta>>,
    recv_buf: Vec<u8>,
}

impl NativeTcpTransport {
    pub fn new() -> Self {
        Self {
            stream: None,
            state: ConnectionState::Disconnected,
            serial: SerialCounter::new(),
            encryptor: ChaCha20Encryptor::new(),
            session: None,
            tls_active: false,
            fetch_positions: HashMap::new(),
            result_columns: HashMap::new(),
            recv_buf: Vec::with_capacity(1 << 20),
        }
    }

    fn stream_mut(&mut self) -> Result<&mut NativeStream, TransportError> {
        self.stream
            .as_mut()
            .ok_or_else(|| TransportError::ProtocolError("Not connected".into()))
    }

    /// Send raw bytes to the stream.
    async fn send_raw(&mut self, data: &[u8]) -> Result<(), TransportError> {
        let stream = self.stream_mut()?;
        stream.write_all(data).await?;
        stream.flush().await
    }

    /// Send a command message with attributes and optional extra data payload.
    async fn send_message(
        &mut self,
        command: u8,
        attrs: &AttributeSet,
        extra_data: Option<&[u8]>,
    ) -> Result<(), TransportError> {
        self.send_message_with_serial(command, attrs, extra_data, None)
            .await
    }

    async fn send_message_with_serial(
        &mut self,
        command: u8,
        attrs: &AttributeSet,
        extra_data: Option<&[u8]>,
        serial_override: Option<u32>,
    ) -> Result<(), TransportError> {
        let attr_bytes = attrs.serialize();
        let extra_len = extra_data.map_or(0, |d| d.len());
        let total_payload_len = attr_bytes.len() + extra_len;

        let header = MessageHeader {
            message_length: total_payload_len as u32,
            command,
            serial: serial_override.unwrap_or_else(|| self.serial.next()),
            num_attributes: attrs.num_attributes(),
            attribute_data_len: attr_bytes.len() as u32,
            num_result_parts: if extra_data.is_some() { 1 } else { 0 },
        };

        let header_bytes = header.serialize();

        let mut payload = Vec::with_capacity(total_payload_len);
        payload.extend_from_slice(&attr_bytes);
        if let Some(extra) = extra_data {
            payload.extend_from_slice(extra);
        }

        if self.encryptor.is_active() {
            self.encryptor.encrypt(&mut payload);
        }

        self.send_raw(&header_bytes).await?;
        self.send_raw(&payload).await
    }

    /// Receive a response message into the reusable `recv_buf`, returning the header.
    ///
    /// After this call, `self.recv_buf` contains the decrypted payload bytes.
    /// The buffer grows on demand but never shrinks, amortising allocation cost across fetches.
    async fn receive_into_buf(&mut self) -> Result<MessageHeader, TransportError> {
        let mut hdr = [0u8; HEADER_SIZE];
        self.stream_mut()?.read_exact(&mut hdr).await?;
        let header = MessageHeader::parse(&hdr)?;
        self.recv_buf.clear();
        if header.message_length > 0 {
            let msg_len = header.message_length as usize;
            self.recv_buf.resize(msg_len, 0);
            // Split borrows: access stream and recv_buf through separate field paths
            // so the borrow checker sees them as disjoint.
            let stream = self
                .stream
                .as_mut()
                .ok_or_else(|| TransportError::ProtocolError("Not connected".into()))?;
            stream.read_exact(&mut self.recv_buf).await?;
            if self.encryptor.is_active() {
                self.encryptor.decrypt(&mut self.recv_buf);
            }
        }
        Ok(header)
    }

    /// Receive a response message: header + payload (decrypted if needed).
    async fn receive_message(&mut self) -> Result<(MessageHeader, Vec<u8>), TransportError> {
        let header = self.receive_into_buf().await?;
        Ok((header, self.recv_buf.clone()))
    }

    /// Send a command and receive its response, handling StillExecuting retries.
    /// The response payload is stored in `self.recv_buf`; only the header is returned.
    /// This avoids cloning the payload — callers access `self.recv_buf` directly.
    async fn send_and_fill_buf(
        &mut self,
        command: u8,
        attrs: &AttributeSet,
        extra_data: Option<&[u8]>,
    ) -> Result<MessageHeader, TransportError> {
        let command_serial = self.serial.next();
        self.send_message_with_serial(command, attrs, extra_data, Some(command_serial))
            .await?;
        loop {
            let header = self.receive_into_buf().await?;
            let still_executing = {
                let result_data = Self::result_data_slice(&header, &self.recv_buf);
                matches!(
                    result_parser::parse_response(result_data)?.terminal,
                    NativeResponse::StillExecuting
                )
            };
            if !still_executing {
                return Ok(header);
            }
        }
    }

    /// Send a command and receive its response, handling StillExecuting retries.
    async fn send_and_receive(
        &mut self,
        command: u8,
        attrs: &AttributeSet,
        extra_data: Option<&[u8]>,
    ) -> Result<(MessageHeader, Vec<u8>), TransportError> {
        let header = self.send_and_fill_buf(command, attrs, extra_data).await?;
        Ok((header, self.recv_buf.clone()))
    }

    /// Extract the result-part data from a response, skipping past any attribute data.
    fn extract_result_data(header: &MessageHeader, payload: &[u8]) -> Vec<u8> {
        let attr_len = header.attribute_data_len as usize;
        if attr_len < payload.len() {
            payload[attr_len..].to_vec()
        } else {
            Vec::new()
        }
    }

    fn result_data_slice<'a>(header: &MessageHeader, payload: &'a [u8]) -> &'a [u8] {
        let skip = header.attribute_data_len as usize;
        if skip < payload.len() {
            &payload[skip..]
        } else {
            &[]
        }
    }

    /// Parse the result-part of a response payload and check for exceptions.
    fn check_response(
        header: &MessageHeader,
        payload: &[u8],
    ) -> Result<NativeResponseEnvelope, TransportError> {
        let result_data = Self::extract_result_data(header, payload);
        let response = result_parser::parse_response(&result_data)?;
        if let NativeResponse::Exception {
            ref message,
            ref sql_state,
        } = response.terminal
        {
            return Err(TransportError::ProtocolError(format!(
                "{} (SQL state: {})",
                message, sql_state
            )));
        }
        Ok(response)
    }

    /// Convert native column metadata to the shared ColumnInfo type.
    fn to_column_info(columns: &[NativeColumnMeta]) -> Vec<ColumnInfo> {
        columns
            .iter()
            .map(|c| ColumnInfo {
                name: c.name.clone(),
                data_type: arrow_builder::native_meta_to_data_type(c),
            })
            .collect()
    }

    /// Convert native result to QueryResult, building Arrow RecordBatch directly
    /// from column-major binary wire data without JSON intermediary.
    fn native_result_to_query_result(
        response: NativeResponse,
    ) -> Result<QueryResult, TransportError> {
        match response {
            NativeResponse::ResultSet {
                handle,
                columns,
                batch,
                total_rows,
                rows_received: _,
            } => {
                let col_infos = Self::to_column_info(&columns);

                let record_batch = batch.unwrap_or_else(|| {
                    arrow::record_batch::RecordBatch::new_empty(std::sync::Arc::new(
                        arrow::datatypes::Schema::empty(),
                    ))
                });

                let result_data = ResultData {
                    columns: col_infos,
                    data: ResultPayload::Arrow(record_batch),
                    total_rows,
                };

                let rs_handle = if handle == SMALL_RESULTSET {
                    None
                } else {
                    Some(ResultSetHandle::new(handle))
                };

                Ok(QueryResult::result_set(rs_handle, result_data))
            }
            NativeResponse::RowCount(count) => Ok(QueryResult::row_count(count)),
            NativeResponse::Empty => Ok(QueryResult::row_count(0)),
            _ => Err(TransportError::ProtocolError(
                "Unexpected response type".into(),
            )),
        }
    }

    /// Build the raw data payload for CMD_EXECUTE_PREPARED.
    ///
    /// Wire format (matching the JDBC driver):
    /// ```text
    /// [handle:4 LE][num_tables:4 LE=1][is_table:1=1][num_columns:4 LE]
    /// [total_rows:8 LE][rows_in_msg:8 LE]
    /// For each column: [name_len:4 LE][name_bytes][type_id:4 LE][type-specific metadata]
    /// For each column, for each row: [null_marker:1] [value (type-specific)]
    /// ```
    fn build_execute_prepared_payload(
        handle: &PreparedStatementHandle,
        parameters: Option<&[Vec<serde_json::Value>]>,
    ) -> Result<Vec<u8>, TransportError> {
        let mut buf = Vec::with_capacity(64);

        buf.extend_from_slice(&handle.handle.to_le_bytes());
        buf.extend_from_slice(&1i32.to_le_bytes()); // num_tables = 1
        buf.push(1u8); // is_table = 1

        let (num_cols, num_rows) = match parameters {
            Some(cols) if !cols.is_empty() => {
                let rows = cols[0].len();
                (cols.len(), rows)
            }
            _ => (0, 0),
        };

        buf.extend_from_slice(&(num_cols as i32).to_le_bytes());
        buf.extend_from_slice(&(num_rows as i64).to_le_bytes()); // total_rows
        buf.extend_from_slice(&(num_rows as i64).to_le_bytes()); // rows_in_msg

        if let Some(cols) = parameters {
            // Write column headers
            for (i, col_values) in cols.iter().enumerate() {
                let (wire_type, col_name) = Self::infer_wire_type(handle, i, col_values);
                let name_bytes = col_name.as_bytes();
                buf.extend_from_slice(&(name_bytes.len() as i32).to_le_bytes());
                buf.extend_from_slice(name_bytes);
                buf.extend_from_slice(&(wire_type as i32).to_le_bytes());

                // Type-specific metadata
                match wire_type {
                    T_CHAR => {
                        buf.push(IS_VARCHAR | IS_UTF8); // vc_flag: is_varchar + utf8
                        buf.extend_from_slice(&2_000_000i32.to_le_bytes()); // max_len
                        buf.extend_from_slice(&(2_000_000i32 * 4).to_le_bytes());
                        // octet_len
                    }
                    T_DECIMAL => {
                        let (prec, scale) = Self::decimal_metadata(handle, i);
                        buf.extend_from_slice(&prec.to_le_bytes());
                        buf.extend_from_slice(&scale.to_le_bytes());
                    }
                    _ => {} // BOOLEAN, DOUBLE, etc. have no extra metadata
                }
            }

            // Write column data (column-major: for each column, for each row)
            for (i, col_values) in cols.iter().enumerate() {
                let (wire_type, _) = Self::infer_wire_type(handle, i, col_values);
                let scale = if wire_type == T_DECIMAL {
                    Self::decimal_metadata(handle, i).1
                } else {
                    0
                };
                for value in col_values {
                    Self::write_param_value(&mut buf, wire_type, value, scale)?;
                }
            }
        }

        Ok(buf)
    }

    /// Determine wire type and column name from handle metadata or by inference.
    fn infer_wire_type(
        handle: &PreparedStatementHandle,
        col_idx: usize,
        col_values: &[serde_json::Value],
    ) -> (u32, String) {
        if col_idx < handle.parameter_types.len() {
            let dt = &handle.parameter_types[col_idx];
            let wire = Self::data_type_to_wire_type(dt);
            let name = handle
                .parameter_names
                .get(col_idx)
                .and_then(|n| n.clone())
                .unwrap_or_else(|| format!("param{}", col_idx));
            (wire, name)
        } else {
            let wire = col_values
                .first()
                .map(Self::json_value_to_wire_type)
                .unwrap_or(T_CHAR);
            (wire, format!("param{}", col_idx))
        }
    }

    /// Get decimal precision and scale from handle metadata.
    fn decimal_metadata(handle: &PreparedStatementHandle, col_idx: usize) -> (i32, i32) {
        if col_idx < handle.parameter_types.len() {
            let dt = &handle.parameter_types[col_idx];
            (dt.precision.unwrap_or(18), dt.scale.unwrap_or(0))
        } else {
            (18, 0)
        }
    }

    /// Map DataType to native wire type ID.
    fn data_type_to_wire_type(dt: &super::messages::DataType) -> u32 {
        match dt.type_name.as_str() {
            "DECIMAL" => T_DECIMAL,
            "DOUBLE" => T_DOUBLE,
            "BOOLEAN" => T_BOOLEAN,
            "VARCHAR" | "CHAR" => T_CHAR,
            "DATE" => T_DATE,
            "TIMESTAMP" => T_TIMESTAMP,
            "TIMESTAMP WITH LOCAL TIME ZONE" => T_TIMESTAMP_UTC,
            "GEOMETRY" => T_GEOMETRY,
            "HASHTYPE" => T_HASHTYPE,
            "INTERVAL YEAR TO MONTH" => T_INTERVAL_YEAR,
            "INTERVAL DAY TO SECOND" => T_INTERVAL_DAY,
            _ => T_CHAR, // fallback
        }
    }

    /// Infer wire type from a JSON value.
    fn json_value_to_wire_type(value: &serde_json::Value) -> u32 {
        match value {
            serde_json::Value::Null => T_CHAR,
            serde_json::Value::Bool(_) => T_BOOLEAN,
            serde_json::Value::Number(n) => {
                if n.is_f64() && !n.is_i64() && !n.is_u64() {
                    T_DOUBLE
                } else {
                    T_DECIMAL
                }
            }
            serde_json::Value::String(_) => T_CHAR,
            _ => T_CHAR,
        }
    }

    /// Write a single parameter value in native binary format.
    ///
    /// `scale` is the Exasol DECIMAL scale; it is only used for `T_DECIMAL`
    /// values, where the wire format expects an already-scaled `i64` integer
    /// (e.g. `1.23` with scale `2` is sent as `123`). It is ignored for all
    /// other types.
    fn write_param_value(
        buf: &mut Vec<u8>,
        wire_type: u32,
        value: &serde_json::Value,
        scale: i32,
    ) -> Result<(), TransportError> {
        if value.is_null() {
            buf.push(0u8); // null marker
            return Ok(());
        }
        buf.push(1u8); // not-null marker

        match wire_type {
            T_BOOLEAN => {
                let b = value.as_bool().unwrap_or(false);
                buf.push(if b { 1u8 } else { 0u8 });
            }
            T_DOUBLE => {
                let d = value.as_f64().unwrap_or(0.0);
                buf.extend_from_slice(&d.to_le_bytes());
            }
            T_DECIMAL => {
                let scaled = Self::scale_decimal_value(value, scale);
                buf.extend_from_slice(&scaled.to_le_bytes());
            }
            T_DATE => {
                // Date as packed integer: (year<<16)+(month<<8)+day
                let s = value.as_str().unwrap_or("2000-01-01");
                let packed = Self::parse_date_to_packed(s);
                buf.extend_from_slice(&packed.to_le_bytes());
            }
            T_TIMESTAMP | T_TIMESTAMP_LOCAL_TZ | T_TIMESTAMP_UTC => {
                // Timestamp: [year:2 LE][month:1][day:1][hour:1][min:1][sec:1][nanos:4 LE]
                let s = value.as_str().unwrap_or("2000-01-01 00:00:00");
                Self::write_timestamp_bytes(buf, s);
            }
            _ => {
                // String-like types (CHAR, VARCHAR, etc.)
                let s = match value {
                    serde_json::Value::String(s) => s.as_str(),
                    _ => {
                        let formatted = value.to_string();
                        let bytes = formatted.as_bytes();
                        buf.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
                        buf.extend_from_slice(bytes);
                        return Ok(());
                    }
                };
                let bytes = s.as_bytes();
                buf.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
        }
        Ok(())
    }

    /// Convert a JSON parameter value into the scaled integer wire
    /// representation required by Exasol for DECIMAL parameters.
    ///
    /// Exasol's native protocol transmits DECIMAL values as already-scaled
    /// integers: a decimal `1.23` with `scale = 2` is sent as `123`. The result
    /// is saturated to `i64::MIN..=i64::MAX` because the current wire encoding
    /// uses 8 bytes for bound DECIMAL parameters.
    fn scale_decimal_value(value: &serde_json::Value, scale: i32) -> i64 {
        let multiplier = 10f64.powi(scale);

        if let Some(i) = value.as_i64() {
            if scale <= 0 {
                if scale == 0 {
                    return i;
                }
                let divisor = 10f64.powi(-scale);
                return ((i as f64) / divisor).round() as i64;
            }
            let pow10 = 10i128.checked_pow(scale as u32);
            if let Some(p) = pow10 {
                let scaled = (i as i128).saturating_mul(p);
                return scaled.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            }
            return ((i as f64) * multiplier).round() as i64;
        }

        if let Some(f) = value.as_f64() {
            let scaled = (f * multiplier).round();
            if scaled.is_nan() {
                return 0;
            }
            return scaled.clamp(i64::MIN as f64, i64::MAX as f64) as i64;
        }

        0
    }

    /// Parse a date string "YYYY-MM-DD" into packed int format.
    fn parse_date_to_packed(s: &str) -> i32 {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() == 3 {
            let year: i32 = parts[0].parse().unwrap_or(2000);
            let month: i32 = parts[1].parse().unwrap_or(1);
            let day: i32 = parts[2].parse().unwrap_or(1);
            (year << 16) + (month << 8) + day
        } else {
            (2000 << 16) + (1 << 8) + 1
        }
    }

    /// Write timestamp bytes: [year:2 LE][month:1][day:1][hour:1][min:1][sec:1][nanos:4 LE]
    fn write_timestamp_bytes(buf: &mut Vec<u8>, s: &str) {
        // Parse "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD HH:MM:SS.ffffff"
        let date_part = &s[..10.min(s.len())];
        let time_part = if s.len() > 11 { &s[11..] } else { "00:00:00" };

        let parts: Vec<&str> = date_part.split('-').collect();
        let year: i16 = parts.first().and_then(|p| p.parse().ok()).unwrap_or(2000);
        let month: u8 = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(1);
        let day: u8 = parts.get(2).and_then(|p| p.parse().ok()).unwrap_or(1);

        let time_parts: Vec<&str> = time_part.split(':').collect();
        let hour: u8 = time_parts.first().and_then(|p| p.parse().ok()).unwrap_or(0);
        let minute: u8 = time_parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
        let sec_str = time_parts.get(2).unwrap_or(&"0");
        let sec_parts: Vec<&str> = sec_str.split('.').collect();
        let second: u8 = sec_parts[0].parse().unwrap_or(0);
        let nanos: i32 = if sec_parts.len() > 1 {
            let frac = sec_parts[1];
            let padded = format!("{:0<9}", frac);
            padded[..9].parse().unwrap_or(0)
        } else {
            0
        };

        buf.extend_from_slice(&year.to_le_bytes());
        buf.push(month);
        buf.push(day);
        buf.push(hour);
        buf.push(minute);
        buf.push(second);
        buf.extend_from_slice(&nanos.to_le_bytes());
    }

    /// Convert a response to QueryResult, caching column metadata for result sets with handles.
    fn convert_and_cache_result(
        &mut self,
        response: NativeResponse,
    ) -> Result<QueryResult, TransportError> {
        if let NativeResponse::ResultSet {
            ref handle,
            ref columns,
            total_rows: _,
            rows_received: _,
            ..
        } = response
        {
            // Only cache column metadata for handles the server has NOT already closed.
            // The server auto-closes a handle when all rows are returned in the initial
            // response; caching metadata for such handles causes spurious CMD_FETCH2 calls.
            if *handle != SMALL_RESULTSET {
                self.result_columns.insert(*handle, columns.clone());
            }
        }
        Self::native_result_to_query_result(response)
    }
}

impl Default for NativeTcpTransport {
    fn default() -> Self {
        Self::new()
    }
}

// --- TLS certificate verifiers (reused from websocket.rs) ---

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

#[async_trait]
impl TransportProtocol for NativeTcpTransport {
    async fn connect(&mut self, params: &ConnectionParams) -> Result<(), TransportError> {
        if self.state != ConnectionState::Disconnected {
            return Err(TransportError::ProtocolError(
                "Already connected".to_string(),
            ));
        }

        let addr = format!("{}:{}", params.host, params.port);

        let tcp_stream = tokio::time::timeout(
            tokio::time::Duration::from_millis(params.timeout_ms),
            TcpStream::connect(&addr),
        )
        .await
        .map_err(|_| {
            TransportError::IoError(format!("Connection timeout after {}ms", params.timeout_ms))
        })?
        .map_err(|e| TransportError::IoError(e.to_string()))?;

        tcp_stream
            .set_nodelay(true)
            .map_err(|e| TransportError::IoError(e.to_string()))?;

        if params.use_tls {
            let tls_config = if let Some(ref fingerprint) = params.certificate_fingerprint {
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(FingerprintVerifier {
                        expected_fingerprint: fingerprint.clone(),
                    }))
                    .with_no_client_auth()
            } else if params.validate_server_certificate {
                let mut root_store = rustls::RootCertStore::empty();
                let certs = rustls_native_certs::load_native_certs();
                for cert in certs.certs {
                    let _ = root_store.add(cert);
                }
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth()
            } else {
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(NoVerifier))
                    .with_no_client_auth()
            };

            let connector = tokio_rustls::TlsConnector::from(Arc::new(tls_config));
            let server_name = rustls::pki_types::ServerName::try_from(params.host.clone())
                .map_err(|e| TransportError::TlsError(format!("Invalid server name: {}", e)))?;

            let tls_stream = connector
                .connect(server_name, tcp_stream)
                .await
                .map_err(|e| TransportError::TlsError(e.to_string()))?;

            self.stream = Some(NativeStream::Tls(Box::new(tls_stream)));
            self.tls_active = true;
        } else {
            self.stream = Some(NativeStream::Plain(tcp_stream));
        }

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

        // Phase 1: Send login packet
        let login_packet = handshake::build_login_packet(&credentials.username);
        self.send_raw(&login_packet).await?;

        // Phase 1 response: Server responds with a standard 21-byte header + attribute payload
        let (login_header, login_payload) = self.receive_message().await?;

        // Parse attributes from the login response payload
        let server_attrs = super::native::attributes::parse_attributes(
            &login_payload,
            login_header.num_attributes,
        )?;

        // Extract public key (binary: [exponent:128 BE][modulus:128 BE])
        let public_key = match server_attrs.get(ATTR_PUBLIC_KEY) {
            Some(AttributeValue::String(s)) => s.as_bytes().to_vec(),
            Some(AttributeValue::Binary(b)) => b.clone(),
            _ => {
                return Err(TransportError::ProtocolError(
                    "Server did not send public key".into(),
                ))
            }
        };

        // Extract random phrase for RSA interleaving
        let random_phrase = match server_attrs.get(ATTR_RANDOM_PHRASE) {
            Some(AttributeValue::Binary(b)) => b.clone(),
            Some(AttributeValue::String(s)) => s.as_bytes().to_vec(),
            _ => {
                return Err(TransportError::ProtocolError(
                    "Server did not send random phrase".into(),
                ))
            }
        };

        // Extract session info from server attributes
        let session_id = match server_attrs.get(ATTR_SESSIONID) {
            Some(AttributeValue::Int64(id)) => id.to_string(),
            Some(AttributeValue::Int32(id)) => id.to_string(),
            _ => "0".to_string(),
        };

        let protocol_version = match server_attrs.get(ATTR_PROTOCOL_VERSION) {
            Some(AttributeValue::Int32(v)) => *v,
            _ => PROTOCOL_VERSION as i32,
        };

        let release_version = match server_attrs.get(ATTR_RELEASE_VERSION) {
            Some(AttributeValue::String(s)) => s.clone(),
            _ => String::new(),
        };

        let database_name = match server_attrs.get(ATTR_DATABASE_NAME) {
            Some(AttributeValue::String(s)) => s.clone(),
            _ => String::new(),
        };

        let product_name = match server_attrs.get(ATTR_PRODUCT_NAME) {
            Some(AttributeValue::String(s)) => s.clone(),
            _ => String::new(),
        };

        let max_data_msg_size = match server_attrs.get(ATTR_DATA_MESSAGE_SIZE) {
            Some(AttributeValue::Int64(v)) => *v,
            _ => MAX_DATA_MESSAGE_SIZE as i64,
        };

        let time_zone = match server_attrs.get(ATTR_TIMEZONE) {
            Some(AttributeValue::String(s)) => Some(s.clone()),
            _ => None,
        };

        // Phase 2: Send password + ChaCha20 keys
        let use_chacha20 = !self.tls_active;
        let auth = handshake::build_auth_message(
            &credentials.password,
            &public_key,
            &random_phrase,
            &self.serial,
            use_chacha20,
        )?;
        self.send_raw(&auth.wire_bytes).await?;

        // Phase 2 response: Read the server's response to CMD_SET_ATTRIBUTES
        let (header, payload) = self.receive_message().await?;

        // Check for exception in the result part (skip attribute data)
        let result_data = Self::extract_result_data(&header, &payload);
        if !result_data.is_empty() {
            let response = result_parser::parse_response(&result_data)?;
            if let NativeResponse::Exception { message, sql_state } = response.terminal {
                return Err(TransportError::ProtocolError(format!(
                    "Authentication failed: {} (SQL state: {})",
                    message, sql_state
                )));
            }
        }

        // Activate ChaCha20 encryption for all subsequent messages (skip over TLS)
        if use_chacha20 && !auth.send_key.is_empty() {
            self.encryptor.set_keys(&auth.send_key, &auth.recv_key);
        }

        // Phase 3: CMD_GET_ATTRIBUTES to retrieve session info
        let empty_attrs = AttributeSet::new();
        self.send_message(CMD_GET_ATTRIBUTES, &empty_attrs, None)
            .await?;
        let (ga_header, ga_payload) = self.receive_message().await?;

        // Parse session attributes from the response
        let ga_result_data = Self::extract_result_data(&ga_header, &ga_payload);
        if !ga_result_data.is_empty() {
            let response = result_parser::parse_response(&ga_result_data)?;
            if let NativeResponse::Exception { message, sql_state } = response.terminal {
                return Err(TransportError::ProtocolError(format!(
                    "GET_ATTRIBUTES failed: {} (SQL state: {})",
                    message, sql_state
                )));
            }
        }

        // The GET_ATTRIBUTES response header has attributes
        let ga_attrs = if ga_header.num_attributes > 0 && ga_header.attribute_data_len > 0 {
            let attr_data =
                &ga_payload[..(ga_header.attribute_data_len as usize).min(ga_payload.len())];
            super::native::attributes::parse_attributes(attr_data, ga_header.num_attributes)?
        } else {
            AttributeSet::new()
        };

        let session_id = match ga_attrs.get(ATTR_SESSIONID) {
            Some(AttributeValue::Int64(id)) => id.to_string(),
            Some(AttributeValue::Int32(id)) => id.to_string(),
            _ => session_id,
        };

        let release_version = match ga_attrs.get(ATTR_RELEASE_VERSION) {
            Some(AttributeValue::String(s)) => s.clone(),
            _ => release_version,
        };

        let database_name = match ga_attrs.get(ATTR_DATABASE_NAME) {
            Some(AttributeValue::String(s)) => s.clone(),
            _ => database_name,
        };

        let product_name = match ga_attrs.get(ATTR_PRODUCT_NAME) {
            Some(AttributeValue::String(s)) => s.clone(),
            _ => product_name,
        };

        let time_zone = match ga_attrs.get(ATTR_TIMEZONE) {
            Some(AttributeValue::String(s)) => Some(s.clone()),
            _ => time_zone,
        };

        let session_info = SessionInfo {
            session_id,
            protocol_version,
            release_version,
            database_name,
            product_name,
            max_data_message_size: max_data_msg_size,
            time_zone,
        };

        self.session = Some(session_info.clone());
        self.state = ConnectionState::Authenticated;

        Ok(session_info)
    }

    async fn execute_query(&mut self, sql: &str) -> Result<QueryResult, TransportError> {
        if self.state != ConnectionState::Authenticated {
            return Err(TransportError::ProtocolError(
                "Must authenticate before executing queries".to_string(),
            ));
        }

        let attrs = AttributeSet::new();
        let header = self
            .send_and_fill_buf(CMD_EXECUTE, &attrs, Some(sql.as_bytes()))
            .await?;
        let response = {
            let result_data = Self::result_data_slice(&header, &self.recv_buf);
            let r = result_parser::parse_response(result_data)?;
            if let NativeResponse::Exception {
                ref message,
                ref sql_state,
            } = r.terminal
            {
                return Err(TransportError::ProtocolError(format!(
                    "{} (SQL state: {})",
                    message, sql_state
                )));
            }
            r.terminal
        };
        self.convert_and_cache_result(response)
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

        let handle_id = handle.as_i32();
        let start_position = *self.fetch_positions.get(&handle_id).unwrap_or(&0);
        let fetch_size_bytes = self
            .session
            .as_ref()
            .map(|s| s.max_data_message_size)
            .unwrap_or(MAX_DATA_MESSAGE_SIZE as i64);

        // CMD_FETCH2 payload: [handle:4 LE] [start_position:8 LE] [fetch_size_bytes:8 LE]
        let mut data = Vec::with_capacity(20);
        data.extend_from_slice(&handle_id.to_le_bytes());
        data.extend_from_slice(&start_position.to_le_bytes());
        data.extend_from_slice(&fetch_size_bytes.to_le_bytes());

        // Get cached columns before the zero-copy borrow of recv_buf
        let cached_columns = self
            .result_columns
            .get(&handle_id)
            .cloned()
            .ok_or_else(|| {
                TransportError::ProtocolError("No cached column metadata for fetch".into())
            })?;

        let attrs = AttributeSet::new();
        let header = self
            .send_and_fill_buf(CMD_FETCH2, &attrs, Some(&data))
            .await?;

        let response = {
            let payload = &self.recv_buf;
            Self::check_response(&header, payload)?.terminal
        };
        match response {
            NativeResponse::MoreRows(data) => {
                let (rows_received, batch) =
                    result_parser::parse_fetch_to_record_batch(&data, &cached_columns)?;
                self.fetch_positions
                    .insert(handle_id, start_position + rows_received);
                let col_infos = Self::to_column_info(&cached_columns);
                Ok(ResultData {
                    columns: col_infos,
                    data: ResultPayload::Arrow(batch),
                    total_rows: rows_received,
                })
            }
            NativeResponse::ResultSet {
                batch,
                rows_received,
                total_rows,
                columns,
                ..
            } => {
                self.fetch_positions
                    .insert(handle_id, start_position + rows_received);
                let col_infos = Self::to_column_info(&columns);
                let record_batch = batch.unwrap_or_else(|| {
                    arrow::record_batch::RecordBatch::new_empty(std::sync::Arc::new(
                        arrow::datatypes::Schema::empty(),
                    ))
                });
                Ok(ResultData {
                    columns: col_infos,
                    data: ResultPayload::Arrow(record_batch),
                    total_rows,
                })
            }
            NativeResponse::Empty => Ok(ResultData {
                columns: vec![],
                data: ResultPayload::Arrow(arrow::record_batch::RecordBatch::new_empty(
                    std::sync::Arc::new(arrow::datatypes::Schema::empty()),
                )),
                total_rows: 0,
            }),
            _ => Err(TransportError::ProtocolError(
                "Expected result set from fetch".into(),
            )),
        }
    }

    async fn close_result_set(&mut self, handle: ResultSetHandle) -> Result<(), TransportError> {
        if self.state != ConnectionState::Authenticated {
            return Err(TransportError::ProtocolError(
                "Must authenticate before closing result sets".to_string(),
            ));
        }

        self.fetch_positions.remove(&handle.as_i32());
        self.result_columns.remove(&handle.as_i32());

        // Send handle as data payload
        let data = handle.as_i32().to_le_bytes();
        let attrs = AttributeSet::new();
        let (header, payload) = self
            .send_and_receive(CMD_CLOSE_RESULTSET, &attrs, Some(&data))
            .await?;

        // Check for exception
        if !payload.is_empty() {
            let _ = Self::check_response(&header, &payload)?;
        }
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

        let attrs = AttributeSet::new();
        let sql_bytes = sql.as_bytes();

        let (header, payload) = self
            .send_and_receive(CMD_CREATE_PREPARED, &attrs, Some(sql_bytes))
            .await?;
        let response = Self::check_response(&header, &payload)?.terminal;

        match response {
            NativeResponse::ResultSet {
                handle: stmt_handle,
                columns,
                total_rows: sub_handle_indicator,
                ..
            } => {
                // sub_handle_indicator carries the sub-result handle from R_HANDLE:
                // PARAMETER_DESCRIPTION (-5) means parameter metadata.
                // Any other value (e.g. SMALL_RESULTSET = -3) means result column metadata.
                if sub_handle_indicator == PARAMETER_DESCRIPTION as i64 {
                    let param_types: Vec<_> = columns
                        .iter()
                        .map(arrow_builder::native_meta_to_data_type)
                        .collect();
                    let param_names: Vec<_> = columns
                        .iter()
                        .map(|c| {
                            if c.name.is_empty() {
                                None
                            } else {
                                Some(c.name.clone())
                            }
                        })
                        .collect();
                    Ok(PreparedStatementHandle::new(
                        stmt_handle,
                        columns.len() as i32,
                        param_types,
                        param_names,
                    ))
                } else {
                    // Result column metadata, not parameters
                    Ok(PreparedStatementHandle::new(stmt_handle, 0, vec![], vec![]))
                }
            }
            NativeResponse::Empty => Ok(PreparedStatementHandle::new(0, 0, vec![], vec![])),
            _ => Err(TransportError::ProtocolError(
                "Unexpected response from CREATE PREPARED".into(),
            )),
        }
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

        let data = Self::build_execute_prepared_payload(handle, parameters.as_deref())?;

        let attrs = AttributeSet::new();
        let (header, payload) = self
            .send_and_receive(CMD_EXECUTE_PREPARED, &attrs, Some(&data))
            .await?;
        let response = Self::check_response(&header, &payload)?.terminal;
        self.convert_and_cache_result(response)
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

        // Send handle as data payload
        let data = handle.handle.to_le_bytes();
        let attrs = AttributeSet::new();
        let (header, payload) = self
            .send_and_receive(CMD_CLOSE_PREPARED, &attrs, Some(&data))
            .await?;

        if !payload.is_empty() {
            let _ = Self::check_response(&header, &payload)?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), TransportError> {
        if self.state == ConnectionState::Disconnected || self.state == ConnectionState::Closed {
            return Ok(());
        }

        if self.state == ConnectionState::Authenticated {
            let empty_attrs = AttributeSet::new();
            let _ = self
                .send_and_receive(CMD_DISCONNECT, &empty_attrs, None)
                .await;
        }

        self.stream = None;
        self.state = ConnectionState::Closed;
        self.session = None;
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

        let mut attrs = AttributeSet::new();
        attrs.add(ATTR_AUTOCOMMIT, AttributeValue::Bool(enabled));

        let (header, payload) = self
            .send_and_receive(CMD_SET_ATTRIBUTES, &attrs, None)
            .await?;

        if !payload.is_empty() {
            let _ = Self::check_response(&header, &payload)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_new_is_disconnected() {
        let transport = NativeTcpTransport::new();
        assert!(!transport.is_connected());
        assert_eq!(transport.state, ConnectionState::Disconnected);
    }

    #[test]
    fn transport_default_is_disconnected() {
        let transport = NativeTcpTransport::default();
        assert!(!transport.is_connected());
    }

    #[tokio::test]
    async fn connect_requires_disconnected_state() {
        let mut transport = NativeTcpTransport::new();
        transport.state = ConnectionState::Connected;

        let params = ConnectionParams::new("localhost".to_string(), 8563);
        let result = transport.connect(&params).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn authenticate_requires_connected_state() {
        let mut transport = NativeTcpTransport::new();
        let creds = Credentials::new("sys".to_string(), "exasol".to_string());
        let result = transport.authenticate(&creds).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_requires_authenticated_state() {
        let mut transport = NativeTcpTransport::new();
        let result = transport.execute_query("SELECT 1").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn close_is_idempotent() {
        let mut transport = NativeTcpTransport::new();
        assert!(transport.close().await.is_ok());
        transport.state = ConnectionState::Closed;
        assert!(transport.close().await.is_ok());
    }

    #[test]
    fn scale_decimal_value_scales_positive_float_by_power_of_ten() {
        let v = serde_json::json!(1.23);
        assert_eq!(NativeTcpTransport::scale_decimal_value(&v, 2), 123);
    }

    #[test]
    fn scale_decimal_value_rounds_half_up_for_floats() {
        let v = serde_json::json!(1.235);
        assert_eq!(NativeTcpTransport::scale_decimal_value(&v, 2), 124);
    }

    #[test]
    fn scale_decimal_value_passes_integer_unchanged_when_scale_zero() {
        let v = serde_json::json!(42i64);
        assert_eq!(NativeTcpTransport::scale_decimal_value(&v, 0), 42);
    }

    #[test]
    fn scale_decimal_value_scales_integer_exactly_without_float_rounding() {
        // 1 with scale=18 should be 10^18 exactly.
        let v = serde_json::json!(1i64);
        assert_eq!(
            NativeTcpTransport::scale_decimal_value(&v, 18),
            1_000_000_000_000_000_000i64
        );
    }

    #[test]
    fn scale_decimal_value_saturates_on_integer_overflow() {
        // 10^19 overflows i64, the helper must clamp instead of panic.
        let v = serde_json::json!(1i64);
        let out = NativeTcpTransport::scale_decimal_value(&v, 19);
        assert_eq!(out, i64::MAX);
    }

    #[test]
    fn scale_decimal_value_handles_negative_integer_values() {
        let v = serde_json::json!(-1i64);
        assert_eq!(NativeTcpTransport::scale_decimal_value(&v, 3), -1000);
    }

    #[test]
    fn scale_decimal_value_returns_zero_for_non_numeric_input() {
        let v = serde_json::json!("oops");
        assert_eq!(NativeTcpTransport::scale_decimal_value(&v, 2), 0);
    }

    #[test]
    fn scale_decimal_value_round_trips_negative_float() {
        let v = serde_json::json!(-1.23);
        assert_eq!(NativeTcpTransport::scale_decimal_value(&v, 2), -123);
    }
}
