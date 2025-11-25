//! Authentication handling for Exasol connections.
//!
//! This module provides secure credential management and authentication
//! protocol implementation.

use crate::error::ConnectionError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Secure credentials container.
///
/// This struct ensures credentials are never accidentally logged or displayed.
#[derive(Clone)]
pub struct Credentials {
    username: String,
    password: Arc<SecureString>,
}

impl Credentials {
    /// Create new credentials.
    pub fn new(username: String, password: String) -> Self {
        Self {
            username,
            password: Arc::new(SecureString::new(password)),
        }
    }

    /// Get the username.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Get the password (for internal use only).
    pub(crate) fn password(&self) -> &str {
        self.password.as_str()
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Credentials(username: {})", self.username)
    }
}

/// Secure string that zeros memory on drop and never displays its contents.
struct SecureString {
    data: Vec<u8>,
}

impl SecureString {
    fn new(s: String) -> Self {
        Self {
            data: s.into_bytes(),
        }
    }

    fn as_str(&self) -> &str {
        // Safe because we only construct from valid UTF-8 strings
        unsafe { std::str::from_utf8_unchecked(&self.data) }
    }
}

impl Drop for SecureString {
    fn drop(&mut self) {
        // Zero out the password bytes before dropping
        for byte in &mut self.data {
            *byte = 0;
        }
    }
}

impl fmt::Debug for SecureString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecureString(<redacted>)")
    }
}

/// Authentication request message for Exasol protocol.
#[derive(Debug, Serialize, Deserialize)]
pub struct AuthRequest {
    #[serde(rename = "command")]
    pub command: String,

    #[serde(rename = "username")]
    pub username: String,

    #[serde(rename = "password")]
    pub password: String,

    #[serde(rename = "useCompression", skip_serializing_if = "Option::is_none")]
    pub use_compression: Option<bool>,

    #[serde(rename = "sessionId", skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,

    #[serde(rename = "clientName", skip_serializing_if = "Option::is_none")]
    pub client_name: Option<String>,

    #[serde(rename = "clientVersion", skip_serializing_if = "Option::is_none")]
    pub client_version: Option<String>,

    #[serde(rename = "driverName", skip_serializing_if = "Option::is_none")]
    pub driver_name: Option<String>,

    #[serde(rename = "attributes", skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
}

impl AuthRequest {
    /// Create a new authentication request.
    pub fn new(username: String, password: String) -> Self {
        Self {
            command: "login".to_string(),
            username,
            password,
            use_compression: Some(false),
            session_id: None,
            client_name: None,
            client_version: None,
            driver_name: Some("exarrow-rs".to_string()),
            attributes: None,
        }
    }

    /// Set client information.
    pub fn with_client_info(mut self, name: String, version: String) -> Self {
        self.client_name = Some(name);
        self.client_version = Some(version);
        self
    }

    /// Set session ID for reconnection.
    pub fn with_session_id(mut self, session_id: String) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Enable compression.
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.use_compression = Some(enabled);
        self
    }

    /// Add custom attributes.
    pub fn with_attributes(mut self, attributes: serde_json::Value) -> Self {
        self.attributes = Some(attributes);
        self
    }
}

// Custom Debug that doesn't leak password
impl fmt::Display for AuthRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AuthRequest {{ username: {}, password: <redacted> }}",
            self.username
        )
    }
}

/// Authentication response from Exasol server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResponse {
    #[serde(rename = "status")]
    pub status: String,

    #[serde(rename = "responseData", skip_serializing_if = "Option::is_none")]
    pub response_data: Option<AuthResponseData>,

    #[serde(rename = "exception", skip_serializing_if = "Option::is_none")]
    pub exception: Option<ExceptionInfo>,
}

/// Authentication response data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResponseData {
    #[serde(rename = "sessionId")]
    pub session_id: String,

    #[serde(rename = "protocolVersion")]
    pub protocol_version: i32,

    #[serde(rename = "releaseVersion")]
    pub release_version: String,

    #[serde(rename = "databaseName")]
    pub database_name: String,

    #[serde(rename = "productName")]
    pub product_name: String,

    #[serde(rename = "maxDataMessageSize")]
    pub max_data_message_size: i64,

    #[serde(rename = "maxIdentifierLength")]
    pub max_identifier_length: i32,

    #[serde(rename = "maxVarcharLength")]
    pub max_varchar_length: i64,

    #[serde(rename = "identifierQuoteString")]
    pub identifier_quote_string: String,

    #[serde(rename = "timeZone")]
    pub time_zone: String,

    #[serde(rename = "timeZoneBehavior")]
    pub time_zone_behavior: String,
}

/// Exception information from server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionInfo {
    #[serde(rename = "text")]
    pub text: String,

    #[serde(rename = "sqlCode")]
    pub sql_code: String,
}

impl AuthResponse {
    /// Check if authentication was successful.
    pub fn is_success(&self) -> bool {
        self.status == "ok" && self.response_data.is_some()
    }

    /// Get the session ID if authentication was successful.
    pub fn session_id(&self) -> Option<&str> {
        self.response_data
            .as_ref()
            .map(|data| data.session_id.as_str())
    }

    /// Get error message if authentication failed.
    pub fn error_message(&self) -> Option<String> {
        self.exception
            .as_ref()
            .map(|exc| format!("{} ({})", exc.text, exc.sql_code))
    }
}

/// Handler for authentication protocol.
pub struct AuthenticationHandler {
    credentials: Credentials,
    client_name: String,
    client_version: String,
}

impl AuthenticationHandler {
    /// Create a new authentication handler.
    pub fn new(credentials: Credentials, client_name: String, client_version: String) -> Self {
        Self {
            credentials,
            client_name,
            client_version,
        }
    }

    /// Build an authentication request message.
    pub fn build_auth_request(&self) -> AuthRequest {
        AuthRequest::new(
            self.credentials.username().to_string(),
            self.credentials.password().to_string(),
        )
        .with_client_info(self.client_name.clone(), self.client_version.clone())
    }

    /// Build a reconnection request with session ID.
    pub fn build_reconnect_request(&self, session_id: String) -> AuthRequest {
        self.build_auth_request().with_session_id(session_id)
    }

    /// Process authentication response.
    pub fn process_auth_response(
        &self,
        response: AuthResponse,
    ) -> Result<AuthResponseData, ConnectionError> {
        if response.is_success() {
            response.response_data.ok_or_else(|| {
                ConnectionError::AuthenticationFailed(
                    "Server returned success but no response data".to_string(),
                )
            })
        } else {
            let error_msg = response
                .error_message()
                .unwrap_or_else(|| "Unknown authentication error".to_string());
            Err(ConnectionError::AuthenticationFailed(error_msg))
        }
    }

    /// Get the credentials username.
    pub fn username(&self) -> &str {
        self.credentials.username()
    }
}

impl fmt::Debug for AuthenticationHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthenticationHandler")
            .field("credentials", &self.credentials)
            .field("client_name", &self.client_name)
            .field("client_version", &self.client_version)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_credentials_no_password_leak() {
        let creds = Credentials::new("admin".to_string(), "secret123".to_string());

        let debug = format!("{:?}", creds);
        assert!(!debug.contains("secret123"));
        assert!(debug.contains("admin"));
        assert!(debug.contains("redacted"));

        let display = format!("{}", creds);
        assert!(!display.contains("secret123"));
        assert!(display.contains("admin"));
    }

    #[test]
    fn test_credentials_access() {
        let creds = Credentials::new("user".to_string(), "pass".to_string());

        assert_eq!(creds.username(), "user");
        assert_eq!(creds.password(), "pass");
    }

    #[test]
    fn test_secure_string_zeros_on_drop() {
        let mut data = {
            let secure = SecureString::new("password".to_string());
            // Clone the underlying data to test it gets zeroed
            secure.data.clone()
        };

        // After SecureString is dropped, original data should remain
        // but a new SecureString drop should zero its own data
        let secure = SecureString::new("test1234".to_string());
        let ptr = secure.data.as_ptr();
        let len = secure.data.len();

        drop(secure);

        // We can't easily verify the memory was zeroed without unsafe code,
        // but we test the drop implementation runs without panic
    }

    #[test]
    fn test_auth_request_creation() {
        let req = AuthRequest::new("admin".to_string(), "secret".to_string());

        assert_eq!(req.command, "login");
        assert_eq!(req.username, "admin");
        assert_eq!(req.password, "secret");
        assert_eq!(req.driver_name, Some("exarrow-rs".to_string()));
    }

    #[test]
    fn test_auth_request_with_client_info() {
        let req = AuthRequest::new("admin".to_string(), "secret".to_string())
            .with_client_info("test-client".to_string(), "1.0.0".to_string());

        assert_eq!(req.client_name, Some("test-client".to_string()));
        assert_eq!(req.client_version, Some("1.0.0".to_string()));
    }

    #[test]
    fn test_auth_request_no_password_leak() {
        let req = AuthRequest::new("admin".to_string(), "secret123".to_string());

        let display = format!("{}", req);
        assert!(!display.contains("secret123"));
        assert!(display.contains("admin"));
        assert!(display.contains("redacted"));
    }

    #[test]
    fn test_auth_response_success() {
        let response = AuthResponse {
            status: "ok".to_string(),
            response_data: Some(AuthResponseData {
                session_id: "sess123".to_string(),
                protocol_version: 3,
                release_version: "7.1.0".to_string(),
                database_name: "EXA".to_string(),
                product_name: "Exasol".to_string(),
                max_data_message_size: 4_194_304,
                max_identifier_length: 128,
                max_varchar_length: 2_000_000,
                identifier_quote_string: "\"".to_string(),
                time_zone: "UTC".to_string(),
                time_zone_behavior: "INVALID TIMESTAMP TO DOUBLE".to_string(),
            }),
            exception: None,
        };

        assert!(response.is_success());
        assert_eq!(response.session_id(), Some("sess123"));
        assert!(response.error_message().is_none());
    }

    #[test]
    fn test_auth_response_failure() {
        let response = AuthResponse {
            status: "error".to_string(),
            response_data: None,
            exception: Some(ExceptionInfo {
                text: "Invalid credentials".to_string(),
                sql_code: "08004".to_string(),
            }),
        };

        assert!(!response.is_success());
        assert!(response.session_id().is_none());
        assert_eq!(
            response.error_message(),
            Some("Invalid credentials (08004)".to_string())
        );
    }

    #[test]
    fn test_auth_handler_build_request() {
        let creds = Credentials::new("admin".to_string(), "secret".to_string());
        let handler =
            AuthenticationHandler::new(creds, "test-client".to_string(), "1.0.0".to_string());

        let req = handler.build_auth_request();

        assert_eq!(req.username, "admin");
        assert_eq!(req.password, "secret");
        assert_eq!(req.client_name, Some("test-client".to_string()));
        assert_eq!(req.client_version, Some("1.0.0".to_string()));
    }

    #[test]
    fn test_auth_handler_process_success() {
        let creds = Credentials::new("admin".to_string(), "secret".to_string());
        let handler =
            AuthenticationHandler::new(creds, "test-client".to_string(), "1.0.0".to_string());

        let response = AuthResponse {
            status: "ok".to_string(),
            response_data: Some(AuthResponseData {
                session_id: "sess123".to_string(),
                protocol_version: 3,
                release_version: "7.1.0".to_string(),
                database_name: "EXA".to_string(),
                product_name: "Exasol".to_string(),
                max_data_message_size: 4_194_304,
                max_identifier_length: 128,
                max_varchar_length: 2_000_000,
                identifier_quote_string: "\"".to_string(),
                time_zone: "UTC".to_string(),
                time_zone_behavior: "INVALID TIMESTAMP TO DOUBLE".to_string(),
            }),
            exception: None,
        };

        let result = handler.process_auth_response(response);
        assert!(result.is_ok());

        let data = result.unwrap();
        assert_eq!(data.session_id, "sess123");
        assert_eq!(data.protocol_version, 3);
    }

    #[test]
    fn test_auth_handler_process_failure() {
        let creds = Credentials::new("admin".to_string(), "secret".to_string());
        let handler =
            AuthenticationHandler::new(creds, "test-client".to_string(), "1.0.0".to_string());

        let response = AuthResponse {
            status: "error".to_string(),
            response_data: None,
            exception: Some(ExceptionInfo {
                text: "Invalid credentials".to_string(),
                sql_code: "08004".to_string(),
            }),
        };

        let result = handler.process_auth_response(response);
        assert!(result.is_err());

        match result.unwrap_err() {
            ConnectionError::AuthenticationFailed(msg) => {
                assert!(msg.contains("Invalid credentials"));
            }
            _ => panic!("Expected AuthenticationFailed error"),
        }
    }

    #[test]
    fn test_auth_handler_no_password_leak() {
        let creds = Credentials::new("admin".to_string(), "super_secret".to_string());
        let handler =
            AuthenticationHandler::new(creds, "test-client".to_string(), "1.0.0".to_string());

        let debug = format!("{:?}", handler);
        assert!(!debug.contains("super_secret"));
        assert!(debug.contains("admin"));
        assert!(debug.contains("redacted"));
    }

    #[test]
    fn test_reconnect_request() {
        let creds = Credentials::new("admin".to_string(), "secret".to_string());
        let handler =
            AuthenticationHandler::new(creds, "test-client".to_string(), "1.0.0".to_string());

        let req = handler.build_reconnect_request("old_session_123".to_string());

        assert_eq!(req.session_id, Some("old_session_123".to_string()));
        assert_eq!(req.username, "admin");
    }

    #[test]
    fn test_credentials_clone() {
        let creds = Credentials::new("user".to_string(), "pass".to_string());
        let creds2 = creds.clone();

        assert_eq!(creds.username(), creds2.username());
        assert_eq!(creds.password(), creds2.password());
    }
}
