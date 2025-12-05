//! WebSocket message types for Exasol protocol.
//!
//! This module defines the JSON message structures used in Exasol's WebSocket API.
//! Messages follow the Exasol WebSocket protocol specification.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Login Protocol Messages (4-step handshake)
// ============================================================================

/// Step 1: Login init request - initiates the login handshake.
///
/// This is the first message sent to start the authentication process.
/// The server responds with a public key for encrypting credentials.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginInitRequest {
    /// Command name (always "login")
    pub command: String,
    /// Protocol version
    pub protocol_version: i32,
}

impl LoginInitRequest {
    /// Create a new login init request.
    pub fn new() -> Self {
        Self {
            command: "login".to_string(),
            protocol_version: 3,
        }
    }
}

impl Default for LoginInitRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Step 2: Public key response - server returns RSA public key.
///
/// The server responds to LoginInitRequest with this message containing
/// the RSA public key to use for encrypting the password.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeyResponse {
    /// Status of the response ("ok" or "error")
    pub status: String,
    /// Response data containing the public key
    pub response_data: Option<PublicKeyData>,
    /// Exception information if failed
    pub exception: Option<ExceptionInfo>,
}

/// Public key data returned by the server.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PublicKeyData {
    /// PEM-encoded RSA public key
    pub public_key_pem: String,
    /// Hex-encoded RSA modulus
    pub public_key_modulus: String,
    /// Hex-encoded RSA exponent
    pub public_key_exponent: String,
}

/// Step 3: Authentication request - client sends encrypted credentials.
///
/// After receiving the public key, the client encrypts the password
/// using RSA and sends this authentication request.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuthRequest {
    /// Username
    pub username: String,
    /// Base64-encoded RSA-encrypted password
    pub password: String,
    /// Whether to use compression
    pub use_compression: bool,
    /// Client name identifier
    pub client_name: String,
    /// Driver name identifier
    pub driver_name: String,
    /// Client version string
    pub client_version: String,
    /// Optional client OS username
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_os_username: Option<String>,
    /// Optional session attributes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, serde_json::Value>>,
}

impl AuthRequest {
    /// Create a new authentication request.
    ///
    /// # Arguments
    /// * `username` - The database username
    /// * `encrypted_password` - The Base64-encoded RSA-encrypted password
    /// * `client_name` - Name of the client application
    pub fn new(username: String, encrypted_password: String, client_name: String) -> Self {
        Self {
            username,
            password: encrypted_password,
            use_compression: false,
            client_name: client_name.clone(),
            driver_name: client_name,
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            client_os_username: None,
            attributes: None,
        }
    }

    /// Set the driver name.
    pub fn with_driver_name(mut self, driver_name: String) -> Self {
        self.driver_name = driver_name;
        self
    }

    /// Set the client version.
    pub fn with_client_version(mut self, version: String) -> Self {
        self.client_version = version;
        self
    }

    /// Set the client OS username.
    pub fn with_os_username(mut self, username: String) -> Self {
        self.client_os_username = Some(username);
        self
    }

    /// Set session attributes.
    pub fn with_attributes(mut self, attributes: HashMap<String, serde_json::Value>) -> Self {
        self.attributes = Some(attributes);
        self
    }
}

// ============================================================================
// Legacy Login Request (kept for reference/compatibility)
// ============================================================================

/// Login request message (legacy single-step - NOT USED for actual authentication).
///
/// Note: Exasol requires a 4-step handshake. Use `LoginInitRequest` and `AuthRequest`
/// for proper authentication.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequest {
    /// Command name
    pub command: String,
    /// Protocol version
    pub protocol_version: i32,
    /// Authentication credentials
    #[serde(flatten)]
    pub credentials: Credentials,
}

/// Authentication credentials.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Credentials {
    /// Username
    pub username: String,
    /// Password
    pub password: String,
}

impl LoginRequest {
    /// Create a new login request.
    pub fn new(username: String, password: String) -> Self {
        Self {
            command: "login".to_string(),
            protocol_version: 3,
            credentials: Credentials { username, password },
        }
    }
}

// ============================================================================
// Step 4: Login Response (final authentication response)
// ============================================================================

/// Login response message (Step 4 of the handshake).
///
/// This is the final response after successful authentication,
/// containing session information.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponse {
    /// Status of the response
    pub status: String,
    /// Response data
    pub response_data: Option<LoginResponseData>,
    /// Exception information if failed
    pub exception: Option<ExceptionInfo>,
}

/// Login response data.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponseData {
    /// Session ID (numeric in Exasol WebSocket API)
    pub session_id: i64,
    /// Protocol version accepted
    pub protocol_version: i32,
    /// Release version
    pub release_version: String,
    /// Database name
    pub database_name: String,
    /// Product name
    pub product_name: String,
    /// Maximum data message size
    pub max_data_message_size: Option<i64>,
    /// Maximum varchar size
    pub max_varchar_size: Option<i64>,
    /// Maximum identifier length
    pub max_identifier_length: Option<i64>,
    /// Identifier quote string
    pub identifier_quote_string: Option<String>,
    /// Time zone
    pub time_zone: Option<String>,
    /// Time zone behavior
    pub time_zone_behavior: Option<String>,
}

// ============================================================================
// Query Execution Messages
// ============================================================================

/// Execute SQL statement request.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteRequest {
    /// Command name
    pub command: String,
    /// SQL text to execute
    pub sql_text: String,
    /// Result set max rows (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_set_max_rows: Option<i64>,
    /// Attributes (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<HashMap<String, String>>,
}

impl ExecuteRequest {
    /// Create a new execute request.
    pub fn new(sql: String) -> Self {
        Self {
            command: "execute".to_string(),
            sql_text: sql,
            result_set_max_rows: None,
            attributes: None,
        }
    }

    /// Set maximum rows for result set.
    pub fn with_max_rows(mut self, max_rows: i64) -> Self {
        self.result_set_max_rows = Some(max_rows);
        self
    }

    /// Add an attribute.
    pub fn with_attribute(mut self, key: String, value: String) -> Self {
        self.attributes
            .get_or_insert_with(HashMap::new)
            .insert(key, value);
        self
    }
}

/// Execute response message.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteResponse {
    /// Status of the response
    pub status: String,
    /// Response data
    pub response_data: Option<ExecuteResponseData>,
    /// Exception information if failed
    pub exception: Option<ExceptionInfo>,
}

/// Execute response data.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteResponseData {
    /// Number of result sets
    pub num_results: i32,
    /// Result sets
    pub results: Vec<ResultSetInfo>,
    /// Attributes
    pub attributes: Option<HashMap<String, String>>,
}

/// Result set information (outer wrapper in results array).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultSetInfo {
    /// Result type: "resultSet" for SELECT, "rowCount" for DML
    pub result_type: String,
    /// Row count (for DML statements like INSERT/UPDATE/DELETE)
    pub row_count: Option<i64>,
    /// Nested result set data (for SELECT statements)
    pub result_set: Option<ResultSetData>,
}

/// Result set data (nested inside ResultSetInfo for SELECT queries).
///
/// **Note**: Data is deserialized from Exasol's column-major format to row-major format.
/// Access data as `data[row_idx][col_idx]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultSetData {
    /// Result set handle for fetching more data
    pub result_set_handle: Option<i32>,
    /// Number of columns
    pub num_columns: Option<i32>,
    /// Total number of rows in result set
    pub num_rows: Option<i64>,
    /// Number of rows in this message chunk
    pub num_rows_in_message: Option<i64>,
    /// Column information
    pub columns: Option<Vec<ColumnInfo>>,
    /// Data in row-major format: data[row_idx][col_idx]
    /// Deserialized and transposed from Exasol's column-major wire format.
    #[serde(default, deserialize_with = "super::deserialize::to_row_major_option")]
    pub data: Option<Vec<Vec<serde_json::Value>>>,
}

/// Column metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
}

/// Exasol data type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataType {
    /// Type name
    #[serde(rename = "type")]
    pub type_name: String,
    /// Precision (for numeric types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<i32>,
    /// Scale (for decimal types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,
    /// Size (for string types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// Character set (for string types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub character_set: Option<String>,
    /// With local time zone (for timestamp types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub with_local_time_zone: Option<bool>,
    /// Fraction (for interval types)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fraction: Option<i32>,
}

impl DataType {
    /// Create a DECIMAL type with specified precision and scale.
    pub fn decimal(precision: i32, scale: i32) -> Self {
        Self {
            type_name: "DECIMAL".to_string(),
            precision: Some(precision),
            scale: Some(scale),
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        }
    }

    /// Create a DOUBLE type.
    pub fn double() -> Self {
        Self {
            type_name: "DOUBLE".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        }
    }

    /// Create a VARCHAR type with specified size.
    pub fn varchar(size: i64) -> Self {
        Self {
            type_name: "VARCHAR".to_string(),
            precision: None,
            scale: None,
            size: Some(size),
            character_set: Some("UTF8".to_string()),
            with_local_time_zone: None,
            fraction: None,
        }
    }

    /// Create a BOOLEAN type.
    pub fn boolean() -> Self {
        Self {
            type_name: "BOOLEAN".to_string(),
            precision: None,
            scale: None,
            size: None,
            character_set: None,
            with_local_time_zone: None,
            fraction: None,
        }
    }

    /// Infer a DataType from a JSON value.
    /// Used when parameter types are not provided by the server.
    pub fn infer_from_json(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::varchar(2_000_000),
            serde_json::Value::Bool(_) => Self::boolean(),
            serde_json::Value::Number(n) => {
                if n.is_f64() {
                    Self::double()
                } else {
                    // Integer - use DECIMAL(18, 0) to handle large integers
                    Self::decimal(18, 0)
                }
            }
            serde_json::Value::String(_) => Self::varchar(2_000_000),
            // Arrays and objects are serialized as strings
            serde_json::Value::Array(_) | serde_json::Value::Object(_) => Self::varchar(2_000_000),
        }
    }
}

// ============================================================================
// Fetch Messages
// ============================================================================

/// Fetch request to retrieve more result data.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchRequest {
    /// Command name
    pub command: String,
    /// Result set handle
    pub result_set_handle: i32,
    /// Starting position (0-based)
    pub start_position: i64,
    /// Number of rows to fetch
    pub num_bytes: i64,
}

impl FetchRequest {
    /// Create a new fetch request.
    pub fn new(result_set_handle: i32, start_position: i64, num_bytes: i64) -> Self {
        Self {
            command: "fetch".to_string(),
            result_set_handle,
            start_position,
            num_bytes,
        }
    }
}

/// Fetch response message.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchResponse {
    /// Status of the response
    pub status: String,
    /// Response data
    pub response_data: Option<FetchResponseData>,
    /// Exception information if failed
    pub exception: Option<ExceptionInfo>,
}

/// Fetch response data.
///
/// **Note**: Data is deserialized from Exasol's column-major format to row-major format.
/// Access data as `data[row_idx][col_idx]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchResponseData {
    /// Number of rows in this message
    pub num_rows: i64,
    /// Data in row-major format: data[row_idx][col_idx]
    /// Deserialized and transposed from Exasol's column-major wire format.
    #[serde(deserialize_with = "super::deserialize::to_row_major")]
    pub data: Vec<Vec<serde_json::Value>>,
}

// ============================================================================
// Result Set Management Messages
// ============================================================================

/// Close result set request.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseResultSetRequest {
    /// Command name
    pub command: String,
    /// Result set handles to close
    pub result_set_handles: Vec<i32>,
}

impl CloseResultSetRequest {
    /// Create a new close result set request.
    pub fn new(handles: Vec<i32>) -> Self {
        Self {
            command: "closeResultSet".to_string(),
            result_set_handles: handles,
        }
    }
}

/// Close result set response.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseResultSetResponse {
    /// Status of the response
    pub status: String,
    /// Exception information if failed
    pub exception: Option<ExceptionInfo>,
}

// ============================================================================
// Prepared Statement Messages
// ============================================================================

/// Request to create a prepared statement.
///
/// Sends SQL to the server for parsing and returns a statement handle
/// along with parameter metadata.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePreparedStatementRequest {
    /// Command name (always "createPreparedStatement")
    pub command: String,
    /// SQL text with parameter placeholders
    pub sql_text: String,
    /// Optional session attributes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
}

impl CreatePreparedStatementRequest {
    /// Create a new prepared statement request.
    ///
    /// # Arguments
    /// * `sql` - SQL text with parameter placeholders (?)
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            command: "createPreparedStatement".to_string(),
            sql_text: sql.into(),
            attributes: None,
        }
    }

    /// Set optional attributes.
    pub fn with_attributes(mut self, attributes: serde_json::Value) -> Self {
        self.attributes = Some(attributes);
        self
    }
}

/// Parameter metadata from prepared statement.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParameterInfo {
    /// Data type of the parameter
    pub data_type: DataType,
}

/// Parameter data in prepared statement response.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ParameterData {
    /// Number of parameter columns
    pub num_columns: i32,
    /// Parameter column metadata
    pub columns: Vec<ParameterInfo>,
}

/// Response from createPreparedStatement.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreatePreparedStatementResponse {
    /// Status of the response ("ok" or "error")
    pub status: String,
    /// Response data containing statement handle and metadata
    pub response_data: Option<PreparedStatementResponseData>,
    /// Exception information if failed
    pub exception: Option<ExceptionInfo>,
}

/// Response data containing statement handle and metadata.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PreparedStatementResponseData {
    /// Server-assigned statement handle
    pub statement_handle: i32,
    /// Parameter metadata (if statement has parameters)
    pub parameter_data: Option<ParameterData>,
    /// Number of results (for SELECT statements)
    pub num_results: Option<i32>,
    /// Result metadata (for SELECT statements)
    pub results: Option<Vec<ResultSetInfo>>,
}

/// Request to execute a prepared statement.
///
/// Executes a previously prepared statement with optional parameter values.
/// Parameters are provided in column-major format.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutePreparedStatementRequest {
    /// Command name (always "executePreparedStatement")
    pub command: String,
    /// Statement handle from createPreparedStatement
    pub statement_handle: i32,
    /// Number of parameter columns
    pub num_columns: i32,
    /// Number of rows of parameters (for batch execution)
    pub num_rows: i32,
    /// Column metadata for parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<Vec<ColumnInfo>>,
    /// Parameter data in column-major format
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<Vec<serde_json::Value>>>,
    /// Optional session attributes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
}

impl ExecutePreparedStatementRequest {
    /// Create a new execute prepared statement request.
    ///
    /// # Arguments
    /// * `statement_handle` - Handle from createPreparedStatement
    pub fn new(statement_handle: i32) -> Self {
        Self {
            command: "executePreparedStatement".to_string(),
            statement_handle,
            num_columns: 0,
            num_rows: 0,
            columns: None,
            data: None,
            attributes: None,
        }
    }

    /// Set parameter data for execution.
    ///
    /// # Arguments
    /// * `columns` - Column metadata describing parameter types
    /// * `data` - Parameter values in column-major format (each inner Vec is one column)
    pub fn with_data(
        mut self,
        columns: Vec<ColumnInfo>,
        data: Vec<Vec<serde_json::Value>>,
    ) -> Self {
        self.num_columns = columns.len() as i32;
        self.num_rows = if data.is_empty() {
            0
        } else {
            data[0].len() as i32
        };
        self.columns = Some(columns);
        self.data = Some(data);
        self
    }

    /// Set optional attributes.
    pub fn with_attributes(mut self, attributes: serde_json::Value) -> Self {
        self.attributes = Some(attributes);
        self
    }
}

/// Request to close a prepared statement.
///
/// Releases server-side resources associated with a prepared statement.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosePreparedStatementRequest {
    /// Command name (always "closePreparedStatement")
    pub command: String,
    /// Statement handle to close
    pub statement_handle: i32,
}

impl ClosePreparedStatementRequest {
    /// Create a new close prepared statement request.
    ///
    /// # Arguments
    /// * `statement_handle` - Handle from createPreparedStatement
    pub fn new(statement_handle: i32) -> Self {
        Self {
            command: "closePreparedStatement".to_string(),
            statement_handle,
        }
    }
}

/// Response from closePreparedStatement.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClosePreparedStatementResponse {
    /// Status of the response ("ok" or "error")
    pub status: String,
    /// Exception information if failed
    pub exception: Option<ExceptionInfo>,
}

// ============================================================================
// Session Management Messages
// ============================================================================

/// Disconnect request.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DisconnectRequest {
    /// Command name
    pub command: String,
}

impl DisconnectRequest {
    /// Create a new disconnect request.
    pub fn new() -> Self {
        Self {
            command: "disconnect".to_string(),
        }
    }
}

impl Default for DisconnectRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Disconnect response.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisconnectResponse {
    /// Status of the response
    pub status: String,
    /// Exception information if failed
    pub exception: Option<ExceptionInfo>,
}

// ============================================================================
// Common Types
// ============================================================================

/// Exception information from Exasol.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExceptionInfo {
    /// SQL code
    pub sql_code: Option<String>,
    /// Error message
    pub text: String,
}

/// Session information returned after successful login.
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// Session ID
    pub session_id: String,
    /// Protocol version
    pub protocol_version: i32,
    /// Database release version
    pub release_version: String,
    /// Database name
    pub database_name: String,
    /// Product name
    pub product_name: String,
    /// Maximum data message size in bytes
    pub max_data_message_size: i64,
    /// Time zone
    pub time_zone: Option<String>,
}

impl From<LoginResponseData> for SessionInfo {
    fn from(data: LoginResponseData) -> Self {
        Self {
            session_id: data.session_id.to_string(),
            protocol_version: data.protocol_version,
            release_version: data.release_version,
            database_name: data.database_name,
            product_name: data.product_name,
            max_data_message_size: data.max_data_message_size.unwrap_or(1024 * 1024), // 1MB default
            time_zone: data.time_zone,
        }
    }
}

/// Result set handle for fetching data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ResultSetHandle(pub i32);

impl ResultSetHandle {
    /// Create a new result set handle.
    pub fn new(handle: i32) -> Self {
        Self(handle)
    }

    /// Get the raw handle value.
    pub fn as_i32(&self) -> i32 {
        self.0
    }
}

impl From<i32> for ResultSetHandle {
    fn from(handle: i32) -> Self {
        Self(handle)
    }
}

/// Result data containing row-major data and metadata.
///
/// **Note**: While Exasol WebSocket API returns data in column-major format,
/// this struct stores data in **row-major format** after streaming deserialization.
/// Access data as `data[row_idx][col_idx]`.
///
/// # Example
///
/// For a query `SELECT id, name FROM users` returning 3 rows:
/// ```rust,ignore
/// // data[row_idx][col_idx]
/// data[0] = [1, "Alice"]     // Row 0
/// data[1] = [2, "Bob"]       // Row 1
/// data[2] = [3, "Carol"]     // Row 2
/// ```
#[derive(Debug, Clone)]
pub struct ResultData {
    /// Column metadata
    pub columns: Vec<ColumnInfo>,
    /// Data in row-major format: data[row_idx][col_idx]
    /// Each inner Vec contains all column values for one row.
    pub data: Vec<Vec<serde_json::Value>>,
    /// Total number of rows
    pub total_rows: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_login_init_request_serialization() {
        let request = LoginInitRequest::new();
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"command\":\"login\""));
        assert!(json.contains("\"protocolVersion\":3"));
        // Should NOT contain username or password
        assert!(!json.contains("username"));
        assert!(!json.contains("password"));
    }

    #[test]
    fn test_public_key_response_deserialization() {
        let json = r#"{
            "status": "ok",
            "responseData": {
                "publicKeyPem": "-----BEGIN RSA PUBLIC KEY-----\nMIIBCg...\n-----END RSA PUBLIC KEY-----",
                "publicKeyModulus": "abc123",
                "publicKeyExponent": "010001"
            }
        }"#;

        let response: PublicKeyResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status, "ok");

        let data = response.response_data.unwrap();
        assert!(data.public_key_pem.contains("BEGIN RSA PUBLIC KEY"));
        assert_eq!(data.public_key_modulus, "abc123");
        assert_eq!(data.public_key_exponent, "010001");
    }

    #[test]
    fn test_auth_request_serialization() {
        let request = AuthRequest::new(
            "sys".to_string(),
            "encrypted_password_base64".to_string(),
            "exarrow-rs".to_string(),
        );
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"username\":\"sys\""));
        assert!(json.contains("\"password\":\"encrypted_password_base64\""));
        assert!(json.contains("\"useCompression\":false"));
        assert!(json.contains("\"clientName\":\"exarrow-rs\""));
        assert!(json.contains("\"driverName\":\"exarrow-rs\""));
        assert!(json.contains("\"clientVersion\":"));
        // Should not contain null attributes
        assert!(!json.contains("\"attributes\":null"));
    }

    #[test]
    fn test_login_request_serialization() {
        let request = LoginRequest::new("sys".to_string(), "exasol".to_string());
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"command\":\"login\""));
        assert!(json.contains("\"username\":\"sys\""));
        assert!(json.contains("\"password\":\"exasol\""));
        assert!(json.contains("\"protocolVersion\":3"));
    }

    #[test]
    fn test_execute_request_serialization() {
        let request = ExecuteRequest::new("SELECT * FROM test".to_string())
            .with_max_rows(1000)
            .with_attribute("autocommit".to_string(), "true".to_string());

        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"command\":\"execute\""));
        assert!(json.contains("SELECT * FROM test"));
        assert!(json.contains("\"resultSetMaxRows\":1000"));
        assert!(json.contains("\"autocommit\""));
    }

    #[test]
    fn test_fetch_request_serialization() {
        let request = FetchRequest::new(1, 0, 1024 * 1024);
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"command\":\"fetch\""));
        assert!(json.contains("\"resultSetHandle\":1"));
        assert!(json.contains("\"startPosition\":0"));
    }

    #[test]
    fn test_disconnect_request_serialization() {
        let request = DisconnectRequest::new();
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"command\":\"disconnect\""));
    }

    #[test]
    fn test_login_response_deserialization() {
        let json = r#"{
            "status": "ok",
            "responseData": {
                "sessionId": 1234567890,
                "protocolVersion": 3,
                "releaseVersion": "8.0.0",
                "databaseName": "MYDB",
                "productName": "Exasol",
                "maxDataMessageSize": 2097152,
                "timeZone": "UTC"
            }
        }"#;

        let response: LoginResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status, "ok");

        let data = response.response_data.unwrap();
        assert_eq!(data.session_id, 1234567890);
        assert_eq!(data.protocol_version, 3);
        assert_eq!(data.release_version, "8.0.0");
        assert_eq!(data.database_name, "MYDB");
    }

    #[test]
    fn test_execute_response_deserialization() {
        // Use the correct nested structure: results[].resultSet.{handle, columns, data}
        // Note: data is in COLUMN-MAJOR format
        let json = r#"{
            "status": "ok",
            "responseData": {
                "numResults": 1,
                "results": [
                    {
                        "resultType": "resultSet",
                        "resultSet": {
                            "resultSetHandle": 1,
                            "numColumns": 2,
                            "numRows": 2,
                            "numRowsInMessage": 2,
                            "columns": [
                                {
                                    "name": "ID",
                                    "dataType": {
                                        "type": "DECIMAL",
                                        "precision": 18,
                                        "scale": 0
                                    }
                                },
                                {
                                    "name": "NAME",
                                    "dataType": {
                                        "type": "VARCHAR",
                                        "size": 100,
                                        "characterSet": "UTF8"
                                    }
                                }
                            ],
                            "data": [
                                [1, 2],
                                ["Alice", "Bob"]
                            ]
                        }
                    }
                ]
            }
        }"#;

        let response: ExecuteResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status, "ok");

        let data = response.response_data.unwrap();
        assert_eq!(data.num_results, 1);
        assert_eq!(data.results.len(), 1);

        let result = &data.results[0];
        assert_eq!(result.result_type, "resultSet");

        let result_set = result.result_set.as_ref().unwrap();
        assert_eq!(result_set.result_set_handle.unwrap(), 1);
        assert_eq!(result_set.num_rows.unwrap(), 2);

        let columns = result_set.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "ID");
        assert_eq!(columns[0].data_type.type_name, "DECIMAL");
        assert_eq!(columns[1].name, "NAME");
        assert_eq!(columns[1].data_type.type_name, "VARCHAR");

        // Column-major: 2 columns, each with 2 values
        let data_cols = result_set.data.as_ref().unwrap();
        assert_eq!(data_cols.len(), 2);
        assert_eq!(data_cols[0].len(), 2); // Column 0 has 2 rows
        assert_eq!(data_cols[1].len(), 2); // Column 1 has 2 rows
    }

    #[test]
    fn test_error_response_deserialization() {
        let json = r#"{
            "status": "error",
            "exception": {
                "sqlCode": "42000",
                "text": "Syntax error at position 10"
            }
        }"#;

        let response: ExecuteResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status, "error");

        let exception = response.exception.unwrap();
        assert_eq!(exception.sql_code.unwrap(), "42000");
        assert!(exception.text.contains("Syntax error"));
    }

    #[test]
    fn test_result_set_handle() {
        let handle = ResultSetHandle::new(42);
        assert_eq!(handle.as_i32(), 42);

        let handle2: ResultSetHandle = 42.into();
        assert_eq!(handle, handle2);
    }

    #[test]
    fn test_session_info_from_login_response() {
        let response_data = LoginResponseData {
            session_id: 1234567890,
            protocol_version: 3,
            release_version: "8.0.0".to_string(),
            database_name: "TEST_DB".to_string(),
            product_name: "Exasol".to_string(),
            max_data_message_size: Some(5242880),
            max_varchar_size: None,
            max_identifier_length: None,
            identifier_quote_string: None,
            time_zone: Some("Europe/Berlin".to_string()),
            time_zone_behavior: None,
        };

        let session_info: SessionInfo = response_data.into();
        assert_eq!(session_info.session_id, "1234567890");
        assert_eq!(session_info.protocol_version, 3);
        assert_eq!(session_info.max_data_message_size, 5242880);
        assert_eq!(session_info.time_zone.unwrap(), "Europe/Berlin");
    }

    // ========================================================================
    // Prepared Statement Tests
    // ========================================================================

    #[test]
    fn test_create_prepared_statement_request_serialization() {
        let request = CreatePreparedStatementRequest::new("SELECT * FROM test WHERE id = ?");
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"command\":\"createPreparedStatement\""));
        assert!(json.contains("\"sqlText\":\"SELECT * FROM test WHERE id = ?\""));
        // Should not contain null attributes
        assert!(!json.contains("\"attributes\":null"));
    }

    #[test]
    fn test_create_prepared_statement_response_deserialization() {
        let json = r#"{
            "status": "ok",
            "responseData": {
                "statementHandle": 42,
                "parameterData": {
                    "numColumns": 1,
                    "columns": [
                        {
                            "dataType": {
                                "type": "DECIMAL",
                                "precision": 18,
                                "scale": 0
                            }
                        }
                    ]
                }
            }
        }"#;

        let response: CreatePreparedStatementResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status, "ok");

        let data = response.response_data.unwrap();
        assert_eq!(data.statement_handle, 42);

        let param_data = data.parameter_data.unwrap();
        assert_eq!(param_data.num_columns, 1);
        assert_eq!(param_data.columns.len(), 1);
        assert_eq!(param_data.columns[0].data_type.type_name, "DECIMAL");
    }

    #[test]
    fn test_execute_prepared_statement_request_serialization() {
        let columns = vec![ColumnInfo {
            name: "ID".to_string(),
            data_type: DataType {
                type_name: "DECIMAL".to_string(),
                precision: Some(18),
                scale: Some(0),
                size: None,
                character_set: None,
                with_local_time_zone: None,
                fraction: None,
            },
        }];
        let data = vec![vec![serde_json::json!(1), serde_json::json!(2)]];

        let request = ExecutePreparedStatementRequest::new(42).with_data(columns, data);
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"command\":\"executePreparedStatement\""));
        assert!(json.contains("\"statementHandle\":42"));
        assert!(json.contains("\"numColumns\":1"));
        assert!(json.contains("\"numRows\":2"));
        assert!(json.contains("\"columns\""));
        assert!(json.contains("\"data\""));
    }

    #[test]
    fn test_execute_prepared_statement_request_no_params() {
        let request = ExecutePreparedStatementRequest::new(42);
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"command\":\"executePreparedStatement\""));
        assert!(json.contains("\"statementHandle\":42"));
        assert!(json.contains("\"numColumns\":0"));
        assert!(json.contains("\"numRows\":0"));
        // Should not contain null columns/data
        assert!(!json.contains("\"columns\":null"));
        assert!(!json.contains("\"data\":null"));
    }

    #[test]
    fn test_close_prepared_statement_request_serialization() {
        let request = ClosePreparedStatementRequest::new(42);
        let json = serde_json::to_string(&request).unwrap();

        assert!(json.contains("\"command\":\"closePreparedStatement\""));
        assert!(json.contains("\"statementHandle\":42"));
    }

    #[test]
    fn test_close_prepared_statement_response_deserialization() {
        let json = r#"{
            "status": "ok"
        }"#;

        let response: ClosePreparedStatementResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status, "ok");
        assert!(response.exception.is_none());
    }

    #[test]
    fn test_close_prepared_statement_error_response() {
        let json = r#"{
            "status": "error",
            "exception": {
                "sqlCode": "00000",
                "text": "Invalid statement handle"
            }
        }"#;

        let response: ClosePreparedStatementResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.status, "error");

        let exception = response.exception.unwrap();
        assert!(exception.text.contains("Invalid statement handle"));
    }

    // ========================================================================
    // DataType Helper Tests
    // ========================================================================

    #[test]
    fn test_data_type_decimal() {
        let dt = DataType::decimal(18, 0);
        assert_eq!(dt.type_name, "DECIMAL");
        assert_eq!(dt.precision, Some(18));
        assert_eq!(dt.scale, Some(0));
        assert!(dt.size.is_none());
    }

    #[test]
    fn test_data_type_double() {
        let dt = DataType::double();
        assert_eq!(dt.type_name, "DOUBLE");
        assert!(dt.precision.is_none());
        assert!(dt.scale.is_none());
    }

    #[test]
    fn test_data_type_varchar() {
        let dt = DataType::varchar(2_000_000);
        assert_eq!(dt.type_name, "VARCHAR");
        assert_eq!(dt.size, Some(2_000_000));
        assert_eq!(dt.character_set, Some("UTF8".to_string()));
    }

    #[test]
    fn test_data_type_boolean() {
        let dt = DataType::boolean();
        assert_eq!(dt.type_name, "BOOLEAN");
    }

    #[test]
    fn test_data_type_infer_from_json_null() {
        let dt = DataType::infer_from_json(&serde_json::Value::Null);
        assert_eq!(dt.type_name, "VARCHAR");
        assert_eq!(dt.size, Some(2_000_000));
    }

    #[test]
    fn test_data_type_infer_from_json_bool() {
        let dt = DataType::infer_from_json(&serde_json::json!(true));
        assert_eq!(dt.type_name, "BOOLEAN");
    }

    #[test]
    fn test_data_type_infer_from_json_integer() {
        let dt = DataType::infer_from_json(&serde_json::json!(42));
        assert_eq!(dt.type_name, "DECIMAL");
        assert_eq!(dt.precision, Some(18));
        assert_eq!(dt.scale, Some(0));
    }

    #[test]
    fn test_data_type_infer_from_json_float() {
        let dt = DataType::infer_from_json(&serde_json::json!(3.125));
        assert_eq!(dt.type_name, "DOUBLE");
    }

    #[test]
    fn test_data_type_infer_from_json_string() {
        let dt = DataType::infer_from_json(&serde_json::json!("hello"));
        assert_eq!(dt.type_name, "VARCHAR");
        assert_eq!(dt.size, Some(2_000_000));
    }

    #[test]
    fn test_data_type_infer_from_json_array() {
        let dt = DataType::infer_from_json(&serde_json::json!([1, 2, 3]));
        assert_eq!(dt.type_name, "VARCHAR");
    }

    #[test]
    fn test_data_type_infer_from_json_object() {
        let dt = DataType::infer_from_json(&serde_json::json!({"key": "value"}));
        assert_eq!(dt.type_name, "VARCHAR");
    }
}
