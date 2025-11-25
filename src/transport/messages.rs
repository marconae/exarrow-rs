//! WebSocket message types for Exasol protocol.
//!
//! This module defines the JSON message structures used in Exasol's WebSocket API.
//! Messages follow the Exasol WebSocket protocol specification.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Login request message.
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

/// Login response message.
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
    /// Session ID
    pub session_id: String,
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
    pub identifier_quote_string: Option<String>,
    /// Time zone
    pub time_zone: Option<String>,
    /// Time zone behavior
    pub time_zone_behavior: Option<String>,
}

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

/// Result set information.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultSetInfo {
    /// Result type
    pub result_type: String,
    /// Row count (for non-SELECT statements)
    pub row_count: Option<i64>,
    /// Result set handle (for SELECT statements)
    pub result_set_handle: Option<i32>,
    /// Column information
    pub columns: Option<Vec<ColumnInfo>>,
    /// Number of rows in result set
    pub num_rows: Option<i64>,
    /// Number of rows in first chunk
    pub num_rows_in_message: Option<i64>,
    /// Data (if included in response)
    pub data: Option<Vec<Vec<serde_json::Value>>>,
}

/// Column metadata.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnInfo {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
}

/// Exasol data type.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataType {
    /// Type name
    #[serde(rename = "type")]
    pub type_name: String,
    /// Precision (for numeric types)
    pub precision: Option<i32>,
    /// Scale (for decimal types)
    pub scale: Option<i32>,
    /// Size (for string types)
    pub size: Option<i64>,
    /// Character set (for string types)
    pub character_set: Option<String>,
    /// With local time zone (for timestamp types)
    pub with_local_time_zone: Option<bool>,
    /// Fraction (for interval types)
    pub fraction: Option<i32>,
}

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
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FetchResponseData {
    /// Number of rows in this message
    pub num_rows: i64,
    /// Data rows
    pub data: Vec<Vec<serde_json::Value>>,
}

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
            session_id: data.session_id,
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

/// Result data containing rows and metadata.
#[derive(Debug, Clone)]
pub struct ResultData {
    /// Column metadata
    pub columns: Vec<ColumnInfo>,
    /// Data rows
    pub rows: Vec<Vec<serde_json::Value>>,
    /// Total number of rows
    pub total_rows: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

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
                "sessionId": "1234567890",
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
        assert_eq!(data.session_id, "1234567890");
        assert_eq!(data.protocol_version, 3);
        assert_eq!(data.release_version, "8.0.0");
        assert_eq!(data.database_name, "MYDB");
    }

    #[test]
    fn test_execute_response_deserialization() {
        let json = r#"{
            "status": "ok",
            "responseData": {
                "numResults": 1,
                "results": [
                    {
                        "resultType": "resultSet",
                        "resultSetHandle": 1,
                        "numRows": 100,
                        "numRowsInMessage": 10,
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
                            [1, "Alice"],
                            [2, "Bob"]
                        ]
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
        assert_eq!(result.result_set_handle.unwrap(), 1);
        assert_eq!(result.num_rows.unwrap(), 100);

        let columns = result.columns.as_ref().unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "ID");
        assert_eq!(columns[0].data_type.type_name, "DECIMAL");
        assert_eq!(columns[1].name, "NAME");
        assert_eq!(columns[1].data_type.type_name, "VARCHAR");

        let data_rows = result.data.as_ref().unwrap();
        assert_eq!(data_rows.len(), 2);
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
            session_id: "test-session".to_string(),
            protocol_version: 3,
            release_version: "8.0.0".to_string(),
            database_name: "TEST_DB".to_string(),
            product_name: "Exasol".to_string(),
            max_data_message_size: Some(5242880),
            max_varchar_size: None,
            identifier_quote_string: None,
            time_zone: Some("Europe/Berlin".to_string()),
            time_zone_behavior: None,
        };

        let session_info: SessionInfo = response_data.into();
        assert_eq!(session_info.session_id, "test-session");
        assert_eq!(session_info.protocol_version, 3);
        assert_eq!(session_info.max_data_message_size, 5242880);
        assert_eq!(session_info.time_zone.unwrap(), "Europe/Berlin");
    }
}
