//! Connection parameter parsing and validation.
//!
//! This module handles parsing connection strings and building connection
//! parameters with validation.

use crate::error::ConnectionError;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::Duration;

/// Connection parameters for establishing a database connection.
#[derive(Clone)]
pub struct ConnectionParams {
    /// Database host address
    pub host: String,

    /// Database port (default: 8563)
    pub port: u16,

    /// Username for authentication
    pub username: String,

    /// Password for authentication (stored securely)
    password: String,

    /// Optional schema to use after connection
    pub schema: Option<String>,

    /// Connection timeout
    pub connection_timeout: Duration,

    /// Query execution timeout
    pub query_timeout: Duration,

    /// Idle connection timeout
    pub idle_timeout: Duration,

    /// Enable TLS/SSL encryption
    pub use_tls: bool,

    /// TLS certificate validation mode
    pub validate_server_certificate: bool,

    /// Client name for session identification
    pub client_name: String,

    /// Client version
    pub client_version: String,

    /// Additional connection attributes
    pub attributes: HashMap<String, String>,
}

impl ConnectionParams {
    /// Get the password (for internal use only, never logged).
    pub(crate) fn password(&self) -> &str {
        &self.password
    }

    /// Create a new ConnectionBuilder.
    pub fn builder() -> ConnectionBuilder {
        ConnectionBuilder::new()
    }
}

impl FromStr for ConnectionParams {
    type Err = ConnectionError;

    /// Parse a connection string in the format:
    /// `exasol://[username[:password]@]host[:port][/schema][?param=value&...]`
    ///
    /// # Examples
    ///
    /// ```
    /// # use exarrow_rs::connection::ConnectionParams;
    /// # use std::str::FromStr;
    /// // Basic connection
    /// let params = ConnectionParams::from_str("exasol://localhost:8563")?;
    ///
    /// // With authentication
    /// let params = ConnectionParams::from_str("exasol://user:pass@localhost:8563")?;
    ///
    /// // With schema and parameters
    /// let params = ConnectionParams::from_str(
    ///     "exasol://user:pass@localhost:8563/MY_SCHEMA?timeout=10&tls=true"
    /// )?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Parse the connection string
        let url = s.trim();

        // Check for exasol:// prefix
        if !url.starts_with("exasol://") {
            return Err(ConnectionError::ParseError(
                "Connection string must start with 'exasol://'".to_string(),
            ));
        }

        let url = &url[9..]; // Skip "exasol://"

        // Split into main part and query string
        let (main_part, query_string) = match url.split_once('?') {
            Some((main, query)) => (main, Some(query)),
            None => (url, None),
        };

        // Parse query parameters
        let mut params = parse_query_params(query_string)?;

        // Split main part into auth@host/schema
        let (auth_part, host_part) = match main_part.rfind('@') {
            Some(pos) => {
                let auth = &main_part[..pos];
                let host = &main_part[pos + 1..];
                (Some(auth), host)
            }
            None => (None, main_part),
        };

        // Parse authentication
        let (username, password) = if let Some(auth) = auth_part {
            parse_auth(auth)?
        } else {
            // Check query params for username/password
            let username = params
                .remove("user")
                .or_else(|| params.remove("username"))
                .ok_or_else(|| ConnectionError::ParseError("Username is required".to_string()))?;
            let password = params
                .remove("password")
                .or_else(|| params.remove("pass"))
                .unwrap_or_default();
            (username, password)
        };

        // Parse host and schema
        let (host_port, schema) = match host_part.split_once('/') {
            Some((host, schema)) => {
                let schema = if schema.is_empty() {
                    None
                } else {
                    Some(schema.to_string())
                };
                (host, schema)
            }
            None => (host_part, None),
        };

        // Parse host and port
        let (host, port) = parse_host_port(host_port)?;

        // Build connection params
        let mut builder = ConnectionBuilder::new()
            .host(&host)
            .port(port)
            .username(&username)
            .password(&password);

        if let Some(schema) = schema {
            builder = builder.schema(&schema);
        }

        // Apply query parameters
        builder = apply_query_params(builder, params)?;

        builder.build()
    }
}

// Prevent password from being displayed in debug or display output
impl fmt::Debug for ConnectionParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionParams")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("schema", &self.schema)
            .field("connection_timeout", &self.connection_timeout)
            .field("query_timeout", &self.query_timeout)
            .field("idle_timeout", &self.idle_timeout)
            .field("use_tls", &self.use_tls)
            .field(
                "validate_server_certificate",
                &self.validate_server_certificate,
            )
            .field("client_name", &self.client_name)
            .field("client_version", &self.client_version)
            .field("attributes", &self.attributes)
            .finish()
    }
}

impl fmt::Display for ConnectionParams {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ConnectionParams {{ host: {}, port: {}, username: {}, schema: {:?}, use_tls: {} }}",
            self.host, self.port, self.username, self.schema, self.use_tls
        )
    }
}

/// Builder for constructing ConnectionParams with validation.
#[derive(Debug, Clone)]
pub struct ConnectionBuilder {
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
    schema: Option<String>,
    connection_timeout: Option<Duration>,
    query_timeout: Option<Duration>,
    idle_timeout: Option<Duration>,
    use_tls: Option<bool>,
    validate_server_certificate: Option<bool>,
    client_name: Option<String>,
    client_version: Option<String>,
    attributes: HashMap<String, String>,
}

impl ConnectionBuilder {
    /// Create a new ConnectionBuilder with default values.
    pub fn new() -> Self {
        Self {
            host: None,
            port: None,
            username: None,
            password: None,
            schema: None,
            connection_timeout: None,
            query_timeout: None,
            idle_timeout: None,
            use_tls: None,
            validate_server_certificate: None,
            client_name: None,
            client_version: None,
            attributes: HashMap::new(),
        }
    }

    /// Set the database host.
    pub fn host(mut self, host: &str) -> Self {
        self.host = Some(host.to_string());
        self
    }

    /// Set the database port.
    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    /// Set the username.
    pub fn username(mut self, username: &str) -> Self {
        self.username = Some(username.to_string());
        self
    }

    /// Set the password.
    pub fn password(mut self, password: &str) -> Self {
        self.password = Some(password.to_string());
        self
    }

    /// Set the default schema.
    pub fn schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.to_string());
        self
    }

    /// Set the connection timeout.
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }

    /// Set the query execution timeout.
    pub fn query_timeout(mut self, timeout: Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }

    /// Set the idle connection timeout.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    /// Enable or disable TLS/SSL.
    pub fn use_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = Some(use_tls);
        self
    }

    /// Enable or disable server certificate validation.
    pub fn validate_server_certificate(mut self, validate: bool) -> Self {
        self.validate_server_certificate = Some(validate);
        self
    }

    /// Set the client name.
    pub fn client_name(mut self, name: &str) -> Self {
        self.client_name = Some(name.to_string());
        self
    }

    /// Set the client version.
    pub fn client_version(mut self, version: &str) -> Self {
        self.client_version = Some(version.to_string());
        self
    }

    /// Add a custom connection attribute.
    pub fn attribute(mut self, key: &str, value: &str) -> Self {
        self.attributes.insert(key.to_string(), value.to_string());
        self
    }

    /// Build the ConnectionParams with validation.
    pub fn build(self) -> Result<ConnectionParams, ConnectionError> {
        // Validate required fields
        let host = self.host.ok_or_else(|| ConnectionError::InvalidParameter {
            parameter: "host".to_string(),
            message: "Host is required".to_string(),
        })?;

        let username = self
            .username
            .ok_or_else(|| ConnectionError::InvalidParameter {
                parameter: "username".to_string(),
                message: "Username is required".to_string(),
            })?;

        // Validate host is not empty
        if host.is_empty() {
            return Err(ConnectionError::InvalidParameter {
                parameter: "host".to_string(),
                message: "Host cannot be empty".to_string(),
            });
        }

        // Validate username is not empty
        if username.is_empty() {
            return Err(ConnectionError::InvalidParameter {
                parameter: "username".to_string(),
                message: "Username cannot be empty".to_string(),
            });
        }

        let port = self.port.unwrap_or(8563);

        // Validate port range
        if port == 0 {
            return Err(ConnectionError::InvalidParameter {
                parameter: "port".to_string(),
                message: "Port must be greater than 0".to_string(),
            });
        }

        // Validate timeouts
        let connection_timeout = self.connection_timeout.unwrap_or(Duration::from_secs(30));
        let query_timeout = self.query_timeout.unwrap_or(Duration::from_secs(300));
        let idle_timeout = self.idle_timeout.unwrap_or(Duration::from_secs(600));

        if connection_timeout.as_secs() > 300 {
            return Err(ConnectionError::InvalidParameter {
                parameter: "connection_timeout".to_string(),
                message: "Connection timeout cannot exceed 300 seconds".to_string(),
            });
        }

        Ok(ConnectionParams {
            host,
            port,
            username,
            password: self.password.unwrap_or_default(),
            schema: self.schema,
            connection_timeout,
            query_timeout,
            idle_timeout,
            use_tls: self.use_tls.unwrap_or(false),
            validate_server_certificate: self.validate_server_certificate.unwrap_or(true),
            client_name: self.client_name.unwrap_or_else(|| "exarrow-rs".to_string()),
            client_version: self
                .client_version
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string()),
            attributes: self.attributes,
        })
    }
}

impl Default for ConnectionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse query parameters from URL query string.
fn parse_query_params(query: Option<&str>) -> Result<HashMap<String, String>, ConnectionError> {
    let mut params = HashMap::new();

    if let Some(query) = query {
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }

            let (key, value) = match pair.split_once('=') {
                Some((k, v)) => (k, v),
                None => {
                    return Err(ConnectionError::ParseError(format!(
                        "Invalid query parameter format: {}",
                        pair
                    )));
                }
            };

            // URL decode the values
            let key = urlencoding::decode(key)
                .map_err(|e| ConnectionError::ParseError(format!("Failed to decode key: {}", e)))?
                .into_owned();
            let value = urlencoding::decode(value)
                .map_err(|e| ConnectionError::ParseError(format!("Failed to decode value: {}", e)))?
                .into_owned();

            params.insert(key, value);
        }
    }

    Ok(params)
}

/// Parse authentication part (username:password).
fn parse_auth(auth: &str) -> Result<(String, String), ConnectionError> {
    match auth.split_once(':') {
        Some((user, pass)) => {
            let user = urlencoding::decode(user)
                .map_err(|e| {
                    ConnectionError::ParseError(format!("Failed to decode username: {}", e))
                })?
                .into_owned();
            let pass = urlencoding::decode(pass)
                .map_err(|e| {
                    ConnectionError::ParseError(format!("Failed to decode password: {}", e))
                })?
                .into_owned();
            Ok((user, pass))
        }
        None => {
            let user = urlencoding::decode(auth)
                .map_err(|e| {
                    ConnectionError::ParseError(format!("Failed to decode username: {}", e))
                })?
                .into_owned();
            Ok((user, String::new()))
        }
    }
}

/// Parse host and port.
fn parse_host_port(host_port: &str) -> Result<(String, u16), ConnectionError> {
    // Check for IPv6 address format [host]:port
    if host_port.starts_with('[') {
        if let Some(close_bracket) = host_port.find(']') {
            let host = host_port[1..close_bracket].to_string();
            let port_part = &host_port[close_bracket + 1..];

            let port = if let Some(stripped) = port_part.strip_prefix(':') {
                stripped.parse().map_err(|_| {
                    ConnectionError::ParseError(format!("Invalid port: {}", port_part))
                })?
            } else {
                8563
            };

            return Ok((host, port));
        }
    }

    // Regular host:port or just host
    match host_port.rsplit_once(':') {
        Some((host, port_str)) => {
            let port = port_str
                .parse()
                .map_err(|_| ConnectionError::ParseError(format!("Invalid port: {}", port_str)))?;
            Ok((host.to_string(), port))
        }
        None => Ok((host_port.to_string(), 8563)),
    }
}

/// Apply query parameters to builder.
fn apply_query_params(
    mut builder: ConnectionBuilder,
    params: HashMap<String, String>,
) -> Result<ConnectionBuilder, ConnectionError> {
    for (key, value) in params {
        match key.as_str() {
            "timeout" | "connection_timeout" => {
                let secs: u64 = value
                    .parse()
                    .map_err(|_| ConnectionError::InvalidParameter {
                        parameter: key.clone(),
                        message: format!("Invalid timeout value: {}", value),
                    })?;
                builder = builder.connection_timeout(Duration::from_secs(secs));
            }
            "query_timeout" => {
                let secs: u64 = value
                    .parse()
                    .map_err(|_| ConnectionError::InvalidParameter {
                        parameter: key.clone(),
                        message: format!("Invalid timeout value: {}", value),
                    })?;
                builder = builder.query_timeout(Duration::from_secs(secs));
            }
            "idle_timeout" => {
                let secs: u64 = value
                    .parse()
                    .map_err(|_| ConnectionError::InvalidParameter {
                        parameter: key.clone(),
                        message: format!("Invalid timeout value: {}", value),
                    })?;
                builder = builder.idle_timeout(Duration::from_secs(secs));
            }
            "tls" | "use_tls" | "ssl" => {
                let use_tls = parse_bool(&value)?;
                builder = builder.use_tls(use_tls);
            }
            "validate_certificate" | "verify_certificate" => {
                let validate = parse_bool(&value)?;
                builder = builder.validate_server_certificate(validate);
            }
            "client_name" => {
                builder = builder.client_name(&value);
            }
            "client_version" => {
                builder = builder.client_version(&value);
            }
            _ => {
                // Store as custom attribute
                builder = builder.attribute(&key, &value);
            }
        }
    }

    Ok(builder)
}

/// Parse boolean value from string.
fn parse_bool(s: &str) -> Result<bool, ConnectionError> {
    match s.to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(ConnectionError::InvalidParameter {
            parameter: "boolean".to_string(),
            message: format!("Invalid boolean value: {}", s),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_minimal() {
        let params = ConnectionBuilder::new()
            .host("localhost")
            .username("test")
            .build()
            .unwrap();

        assert_eq!(params.host, "localhost");
        assert_eq!(params.port, 8563);
        assert_eq!(params.username, "test");
        assert_eq!(params.password(), "");
    }

    #[test]
    fn test_builder_full() {
        let params = ConnectionBuilder::new()
            .host("db.example.com")
            .port(9000)
            .username("admin")
            .password("secret")
            .schema("MY_SCHEMA")
            .connection_timeout(Duration::from_secs(20))
            .query_timeout(Duration::from_secs(60))
            .use_tls(true)
            .client_name("test-client")
            .attribute("custom", "value")
            .build()
            .unwrap();

        assert_eq!(params.host, "db.example.com");
        assert_eq!(params.port, 9000);
        assert_eq!(params.username, "admin");
        assert_eq!(params.password(), "secret");
        assert_eq!(params.schema, Some("MY_SCHEMA".to_string()));
        assert_eq!(params.connection_timeout, Duration::from_secs(20));
        assert_eq!(params.query_timeout, Duration::from_secs(60));
        assert!(params.use_tls);
        assert_eq!(params.client_name, "test-client");
        assert_eq!(params.attributes.get("custom"), Some(&"value".to_string()));
    }

    #[test]
    fn test_builder_validation_missing_host() {
        let result = ConnectionBuilder::new().username("test").build();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ConnectionError::InvalidParameter { parameter, .. } if parameter == "host"
        ));
    }

    #[test]
    fn test_builder_validation_empty_host() {
        let result = ConnectionBuilder::new().host("").username("test").build();

        assert!(result.is_err());
    }

    #[test]
    fn test_builder_validation_timeout() {
        let result = ConnectionBuilder::new()
            .host("localhost")
            .username("test")
            .connection_timeout(Duration::from_secs(400))
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_basic() {
        let params = ConnectionParams::from_str("exasol://user@localhost").unwrap();

        assert_eq!(params.host, "localhost");
        assert_eq!(params.port, 8563);
        assert_eq!(params.username, "user");
    }

    #[test]
    fn test_parse_with_port() {
        let params = ConnectionParams::from_str("exasol://user@localhost:9000").unwrap();

        assert_eq!(params.host, "localhost");
        assert_eq!(params.port, 9000);
    }

    #[test]
    fn test_parse_with_password() {
        let params = ConnectionParams::from_str("exasol://user:pass@localhost").unwrap();

        assert_eq!(params.username, "user");
        assert_eq!(params.password(), "pass");
    }

    #[test]
    fn test_parse_with_schema() {
        let params = ConnectionParams::from_str("exasol://user@localhost/MY_SCHEMA").unwrap();

        assert_eq!(params.schema, Some("MY_SCHEMA".to_string()));
    }

    #[test]
    fn test_parse_with_query_params() {
        let params = ConnectionParams::from_str(
            "exasol://user@localhost?timeout=20&tls=true&client_name=test",
        )
        .unwrap();

        assert_eq!(params.connection_timeout, Duration::from_secs(20));
        assert!(params.use_tls);
        assert_eq!(params.client_name, "test");
    }

    #[test]
    fn test_parse_full_url() {
        let params = ConnectionParams::from_str(
            "exasol://admin:secret@db.example.com:9000/PROD?timeout=30&tls=true",
        )
        .unwrap();

        assert_eq!(params.host, "db.example.com");
        assert_eq!(params.port, 9000);
        assert_eq!(params.username, "admin");
        assert_eq!(params.password(), "secret");
        assert_eq!(params.schema, Some("PROD".to_string()));
        assert_eq!(params.connection_timeout, Duration::from_secs(30));
        assert!(params.use_tls);
    }

    #[test]
    fn test_parse_url_encoded() {
        let params = ConnectionParams::from_str("exasol://user%40test:p%40ss@localhost").unwrap();

        assert_eq!(params.username, "user@test");
        assert_eq!(params.password(), "p@ss");
    }

    #[test]
    fn test_parse_ipv6() {
        let params = ConnectionParams::from_str("exasol://user@[::1]:8563").unwrap();

        assert_eq!(params.host, "::1");
        assert_eq!(params.port, 8563);
    }

    #[test]
    fn test_parse_invalid_scheme() {
        let result = ConnectionParams::from_str("postgres://user@localhost");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_username() {
        let result = ConnectionParams::from_str("exasol://localhost");
        assert!(result.is_err());
    }

    #[test]
    fn test_display_no_password_leak() {
        let params = ConnectionBuilder::new()
            .host("localhost")
            .username("admin")
            .password("super_secret")
            .build()
            .unwrap();

        let display = format!("{}", params);
        assert!(!display.contains("super_secret"));
        assert!(display.contains("localhost"));
        assert!(display.contains("admin"));
    }

    #[test]
    fn test_debug_no_password_leak() {
        let params = ConnectionBuilder::new()
            .host("localhost")
            .username("admin")
            .password("super_secret")
            .build()
            .unwrap();

        let debug = format!("{:?}", params);
        // Debug output should not contain the password
        assert!(!debug.contains("super_secret"));
    }
}
