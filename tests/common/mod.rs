//! Common test utilities for exarrow-rs integration tests.
//!
//! # Integration Test Prerequisites
//!
//! These integration tests require a running Exasol database instance.
//! The recommended approach is to use the Exasol Docker image:
//!
//! ```bash
//! docker run -d --name exasol-test \
//!   -p 8563:8563 \
//!   --privileged \
//!   exasol/docker-db:latest
//! ```
//!
//! Wait for the database to be ready (may take 1-2 minutes on first run).
//! You can check readiness with:
//!
//! ```bash
//! docker logs exasol-test 2>&1 | grep -i "started"
//! ```
//!
//! # Configuration
//!
//! Tests use the following defaults which can be overridden via environment variables:
//!
//! | Default Constant   | Environment Variable | Default Value |
//! |--------------------|----------------------|---------------|
//! | `DEFAULT_HOST`     | `EXASOL_HOST`        | "localhost"   |
//! | `DEFAULT_PORT`     | `EXASOL_PORT`        | 8563          |
//! | `DEFAULT_USER`     | `EXASOL_USER`        | "sys"         |
//! | `DEFAULT_PASSWORD` | `EXASOL_PASSWORD`    | "exasol"      |
//!
//! # Running Integration Tests
//!
//! Integration tests automatically skip if Exasol is not available at the
//! configured host and port. To run them:
//!
//! ```bash
//! # Run all integration tests (skips if Exasol unavailable)
//! cargo test --test integration_tests
//!
//! # Run a specific integration test
//! cargo test --test integration_tests test_connection_succeeds
//!
//! # Run with custom configuration
//! EXASOL_HOST=myhost EXASOL_PORT=9563 cargo test --test integration_tests
//! ```
//!
//! # Test Cleanup
//!
//! All tests should clean up after themselves by dropping any created schemas
//! or tables. Use unique identifiers (e.g., timestamps) in schema names to
//! avoid conflicts when tests run in parallel.

use exarrow_rs::adbc::{Connection, Driver};
use std::env;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

// Connection Constants with Default Values

/// Default host for Exasol database connection.
pub const DEFAULT_HOST: &str = "localhost";

/// Default port for Exasol database connection.
pub const DEFAULT_PORT: u16 = 8563;

/// Default username for Exasol database connection.
pub const DEFAULT_USER: &str = "sys";

/// Default password for Exasol database connection.
pub const DEFAULT_PASSWORD: &str = "exasol";

// Environment Variable Names

/// Environment variable name for overriding the Exasol host.
const ENV_EXASOL_HOST: &str = "EXASOL_HOST";

/// Environment variable name for overriding the Exasol port.
const ENV_EXASOL_PORT: &str = "EXASOL_PORT";

/// Environment variable name for overriding the Exasol username.
const ENV_EXASOL_USER: &str = "EXASOL_USER";

/// Environment variable name for overriding the Exasol password.
const ENV_EXASOL_PASSWORD: &str = "EXASOL_PASSWORD";

// Configuration Helpers

/// Get the Exasol host from environment or use default.
///
/// Reads from `EXASOL_HOST` environment variable, falling back to `DEFAULT_HOST`.
pub fn get_host() -> String {
    env::var(ENV_EXASOL_HOST).unwrap_or_else(|_| DEFAULT_HOST.to_string())
}

/// Get the Exasol port from environment or use default.
///
/// Reads from `EXASOL_PORT` environment variable, falling back to `DEFAULT_PORT`.
/// If the environment variable contains an invalid port number, returns the default.
pub fn get_port() -> u16 {
    env::var(ENV_EXASOL_PORT)
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PORT)
}

/// Get the Exasol username from environment or use default.
///
/// Reads from `EXASOL_USER` environment variable, falling back to `DEFAULT_USER`.
pub fn get_user() -> String {
    env::var(ENV_EXASOL_USER).unwrap_or_else(|_| DEFAULT_USER.to_string())
}

/// Get the Exasol password from environment or use default.
///
/// Reads from `EXASOL_PASSWORD` environment variable, falling back to `DEFAULT_PASSWORD`.
pub fn get_password() -> String {
    env::var(ENV_EXASOL_PASSWORD).unwrap_or_else(|_| DEFAULT_PASSWORD.to_string())
}

/// Build a connection string from the current configuration.
///
/// Constructs a connection string in the format:
/// `exasol://user:password@host:port?validate_certificate=false`
///
/// Certificate validation is disabled by default for integration tests
/// since Exasol Docker uses self-signed certificates.
///
/// Uses environment variables if set, otherwise falls back to defaults.
///
/// # Example
///
/// ```ignore
/// let conn_str = get_test_connection_string();
/// // Returns something like: "exasol://sys:exasol@localhost:8563?validate_certificate=false"
/// ```
pub fn get_test_connection_string() -> String {
    format!(
        "exasol://{}:{}@{}:{}?tls=true&validateservercertificate=0",
        get_user(),
        get_password(),
        get_host(),
        get_port()
    )
}

/// Build a connection string for a specific transport type.
///
/// Appends `&transport=<transport>` to the base connection string.
#[allow(dead_code)]
pub fn get_test_connection_string_with_transport(transport: &str) -> String {
    format!("{}&transport={}", get_test_connection_string(), transport)
}

/// Establish a test connection to Exasol.
///
/// Creates a new connection using the test configuration (from environment
/// variables or defaults). Uses the default transport (native when the `native`
/// feature is enabled, websocket otherwise).
///
/// # Returns
///
/// A connected `Connection` instance.
///
/// # Errors
///
/// Returns an error if the connection cannot be established.
#[allow(dead_code)]
pub async fn get_test_connection() -> Result<Connection, exarrow_rs::error::ExasolError> {
    let driver = Driver::new();
    let conn_string = get_test_connection_string();

    let mut last_error = None;
    for attempt in 1..=5u32 {
        let database = driver.open(&conn_string)?;
        match database.connect().await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                eprintln!("Connection attempt {}/5 failed: {}", attempt, e);
                last_error = Some(e);
                if attempt < 5 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    Err(exarrow_rs::error::ExasolError::Connection(
        last_error.unwrap(),
    ))
}

/// Establish a test connection using a specific transport.
///
/// Creates a connection that explicitly uses the given transport type,
/// regardless of the default feature flags.
///
/// # Arguments
///
/// * `transport` - Transport type: "native" or "websocket"
///
/// # Returns
///
/// A connected `Connection` instance.
///
/// # Errors
///
/// Returns an error if the connection cannot be established.
#[allow(dead_code)]
pub async fn get_test_connection_with_transport(
    transport: &str,
) -> Result<Connection, exarrow_rs::error::ExasolError> {
    let driver = Driver::new();
    let conn_string = get_test_connection_string_with_transport(transport);

    let mut last_error = None;
    for attempt in 1..=5u32 {
        let database = driver.open(&conn_string)?;
        match database.connect().await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                eprintln!(
                    "Connection attempt {}/5 ({}) failed: {}",
                    attempt, transport, e
                );
                last_error = Some(e);
                if attempt < 5 {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }

    Err(exarrow_rs::error::ExasolError::Connection(
        last_error.unwrap(),
    ))
}

// Exasol Availability Check

/// Check if Exasol is available at the configured host and port.
///
/// Performs a simple TCP connection check to determine if the Exasol
/// database is reachable. This does not verify authentication or
/// database readiness, only network connectivity.
///
/// # Returns
///
/// `true` if a TCP connection can be established, `false` otherwise.
///
/// # Example
///
/// ```ignore
/// if !is_exasol_available() {
///     println!("Skipping test: Exasol not available");
///     return;
/// }
/// ```
pub fn is_exasol_available() -> bool {
    let host = get_host();
    let port = get_port();
    let addr = format!("{}:{}", host, port);

    // Resolve the hostname to socket addresses (handles both hostnames and IPs)
    let socket_addrs: Vec<_> = match addr.to_socket_addrs() {
        Ok(addrs) => addrs.collect(),
        Err(_) => return false,
    };

    // Try connecting to any of the resolved addresses
    for socket_addr in socket_addrs {
        if TcpStream::connect_timeout(&socket_addr, Duration::from_secs(2)).is_ok() {
            return true;
        }
    }
    false
}

/// Skip a test if Exasol is not available.
///
/// Use this at the beginning of integration tests to gracefully skip
/// when no Exasol instance is running. Combined with `#[ignore]`, this
/// provides a double layer of protection.
///
/// # Example
///
/// ```ignore
/// #[tokio::test]
/// #[ignore]
/// async fn test_query() {
///     skip_if_no_exasol!();
///     // Test code here...
/// }
/// ```
#[macro_export]
macro_rules! skip_if_no_exasol {
    () => {
        if !$crate::common::is_exasol_available() {
            if std::env::var("REQUIRE_EXASOL").is_ok() {
                panic!(
                    "REQUIRE_EXASOL is set but Exasol is not available at {}:{}",
                    $crate::common::get_host(),
                    $crate::common::get_port()
                );
            }
            eprintln!(
                "Skipping test: Exasol not available at {}:{}",
                $crate::common::get_host(),
                $crate::common::get_port()
            );
            return;
        }
    };
}

/// Generate a unique test object name.
///
/// Uses a nanosecond timestamp plus a process-local counter so names stay
/// unique even when multiple tests start within the same clock tick.
pub fn generate_unique_test_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid = std::process::id();

    format!("{}_{}_{}_{}", prefix, pid, timestamp, counter)
}

/// Generate a unique test schema name.
///
/// # Example
///
/// ```ignore
/// let schema = generate_test_schema_name();
/// // Returns something like: "TEST_INTEGRATION_12345_1700000000123456789_0"
/// ```
pub fn generate_test_schema_name() -> String {
    generate_unique_test_name("TEST_INTEGRATION")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_HOST, "localhost");
        assert_eq!(DEFAULT_PORT, 8563);
        assert_eq!(DEFAULT_USER, "sys");
        assert_eq!(DEFAULT_PASSWORD, "exasol");
    }

    #[test]
    fn test_get_host_default() {
        // Clear env var if set
        env::remove_var(ENV_EXASOL_HOST);
        assert_eq!(get_host(), DEFAULT_HOST);
    }

    #[test]
    fn test_get_port_default() {
        env::remove_var(ENV_EXASOL_PORT);
        assert_eq!(get_port(), DEFAULT_PORT);
    }

    #[test]
    fn test_get_user_default() {
        env::remove_var(ENV_EXASOL_USER);
        assert_eq!(get_user(), DEFAULT_USER);
    }

    #[test]
    fn test_get_password_default() {
        env::remove_var(ENV_EXASOL_PASSWORD);
        assert_eq!(get_password(), DEFAULT_PASSWORD);
    }

    #[test]
    fn test_connection_string_format() {
        // Clear all env vars to use defaults
        env::remove_var(ENV_EXASOL_HOST);
        env::remove_var(ENV_EXASOL_PORT);
        env::remove_var(ENV_EXASOL_USER);
        env::remove_var(ENV_EXASOL_PASSWORD);

        let conn_str = get_test_connection_string();
        assert_eq!(
            conn_str,
            "exasol://sys:exasol@localhost:8563?tls=true&validateservercertificate=0"
        );
    }

    #[test]
    fn test_generate_test_schema_name() {
        let schema1 = generate_test_schema_name();
        let schema2 = generate_test_schema_name();

        assert!(schema1.starts_with("TEST_INTEGRATION_"));
        assert!(schema2.starts_with("TEST_INTEGRATION_"));
        assert_ne!(schema1, schema2);
        assert!(schema1.len() > 17); // "TEST_INTEGRATION_" is 17 chars
    }
}
