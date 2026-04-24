#![cfg(feature = "native")]

use exarrow_rs::transport::{ConnectionParams, Credentials, NativeTcpTransport, TransportProtocol};

fn test_params() -> ConnectionParams {
    ConnectionParams::new("localhost".to_string(), 8563)
        .with_tls(true)
        .with_validate_server_certificate(false)
}

fn test_credentials() -> Credentials {
    Credentials::new("sys".to_string(), "exasol".to_string())
}

#[tokio::test]
async fn native_connect_and_authenticate() {
    let params = test_params();
    let creds = test_credentials();
    let mut transport = NativeTcpTransport::new();

    transport.connect(&params).await.expect("connect failed");
    assert!(transport.is_connected());

    let session = transport
        .authenticate(&creds)
        .await
        .expect("authenticate failed");
    eprintln!("Session: {:?}", session);
    // Auth succeeded — session fields may be populated later via separate query
    assert!(session.protocol_version > 0);

    transport.close().await.expect("close failed");
    assert!(!transport.is_connected());
}

#[tokio::test]
async fn native_connect_no_tls() {
    let params = ConnectionParams::new("localhost".to_string(), 8563).with_tls(false);
    let creds = test_credentials();
    let mut transport = NativeTcpTransport::new();

    transport
        .connect(&params)
        .await
        .expect("connect (no TLS) failed");
    assert!(transport.is_connected());

    match transport.authenticate(&creds).await {
        Ok(session) => {
            eprintln!("Session ID: {}", session.session_id);
            eprintln!("Protocol version: {}", session.protocol_version);
            eprintln!("Release version: {}", session.release_version);
            eprintln!("Database: {}", session.database_name);
        }
        Err(exarrow_rs::error::TransportError::ProtocolError(message)) => {
            assert!(
                message.contains("public key"),
                "unexpected no-TLS protocol failure: {message}"
            );
        }
        Err(err) => panic!("authenticate (no TLS) failed unexpectedly: {err:?}"),
    }

    transport.close().await.expect("close failed");
}

#[tokio::test]
async fn native_select_one() {
    let params = test_params();
    let creds = test_credentials();
    let mut transport = NativeTcpTransport::new();

    transport.connect(&params).await.unwrap();
    transport.authenticate(&creds).await.unwrap();

    let result = transport
        .execute_query("SELECT 1")
        .await
        .expect("execute_query failed");

    assert!(result.is_result_set());

    transport.close().await.unwrap();
}

#[tokio::test]
async fn native_select_multiple_types() {
    let params = test_params();
    let creds = test_credentials();
    let mut transport = NativeTcpTransport::new();

    transport.connect(&params).await.unwrap();
    transport.authenticate(&creds).await.unwrap();

    let result = transport
        .execute_query("SELECT 'hello' AS txt, 42 AS num, NULL AS n")
        .await
        .expect("execute_query failed");

    assert!(result.is_result_set());

    transport.close().await.unwrap();
}
