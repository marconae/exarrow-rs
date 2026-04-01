# Feature: Auth and Security

Defines connection parameter handling, authentication mechanisms, and credential security for Exasol database connections.

## Background

Connection parameters are required for Exasol connectivity and SHALL be validated before use. Authentication mechanisms SHALL operate securely, with credentials protected in memory and never exposed through logging. TLS is enabled by default for production Exasol connections.

## Scenarios

### Scenario: Connection string parsing

* *GIVEN* connection parameters are configured
* *WHEN* a connection string is provided in the format `exasol://host:port`
* *THEN* it SHALL parse host and port correctly
* *AND* it SHALL support optional parameters (schema, encryption settings)
* *AND* it SHALL parse `certificate_fingerprint` (alias `certificatefingerprint`) as the expected server certificate fingerprint

### Scenario: Parameter validation

* *GIVEN* connection parameters are configured
* *WHEN* connection parameters are validated
* *THEN* it SHALL reject missing required parameters (host, credentials)
* *AND* it SHALL validate port numbers are in valid range (1-65535)
* *AND* it SHALL provide clear error messages for invalid parameters

### Scenario: TLS configuration

* *GIVEN* connection parameters are configured
* *WHEN* TLS/SSL is requested
* *THEN* it SHALL support enabling encrypted connections
* *AND* it SHALL validate certificate settings if certificate validation is enabled
* *AND* it SHALL accept an optional certificate fingerprint for pin-based validation

### Scenario: Username and password authentication

* *GIVEN* a connection to Exasol is being established
* *WHEN* authenticating with username and password
* *THEN* it SHALL send credentials securely over the connection
* *AND* it SHALL support encrypted password transmission

### Scenario: Authentication failure

* *GIVEN* a connection to Exasol is being established
* *WHEN* authentication fails
* *THEN* it SHALL return an error with the authentication failure reason
* *AND* it SHALL close the connection
* *AND* it SHALL NOT retry automatically to avoid account lockout

### Scenario: Authentication success

* *GIVEN* a connection to Exasol is being established
* *WHEN* authentication succeeds
* *THEN* it SHALL establish an authenticated session
* *AND* it SHALL store session information for subsequent requests

### Scenario: Secure credential storage

* *GIVEN* credentials are provided for authentication
* *WHEN* credentials are stored temporarily
* *THEN* it SHALL minimize credential lifetime in memory
* *AND* it SHALL consider zeroing credential memory after use where feasible

### Scenario: No credential logging

* *GIVEN* credentials are provided for authentication
* *WHEN* logging connection events
* *THEN* it SHALL NOT log passwords or sensitive authentication tokens
* *AND* it SHALL redact credentials from error messages

### Scenario: Certificate fingerprint validation success

* *GIVEN* a TLS connection is being established
* *AND* `certificate_fingerprint` is set to the SHA-256 hex fingerprint of the server's certificate
* *WHEN* the TLS handshake completes
* *THEN* the driver SHALL compute the SHA-256 hex digest of the server's DER-encoded certificate
* *AND* it SHALL accept the connection when the fingerprints match

### Scenario: Certificate fingerprint validation failure

* *GIVEN* a TLS connection is being established
* *AND* `certificate_fingerprint` is set to an incorrect value
* *WHEN* the TLS handshake completes
* *THEN* the driver SHALL reject the connection with a certificate fingerprint mismatch error
* *AND* the error MUST include both the expected fingerprint and the actual server fingerprint

### Scenario: Certificate fingerprint bypasses hostname validation

* *GIVEN* a TLS connection is being established
* *AND* `certificate_fingerprint` is set
* *WHEN* the TLS handshake completes
* *THEN* the driver SHALL skip standard certificate chain and hostname validation
* *AND* it SHALL rely solely on the fingerprint match for trust
