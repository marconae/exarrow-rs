# Feature: Protocol

Specifies the WebSocket protocol implementation for the Exasol WebSocket API, including connection establishment, protocol handshake, command execution, and message serialization.

## Background

The system implements the Exasol WebSocket API protocol as defined in https://github.com/exasol/websocket-api. Connections use secure WebSocket (wss://) when TLS is enabled. All commands are serialized to JSON format with required fields (command type, attributes) and responses are deserialized to structured objects with validation. Error responses from Exasol are parsed into appropriate Rust error types with error codes and messages.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Set session attributes command

* *GIVEN* an authenticated WebSocket session exists
* *WHEN* setting session attributes (e.g., autocommit mode)
* *THEN* the system SHALL send a `setAttributes` command with the attributes to modify
* *AND* the system SHALL validate the server response status
* *AND* the system SHALL return an error if the server rejects the attribute change
<!-- /DELTA:NEW -->
