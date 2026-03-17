# Feature: Protocol

Specifies the WebSocket protocol implementation for the Exasol WebSocket API, including connection establishment, protocol handshake, command execution, and message serialization.

## Background

The system implements the Exasol WebSocket API protocol as defined in https://github.com/exasol/websocket-api. Connections use secure WebSocket (wss://) when TLS is enabled. All commands are serialized to JSON format with required fields (command type, attributes) and responses are deserialized to structured objects with validation. Error responses from Exasol are parsed into appropriate Rust error types with error codes and messages.

## Scenarios

### Scenario: WebSocket connection establishment

* *GIVEN* a WebSocket endpoint is reachable
* *WHEN* connecting to an Exasol database
* *THEN* it SHALL establish a WebSocket connection to the specified host and port
* *AND* it SHALL use secure WebSocket (wss://) when TLS is enabled
* *AND* it SHALL handle connection timeouts gracefully
* *AND* it SHALL configure the WebSocket with no frame size limit and no message size limit

### Scenario: Large result set transfer via WebSocket

* *GIVEN* an authenticated WebSocket session exists
* *AND* a table contains enough data to produce a response exceeding 16 MiB
* *WHEN* executing a SELECT query that returns the full result set
* *THEN* the system SHALL receive the complete response without frame size errors
* *AND* the system SHALL return all rows as Arrow RecordBatches

### Scenario: Protocol handshake

* *GIVEN* a WebSocket endpoint is reachable
* *WHEN* WebSocket connection is established
* *THEN* it SHALL perform the Exasol-specific protocol handshake
* *AND* it SHALL negotiate protocol version compatibility

### Scenario: Login command

* *GIVEN* a WebSocket endpoint is reachable
* *WHEN* authenticating with the database
* *THEN* it SHALL send a login command with credentials
* *AND* it SHALL handle authentication success and failure responses

### Scenario: Execute SQL command

* *GIVEN* an authenticated WebSocket session exists
* *WHEN* executing a SQL query
* *THEN* it SHALL send an execute command with the SQL text
* *AND* it SHALL include execution parameters (e.g., result set handle)

### Scenario: Fetch results command

* *GIVEN* an authenticated WebSocket session exists
* *WHEN* retrieving query results
* *THEN* it SHALL send fetch commands for result data
* *AND* it SHALL handle pagination for large result sets

### Scenario: Disconnect command

* *GIVEN* an authenticated WebSocket session exists
* *WHEN* closing a connection
* *THEN* it SHALL send a disconnect command before closing the WebSocket
* *AND* it SHALL wait for acknowledgment or timeout

### Scenario: JSON request serialization

* *GIVEN* an authenticated WebSocket session exists
* *WHEN* sending a command to Exasol
* *THEN* it SHALL serialize the command to JSON format
* *AND* it SHALL include required fields (command type, attributes)

### Scenario: JSON response deserialization

* *GIVEN* an authenticated WebSocket session exists
* *WHEN* receiving a response from Exasol
* *THEN* it SHALL deserialize JSON to structured response objects
* *AND* it SHALL validate response structure and required fields

### Scenario: Error response handling

* *GIVEN* an authenticated WebSocket session exists
* *WHEN* Exasol returns an error response
* *THEN* it SHALL parse error codes and messages
* *AND* it SHALL convert them to appropriate Rust error types

### Scenario: Set session attributes command

* *GIVEN* an authenticated WebSocket session exists
* *WHEN* setting session attributes (e.g., autocommit mode)
* *THEN* the system SHALL send a `setAttributes` command with the attributes to modify
* *AND* the system SHALL validate the server response status
* *AND* the system SHALL return an error if the server rejects the attribute change
