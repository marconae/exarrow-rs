# websocket-client Specification

## Purpose

Specifies the WebSocket client implementation for the Exasol WebSocket API, including connection establishment, command
execution, message serialization, and async communication using tokio.

## Requirements

### Requirement: WebSocket Protocol Implementation

The system SHALL implement the Exasol WebSocket API protocol as defined in https://github.com/exasol/websocket-api.

#### Scenario: WebSocket connection establishment

- **WHEN** connecting to an Exasol database
- **THEN** it SHALL establish a WebSocket connection to the specified host and port
- **AND** it SHALL use secure WebSocket (wss://) when TLS is enabled
- **AND** it SHALL handle connection timeouts gracefully

#### Scenario: Protocol handshake

- **WHEN** WebSocket connection is established
- **THEN** it SHALL perform the Exasol-specific protocol handshake
- **AND** it SHALL negotiate protocol version compatibility

### Requirement: Command Execution

The system SHALL send commands to Exasol using the WebSocket API command format.

#### Scenario: Login command

- **WHEN** authenticating with the database
- **THEN** it SHALL send a login command with credentials
- **AND** it SHALL handle authentication success and failure responses

#### Scenario: Execute SQL command

- **WHEN** executing a SQL query
- **THEN** it SHALL send an execute command with the SQL text
- **AND** it SHALL include execution parameters (e.g., result set handle)

#### Scenario: Fetch results command

- **WHEN** retrieving query results
- **THEN** it SHALL send fetch commands for result data
- **AND** it SHALL handle pagination for large result sets

#### Scenario: Disconnect command

- **WHEN** closing a connection
- **THEN** it SHALL send a disconnect command before closing the WebSocket
- **AND** it SHALL wait for acknowledgment or timeout

### Requirement: Message Serialization

The system SHALL serialize and deserialize messages according to the Exasol WebSocket API specification.

#### Scenario: JSON request serialization

- **WHEN** sending a command to Exasol
- **THEN** it SHALL serialize the command to JSON format
- **AND** it SHALL include required fields (command type, attributes)

#### Scenario: JSON response deserialization

- **WHEN** receiving a response from Exasol
- **THEN** it SHALL deserialize JSON to structured response objects
- **AND** it SHALL validate response structure and required fields

#### Scenario: Error response handling

- **WHEN** Exasol returns an error response
- **THEN** it SHALL parse error codes and messages
- **AND** it SHALL convert them to appropriate Rust error types

### Requirement: Async Communication

The system SHALL implement asynchronous WebSocket communication using tokio.

#### Scenario: Non-blocking send

- **WHEN** sending a command
- **THEN** it SHALL not block the calling thread
- **AND** it SHALL return a Future that resolves when the send completes

#### Scenario: Non-blocking receive

- **WHEN** waiting for a response
- **THEN** it SHALL not block the calling thread
- **AND** it SHALL support cancellation via tokio task cancellation

#### Scenario: Concurrent requests

- **WHEN** multiple requests are in-flight
- **THEN** it SHALL correctly match responses to their requests
- **AND** it SHALL maintain request ordering where required by protocol

### Requirement: Connection Lifecycle Management

The system SHALL manage WebSocket connection state throughout its lifecycle.

#### Scenario: Connection state tracking

- **WHEN** a connection is active
- **THEN** it SHALL track connection state (connecting, authenticated, idle, closed)
- **AND** it SHALL prevent invalid operations in wrong states

#### Scenario: Automatic reconnection

- **WHEN** a connection is dropped unexpectedly
- **THEN** it SHALL detect the disconnection
- **AND** it SHALL provide error information to the caller
- **AND** it SHALL NOT automatically reconnect (explicit reconnection required)

#### Scenario: Heartbeat and keepalive

- **WHEN** a connection is idle
- **THEN** it SHALL send keepalive messages if required by the protocol
- **AND** it SHALL detect connection failures via missing heartbeat responses

