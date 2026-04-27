# Feature: Native TCP Protocol

The system implements Exasol's native binary TCP protocol as a high-performance alternative to the WebSocket JSON protocol. The native protocol uses binary message framing with 21-byte headers, little-endian byte ordering, and protocol version negotiation starting at v14. All commands are serialized as binary attribute sets, and responses are parsed from binary frames into structured Rust types.

## Background

The native TCP protocol connects to the same Exasol port (8563) as the WebSocket protocol. The server dispatches based on the first bytes received: `LOGIN_MAGIC` (0x01121201) for native TCP, `GET ` for WebSocket. Protocol version 14 is the minimum supported version, which requires ChaCha20 encryption and deprecates RC4.

## Scenarios

### Scenario: Native TCP connection establishment

* *GIVEN* an Exasol database is reachable on a configured host and port
* *WHEN* connecting via the native TCP protocol
* *THEN* the system SHALL open a TCP connection to the specified host and port
* *AND* the system SHALL apply TLS when TLS is enabled (default)
* *AND* the system SHALL enforce the configured connection timeout

### Scenario: Protocol version negotiation

* *GIVEN* a TCP connection is established to Exasol
* *WHEN* initiating the native protocol handshake
* *THEN* the system SHALL send a login packet starting with `LOGIN_MAGIC` (0x01121201)
* *AND* the login packet SHALL include the client's maximum supported protocol version
* *AND* the system SHALL accept any server-negotiated version between 14 and the client's maximum
* *AND* the system SHALL reject protocol versions below 14 with a clear error

### Scenario: Login handshake

* *GIVEN* a TCP connection is established and TLS negotiation is complete
* *WHEN* performing the login handshake
* *THEN* the system SHALL send a login packet containing: magic (4 bytes), message length (4 bytes), protocol version (4 bytes), change date (4 bytes), and connection attributes
* *AND* all multi-byte integers SHALL be encoded in little-endian byte order
* *AND* the system SHALL receive the server's response containing `ATTR_PUBLIC_KEY`, `ATTR_RANDOM_PHRASE`, and `ATTR_PROTOCOL_VERSION`

### Scenario: Binary message framing

* *GIVEN* an authenticated native TCP session exists
* *WHEN* sending a command to Exasol
* *THEN* the system SHALL prefix each message with a 21-byte header containing: message length (4 bytes), command type (1 byte), serial number (4 bytes), number of attributes (4 bytes), attribute data length (4 bytes), and number of result parts (4 bytes)
* *AND* all multi-byte values in the header SHALL be little-endian

### Scenario: Execute SQL command

* *GIVEN* an authenticated native TCP session exists
* *WHEN* executing a SQL query
* *THEN* the system SHALL send a `CMD_EXECUTE` (12) command with the SQL text as an attribute
* *AND* the system SHALL parse the response as one of: `R_ResultSet`, `R_RowCount`, `R_Empty`, or `R_Exception`

### Scenario: Fetch results for large result sets

* *GIVEN* an authenticated native TCP session exists
* *AND* a query has returned a result set handle (not `SMALL_RESULTSET`)
* *WHEN* fetching additional rows
* *THEN* the system SHALL send a `CMD_FETCH2` (36) command with the result set handle
* *AND* the system SHALL parse the returned rows in column-major binary format
* *AND* the system SHALL continue fetching until all rows are received

### Scenario: Prepared statement lifecycle

* *GIVEN* an authenticated native TCP session exists
* *WHEN* creating a prepared statement
* *THEN* the system SHALL send `CMD_CREATE_PREPARED` (10) with the SQL text
* *AND* the system SHALL parse the returned statement handle and parameter metadata
* *WHEN* executing the prepared statement with parameters
* *THEN* the system SHALL send `CMD_EXECUTE_PREPARED` (11) with the handle and binary parameter data
* *WHEN* closing the prepared statement
* *THEN* the system SHALL send `CMD_CLOSE_PREPARED` (18) with the handle

### Scenario: Disconnect

* *GIVEN* an authenticated native TCP session exists
* *WHEN* closing the connection
* *THEN* the system SHALL send `CMD_DISCONNECT` (32)
* *AND* the system SHALL close the TCP connection after sending the disconnect command

### Scenario: Error response handling

* *GIVEN* an authenticated native TCP session exists
* *WHEN* the server responds with `R_Exception` (-1)
* *THEN* the system SHALL parse the error message length, error message text, and 5-byte SQL state
* *AND* the system SHALL convert the error into an appropriate Rust error type

### Scenario: Still-executing handling

* *GIVEN* an authenticated native TCP session exists
* *WHEN* the server responds with `R_StillExecuting` (5)
* *THEN* the system SHALL send `CMD_CONTINUE` (38) to poll for completion
* *AND* the system SHALL repeat until a final result type is received

### Scenario: Set session attributes

* *GIVEN* an authenticated native TCP session exists
* *WHEN* setting session attributes (e.g., autocommit, current schema)
* *THEN* the system SHALL send `CMD_SET_ATTRIBUTES` (35) with the attribute key-value pairs encoded in binary format
* *AND* the system SHALL validate the server response
