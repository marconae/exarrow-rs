# Feature: Binding and Execution

Specifies the native prepared statement protocol, type-safe parameter binding, and execution behavior for Exasol, enabling secure and efficient parameterized query execution with SQL injection prevention by protocol design.

## Background

The system implements Exasol's native prepared statement protocol for secure parameterized query execution. Parameters are sent separately from SQL text via the wire protocol, preventing SQL injection by design. Parameter binding is positional (1-indexed) and type-safe, with validation against expected parameter types. Prepared statements can be reused across multiple executions with different parameter values without re-parsing SQL.

## Scenarios

### Scenario: Prepared statement creation

* *GIVEN* a connection to Exasol is established
* *WHEN* a SQL statement with parameters is prepared
* *THEN* it SHALL send a createPreparedStatement request to Exasol via WebSocket
* *AND* it SHALL receive a statement handle and parameter metadata
* *AND* it SHALL store parameter type information for validation

### Scenario: Parameter metadata retrieval

* *GIVEN* a connection to Exasol is established
* *WHEN* a prepared statement is created
* *THEN* it SHALL provide parameter count
* *AND* it SHALL provide parameter types when available from the database
* *AND* it SHALL allow querying parameter information before binding

### Scenario: Parameter value binding

* *GIVEN* a connection to Exasol is established
* *WHEN* binding parameter values to a prepared statement
* *THEN* it SHALL validate value types match expected parameter types
* *AND* it SHALL convert Rust types to Exasol wire format
* *AND* it SHALL send parameters separately from SQL text

### Scenario: NULL parameter binding

* *GIVEN* a connection to Exasol is established
* *WHEN* binding a NULL value to a parameter
* *THEN* it SHALL correctly represent NULL in the wire protocol
* *AND* it SHALL handle typed NULLs appropriately

### Scenario: Multiple parameter binding

* *GIVEN* a connection to Exasol is established
* *WHEN* binding multiple parameters
* *THEN* it SHALL bind parameters by position (1-indexed)
* *AND* it SHALL validate all required parameters are bound before execution

### Scenario: Single execution with parameters

* *GIVEN* a prepared statement exists with bound parameters
* *WHEN* executing a prepared statement with bound parameters via the ADBC FFI interface
* *THEN* the driver SHALL use the prepared statement handle and bound Arrow RecordBatch data
* *AND* the driver SHALL convert Arrow column data to Exasol wire-protocol parameters
* *AND* the driver SHALL return results in Arrow format

### Scenario: Re-execution with different parameters

* *GIVEN* a prepared statement exists with bound parameters
* *WHEN* executing a prepared statement multiple times with different parameter values
* *THEN* it SHALL reuse the server-side prepared statement
* *AND* it SHALL allow re-binding parameters between executions
* *AND* it SHALL avoid re-parsing the SQL statement

### Scenario: Prepared statement with no parameters

* *GIVEN* a connection to Exasol is established
* *WHEN* preparing and executing a statement with no parameters
* *THEN* it SHALL handle the statement through the prepared statement protocol
* *AND* it SHALL NOT require parameter binding

### Scenario: FFI bind and execute_update with parameters

* *GIVEN* an ADBC statement with SQL set and a RecordBatch bound via `bind()`
* *WHEN* `execute_update` is called
* *THEN* the driver SHALL prepare the statement if not already prepared
* *AND* the driver SHALL extract parameter values from the bound RecordBatch
* *AND* the driver SHALL execute the prepared statement with the bound parameters

### Scenario: FFI bind and execute_query with parameters

* *GIVEN* an ADBC statement with SQL set and a RecordBatch bound via `bind()`
* *WHEN* `execute` (execute_query) is called
* *THEN* the driver SHALL prepare the statement if not already prepared
* *AND* the driver SHALL extract parameter values from the bound RecordBatch
* *AND* the driver SHALL return the result set as Arrow RecordBatches
