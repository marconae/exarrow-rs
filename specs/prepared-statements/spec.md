# prepared-statements Specification

## Purpose

Specifies the native prepared statement protocol implementation for Exasol, enabling secure and efficient parameterized
query execution with type-safe parameter binding, statement lifecycle management, and SQL injection prevention by
protocol design.

## Requirements

### Requirement: Native Prepared Statement Protocol

The system SHALL implement Exasol's native prepared statement protocol for secure and efficient parameterized query
execution, replacing string-based parameter interpolation.

#### Scenario: Prepared statement creation

- **WHEN** a SQL statement with parameters is prepared
- **THEN** it SHALL send a createPreparedStatement request to Exasol via WebSocket
- **AND** it SHALL receive a statement handle and parameter metadata
- **AND** it SHALL store parameter type information for validation

#### Scenario: Parameter metadata retrieval

- **WHEN** a prepared statement is created
- **THEN** it SHALL provide parameter count
- **AND** it SHALL provide parameter types when available from the database
- **AND** it SHALL allow querying parameter information before binding

### Requirement: Type-Safe Parameter Binding

The system SHALL support type-safe parameter binding that validates parameter types and prevents SQL injection by
design.

#### Scenario: Parameter value binding

- **WHEN** binding parameter values to a prepared statement
- **THEN** it SHALL validate value types match expected parameter types
- **AND** it SHALL convert Rust types to Exasol wire format
- **AND** it SHALL send parameters separately from SQL text

#### Scenario: NULL parameter binding

- **WHEN** binding a NULL value to a parameter
- **THEN** it SHALL correctly represent NULL in the wire protocol
- **AND** it SHALL handle typed NULLs appropriately

#### Scenario: Multiple parameter binding

- **WHEN** binding multiple parameters
- **THEN** it SHALL bind parameters by position (1-indexed)
- **AND** it SHALL validate all required parameters are bound before execution

### Requirement: Prepared Statement Execution

The system SHALL execute prepared statements with bound parameters securely and efficiently.

#### Scenario: Single execution with parameters

- **WHEN** executing a prepared statement with bound parameters
- **THEN** it SHALL send parameters via the protocol (not SQL interpolation)
- **AND** it SHALL return results in Arrow format
- **AND** it SHALL protect against SQL injection by protocol design

#### Scenario: Re-execution with different parameters

- **WHEN** executing a prepared statement multiple times with different parameter values
- **THEN** it SHALL reuse the server-side prepared statement
- **AND** it SHALL allow re-binding parameters between executions
- **AND** it SHALL avoid re-parsing the SQL statement

#### Scenario: Prepared statement with no parameters

- **WHEN** preparing and executing a statement with no parameters
- **THEN** it SHALL handle the statement through the prepared statement protocol
- **AND** it SHALL not require parameter binding

### Requirement: Prepared Statement Lifecycle

The system SHALL properly manage prepared statement resources on both client and server.

#### Scenario: Prepared statement cleanup

- **WHEN** a prepared statement is no longer needed
- **THEN** it SHALL send a close request to release server resources
- **AND** it SHALL invalidate the local statement handle
- **AND** it SHALL prevent use-after-close errors

#### Scenario: Connection close with active prepared statements

- **WHEN** a connection is closed with active prepared statements
- **THEN** it SHALL close all associated prepared statements
- **AND** it SHALL release server-side resources

#### Scenario: Prepared statement timeout

- **WHEN** a prepared statement is unused for an extended period
- **THEN** it SHALL handle server-side expiration gracefully
- **AND** it SHALL provide clear error messages if statement expired

### Requirement: Test Validation Gate

ALL changes related to prepared statements MUST pass comprehensive testing before completion.

#### Scenario: Unit test requirement

- **WHEN** prepared statement implementation is complete
- **THEN** `cargo test` MUST pass with zero failures
- **AND** all prepared statement unit tests MUST be present and passing

#### Scenario: Integration test requirement

- **WHEN** prepared statement implementation is complete
- **THEN** integration tests with real Exasol database MUST pass
- **AND** parameterized query tests MUST demonstrate SQL injection protection

#### Scenario: Code quality requirement

- **WHEN** prepared statement implementation is complete
- **THEN** `cargo clippy --all-targets --all-features -- -W clippy::all` MUST produce zero warnings
- **AND** all new code MUST follow project conventions

