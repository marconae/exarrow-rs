# query-execution Specification

## Purpose

Specifies SQL query execution capabilities including statement preparation, parameter binding, result set retrieval,
transaction support, and query lifecycle management.

## Requirements

### Requirement: SQL Query Execution

The system SHALL execute SQL queries against Exasol database via WebSocket protocol.

#### Scenario: Simple SELECT query

- **WHEN** executing a simple SELECT statement
- **THEN** it SHALL send the query to Exasol via WebSocket
- **AND** it SHALL retrieve the complete result set
- **AND** it SHALL return results as Arrow RecordBatch

#### Scenario: DDL statement execution

- **WHEN** executing DDL statements (CREATE, ALTER, DROP)
- **THEN** it SHALL execute the statement
- **AND** it SHALL return success or error status
- **AND** it SHALL return affected object information

#### Scenario: DML statement execution

- **WHEN** executing DML statements (INSERT, UPDATE, DELETE)
- **THEN** it SHALL execute the statement
- **AND** it SHALL return the number of affected rows

### Requirement: Prepared Statements

The system SHALL support prepared statement execution with parameter binding.

#### Scenario: Statement preparation

- **WHEN** preparing a SQL statement with parameters
- **THEN** it SHALL validate the SQL syntax
- **AND** it SHALL identify parameter placeholders
- **AND** it SHALL return a prepared statement handle

#### Scenario: Parameter binding

- **WHEN** binding parameters to a prepared statement
- **THEN** it SHALL validate parameter types against expected types
- **AND** it SHALL convert Rust types to Exasol-compatible values
- **AND** it SHALL prevent SQL injection through proper escaping

#### Scenario: Prepared statement execution

- **WHEN** executing a prepared statement with bound parameters
- **THEN** it SHALL substitute parameters safely
- **AND** it SHALL execute the query
- **AND** it SHALL return results in Arrow format

### Requirement: Result Set Retrieval

The system SHALL retrieve and process query result sets efficiently.

#### Scenario: Small result set retrieval

- **WHEN** a query returns a small result set (< 1000 rows)
- **THEN** it SHALL fetch all rows in a single request
- **AND** it SHALL convert data to Arrow RecordBatch

#### Scenario: Large result set pagination

- **WHEN** a query returns a large result set
- **THEN** it SHALL support fetching results in batches
- **AND** it SHALL provide mechanisms to retrieve subsequent batches
- **AND** it SHALL maintain result set handle for pagination

#### Scenario: Result set metadata

- **WHEN** retrieving result metadata
- **THEN** it SHALL provide column names and types
- **AND** it SHALL provide row count (if available)
- **AND** it SHALL provide Arrow schema

### Requirement: Transaction Support

The system SHALL support basic transaction operations.

#### Scenario: Explicit transaction begin

- **WHEN** beginning a transaction explicitly
- **THEN** it SHALL execute BEGIN or START TRANSACTION
- **AND** it SHALL track transaction state

#### Scenario: Transaction commit

- **WHEN** committing a transaction
- **THEN** it SHALL execute COMMIT
- **AND** it SHALL update transaction state to committed

#### Scenario: Transaction rollback

- **WHEN** rolling back a transaction
- **THEN** it SHALL execute ROLLBACK
- **AND** it SHALL update transaction state to rolled back

#### Scenario: Auto-commit mode

- **WHEN** auto-commit is enabled (default)
- **THEN** each statement SHALL be automatically committed
- **AND** explicit transaction commands SHALL override auto-commit temporarily

### Requirement: Batch Query Execution

The system SHALL support executing multiple queries in sequence.

#### Scenario: Sequential query execution

- **WHEN** multiple queries are submitted for execution
- **THEN** it SHALL execute them in order
- **AND** it SHALL return results for each query separately
- **AND** it SHALL stop on first error if specified

#### Scenario: Independent query execution

- **WHEN** queries are marked as independent
- **THEN** it SHALL execute all queries regardless of individual failures
- **AND** it SHALL collect results and errors for each query

### Requirement: Query Cancellation

The system SHALL support canceling in-flight queries.

#### Scenario: Cancel running query

- **WHEN** a query is cancelled during execution
- **THEN** it SHALL send a cancellation request to Exasol
- **AND** it SHALL wait for cancellation acknowledgment or timeout
- **AND** it SHALL return a cancellation error

#### Scenario: Cancel with timeout

- **WHEN** cancellation takes longer than timeout
- **THEN** it SHALL forcefully abort the local query execution
- **AND** it SHALL close the connection if necessary

### Requirement: Query Metadata

The system SHALL provide metadata about query execution.

#### Scenario: Execution statistics

- **WHEN** a query completes
- **THEN** it SHALL provide execution time
- **AND** it SHALL provide rows affected/returned
- **AND** it SHALL provide any warnings from the database

#### Scenario: Query plan information

- **WHEN** EXPLAIN is used
- **THEN** it SHALL return query execution plan information
- **AND** it SHALL format plan data appropriately for display

