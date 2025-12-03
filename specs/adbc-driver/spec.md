# adbc-driver Specification

## Purpose

Defines the ADBC (Arrow Database Connectivity) driver interface for exarrow-rs, providing standardized database
connectivity that enables interoperability with ADBC-compatible tools and applications.

## Requirements

### Requirement: ADBC Driver Implementation

The system SHALL implement the ADBC (Arrow Database Connectivity) driver interface to provide standardized database
connectivity for Exasol.

#### Scenario: Driver registration

- **WHEN** the driver is loaded by an ADBC driver manager
- **THEN** it SHALL expose driver metadata including name, version, and vendor information

#### Scenario: Driver initialization

- **WHEN** the driver is initialized with connection parameters
- **THEN** it SHALL validate required parameters and return a connection handle
- **AND** it SHALL support connection strings in the format `exasol://host:port`

### Requirement: Database Connection Interface

The system SHALL provide an ADBC-compliant database connection interface.

#### Scenario: Connection creation

- **WHEN** a connection is requested with valid credentials
- **THEN** it SHALL establish a WebSocket connection to Exasol
- **AND** it SHALL authenticate using provided credentials
- **AND** it SHALL return a usable connection handle

#### Scenario: Connection properties

- **WHEN** connection properties are queried
- **THEN** it SHALL return database metadata (version, capabilities, catalog info)

#### Scenario: Connection closure

- **WHEN** a connection is closed
- **THEN** it SHALL gracefully terminate the WebSocket connection
- **AND** it SHALL release all associated resources

### Requirement: Statement Execution Interface

The system SHALL provide an ADBC-compliant statement interface for query execution.

#### Scenario: Query preparation

- **WHEN** a SQL query is prepared
- **THEN** it SHALL validate SQL syntax locally where possible
- **AND** it SHALL return a statement handle

#### Scenario: Query execution

- **WHEN** a prepared statement is executed
- **THEN** it SHALL send the query via WebSocket protocol
- **AND** it SHALL return results as Arrow RecordBatch
- **AND** it SHALL include result metadata (schema, row count)

#### Scenario: Parameterized queries

- **WHEN** a statement is executed with parameters
- **THEN** it SHALL bind parameters safely to prevent SQL injection
- **AND** it SHALL support Exasol's parameter binding syntax

### Requirement: Result Set Handling

The system SHALL return query results in Arrow format per ADBC specification.

#### Scenario: Arrow RecordBatch results

- **WHEN** a query returns data
- **THEN** results SHALL be provided as Arrow RecordBatch
- **AND** the schema SHALL accurately reflect Exasol column types

#### Scenario: Empty result sets

- **WHEN** a query returns no rows
- **THEN** it SHALL return an empty RecordBatch with correct schema

#### Scenario: Result set metadata

- **WHEN** result metadata is requested
- **THEN** it SHALL provide row count, column count, and schema information

### Requirement: Error Handling

The system SHALL provide comprehensive error information through ADBC error interfaces.

#### Scenario: Connection errors

- **WHEN** a connection attempt fails
- **THEN** it SHALL return an ADBC-compliant error with detailed message
- **AND** it SHALL include error codes and SQL state where applicable

#### Scenario: Query execution errors

- **WHEN** a query execution fails
- **THEN** it SHALL propagate Exasol error messages through ADBC error interface
- **AND** it SHALL include server-side error details

#### Scenario: Type conversion errors

- **WHEN** a type conversion fails
- **THEN** it SHALL return a clear error indicating the problematic type
- **AND** it SHALL specify which column and row caused the error

