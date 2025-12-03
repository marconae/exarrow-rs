# integration-testing Specification

## Purpose

Specifies the integration testing infrastructure for validating exarrow-rs against a real Exasol database, including
test environment configuration, connection testing, query execution validation, transaction testing, Arrow data
conversion verification, and ADBC driver manager compatibility.

## Requirements

### Requirement: Integration Test Infrastructure

The system SHALL provide integration test infrastructure for validating exarrow-rs against a real Exasol database
instance.

#### Scenario: Test environment configuration via constants

- **WHEN** integration tests are executed
- **THEN** they SHALL use shared constants for default connection parameters (DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USER,
  DEFAULT_PASSWORD)
- **AND** the default values SHALL be: host="localhost", port=8563, user="sys", password="exasol"
- **AND** all test methods SHALL obtain connection configuration through shared helper functions

#### Scenario: Test environment override via environment variables

- **WHEN** environment variables (EXASOL_HOST, EXASOL_PORT, EXASOL_USER, EXASOL_PASSWORD) are set
- **THEN** they SHALL override the default constants
- **AND** tests SHALL use the overridden values for connecting to Exasol

#### Scenario: TLS certificate validation configuration

- **WHEN** connecting to Exasol Docker with self-signed certificates
- **THEN** the connection string SHALL support `validateservercertificate=0` parameter
- **AND** the WebSocket transport SHALL disable TLS certificate validation when configured
- **AND** this allows integration tests to connect without trusted CA certificates

#### Scenario: Exasol availability detection

- **WHEN** integration tests start
- **THEN** they SHALL detect if Exasol is available
- **AND** they SHALL skip gracefully with a clear message if Exasol is not reachable

#### Scenario: Test isolation

- **WHEN** integration tests create database objects
- **THEN** they SHALL use unique schema names to avoid conflicts with other test runs
- **AND** they SHALL clean up all created objects after test completion

### Requirement: Connection Integration Testing

The system SHALL verify connection functionality against a real Exasol database.

#### Scenario: Successful connection establishment

- **WHEN** connecting to Exasol with valid credentials (sys/exasol)
- **THEN** the connection SHALL be established successfully
- **AND** the session SHALL be in Ready state

#### Scenario: Connection failure handling

- **WHEN** connecting with invalid credentials
- **THEN** the connection attempt SHALL fail with an appropriate error
- **AND** the error message SHALL indicate authentication failure

#### Scenario: Connection closure

- **WHEN** a connection is closed after use
- **THEN** all resources SHALL be released
- **AND** subsequent operations on the closed connection SHALL return appropriate errors

### Requirement: Query Execution Integration Testing

The system SHALL verify query execution against a real Exasol database.

#### Scenario: SELECT from DUAL

- **WHEN** executing "SELECT 1 FROM DUAL"
- **THEN** the query SHALL return a RecordBatch with one row
- **AND** the value SHALL be the integer 1

#### Scenario: DDL execution (CREATE TABLE)

- **WHEN** executing a CREATE TABLE statement
- **THEN** the table SHALL be created in the database
- **AND** subsequent queries against the table SHALL succeed

#### Scenario: DML execution (INSERT)

- **WHEN** executing an INSERT statement
- **THEN** the row SHALL be inserted into the table
- **AND** the affected row count SHALL be returned correctly

#### Scenario: SELECT verification

- **WHEN** querying inserted data with SELECT
- **THEN** the returned RecordBatch SHALL contain the inserted values
- **AND** the Arrow schema SHALL match the table column types

### Requirement: Transaction Integration Testing

The system SHALL verify transaction operations against a real Exasol database.

#### Scenario: Transaction commit

- **WHEN** inserting data and executing COMMIT
- **THEN** the data SHALL be persisted permanently
- **AND** a new connection SHALL see the committed data

#### Scenario: Transaction rollback

- **WHEN** inserting data and executing ROLLBACK
- **THEN** the data SHALL be discarded
- **AND** the table SHALL not contain the rolled-back data

### Requirement: Arrow Data Conversion Integration Testing

The system SHALL verify that Arrow data conversion is correct for real Exasol data.

#### Scenario: Numeric type conversion

- **WHEN** querying INTEGER and DECIMAL columns
- **THEN** the values SHALL be correctly converted to Arrow Int64 and Decimal128
- **AND** precision and scale SHALL be preserved for DECIMAL types

#### Scenario: String type conversion

- **WHEN** querying VARCHAR columns with various content
- **THEN** the values SHALL be correctly converted to Arrow Utf8
- **AND** UTF-8 encoding SHALL be handled correctly

#### Scenario: NULL value handling

- **WHEN** querying columns containing NULL values
- **THEN** the Arrow arrays SHALL correctly represent nulls
- **AND** the null bitmap SHALL be set appropriately
- **AND** the null_count() method SHALL return the exact count of NULL values in each column

#### Scenario: Temporal type conversion

- **WHEN** querying DATE and TIMESTAMP columns
- **THEN** DATE values SHALL be correctly converted to Arrow Date32 (not Utf8)
- **AND** TIMESTAMP values SHALL be correctly converted to Arrow Timestamp
- **AND** the conversion SHALL handle Exasol's date string format "YYYY-MM-DD"
- **AND** time zone handling SHALL be consistent

#### Scenario: Prepared statement with parameters

- **WHEN** executing a prepared SELECT statement with bound parameters
- **THEN** the parameters SHALL be correctly serialized in column-major format
- **AND** parameter type metadata SHALL be included in the request
- **AND** the query SHALL execute without SQL parsing errors
- **AND** results SHALL be returned in Arrow format

### Requirement: ADBC Driver Manager Compatibility

The system SHALL be loadable by the official Apache Arrow ADBC driver manager for ecosystem compatibility with
third-party tools.

#### Scenario: FFI export for dynamic loading

- **WHEN** the driver is built with the `ffi` feature
- **THEN** it SHALL produce a shared library (.so on Linux, .dylib on macOS)
- **AND** the library SHALL export an ADBC-compatible driver init function

#### Scenario: Driver manager loading

- **WHEN** `adbc_driver_manager::ManagedDriver::load_dynamic_from_filename()` is called with the exarrow-rs library path
- **THEN** the driver SHALL be loaded successfully
- **AND** the driver metadata (name, version, vendor) SHALL be accessible

#### Scenario: Database creation via driver manager

- **WHEN** creating a database handle through the driver manager
- **THEN** the database SHALL be configured with connection parameters
- **AND** the database handle SHALL be valid for subsequent operations

#### Scenario: Connection via driver manager

- **WHEN** establishing a connection through the driver manager with valid Exasol credentials
- **THEN** the connection SHALL be established successfully via WebSocket
- **AND** the connection handle SHALL be valid for query execution

#### Scenario: Query execution via driver manager

- **WHEN** executing "SELECT 1 FROM DUAL" through the driver manager
- **THEN** the query SHALL execute successfully
- **AND** results SHALL be returned as Arrow RecordBatch
- **AND** the result data SHALL match direct API usage

#### Scenario: End-to-end ecosystem compatibility

- **WHEN** a third-party tool loads exarrow-rs via ADBC driver manager
- **THEN** it SHALL be able to connect to Exasol
- **AND** it SHALL be able to execute queries and retrieve Arrow data
- **AND** the behavior SHALL be consistent with other ADBC-compatible drivers

### Requirement: Acceptance Criteria

All integration tests MUST pass successfully for this specification to be accepted.

#### Scenario: All integration tests pass

- **WHEN** running `cargo test --test integration_tests` against a running Exasol Docker instance
- **THEN** all tests SHALL pass (0 failures)
- **AND** the test output SHALL show all scenarios verified

#### Scenario: All driver manager tests pass

- **WHEN** running `cargo test --test driver_manager_tests` with the FFI library built
- **THEN** all tests SHALL pass (0 failures)
- **AND** the driver manager integration SHALL be fully functional

