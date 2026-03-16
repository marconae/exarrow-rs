# Feature: Driver Interface

Defines the ADBC driver interface for exarrow-rs, covering driver registration, database connection management, and error handling through ADBC-compliant interfaces.

## Background

The system implements the ADBC (Arrow Database Connectivity) driver interface to provide standardized database connectivity for Exasol. All connections use WebSocket transport and authenticate with provided credentials. Connection strings follow the format `exasol://host:port`. Errors are propagated through ADBC-compliant error interfaces with detailed messages, error codes, and SQL state where applicable.

## Scenarios

### Scenario: Driver registration

* *GIVEN* an ADBC driver manager is ready to load drivers
* *WHEN* the driver is loaded by an ADBC driver manager
* *THEN* it SHALL expose driver metadata including name, version, and vendor information

### Scenario: Driver initialization

* *GIVEN* an ADBC driver is loaded
* *WHEN* the driver is initialized with connection parameters
* *THEN* it SHALL validate required parameters and return a connection handle
* *AND* it SHALL support connection strings in the format `exasol://host:port`

### Scenario: Connection creation

* *GIVEN* a database connection is configured with valid credentials
* *WHEN* a connection is requested with valid credentials
* *THEN* it SHALL establish a WebSocket connection to Exasol
* *AND* it SHALL authenticate using provided credentials
* *AND* it SHALL return a usable connection handle

### Scenario: Connection properties

* *GIVEN* an active database connection exists
* *WHEN* connection properties are queried
* *THEN* it SHALL return database metadata (version, capabilities, catalog info)

### Scenario: Connection closure

* *GIVEN* an active database connection exists
* *WHEN* a connection is closed
* *THEN* it SHALL gracefully terminate the WebSocket connection
* *AND* it SHALL release all associated resources

### Scenario: Connection errors

* *GIVEN* a database connection is configured with valid credentials
* *WHEN* a connection attempt fails
* *THEN* it SHALL return an ADBC-compliant error with detailed message
* *AND* it SHALL include error codes and SQL state where applicable

### Scenario: Query execution errors

* *GIVEN* an active database connection exists
* *WHEN* a query execution fails
* *THEN* it SHALL propagate Exasol error messages through ADBC error interface
* *AND* it SHALL include server-side error details

### Scenario: Type conversion errors

* *GIVEN* an active database connection exists
* *WHEN* a type conversion fails
* *THEN* it SHALL return a clear error indicating the problematic type
* *AND* it SHALL specify which column and row caused the error

### Scenario: Parameterized TIMESTAMP type parsing in FFI layer

* *GIVEN* an active ADBC connection via the FFI driver manager
* *AND* a table exists with a `TIMESTAMP(3)` column
* *WHEN* retrieving the table schema via `get_table_schema`
* *THEN* the driver SHALL parse `TIMESTAMP(3)` as a valid TIMESTAMP type
* *AND* the driver SHALL NOT return an "Unknown Exasol type" error
