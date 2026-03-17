# Feature: Get Parameter Schema

Implements the ADBC `Statement::get_parameter_schema` method, returning the Arrow Schema describing the parameters of a prepared statement by extracting metadata from the Exasol prepared statement response.

## Background

When a statement is prepared via the Exasol WebSocket protocol, the `CreatePreparedStatementResponse` includes `parameter_data` with column count and column type metadata for each parameter. This information is converted to an Arrow `Schema` where each field represents a parameter with its expected data type. The statement must be prepared before `get_parameter_schema` can be called.

## Scenarios

### Scenario: Get parameter schema after prepare

* *GIVEN* an ADBC statement has been prepared with a SQL query containing parameters
* *WHEN* `get_parameter_schema` is called
* *THEN* the driver SHALL return an Arrow `Schema` with one field per parameter
* *AND* each field SHALL use the name from the Exasol prepared statement metadata
* *AND* each field SHALL have the Arrow data type corresponding to the Exasol parameter type

### Scenario: Get parameter schema without prepare

* *GIVEN* an ADBC statement has NOT been prepared
* *WHEN* `get_parameter_schema` is called
* *THEN* the driver SHALL return an error with status `InvalidState`
* *AND* the error message SHALL indicate that the statement must be prepared first
