# Feature: Get Parameter Schema

## Background

When a statement is prepared via the Exasol WebSocket protocol, the response includes parameter metadata with column names and types. The parameter schema returned by `get_parameter_schema` should use these Exasol-provided names.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Get parameter schema after prepare

* *GIVEN* an ADBC statement has been prepared with a SQL query containing parameters
* *WHEN* `get_parameter_schema` is called
* *THEN* the driver SHALL return an Arrow `Schema` with one field per parameter
* *AND* each field SHALL use the name from the Exasol prepared statement metadata
* *AND* each field SHALL have the Arrow data type corresponding to the Exasol parameter type
<!-- /DELTA:CHANGED -->
