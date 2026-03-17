# Feature: Binding and Execution

## Background

The ADBC FFI interface allows clients to bind an Arrow RecordBatch to a statement before calling execute_update or execute_query. The driver must wire this bound data through to the prepared statement execution path, converting Arrow column values to Exasol wire-protocol parameters.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Single execution with parameters

* *GIVEN* a prepared statement exists with bound parameters
* *WHEN* executing a prepared statement with bound parameters via the ADBC FFI interface
* *THEN* the driver SHALL use the prepared statement handle and bound Arrow RecordBatch data
* *AND* the driver SHALL convert Arrow column data to Exasol wire-protocol parameters
* *AND* the driver SHALL return results in Arrow format
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
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
<!-- /DELTA:NEW -->
