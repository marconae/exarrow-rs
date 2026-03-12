# Feature: CSV Import

Specifies CSV data import capabilities from files, streams, and callbacks into Exasol tables through the HTTP transport tunnel.

## Background

CSV import operations transfer data through the HTTP transport tunnel. The system supports importing from file paths, async streams, iterators, and callbacks. Import operations return row counts on success and handle errors with reject limits and clean connection teardown.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Concurrent SQL and stream completion ordering

* *GIVEN* an import operation is executing SQL and streaming data concurrently
* *AND* the data stream completes successfully before the IMPORT SQL returns
* *WHEN* the system detects the stream has finished
* *THEN* the system SHALL continue waiting for the SQL result instead of aborting
* *AND* the system SHALL return the row count from the SQL response on success

### Scenario: Stream error aborts import immediately

* *GIVEN* an import operation is executing SQL and streaming data concurrently
* *AND* the data stream encounters an error before the IMPORT SQL returns
* *WHEN* the system detects the stream error
* *THEN* the system SHALL abort the import immediately
* *AND* the system SHALL return the stream error without waiting for the SQL response
<!-- /DELTA:NEW -->
