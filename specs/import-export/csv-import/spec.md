# Feature: CSV Import

Specifies CSV data import capabilities from files, streams, and callbacks into Exasol tables through the HTTP transport tunnel.

## Background

CSV import operations transfer data through the HTTP transport tunnel. The system supports importing from file paths, async streams, iterators, and callbacks. Import operations return row counts on success and handle errors with reject limits and clean connection teardown.

## Scenarios

### Scenario: Import CSV file into table

* *GIVEN* a CSV file exists on disk and an HTTP tunnel is available
* *WHEN* user calls import_from_file with table name, file path, and options
* *THEN* system SHALL establish HTTP tunnel and execute IMPORT SQL via WebSocket
* *AND* system SHALL stream CSV file contents as HTTP response
* *AND* system SHALL return row count on success

### Scenario: Import with custom CSV options

* *GIVEN* a CSV file with non-default formatting exists
* *WHEN* user specifies custom column separator, delimiter, encoding, or skip rows
* *THEN* system SHALL include format options in IMPORT SQL statement
* *AND* system SHALL ensure CSV data matches specified format

### Scenario: Import compressed CSV

* *GIVEN* a CSV file needs to be imported with compression
* *WHEN* user specifies gzip or bz2 compression option
* *THEN* system SHALL compress CSV data before sending
* *AND* IMPORT SQL SHALL use appropriate file extension (.csv.gz or .csv.bz2)

### Scenario: Import from async stream

* *GIVEN* an AsyncRead implementation provides CSV data
* *WHEN* user calls import_from_stream with an AsyncRead implementation
* *THEN* system SHALL read from stream and send data through HTTP tunnel
* *AND* system SHALL handle backpressure appropriately

### Scenario: Import from iterator

* *GIVEN* an iterator of CSV rows is available
* *WHEN* user calls import_from_iter with an iterator of CSV rows
* *THEN* system SHALL format rows as CSV and stream through HTTP tunnel

### Scenario: Import from callback

* *GIVEN* a data-producing callback function is available
* *WHEN* user calls import_from_callback with a data-producing callback
* *THEN* system SHALL invoke callback to get data chunks
* *AND* system SHALL stream chunks through HTTP tunnel

### Scenario: Import with reject limit

* *GIVEN* some rows in the import data may be malformed
* *WHEN* user specifies reject limit option
* *THEN* IMPORT SQL SHALL include REJECT LIMIT clause
* *AND* import SHALL continue despite individual row errors up to limit

### Scenario: Import failure cleanup

* *GIVEN* an import operation is in progress
* *WHEN* import fails partway through
* *THEN* HTTP tunnel connection SHALL be closed cleanly
* *AND* error SHALL be reported with useful context

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
