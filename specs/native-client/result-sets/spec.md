# Feature: Binary Result Set to Arrow Conversion

The native TCP protocol returns query results as column-major binary data, which maps directly to Arrow's columnar memory format. The system parses binary result set frames into Arrow RecordBatches without intermediate JSON representation, achieving zero-copy columnar transfer for supported types.

## Background

Native protocol result sets contain: a result type marker (1 byte), result set handle (4 bytes), column count (4 bytes), total rows (8 bytes), rows in this message (8 bytes), column metadata (variable), and column-major row data with per-column null masks. Each column's data is contiguous in the binary stream, matching Arrow's memory layout.

## Scenarios

### Scenario: Column metadata parsing

* *GIVEN* an authenticated native TCP session exists
* *AND* a query has returned an `R_ResultSet` (1)
* *WHEN* parsing the result set header
* *THEN* the system SHALL extract the result set handle, column count, total rows, and rows received
* *AND* for each column the system SHALL parse: column name length, column name, type ID, and type-specific metadata (precision, scale, varchar flag, max length)
* *AND* the system SHALL build an Arrow Schema from the parsed column metadata

### Scenario: Direct binary to Arrow conversion for numeric types

* *GIVEN* a result set contains columns of type `T_double` (8), `T_decimal` (6), `T_integer` (5), `T_smallint` (4), or `T_real` (7)
* *WHEN* parsing column data
* *THEN* the system SHALL read the binary values directly into Arrow numeric arrays (`T_double`/`T_real` → Float64, `T_decimal` → Decimal128, `T_integer` → Int64, `T_smallint` → Int32)
* *AND* the system SHALL handle little-endian to native byte order conversion
* *AND* decimal values SHALL preserve precision and scale

### Scenario: Direct binary to Arrow conversion for string types

* *GIVEN* a result set contains columns of type `T_char` (10) with or without the `IS_VARCHAR` flag
* *WHEN* parsing column data
* *THEN* the system SHALL read length-prefixed UTF-8 strings into Arrow Utf8 or LargeUtf8 arrays
* *AND* the system SHALL respect the `IS_UTF8` flag for encoding

### Scenario: Direct binary to Arrow conversion for temporal types

* *GIVEN* a result set contains columns of type `T_date` (14), `T_timestamp` (21), or `T_timestamp_utc` (125)
* *WHEN* parsing column data
* *THEN* the system SHALL convert dates to Arrow Date32 arrays
* *AND* the system SHALL convert timestamps to Arrow Timestamp arrays with appropriate time unit
* *AND* `T_timestamp_utc` SHALL map to Arrow Timestamp with UTC timezone

### Scenario: Direct binary to Arrow conversion for interval types

* *GIVEN* a result set contains columns of type `T_interval_year` (16) or `T_interval_day` (17)
* *WHEN* parsing column data
* *THEN* the system SHALL convert interval values to Arrow Utf8 arrays (string representation)

### Scenario: Direct binary to Arrow conversion for binary and geometry types

* *GIVEN* a result set contains columns of type `T_binary` (15), `T_hashtype` (126), or `T_geometry` (123)
* *WHEN* parsing column data
* *THEN* `T_binary` SHALL map to Arrow Binary arrays
* *AND* `T_hashtype` and `T_geometry` SHALL map to Arrow Utf8 arrays

### Scenario: Boolean type conversion

* *GIVEN* a result set contains columns of type `T_boolean`
* *WHEN* parsing column data
* *THEN* the system SHALL convert boolean values to Arrow Boolean arrays

### Scenario: NULL handling in column data

* *GIVEN* a result set contains columns with NULL values
* *WHEN* parsing column data
* *THEN* the system SHALL read the null marker byte (0 = NULL, 1 = NOT NULL) before each value
* *AND* NULL positions SHALL be recorded in the Arrow validity bitmap
* *AND* no data bytes SHALL be read for NULL values

### Scenario: Small result set (complete in one message)

* *GIVEN* a query returns a result set with handle `SMALL_RESULTSET` (-3)
* *WHEN* parsing the result
* *THEN* the system SHALL parse all rows from the single response message
* *AND* the system SHALL NOT issue a `CMD_FETCH2` command
* *AND* the system SHALL return a complete Arrow RecordBatch

### Scenario: Large result set (multi-fetch)

* *GIVEN* a query returns a result set with a positive handle
* *AND* the initial message contains fewer rows than total_rows
* *WHEN* retrieving the full result
* *THEN* the system SHALL issue `CMD_FETCH2` commands to retrieve remaining rows
* *AND* each fetch response SHALL be converted to an Arrow RecordBatch
* *AND* the system SHALL close the result set handle after all rows are fetched

### Scenario: Row count result

* *GIVEN* a DML statement (INSERT, UPDATE, DELETE) is executed
* *WHEN* the server responds with `R_RowCount` (0)
* *THEN* the system SHALL extract the 8-byte row count
* *AND* the system SHALL return the count without building a RecordBatch

### Scenario: Empty result

* *GIVEN* a statement that produces no result is executed
* *WHEN* the server responds with `R_Empty` (-2)
* *THEN* the system SHALL return an empty result without error
