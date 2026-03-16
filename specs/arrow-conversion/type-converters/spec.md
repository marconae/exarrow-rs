# Feature: Type Converters

Specifies type-specific conversion logic from Exasol JSON responses to Arrow arrays, covering numeric, string, temporal, and binary data types.

## Background

Exasol returns JSON result data with typed columns. All conversions SHALL produce appropriate Arrow array types with correct precision, encoding, and overflow detection. Numeric conversions SHALL detect out-of-range values. String conversions SHALL validate UTF-8 encoding. Temporal conversions SHALL handle Exasol date, timestamp, and interval formats. Binary conversions SHALL decode Base64 and hexadecimal encodings. The Exasol WebSocket API defines `TIMESTAMP WITH LOCAL TIME ZONE` as a distinct type name separate from `TIMESTAMP`.

## Scenarios

### Scenario: Integer conversion

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* converting JSON integers
* *THEN* it SHALL parse to appropriate Arrow integer types
* *AND* it SHALL detect overflow conditions
* *AND* it SHALL return errors for out-of-range values

### Scenario: Floating-point conversion

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* converting JSON floats
* *THEN* it SHALL parse to Arrow Float64
* *AND* it SHALL handle special values (NaN, Infinity) correctly
* *AND* it SHALL preserve precision as much as possible

### Scenario: Decimal conversion

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* converting Exasol DECIMAL values from JSON
* *THEN* it SHALL parse string representations of decimals
* *AND* it SHALL construct Arrow Decimal128/256 values with correct precision and scale
* *AND* it SHALL validate precision fits within Decimal128 (38 digits) or Decimal256 (76 digits) limits

### Scenario: Decimal validation errors

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* a DECIMAL value does not fit the target precision or has inconsistent scale
* *THEN* it SHALL detect the mismatch before data corruption
* *AND* it SHALL validate scale is consistent with the declared column scale
* *AND* it SHALL return clear errors including the problematic value, expected precision, and actual precision

### Scenario: Decimal overflow detection

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* a DECIMAL value exceeds the target precision
* *THEN* it SHALL detect the overflow before data corruption
* *AND* it SHALL report the column name, row number, and exact value
* *AND* it SHALL explain the precision/scale mismatch

### Scenario: UTF-8 validation

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* converting string values
* *THEN* it SHALL validate UTF-8 encoding
* *AND* it SHALL return errors for invalid UTF-8 sequences

### Scenario: String array construction

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* building Arrow string arrays
* *THEN* it SHALL use Arrow's string array builders efficiently
* *AND* it SHALL handle variable-length strings correctly

### Scenario: Large strings

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* strings exceed standard string array limits
* *THEN* it SHALL use Arrow LargeUtf8 arrays
* *AND* it SHALL handle offset calculations correctly

### Scenario: Date conversion

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* converting Exasol DATE values
* *THEN* it SHALL parse date strings to Arrow Date32
* *AND* it SHALL handle various date formats from Exasol

### Scenario: Timestamp conversion

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* converting Exasol TIMESTAMP values
* *THEN* it SHALL parse timestamp strings to Arrow Timestamp
* *AND* it SHALL handle microsecond precision
* *AND* it SHALL handle timezone information correctly

### Scenario: Time interval conversion

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* converting Exasol INTERVAL types
* *THEN* it SHALL map to appropriate Arrow Duration or Interval types
* *AND* it SHALL preserve interval precision

### Scenario: Base64 decoding

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* binary data is encoded as Base64 in JSON
* *THEN* it SHALL decode Base64 strings to binary
* *AND* it SHALL handle padding and invalid characters

### Scenario: Hexadecimal decoding

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* binary data is encoded as hexadecimal in JSON
* *THEN* it SHALL decode hex strings to binary
* *AND* it SHALL validate hex format

### Scenario: Binary array construction

* *GIVEN* Exasol returns JSON result data with typed columns
* *WHEN* building Arrow binary arrays
* *THEN* it SHALL handle variable-length binary data correctly
* *AND* it SHALL use appropriate offset types

### Scenario: TIMESTAMP WITH LOCAL TIME ZONE type parsing in WebSocket path

* *GIVEN* a query result includes a column of type `TIMESTAMP WITH LOCAL TIME ZONE`
* *WHEN* the WebSocket response contains `type: "TIMESTAMP WITH LOCAL TIME ZONE"` in the column metadata
* *THEN* the converter SHALL parse this as a valid TIMESTAMP type with `with_local_time_zone: true`
* *AND* the converter SHALL NOT return an "Unsupported Exasol type" error
