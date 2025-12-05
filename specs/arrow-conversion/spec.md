# arrow-conversion Specification

## Purpose

Specifies the conversion logic from Exasol WebSocket API JSON responses to Apache Arrow RecordBatch format, ensuring
efficient and accurate data transformation for all supported data types.

## Requirements

### Requirement: JSON to Arrow Conversion

The system SHALL convert Exasol WebSocket API JSON responses to Apache Arrow RecordBatch format using row-major input data.

#### Scenario: Column data conversion

- **WHEN** converting JSON result data to Arrow
- **THEN** it SHALL receive data in row-major format (`data[row_idx][col_idx]`)
- **AND** it SHALL apply correct type conversions based on schema
- **AND** it SHALL build Arrow arrays for each column

#### Scenario: Row-oriented to columnar conversion

- **WHEN** JSON data arrives in row-major format from the deserializer
- **THEN** it SHALL iterate rows and distribute values to column builders
- **AND** it SHALL maintain data ordering and alignment

#### Scenario: Column-oriented conversion

- **WHEN** JSON data is already in columnar format
- **THEN** it SHALL efficiently map to Arrow columnar structure
- **AND** it SHALL minimize data copying

### Requirement: RecordBatch Construction

The system SHALL construct valid Arrow RecordBatch instances from converted data.

#### Scenario: Single batch creation

- **WHEN** creating a RecordBatch from result data
- **THEN** it SHALL validate all columns have equal length
- **AND** it SHALL attach the correct schema
- **AND** it SHALL produce a valid RecordBatch

#### Scenario: Empty RecordBatch

- **WHEN** result set contains no rows
- **THEN** it SHALL create an empty RecordBatch with correct schema
- **AND** it SHALL have zero rows but valid column definitions

#### Scenario: Large batch handling

- **WHEN** result sets exceed batch size limits
- **THEN** it SHALL split data into multiple RecordBatches
- **AND** it SHALL maintain consistent schema across batches

### Requirement: Null Value Handling

The system SHALL correctly represent NULL values in Arrow format.

#### Scenario: NULL bitmap construction

- **WHEN** converting columns containing NULLs
- **THEN** it SHALL build Arrow null bitmaps correctly
- **AND** it SHALL mark NULL positions as invalid in the bitmap

#### Scenario: JSON null representation

- **WHEN** JSON contains null values
- **THEN** it SHALL detect JSON nulls regardless of how they're encoded
- **AND** it SHALL convert them to Arrow nulls

#### Scenario: All-null columns

- **WHEN** a column contains only NULL values
- **THEN** it SHALL create a valid Arrow array with all nulls
- **AND** it SHALL handle this efficiently without unnecessary allocations

### Requirement: String Data Conversion

The system SHALL efficiently convert string data from JSON to Arrow string arrays.

#### Scenario: UTF-8 validation

- **WHEN** converting string values
- **THEN** it SHALL validate UTF-8 encoding
- **AND** it SHALL return errors for invalid UTF-8 sequences

#### Scenario: String array construction

- **WHEN** building Arrow string arrays
- **THEN** it SHALL use Arrow's string array builders efficiently
- **AND** it SHALL handle variable-length strings correctly

#### Scenario: Large strings

- **WHEN** strings exceed standard string array limits
- **THEN** it SHALL use Arrow LargeUtf8 arrays
- **AND** it SHALL handle offset calculations correctly

### Requirement: Numeric Data Conversion

The system SHALL convert numeric data from JSON with appropriate precision and comprehensive overflow detection.

#### Scenario: Integer conversion

- **WHEN** converting JSON integers
- **THEN** it SHALL parse to appropriate Arrow integer types
- **AND** it SHALL detect overflow conditions
- **AND** it SHALL return errors for out-of-range values

#### Scenario: Floating-point conversion

- **WHEN** converting JSON floats
- **THEN** it SHALL parse to Arrow Float64
- **AND** it SHALL handle special values (NaN, Infinity) correctly
- **AND** it SHALL preserve precision as much as possible

#### Scenario: Decimal conversion

- **WHEN** converting Exasol DECIMAL values from JSON
- **THEN** it SHALL parse string representations of decimals
- **AND** it SHALL construct Arrow Decimal128/256 values with correct precision and scale
- **AND** it SHALL detect values that don't fit in target precision
- **AND** it SHALL validate precision fits within Decimal128 (38 digits) or Decimal256 (76 digits) limits
- **AND** it SHALL validate scale is consistent with the declared column scale
- **AND** it SHALL return clear errors including the problematic value, expected precision, and actual precision

#### Scenario: Decimal overflow detection

- **WHEN** a DECIMAL value exceeds the target precision
- **THEN** it SHALL detect the overflow before data corruption
- **AND** it SHALL report the column name, row number, and exact value
- **AND** it SHALL explain the precision/scale mismatch

### Requirement: Temporal Data Conversion

The system SHALL convert temporal data from JSON to Arrow temporal types.

#### Scenario: Date conversion

- **WHEN** converting Exasol DATE values
- **THEN** it SHALL parse date strings to Arrow Date32
- **AND** it SHALL handle various date formats from Exasol

#### Scenario: Timestamp conversion

- **WHEN** converting Exasol TIMESTAMP values
- **THEN** it SHALL parse timestamp strings to Arrow Timestamp
- **AND** it SHALL handle microsecond precision
- **AND** it SHALL handle timezone information correctly

#### Scenario: Time interval conversion

- **WHEN** converting Exasol INTERVAL types
- **THEN** it SHALL map to appropriate Arrow Duration or Interval types
- **AND** it SHALL preserve interval precision

### Requirement: Binary Data Conversion

The system SHALL convert binary data from JSON to Arrow binary arrays.

#### Scenario: Base64 decoding

- **WHEN** binary data is encoded as Base64 in JSON
- **THEN** it SHALL decode Base64 strings to binary
- **AND** it SHALL handle padding and invalid characters

#### Scenario: Hexadecimal decoding

- **WHEN** binary data is encoded as hexadecimal in JSON
- **THEN** it SHALL decode hex strings to binary
- **AND** it SHALL validate hex format

#### Scenario: Binary array construction

- **WHEN** building Arrow binary arrays
- **THEN** it SHALL handle variable-length binary data correctly
- **AND** it SHALL use appropriate offset types

### Requirement: Schema Consistency

The system SHALL ensure schema consistency throughout data conversion.

#### Scenario: Schema validation

- **WHEN** converting data with known schema
- **THEN** it SHALL validate data types match schema expectations
- **AND** it SHALL return errors for type mismatches

#### Scenario: Dynamic schema inference

- **WHEN** schema is not provided upfront
- **THEN** it SHALL infer Arrow schema from JSON metadata
- **AND** it SHALL apply correct type mappings

### Requirement: Error Recovery

The system SHALL provide detailed error information when conversion fails.

#### Scenario: Conversion error reporting

- **WHEN** a data conversion fails
- **THEN** it SHALL report the column name and row number
- **AND** it SHALL include the problematic value
- **AND** it SHALL explain what conversion was attempted

#### Scenario: Partial conversion handling

- **WHEN** some rows fail conversion in a batch
- **THEN** it SHALL have configurable behavior (fail-fast or collect errors)
- **AND** it SHALL provide all error details for debugging

### Requirement: Streaming Column-to-Row Deserialization

The system SHALL transpose column-major JSON data to row-major format during deserialization using custom serde deserializers.

#### Scenario: Custom deserializer application

- **WHEN** deserializing `FetchResponseData` or `ResultSetData` from JSON
- **THEN** the `data` field SHALL use a custom deserializer that transposes during parsing
- **AND** the resulting `Vec<Vec<Value>>` SHALL be in row-major format: `data[row_idx][col_idx]`

#### Scenario: Transposition during deserialization

- **WHEN** Exasol returns column-major JSON data (columns as outer array)
- **THEN** the custom deserializer SHALL iterate columns and distribute values to rows
- **AND** it SHALL use `DeserializeSeed` pattern to accumulate across columns
- **AND** no post-hoc transposition step SHALL be required

#### Scenario: Empty result handling

- **WHEN** deserializing an empty result set
- **THEN** it SHALL return an empty `Vec<Vec<Value>>`
- **AND** it SHALL handle zero columns gracefully

#### Scenario: Single column handling

- **WHEN** deserializing a result with one column
- **THEN** each row SHALL contain exactly one value
- **AND** the format SHALL be consistent with multi-column results

### Requirement: Non-Null Column Fast Path

The system SHALL provide optimized conversion paths for columns without NULL values.

#### Scenario: All-values-present detection

- **WHEN** a column contains no NULL values
- **THEN** it SHALL detect this condition before building the array
- **AND** it SHALL skip null bitmap construction for the column
- **AND** it SHALL use direct array construction without validity buffer

#### Scenario: Null-presence hint

- **WHEN** result metadata indicates null presence or absence
- **THEN** it SHALL use this hint to select optimal conversion path
- **AND** it SHALL fall back to null-safe path when hint is unavailable

### Requirement: String Buffer Optimization

The system SHALL optimize string conversion for large VARCHAR columns.

#### Scenario: Large string pre-allocation

- **WHEN** converting large VARCHAR columns
- **THEN** it SHALL estimate buffer size based on average string length
- **AND** it SHALL pre-allocate string buffers to reduce reallocations

#### Scenario: String array builder hints

- **WHEN** building Arrow string arrays
- **THEN** it SHALL provide capacity hints to StringBuilder
- **AND** it SHALL minimize intermediate string copies

### Requirement: Conversion Test Validation Gate

ALL changes to arrow conversion MUST pass comprehensive testing before completion.

#### Scenario: Unit test requirement

- **WHEN** arrow conversion optimization is complete
- **THEN** `cargo test` MUST pass with zero failures
- **AND** DECIMAL overflow tests MUST be present and passing
- **AND** null handling fast path tests MUST be present and passing

#### Scenario: Performance validation

- **WHEN** arrow conversion optimization is complete
- **THEN** benchmarks MUST demonstrate performance improvement
- **AND** no performance regression MUST occur for existing workloads

#### Scenario: Code quality requirement

- **WHEN** arrow conversion optimization is complete
- **THEN** `cargo clippy --all-targets --all-features -- -W clippy::all` MUST produce zero warnings
- **AND** all new code MUST follow project conventions

