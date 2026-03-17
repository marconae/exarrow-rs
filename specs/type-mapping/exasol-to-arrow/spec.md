# Feature: Exasol to Arrow

Defines the mapping from Exasol data types to Apache Arrow data types, including complex types and metadata preservation.

## Background

The system defines a complete mapping from Exasol data types to Apache Arrow data types. Exasol DECIMAL precision does not exceed 36, so Decimal128 is always sufficient. All Arrow fields are marked nullable when the source Exasol column allows NULL values. Type metadata from Exasol (precision, scale, original type names) is preserved in Arrow field metadata for round-trip fidelity. Unsupported or complex types produce clear error messages with guidance on workarounds.

## Scenarios

### Scenario: Decimal type mapping

* *GIVEN* an Exasol schema is provided with column type information
* *WHEN* mapping Exasol DECIMAL types
* *THEN* DECIMAL(p, s) with p in range 1-36 SHALL map to Arrow Decimal128(p, s)
* *AND* Exasol DECIMAL precision SHALL NOT exceed 36 (Exasol's documented maximum)

### Scenario: Non-decimal numeric type mapping

* *GIVEN* an Exasol schema is provided with column type information
* *WHEN* mapping Exasol non-decimal numeric types
* *THEN* DOUBLE SHALL map to Arrow Float64
* *AND* INTEGER SHALL map to Arrow Int64
* *AND* SMALLINT SHALL map to Arrow Int32 or Int16

### Scenario: String types mapping

* *GIVEN* an Exasol schema is provided with column type information
* *WHEN* mapping Exasol string types
* *THEN* VARCHAR(n) SHALL map to Arrow Utf8
* *AND* CHAR(n) SHALL map to Arrow Utf8 with padding handling
* *AND* CLOB SHALL map to Arrow LargeUtf8

### Scenario: Date and time types mapping

* *GIVEN* an Exasol schema is provided with column type information
* *WHEN* mapping Exasol temporal types
* *THEN* DATE SHALL map to Arrow Date32
* *AND* TIMESTAMP with fractional precision 0-9 SHALL map to Arrow Timestamp with appropriate TimeUnit
* *AND* parameterized TIMESTAMP variants like `TIMESTAMP(3)` SHALL be parsed correctly, ignoring the precision parameter for Arrow mapping
* *AND* INTERVAL types SHALL map to Arrow Duration or Interval types

### Scenario: Boolean type mapping

* *GIVEN* an Exasol schema is provided with column type information
* *WHEN* mapping Exasol BOOLEAN type
* *THEN* it SHALL map to Arrow Boolean

### Scenario: Binary types mapping

* *GIVEN* an Exasol schema is provided with column type information
* *WHEN* mapping Exasol binary types
* *THEN* VARBINARY SHALL map to Arrow Binary
* *AND* BLOB SHALL map to Arrow LargeBinary

### Scenario: NULL handling

* *GIVEN* an Exasol schema is provided with column type information
* *WHEN* a column allows NULL values
* *THEN* the Arrow field SHALL be marked as nullable
* *AND* NULL values SHALL be represented in Arrow null bitmaps

### Scenario: GEOMETRY type handling

* *GIVEN* Exasol returns a result set with typed columns
* *WHEN* encountering Exasol GEOMETRY types
* *THEN* it SHALL map to Arrow Binary or Utf8 (WKT/WKB format)
* *AND* it SHALL document the encoding used

### Scenario: Array type handling

* *GIVEN* Exasol returns a result set with typed columns
* *WHEN* Exasol array types are encountered (if supported)
* *THEN* it SHALL map to Arrow List types
* *AND* it SHALL handle nested type mappings recursively

### Scenario: Unsupported types

* *GIVEN* Exasol returns a result set with typed columns
* *WHEN* an unsupported Exasol type is encountered
* *THEN* it SHALL return a clear error message
* *AND* it SHALL specify the unsupported type name
* *AND* it SHALL provide guidance on workarounds if available

### Scenario: Column metadata

* *GIVEN* Exasol returns a result set with typed columns
* *WHEN* creating Arrow schemas from Exasol result sets
* *THEN* it SHALL include Exasol type information in Arrow field metadata
* *AND* it SHALL preserve nullability information
* *AND* it SHALL include precision/scale for applicable types

### Scenario: Schema documentation

* *GIVEN* Exasol returns a result set with typed columns
* *WHEN* schema is inspected
* *THEN* it SHALL provide access to original Exasol type names
* *AND* it SHALL expose type mapping used for each column
