# Feature: Parquet I/O

Specifies Parquet file import and export capabilities, converting between Parquet and CSV formats for transfer through the HTTP transport tunnel.

## Background

Parquet import and export operates by converting between Parquet and CSV formats. Import reads Parquet files, converts typed columns to CSV representation (preserving NULLs), and streams through the HTTP tunnel. Export receives CSV from Exasol, converts to Parquet format with schema derived from Exasol metadata, and writes to files or streams. The HTTP-transport TLS knob is exposed as a single fluent method `use_tls(bool)` on both option builders, replacing the earlier `with_encryption(bool)` (import) and `use_encryption(bool)` (export) names.

## Scenarios

### Scenario: Import Parquet file into table

* *GIVEN* a Parquet file exists on disk
* *WHEN* user calls import_from_parquet with table name and file path
* *THEN* system SHALL read Parquet file and convert to CSV
* *AND* system SHALL stream CSV through HTTP tunnel to Exasol

### Scenario: Import Parquet preserves data types

* *GIVEN* a Parquet file contains typed columns
* *WHEN* Parquet data is converted to CSV format
* *THEN* system SHALL convert types appropriately for CSV format
* *AND* NULL values SHALL be handled correctly

### Scenario: Import Parquet from stream

* *GIVEN* Parquet data is available as an async stream
* *WHEN* user provides AsyncRead for Parquet data
* *THEN* system SHALL read and convert streaming Parquet to CSV

### Scenario: Export table to Parquet file

* *GIVEN* an Exasol table contains data to export
* *WHEN* user calls export_to_parquet with table name and file path
* *THEN* system SHALL receive CSV from Exasol and convert to Parquet
* *AND* system SHALL write Parquet file

### Scenario: Export preserves schema

* *GIVEN* an export operation has completed
* *WHEN* export completes
* *THEN* Parquet file SHALL contain schema derived from Exasol metadata

### Scenario: Export to Parquet stream

* *GIVEN* an AsyncWrite implementation is available for Parquet output
* *WHEN* user provides AsyncWrite for Parquet output
* *THEN* system SHALL stream Parquet data to writer

### Scenario: TLS for HTTP transport is configured uniformly via use_tls

* *GIVEN* an application configures Parquet import or export options
* *WHEN* the application enables or disables TLS on the HTTP transport tunnel
* *THEN* the option builders `ParquetImportOptions` and `ParquetExportOptions` SHALL expose a single fluent method `use_tls(bool)` to control HTTP-transport TLS
* *AND* the option builders MUST NOT expose `with_encryption(bool)` or `use_encryption(bool)` aliases
* *AND* the underlying option struct field SHALL be named `use_tls` (renamed from `use_encryption`) so that public field access uses the same vocabulary as the builder
* *AND* the default value of `use_tls` SHALL remain `false` to preserve current Docker/self-signed-cert behavior
