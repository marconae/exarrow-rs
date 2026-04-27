# Feature: Arrow RecordBatch Import/Export

Specifies Arrow RecordBatch import and export capabilities, enabling direct transfer of Arrow-formatted data between applications and Exasol tables.

## Background

Arrow RecordBatch import converts RecordBatch data to CSV format for streaming through the HTTP tunnel. Export converts CSV received from Exasol into Arrow RecordBatches with schemas reflecting Exasol column types. Both single RecordBatches and streams of RecordBatches are supported, along with Arrow IPC file format for persistent storage. Streaming export supports configurable batch sizes. The HTTP-transport TLS knob is exposed as a single fluent method `use_tls(bool)` on both option builders, replacing the earlier flag-style `with_encryption()` (import) and `use_encryption(bool)` (export) names.

## Scenarios

### Scenario: Import single RecordBatch into table

* *GIVEN* an Arrow RecordBatch is available in memory
* *WHEN* user calls import_from_record_batch with RecordBatch
* *THEN* system SHALL convert RecordBatch to CSV format
* *AND* system SHALL stream CSV through HTTP tunnel

### Scenario: Import stream of RecordBatches

* *GIVEN* a Stream of RecordBatches is available
* *WHEN* user provides Stream of RecordBatches
* *THEN* system SHALL convert and stream each batch sequentially

### Scenario: Import from Arrow IPC file

* *GIVEN* an Arrow IPC file exists on disk
* *WHEN* user calls import_from_arrow_ipc with file path
* *THEN* system SHALL read Arrow IPC file and convert to CSV
* *AND* system SHALL stream through HTTP tunnel

### Scenario: Arrow type conversion preserves data

* *GIVEN* a RecordBatch contains various Arrow types
* *WHEN* RecordBatch is converted to CSV representation
* *THEN* system SHALL convert to appropriate CSV representation
* *AND* NULL values and special characters SHALL be escaped correctly

### Scenario: Export to RecordBatch stream

* *GIVEN* an Exasol table or query has data to export
* *WHEN* user calls export_to_record_batches
* *THEN* system SHALL return Stream of RecordBatches
* *AND* batches SHALL be created with configurable size

### Scenario: Export to Arrow IPC file

* *GIVEN* an output file path is specified for Arrow IPC
* *WHEN* user calls export_to_arrow_ipc with file path
* *THEN* system SHALL write Arrow IPC format file

### Scenario: Export preserves schema from Exasol metadata

* *GIVEN* an export operation has completed
* *WHEN* export completes
* *THEN* Arrow schema SHALL reflect Exasol column types

### Scenario: Configurable batch size for streaming

* *GIVEN* streaming export is configured with batch size
* *WHEN* user specifies batch size option
* *THEN* RecordBatches SHALL contain approximately specified number of rows

### Scenario: TLS for HTTP transport is configured uniformly via use_tls

* *GIVEN* an application configures Arrow RecordBatch import or export options
* *WHEN* the application enables or disables TLS on the HTTP transport tunnel
* *THEN* the option builders `ArrowImportOptions` and `ArrowExportOptions` SHALL expose a single fluent method `use_tls(bool)` to control HTTP-transport TLS
* *AND* the option builders MUST NOT expose `with_encryption()` (flag-style) or `use_encryption(bool)` aliases
* *AND* the underlying option struct field SHALL be named `use_tls` (renamed from `use_encryption`) so that public field access uses the same vocabulary as the builder
* *AND* the default value of `use_tls` SHALL remain `false` to preserve current Docker/self-signed-cert behavior
