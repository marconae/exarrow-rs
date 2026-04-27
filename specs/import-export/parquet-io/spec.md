# Feature: Parquet I/O

Specifies Parquet file import and export capabilities. Import streams Parquet bytes through the HTTP transport tunnel either natively (Exasol 2025.1.11+) or after CSV conversion (older servers). Export continues to receive CSV from Exasol and convert to Parquet locally.

## Background

Parquet import operates over the same HTTP tunnel as CSV import but selects between two on-the-wire transport models based on the connected server's `release_version`:

- On Exasol 2025.1.11 and newer the server requests the Parquet file from the driver using **HTTP range requests**. The driver responds to `HEAD` with `200 OK` plus `Content-Length` and to `GET` with `Range: bytes=X-Y` using `206 Partial Content` carrying the requested byte slice. Multiple sequential HEAD/GET-Range requests typically arrive on the same connection (footer first, then row groups) until the server closes it. The generated SQL is `IMPORT INTO ... FROM PARQUET AT '...;MaxConcurrentReads=1' [PUBLIC KEY '...'] FILE '...parquet'`.
- On older servers the driver reads each Parquet `RecordBatch`, converts to CSV via `record_batch_to_csv`, streams the CSV body through the existing chunked-encoding response, and emits `FROM CSV ... FILE '...csv'`.

The Parquet variant of the IMPORT statement omits all CSV format options (no `ENCODING`, `COLUMN SEPARATOR`, `COLUMN DELIMITER`, `ROW SEPARATOR`, `SKIP`, `NULL`, `TRIM`, `REJECT LIMIT`) and never emits the `MULTIPLE LOCAL FILES` tag (Exasol opens one HTTP server per file for native Parquet import). The `;MaxConcurrentReads=1` suffix is appended inside the `AT '...'` URL of every file entry, matching the JDBC reference behavior. Path selection is automatic by default and can be overridden via `ParquetImportOptions::with_native_parquet(Some(true|false))`.

Export continues to receive CSV from Exasol, convert to Parquet locally with schema derived from Exasol metadata, and write to files or streams. The HTTP-transport TLS knob is exposed as `use_tls(bool)` on both option builders.

## Scenarios

### Scenario: Import Parquet file into table

* *GIVEN* a Parquet file exists on disk
* *AND* the connected Exasol server's `release_version` is below `2025.1.11`
* *WHEN* user calls import_from_parquet with table name and file path
* *THEN* system SHALL read the Parquet file, convert each RecordBatch to CSV, and stream the CSV body through the HTTP tunnel
* *AND* the generated IMPORT SQL SHALL use the `FROM CSV ... FILE 'NNN.csv'` clause

### Scenario: Native Parquet import on Exasol 2025.1.11+

* *GIVEN* a Parquet file exists on disk
* *AND* the connected Exasol server's `release_version` is at or above `2025.1.11`
* *WHEN* user calls import_from_parquet with table name and file path
* *THEN* the system MUST buffer the entire file's raw Parquet bytes in memory and MUST NOT invoke any CSV conversion path
* *AND* the system MUST serve the bytes via the range-request handler: replying to `HEAD /<file>` with `200 OK` plus `Content-Length: <total bytes>` and to `GET /<file>` with `Range: bytes=<start>-<end>` headers using `206 Partial Content` carrying the requested byte slice, looping until Exasol closes the connection
* *AND* the generated IMPORT SQL MUST use the `FROM PARQUET AT 'http(s)://addr;MaxConcurrentReads=1' [PUBLIC KEY '...'] FILE 'NNN.parquet'` form, with the `.parquet` extension and without any CSV format option clauses or the `MULTIPLE LOCAL FILES` tag

### Scenario: Import Parquet from stream

* *GIVEN* Parquet data is available as an async stream
* *WHEN* user provides AsyncRead for Parquet data
* *THEN* the system SHALL buffer the stream into a `Vec<u8>` in memory before serving it because Parquet requires footer random access
* *AND* on Exasol 2025.1.11+ the system SHALL serve the buffered Parquet bytes via HTTP range requests (no CSV conversion) and SHALL emit `FROM PARQUET AT '...;MaxConcurrentReads=1' ... FILE '...parquet'` SQL
* *AND* on servers below 2025.1.11 the system SHALL parse the Parquet, convert to CSV, and emit `FROM CSV` SQL as before

### Scenario: Import Parquet preserves data types

* *GIVEN* a Parquet file contains typed columns
* *WHEN* the file is imported via the native (2025.1.11+) path
* *THEN* the Parquet bytes SHALL reach Exasol unchanged so type fidelity SHALL be governed by the server's Parquet reader
* *AND* on the legacy CSV-conversion path the system SHALL convert types to CSV-compatible representations and SHALL preserve NULL values per the existing CSV mapping

### Scenario: Native Parquet import option overrides the server-version probe

* *GIVEN* an application that wants to force-test a specific import path
* *WHEN* `ParquetImportOptions::with_native_parquet(Some(true|false))` is set explicitly
* *THEN* the driver MUST honor that override regardless of the server's reported `release_version`
* *AND* when the override is `None` (the default) the driver SHALL probe the session's `release_version` and pick the path automatically
* *AND* if the override forces native streaming against an unsupported server, the resulting Exasol error MUST be surfaced to the caller verbatim

### Scenario: Native Parquet IMPORT SQL has no CSV format clauses and appends MaxConcurrentReads=1

* *GIVEN* the driver builds an IMPORT statement for the native Parquet path
* *WHEN* `ImportQuery::with_format(ImportFormat::Parquet)` is in effect
* *THEN* every `AT '...'` URL MUST end with `;MaxConcurrentReads=1` appended directly to the URL string, before the closing quote
* *AND* the SQL MUST NOT contain any `ENCODING`, `COLUMN SEPARATOR`, `COLUMN DELIMITER`, `ROW SEPARATOR`, `SKIP`, `NULL`, `TRIM`, or `REJECT LIMIT` clause
* *AND* the SQL MUST NOT contain `MULTIPLE LOCAL FILES`, regardless of how many files are listed
* *AND* when TLS is enabled, every `AT '...'` clause MUST be followed by a matching `PUBLIC KEY '<sha256-fingerprint>'` clause exactly as for the CSV path

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
