# Feature: Parallel Import

Specifies parallel file import capabilities for CSV and Parquet formats, leveraging Exasol's native IMPORT parallelization with multiple HTTP transport connections.

## Background

Parallel import establishes N parallel HTTP transport connections (one per file), each with its own EXA tunneling handshake to obtain a unique internal address. The generated IMPORT SQL uses multiple `AT '...' FILE '...'` clauses referencing each internal address. For Parquet, the wire model follows the same dual-path rule as single-file Parquet import: on Exasol 2025.1.11+ the driver serves each file's raw Parquet bytes from memory via HTTP range requests on its own connection and emits `FROM PARQUET AT 'addr;MaxConcurrentReads=1' [PUBLIC KEY '...'] FILE 'NNN.parquet'` (no CSV format clauses, no `MULTIPLE LOCAL FILES` tag); on older servers the driver converts each file to CSV in parallel and emits `FROM CSV ... FILE 'NNN.csv'` with the usual format options. Single-file imports delegate to the existing optimized single-file path for backward compatibility. The system accepts both single paths and collections via the IntoFileSources trait. Error handling follows a fail-fast strategy, aborting all operations immediately upon first failure.

## Scenarios

### Scenario: Import multiple CSV files in parallel

* *GIVEN* multiple CSV files exist on disk for import
* *WHEN* user calls import_csv_from_files with a list of CSV file paths
* *THEN* system SHALL establish N parallel HTTP transport connections (one per file)
* *AND* system SHALL perform EXA tunneling handshake on each connection to obtain unique internal addresses
* *AND* system SHALL build IMPORT SQL with multiple FILE clauses referencing each internal address

### Scenario: Generated SQL uses multiple AT/FILE clauses

* *GIVEN* a parallel import is being prepared with N files
* *WHEN* parallel import is initiated with N files
* *THEN* generated SQL SHALL follow format: `FROM CSV AT 'addr1' FILE '001.csv' AT 'addr2' FILE '002.csv' ...`
* *AND* each file SHALL have a unique internal address

### Scenario: Import multiple Parquet files in parallel

* *GIVEN* multiple Parquet files exist on disk for import
* *AND* the connected Exasol server's `release_version` is below `2025.1.11`
* *WHEN* user calls import_parquet_from_files with a list of Parquet file paths
* *THEN* system SHALL convert all Parquet files to CSV format in parallel using tokio tasks and stream the converted CSV through N parallel HTTP transport connections
* *AND* the generated IMPORT SQL SHALL use `FROM CSV` with `.csv` file names and the existing CSV format option clauses

### Scenario: Native parallel Parquet import on Exasol 2025.1.11+

* *GIVEN* multiple Parquet files exist on disk for import
* *AND* the connected Exasol server's `release_version` is at or above `2025.1.11`
* *WHEN* user calls import_parquet_from_files with a list of Parquet file paths
* *THEN* the system MUST establish N parallel HTTP transport connections and serve each file's raw Parquet bytes through its corresponding connection via HTTP range requests, without any CSV conversion
* *AND* the generated IMPORT SQL MUST emit `FROM PARQUET AT 'addr1;MaxConcurrentReads=1' [PUBLIC KEY '...'] FILE '001.parquet' AT 'addr2;MaxConcurrentReads=1' [PUBLIC KEY '...'] FILE '002.parquet' ...` with one `.parquet` entry per file and the `;MaxConcurrentReads=1` suffix on every URL
* *AND* the generated SQL MUST NOT contain `MULTIPLE LOCAL FILES`, `ENCODING`, `COLUMN SEPARATOR`, `COLUMN DELIMITER`, `ROW SEPARATOR`, `SKIP`, `NULL`, `TRIM`, or `REJECT LIMIT`

### Scenario: Parquet conversion is parallelized

* *GIVEN* multiple Parquet files need conversion to CSV
* *AND* the connected server is below 2025.1.11 so the CSV-conversion path is selected
* *WHEN* multiple Parquet files are provided
* *THEN* system SHALL convert files concurrently using spawn_blocking tasks
* *AND* system SHALL NOT wait for one conversion to complete before starting another
* *AND* on Exasol 2025.1.11+ no client-side conversion SHALL run because the native path serves Parquet bytes directly via range requests

### Scenario: Single file delegates to existing implementation

* *GIVEN* only one file is provided for parallel import
* *WHEN* user calls import_csv_from_files with a single file path
* *THEN* system SHALL delegate to existing import_from_file implementation
* *AND* behavior SHALL be identical to calling import_from_file directly

### Scenario: API accepts both single path and collection

* *GIVEN* the import API is called with file paths
* *WHEN* user provides either PathBuf or Vec<PathBuf> to import method
* *THEN* system SHALL accept both forms via IntoFileSources trait
* *AND* single path SHALL be treated as collection of one

### Scenario: Fail-fast on connection error

* *GIVEN* a parallel import with multiple HTTP connections is in progress
* *WHEN* any HTTP transport connection fails during parallel import
* *THEN* system SHALL abort all remaining connection attempts immediately
* *AND* system SHALL close all established connections gracefully
* *AND* system SHALL return error with context about which connection failed

### Scenario: Fail-fast on streaming error

* *GIVEN* parallel file streaming is in progress
* *WHEN* any file streaming fails during parallel import
* *THEN* system SHALL abort all other streaming operations immediately
* *AND* system SHALL return error indicating failed file index and path

### Scenario: Fail-fast on Parquet conversion error

* *GIVEN* parallel Parquet conversion is in progress on a server below 2025.1.11
* *WHEN* any Parquet file fails to convert to CSV
* *THEN* system SHALL abort all other conversion tasks immediately and SHALL return an error indicating which file failed conversion
* *AND* on Exasol 2025.1.11+ the equivalent fail-fast behavior SHALL apply to file-read errors during native range-request serving and SHALL identify the failing file index and path
