# Changelog

## 0.7.1

- Add support for certificate fingerprints

## 0.7.0

- ADBC bulk ingestion support (create, append, replace, create-append modes) via `IngestTargetTable` and `IngestMode` statement options
- `GetObjects` implementation returning catalog/schema/table/column metadata at configurable depth
- `GetTableSchema` for retrieving Arrow schema of existing Exasol tables
- `GetParameterSchema` for retrieving parameter types from prepared statements with Exasol-provided column names
- Transaction support with autocommit control, explicit `commit()` and `rollback()`
- FFI parameter binding: `bind()` + `execute_update()`/`execute()` flow with Arrow-to-Parameter conversion
- `CurrentCatalog` connection option returns "EXA"
- Fixed autocommit toggle to ensure connection is established before toggling
- Fixed INTERVAL and TIMESTAMP WITH LOCAL TIME ZONE parsing
- Fixed TIMESTAMP precision handling for sub-second values

## 0.6.4

- Removed WebSocket frame/message size limits to support large result sets (fixes #18)

## 0.6.3

- Updated `bytes` (1.11.0 → 1.11.1) and `time` (0.3.45 → 0.3.47) to fix security vulnerabilities

## 0.6.2

- Upgraded ADBC dependency from 0.21 to 0.22 (includes Windows build fix)
- Removed outdated driver manager test

## 0.6.1

- Fixed `import_from_parquet` and `import_from_parquet_files` hanging when `create_table_if_not_exists` is enabled and the CREATE TABLE DDL fails for reasons other than "table already exists" (e.g., nonexistent schema)

## 0.6.0

- CSV schema inference for automatic table creation on CSV imports
- Added schema inference examples for CSV and Parquet formats

## 0.5.3

- Fixed FFI statements opening a new WebSocket per query instead of reusing the connection session
- Added Python usage examples for ADBC driver manager integration
- Updated driver manager documentation examples

## 0.5.2

- Added documentation in [docs/](docs/)

## 0.5.0

- Schema inference for Parquet imports

## 0.4.0

- Parallel CSV and Parquet file imports

## <=0.3.2

- ADBC driver implementation
- Import/export capability via HTTP tunneling
- Arrow type mapping for Exasol types
