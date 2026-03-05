# Changelog

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
