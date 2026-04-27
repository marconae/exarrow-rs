# Changelog

## 0.12.0

- **Native Parquet import on Exasol 2025.1.11+**: When connected to Exasol 2025.1.11 or newer, `import_from_parquet`, `import_from_parquet_stream`, and `import_parquet_from_files` now serve raw Parquet bytes to the server via HTTP range requests instead of converting to CSV. This eliminates client-side CPU cost and reduces wire-bytes by 5–30x for typed columnar data.
- Auto-detected from the server's `release_version` at connection time with no configuration required.
- Override with `ParquetImportOptions::with_native_parquet(Some(false))` to force the CSV conversion path, or `Some(true)` to force native mode (returns a server error on pre-2025.1.11 servers).
- Older Exasol versions (7.x, 8.x) continue to use the existing CSV conversion path unchanged.

## 0.11.0

- **Breaking:** Renamed HTTP transport TLS option to `use_tls(bool)` uniformly across all import/export option builders. The old names `with_encryption` (Parquet import, Arrow import) and `use_encryption` (Parquet export, Arrow export) are removed. CSV builders were already using `use_tls` and are unchanged.
- **Feat:** `Database::connect()` now automatically issues `OPEN SCHEMA` when the connection URI or builder includes a schema. Callers no longer need to call `set_schema()` manually after connecting. If schema activation fails, `connect()` returns an error (no half-open connection).
- **Docs:** Added "WebSocket TLS vs HTTP Transport TLS" section to `docs/import-export.md` with Docker and production examples.
- **Docs:** Added "Which import path should I use?" decision guide to `docs/import-export.md`.
- **Docs:** Added real rustdoc examples to crate root (`src/lib.rs`); added `[package.metadata.docs.rs]` for stable docs.rs builds.
- **Docs:** Fixed example verification patterns in `examples/import_export.rs` to use actual row counts instead of batch counts.
- **Contributor:** Split `CLAUDE.md` into `AGENTS.md` (agent-facing) and `CONTRIBUTING.md` (human-facing). `CLAUDE.md` now imports `AGENTS.md` via `@AGENTS.md`.

## 0.10.0

- Native TCP protocol transport as the default, replacing WebSocket for query execution (16x throughput improvement). The native protocol uses Exasol's little-endian binary framing with ChaCha20 stream encryption and direct binary result set parsing.
- Feature flags: `native` (default) and `websocket` (opt-in fallback). Build with `--no-default-features --features websocket` to use the WebSocket transport exclusively.
- Connection string parameter `transport=native|websocket` to select transport at runtime when both features are compiled in.
- WebSocket transport (`tokio-tungstenite`) is now an optional dependency, reducing binary size for native-only builds.
- Zero-copy fetch optimization: wire bytes parse directly into Arrow builders in a single pass, eliminating the intermediate `ColumnData` allocation. DATE and TIMESTAMP columns convert to `Date32`/`TimestampMicrosecond` via integer arithmetic (no string formatting). Receive buffer is reused across fetches. Result: +36% throughput (900K → 1.22M rows/s), native now 3× faster than WebSocket.

## 0.9.0

- Upgrade ADBC dependency from 0.22 to 0.23 (breaking: methods now return Box<dyn RecordBatchReader>)

## 0.8.0

- **Breaking default change**: connections now default to `tls=true` to match the Exasol server 7.1+ requirement and every official Exasol driver (pyexasol, JDBC, Go, ODBC). Callers that relied on the previous `tls=false` default — e.g. to reach legacy (pre-7.1) Exasol servers — must set `?tls=false` explicitly on the connection string or `.use_tls(false)` on the builder.
- **Docker recipe**: Exasol Docker containers ship with a self-signed certificate. Connection strings must set `?validateservercertificate=0` (alias `validate_certificate=false`) to accept it. Example: `exasol://sys:exasol@localhost:8563?validateservercertificate=0` — no `?tls=true` needed.
- Certificate validation default (`validateservercertificate=true`) is unchanged and matches every official Exasol driver.

## 0.7.3

- Security: update vulnerable transitive dependencies to address 4 Dependabot advisories.
  - `lz4_flex` 0.12.0 → 0.12.1 (GHSA-vvp9-7p8x-rfvv, high — decompression may leak uninitialized memory).
  - `aws-lc-sys` 0.38.0 → 0.39.1 via `aws-lc-rs` 1.16.1 → 1.16.2 (GHSA-394x-vwmw-crm3 high — X.509 Name Constraints bypass; GHSA-9f94-5g5w-gf6r high — CRL Distribution Point scope check logic error).
  - `rustls-webpki` 0.103.9 → 0.103.11 (GHSA-pwjx-qhcg-rvj4, medium — CRL Distribution Point authority matching).

## 0.7.2

- Support RSA 1024-bit public keys during the Exasol login handshake (e.g. `demodb.exasol.com`). Previously failed with `Failed to parse RSA public key` because aws-lc-rs enforces a 2048-bit minimum; the driver now uses an in-tree PKCS#1 v1.5 encryption path covering 1024–8192 bit moduli.

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
