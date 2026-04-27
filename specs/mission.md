# Mission: exarrow-rs

> An ADBC-compatible Exasol driver in Rust that opens up the Apache Arrow ecosystem for Exasol databases.

## Problem Statement

Exasol lacks an Arrow-native driver. Existing connectors require row-based data transfer, which is inefficient for analytical workloads and incompatible with the growing Arrow ecosystem (Polars, DataFusion, DuckDB, ADBC driver managers). exarrow-rs bridges this gap as the only Arrow-compatible driver for Exasol, enabling zero-copy columnar data transfer and integration with any ADBC-compliant tooling.

## Target Users

| Persona | Goal | Key Workflow |
|---------|------|--------------|
| Data engineer | Build pipelines and ETL workflows with Exasol | Connect via Rust API, bulk import/export CSV/Parquet/Arrow data |
| Application developer | Build applications that query Exasol | Use connection strings, execute queries, process Arrow RecordBatches |
| ADBC ecosystem user | Use Exasol from Python, Go, or Java via driver managers | Load the FFI cdylib through an ADBC driver manager (e.g., Python adbc-driver-manager, Polars) |

## Core Capabilities

1. **ADBC-compatible database connectivity** — standard Driver/Database/Connection/Statement hierarchy over Exasol's native TCP protocol (default) or WebSocket protocol (opt-in fallback)
2. **Bulk import/export via HTTP tunneling** — high-throughput data transfer for CSV, Parquet, and Arrow RecordBatch formats with parallel file support
3. **Arrow-native type conversion** — bidirectional mapping between Exasol and Arrow type systems with precision preservation
4. **FFI export for driver manager integration** — cdylib build target enabling any ADBC driver manager to load and use the driver
5. **Prepared statement support** — type-safe parameter binding and execution through the Exasol WebSocket protocol

## Out of Scope

- **Connection pooling** — left to the application layer
- **ORM / query builder** — raw SQL only, no abstraction layer
- **ETL/ELT transformations** — data is transferred as-is, transformations are the caller's responsibility

## Domain Glossary

| Term | Definition |
|------|------------|
| ADBC | Arrow Database Connectivity — standard API for database access using Apache Arrow |
| Arrow | Apache Arrow columnar memory format for efficient analytical data processing |
| RecordBatch | Arrow's unit of columnar data: a collection of equal-length arrays with a shared schema |
| EXA protocol | Exasol's WebSocket JSON protocol for commands and query execution (default port 8563) |
| HTTP tunneling | Reverse-connection pattern where the client opens an outbound TCP connection that Exasol's SQLProcess connects back through for bulk data transfer |
| Native TCP protocol | Exasol's binary protocol over TCP with ChaCha20 stream encryption, little-endian framing, and direct binary result sets (default transport) |
| cdylib | Rust C-compatible dynamic library used by ADBC driver managers to load the driver |

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Language | Rust (2021 edition) | Systems-level performance, memory safety |
| Async runtime | Tokio 1.42 | Multi-threaded async I/O |
| WebSocket | tokio-tungstenite 0.28 (optional) | Exasol WebSocket transport with TLS (opt-in fallback) |
| Arrow | arrow 57.1 / parquet 57.1 | Columnar data format and Parquet I/O |
| TLS | rustls 0.23 / rcgen 0.13 | Connection encryption and ad-hoc certificate generation for HTTP tunnels |
| Crypto | aws-lc-rs 1 | RSA encryption for Exasol password authentication |
| Stream cipher | chacha20 0.9 / cipher 0.4 | ChaCha20 encryption for native TCP protocol |
| Serialization | serde / serde_json | Exasol WebSocket JSON protocol |
| ADBC FFI | adbc_core / adbc_ffi 0.23 (optional) | C FFI bindings for driver manager integration |
| Testing | cargo test / mockall 0.14 | Unit and integration testing with mocking |

## Commands

```bash
# Build
cargo build                              # Debug build (native protocol, default)
cargo build --no-default-features --features websocket  # WebSocket-only build
cargo build --release --features ffi     # Release with FFI cdylib

# Test
cargo test --lib                         # Unit tests
cargo test --test integration_tests      # Integration tests (requires Exasol)
cargo test --test native_protocol_tests  # Native protocol tests (requires Exasol)
cargo test --test driver_manager_tests   # Driver manager tests
cargo test --test import_export_tests -- --ignored  # Import/export tests (requires Exasol)

# Lint & Format
cargo fmt --all                          # Format code
cargo clippy --all-targets --all-features -- -W clippy::all  # Lint (zero warnings required)

# Gather Code Coverage
cargo llvm-cov
```

## Feature Flags

| Flag | Default | Purpose |
|------|---------|---------|
| `native` | Yes | Native TCP binary protocol transport (ChaCha20 encryption, little-endian framing) |
| `websocket` | No | WebSocket JSON protocol transport (opt-in fallback, requires `tokio-tungstenite`) |
| `ffi` | No | ADBC FFI cdylib for driver manager integration |
| `benchmark` | No | Benchmarking tools and data generation |

## Repository Structure

```
benches/           # Rust benchmarks (feature-gated behind "benchmark")
docs/              # User-facing documentation (connection, queries, import/export, type mapping, driver manager)
examples/          # Runnable usage examples (basic_usage, driver_manager_usage, import_export)
scripts/           # CI helper scripts
specs/             # Feature specifications (speq format: specs/<domain>/<feature>/spec.md)
src/               # Library source code (ADBC driver, transport, import/export, Arrow conversion)
tests/             # Integration test suites (integration_tests, driver_manager_tests, import_export_tests)
```

## Architecture

Layered architecture following the ADBC Driver hierarchy: **Driver -> Database -> Connection -> Statement**.

- **ADBC layer** (`adbc/`) is the public API entry point. Driver creates Databases, Databases create Connections, Connections execute Statements.
- **Transport layer** (`transport/`) provides two protocol backends: the native TCP transport (`transport/native/`, default) using Exasol's binary protocol with ChaCha20 encryption, and the WebSocket transport (`transport/ws/`, opt-in via `transport=websocket`) using JSON over WebSocket. Both share HTTP tunneling for bulk transfers. Connection owns transport exclusively (no shared state).
- **Data layer** (`import/`, `export/`, `arrow_conversion/`) handles format conversion and bulk data transfer. Statement is pure data; all execution flows through Connection.
- All I/O is async-first via Tokio. Import/Export uses reverse-connection HTTP tunneling with the EXA protocol handshake.

## Constraints

- **Technical**: TLS enabled by default; production Exasol requires it. Credentials MUST NOT be logged or exposed.
- **Performance**: Arrow-native zero-copy where possible. Results streamed as RecordBatches to avoid memory bloat.
- **Testing**: Integration tests require a running Exasol instance (Docker: `exasol/docker-db:latest` on port 8563, credentials `sys`/`exasol`).

## External Dependencies

| Service | Purpose | Failure Impact |
|---------|---------|----------------|
| Exasol Database | Target database (native TCP or WebSocket API on port 8563) | All operations fail — the driver has no offline mode |
| Docker | Local development and testing (`exasol/docker-db:latest`) | Cannot run integration tests locally |
| GitHub Actions | CI/CD pipeline | No automated testing or release builds |
| Codecov | Coverage reporting | No coverage metrics, CI still passes |
