# Project Mission

## Purpose

`exarrow-rs` is an ADBC (Arrow Database Connectivity) compatible driver for Exasol databases with Apache Arrow data
format support. It enables efficient columnar data transfer, making it ideal for analytical workloads and data science
applications.

**Goals:**

- Provide a standard ADBC interface for Exasol connectivity
- Enable high-performance data transfer using Apache Arrow format
- Support both direct Rust API usage and FFI-based driver manager integration
- Community-maintained alternative to official drivers

## Tech Stack

**Language:** Rust 2021 edition

**Core Dependencies:**

- `arrow` / `arrow-array` / `arrow-schema` (v57) - Apache Arrow columnar format
- `tokio` (v1.42) - Async runtime with multi-threading
- `tokio-tungstenite` (v0.28) - WebSocket client with TLS
- `serde` / `serde_json` - Serialization
- `rsa` / `base64` - Exasol authentication encryption
- `adbc_core` / `adbc_ffi` (v0.21, optional) - ADBC FFI export

**Build & Test:**

- `cargo build` - Build library
- `cargo build --release --features ffi` - Build FFI/cdylib for driver manager
- `cargo test` - Run unit tests
- `cargo test --features ffi --test integration_tests -- --ignored` - Run integration tests (requires Exasol)
- `cargo llvm-cov` - Code coverage

**Linting:**

- `cargo fmt` - Code formatting (see `rustfmt.toml`)
- `cargo clippy` - Linting with warnings as errors

**Formatting Configuration (`rustfmt.toml`):**

- Max width: 100 characters
- Import granularity: Crate
- Import grouping: Std → External → Crate

**Clippy Configuration (`.clippy.toml`):**

- Cognitive complexity threshold: 10

## Project Conventions

### Architecture Patterns

**Module Structure:**

```
src/
├── adbc/           # ADBC interface (Driver, Database, Connection, Statement)
├── arrow_conversion/  # Exasol → Arrow type conversion
├── connection/     # Session management, auth, connection params
├── query/          # Statement execution, prepared statements, results
├── transport/      # WebSocket protocol, message handling
├── types/          # Exasol type system, schema mapping
├── error.rs        # Error types (thiserror-based)
├── lib.rs          # Public API exports
└── adbc_ffi.rs     # C FFI bindings (feature-gated)
```

**Design Patterns:**

- Async-first: All I/O operations are async using Tokio
- Builder pattern: Connection parameters via URL parsing
- Type-safe conversions: Dedicated mappers between Exasol and Arrow types
- Feature flags: FFI support optional via `ffi` feature

**Crate Types:**

- `lib` - Rust library for direct usage
- `cdylib` - C dynamic library for ADBC driver manager

## Domain Context

**ADBC (Arrow Database Connectivity):**

- Standard API for database access using Apache Arrow
- Driver hierarchy: Driver → Database → Connection → Statement
- Results returned as Arrow RecordBatches

**Exasol Specifics:**

- WebSocket-based protocol (port 8563 default)
- RSA-encrypted password authentication
- Connection string format: `exasol://[user[:password]@]host[:port][/schema][?params]`
- TLS enabled by default

**Type Mapping:**
| Exasol Type              | Arrow Type                 | Notes                              |
|--------------------------|----------------------------|------------------------------------|
| `BOOLEAN`                | `Boolean`                  |                                    |
| `CHAR`, `VARCHAR`        | `Utf8`                     |                                    |
| `DECIMAL(p, s)`          | `Decimal128(p, s)`         | Precision 1-36                     |
| `DOUBLE`                 | `Float64`                  |                                    |
| `DATE`                   | `Date32`                   |                                    |
| `TIMESTAMP`              | `Timestamp(Microsecond)`   | Exasol: 0-9 fractional digits      |
| `INTERVAL YEAR TO MONTH` | `Interval(MonthDayNano)`   |                                    |
| `INTERVAL DAY TO SECOND` | `Interval(MonthDayNano)`   | 0-9 fractional seconds             |
| `GEOMETRY`               | `Binary`                   | WKB format                         |

## Important Constraints

- **Community maintained**: Not officially supported by Exasol
- **Exasol dependency**: Integration tests require running Exasol instance (Docker available)
- **TLS requirement**: Production Exasol typically requires TLS
- **Credential security**: Never log or expose connection passwords

## Performance Considerations

- **Columnar format**: Arrow provides efficient memory layout for analytical queries
- **Async I/O**: Non-blocking WebSocket operations via Tokio
- **Batch processing**: Results streamed as RecordBatches to avoid memory bloat
- **Connection pooling**: Not built-in; manage at application level if needed

## External Dependencies

- **Exasol Database**: Target database (WebSocket API on port 8563)
- **Docker**: For local development/testing (`exasol/docker-db:latest`)
- **Codecov**: Coverage reporting in CI
- **GitHub Actions**: CI/CD pipeline
