# Contributing to exarrow-rs

Thank you for your interest in contributing! This guide covers everything you need to get started: setting up a local Exasol instance, running tests, understanding the codebase structure, and following project conventions.

## Prerequisites

You need a running Exasol instance to execute integration tests. The easiest way is to start the official Docker image:

```bash
docker run -d --name exasol-test -p 8563:8563 --privileged exasol/docker-db:latest
```

Default credentials: `sys` / `exasol` on `localhost:8563`.

Verify the database is ready before running tests:

```bash
exapump sql 'select 1'   # Should return "1"
```

## Build & Test Commands

```bash
# Build
cargo build                              # Debug build
cargo build --release --features ffi     # Release with FFI for driver manager

# Linting (must pass before committing)
cargo fmt --all                          # Format code
cargo fmt --all -- --check               # Check formatting only
cargo clippy --all-targets --all-features -- -W clippy::all  # Lint (zero warnings required)

# Tests
cargo test --lib                         # Unit tests only
cargo test --test integration_tests      # Integration tests (requires Exasol)
cargo test --test driver_manager_tests   # Driver manager tests
cargo test --test import_export_tests -- --ignored  # Import/export tests (requires Exasol)

# Run a single test
cargo test test_name                     # By name
cargo test --test integration_tests test_select_from_dual  # Single integration test
```

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

## Key Design Patterns

- **ADBC Driver Hierarchy:** Driver → Database → Connection → Statement
- Async-first: all I/O via Tokio
- Connection owns transport exclusively (no `Arc<Mutex<>>`)
- Statement is pure data; execution goes through Connection
- Import/Export uses HTTP tunneling with EXA protocol handshake

## Feature Flags

- `ffi` — Enable ADBC FFI/cdylib for driver manager integration
- `benchmark` — Enable benchmarking tools

## Important Constraints

- TLS is enabled by default in both the public connection-string/builder API and the low-level transport struct, matching Exasol 7.1+ (which requires TLS on port 8563) and all official Exasol drivers (pyexasol, JDBC, Go, ODBC).
- Certificate validation is on by default. Exasol Docker containers ship a self-signed certificate — test connection strings must include `?validateservercertificate=0`.
- Never log or expose connection passwords.
- Integration tests require a running Exasol instance.

## Running Integration Tests Locally

1. Start the Docker container as shown in the Prerequisites section.
2. Wait for the container to finish initializing (the first start can take 60–120 seconds).
3. Verify readiness with `exapump sql 'select 1'`.
4. Run the desired test suite:

```bash
cargo test --test integration_tests
cargo test --test import_export_tests -- --ignored
```

## Driver Manager Tests & FFI

Driver manager tests (`tests/driver_manager_tests.rs`) load `target/release/libexarrow_rs.so` (or `.dylib`) as a dynamic library at runtime:

- **`cargo test` does NOT rebuild the cdylib.** Always run `cargo build --release --features ffi` before driver manager tests, otherwise tests run against a stale `.so`.
- **Do not run driver manager tests under `cargo-llvm-cov`.** The coverage instrumentation's atexit handlers conflict with the FFI static runtime, causing hangs.

## CI Pipeline

- **Rust toolchain is pinned to 1.92.0** in `.github/workflows/ci.yml`. Keep your local toolchain in sync to avoid formatting drift.
- **GitHub Actions cache is immutable** — once a cache entry exists for a key based on `Cargo.lock`, it cannot be updated. The integration tests job explicitly rebuilds the release cdylib before driver manager tests to avoid stale artifacts.
- **`cargo test` captures stdout/stderr by default.** When adding diagnostic traces for CI debugging, use `--nocapture`.
- The integration tests job has a 30-minute timeout to prevent runaway hangs.

## Changelog

- `CHANGELOG.md` must be updated with every version bump.
- Format: `## <version>` header followed by bullet points describing changes.
- Entries should be concise and user-facing — avoid internal implementation details.
