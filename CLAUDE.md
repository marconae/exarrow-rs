# CLAUDE.md

## Prerequisites

**Before running integration tests or examples**, start the Exasol Docker container yourself (don't ask the user):
```bash
docker run -d --name exasol-test -p 8563:8563 --privileged exasol/docker-db:latest
```

Default credentials: `sys` / `exasol` on `localhost:8563`

**Check Exasol is ready** before running tests:
```bash
exapump sql 'select 1'   # Uses default profile; should return "1"
```

## Build & Test Commands

```bash
# Build
cargo build                              # Debug build
cargo build --release --features ffi     # Release with FFI for driver manager

# Linting (MUST pass before committing)
cargo fmt --all                          # Format code
cargo fmt --all -- --check               # Check formatting
cargo clippy --all-targets --all-features -- -W clippy::all  # Lint (zero warnings required)

# Tests
cargo test --lib                         # Unit tests only
cargo test --test integration_tests      # Integration tests (requires Exasol)
cargo test --test driver_manager_tests   # Driver manager tests
cargo test --test import_export_tests -- --ignored  # Import/export tests (requires Exasol)

# Run single test
cargo test test_name                     # Run specific test by name
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

## Specifications

Specifications live in `specs/` using a `specs/<domain>/<feature>/spec.md` structure. Use the `speq` CLI to explore, search, and validate specs. Use Context7 MCP tools for third-party library research before implementing.

## Key Design Patterns

- **ADBC Driver Hierarchy:** Driver → Database → Connection → Statement
- Async-first: All I/O via Tokio
- Connection owns transport exclusively (no `Arc<Mutex<>>`)
- Statement is pure data; execution goes through Connection
- Import/Export uses HTTP tunneling with EXA protocol handshake

## Feature Flags

- `ffi` - Enable ADBC FFI/cdylib for driver manager integration
- `benchmark` - Enable benchmarking tools

## Important Constraints

- TLS enabled by default; production Exasol requires it
- Never log or expose connection passwords
- Integration tests require running Exasol instance

## Changelog

- `CHANGELOG.md` must be updated with every version bump
- Format: `## <version>` header followed by bullet points describing changes
- Entries should be concise, user-facing descriptions (not internal implementation details)
