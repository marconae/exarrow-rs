# AGENTS.md

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

## Sandbox Configuration

Claude Code runs commands in a sandbox. Testing relies on the following already being configured in `.claude/settings.local.json` and the global `~/.claude/settings.json` — do not re-prompt the user about sandbox failures for these:

- **Network**: `localhost`, `127.0.0.1`, `*.exasol.com`, `crates.io`, `*.crates.io`, `docs.rs`, `github.com`, `*.github.com` are allowed (covers `cargo build/test`, `cargo fetch`, Exasol WebSocket on `localhost:8563`).
- **Filesystem**: the repository root is writable (covers `target/`, `Cargo.lock`, test fixtures).
- **Commands excluded from the sandbox** (run with full privileges): `docker`, `exapump`, `uv`, `qmd`, `zola`, `agent-browser`. `docker` is excluded because it needs `/var/run/docker.sock`, which the sandbox blocks.

If a test genuinely fails because of sandbox restrictions (not a real code bug), the fix is to extend `.claude/settings.local.json` — not to bypass the sandbox per-command. In particular, `cargo test`, `cargo clippy`, `cargo fmt`, `cargo build` all run fine inside the sandbox; do not disable sandboxing for them.

**Gotchas**:
- `excludedCommands` in project settings **replaces** (does not merge with) the global list. Always re-list every command you still need excluded (`docker`, `exapump`, `uv`, `qmd`, `zola`, `agent-browser`).
- Sandbox rules are loaded at **session start**. Edits to `settings.local.json` only take effect after restarting Claude Code.

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

- TLS is enabled by default in both the public connection-string/builder API and the low-level transport struct, matching Exasol 7.1+ (which requires TLS on port 8563) and all official Exasol drivers (pyexasol, JDBC, Go, ODBC).
- Certificate validation is on by default. Exasol Docker containers ship a self-signed certificate — test connection strings must set `?validateservercertificate=0` to accept it.
- Never log or expose connection passwords
- Integration tests require running Exasol instance

## Driver Manager Tests & FFI

Driver manager tests (`tests/driver_manager_tests.rs`) load `target/release/libexarrow_rs.so` (or `.dylib`) as a **dynamic library** at runtime. This has critical implications:

- **`cargo test` does NOT rebuild the cdylib.** Always run `cargo build --release --features ffi` before driver manager tests, otherwise tests run against a stale `.so`.
- **The FFI runtime is `multi_thread(2)`** (`src/adbc_ffi.rs`). This is required for import operations that need concurrent WebSocket + HTTP I/O inside `block_on`.
- **Do not run driver manager tests under `cargo-llvm-cov`.** The coverage instrumentation's atexit handlers conflict with the FFI static runtime, causing hangs.
- The FFI `OnceLock<Runtime>` is a process-global static inside the `.so` — it persists across all test invocations in the same process.

## CI Pipeline

- **Rust toolchain is pinned to 1.92.0** in `.github/workflows/ci.yml`. Keep CI and local toolchains in sync to avoid formatting drift.
- **GitHub Actions cache is immutable** — once a cache entry exists for a key based on `Cargo.lock`, it cannot be updated. The integration tests job explicitly rebuilds the release cdylib before driver manager tests to avoid stale artifacts.
- **`cargo test` captures stdout/stderr by default.** When adding diagnostic `eprintln!` traces for CI debugging, use `--nocapture`. Always verify diagnostic output is visible before adding more instrumentation.
- Integration tests job has `timeout-minutes: 30` to prevent runaway hangs.

## Debugging CI Hangs

When a CI test hangs:
1. **Verify your fix takes effect first** — if the test loads a dynamic library, confirm the library is rebuilt with your changes (not served from cache).
2. **Add ONE minimal trace and confirm it appears** in the CI log before adding elaborate instrumentation. Remember `--nocapture`.
3. **Don't trust a root cause analysis blindly** — verify the hypothesis by observing the actual behavior change. If 2 iterations of a fix don't work, the RCA is likely wrong; investigate from scratch.

## Changelog

- `CHANGELOG.md` must be updated with every version bump
- Format: `## <version>` header followed by bullet points describing changes
- Entries should be concise, user-facing descriptions (not internal implementation details)
