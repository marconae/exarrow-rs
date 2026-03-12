# Plan: fix-import-stream-concurrency

## Summary

Fixes the CI pipeline hang caused by the `current_thread` tokio runtime in the FFI layer being unable to drive concurrent I/O for the import protocol. The import requires simultaneous WebSocket (SQL) and HTTP (data) channels, which only works reliably on a multi-threaded runtime. Also fixes a nested-runtime UB bug in a driver manager test and adds CI safeguards.

See `root-cause-analysis.md` for the full investigation with evidence.

## Design

### Goals / Non-Goals

- Goals
    - Fix the CI hang by changing the FFI runtime to `multi_thread`
    - Fix the nested-runtime UB in `test_driver_manager_matches_direct_api`
    - Add CI timeout to prevent 6h hangs in the future
    - Keep all spec deltas from the previous iteration accurate
- Non-Goals
    - Refactoring the import pipeline architecture
    - Adding import-level timeouts (deferred — not blocking CI)

### Architecture

The ADBC FFI layer uses a `static OnceLock<Runtime>` shared across all FFI calls.
Changing from `current_thread` to `multi_thread(2)` ensures:

1. A dedicated I/O reactor thread services all socket events
2. `block_on` parks the calling thread (doesn't drive the reactor)
3. `tokio::select!` between WebSocket and HTTP futures works reliably
4. No behavioral change for simple queries (they still use `block_on`)

### Trade-offs

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `multi_thread(2)` runtime | Keep `current_thread` + spawn I/O to background thread | `multi_thread` is simpler, proven, and matches how ADBC drivers are typically used |
| 2 worker threads | 1 worker thread | 2 ensures the reactor thread is always available even if one worker is blocked |
| `#[test]` with explicit runtime | Remove `test_driver_manager_matches_direct_api` | The test validates an important property (FFI parity with direct API) |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| CSV Import | CHANGED | `import-export/csv-import/spec.md` |
| Transactions (ADBC) | CHANGED | `adbc-driver/transactions/spec.md` |
| Bulk Ingestion | CHANGED | `adbc-driver/bulk-ingestion/spec.md` |
| WebSocket Protocol | CHANGED | `websocket-client/protocol/spec.md` |

## Implementation Tasks

1. In `src/adbc_ffi.rs:82-90`: change `new_current_thread()` to `new_multi_thread().worker_threads(2)`
2. In `tests/driver_manager_tests.rs`: change `test_driver_manager_matches_direct_api` from `#[tokio::test]` to `#[test]` with explicit `tokio::runtime::Runtime` for the async part
3. In `.github/workflows/ci.yml`: add `timeout-minutes: 30` to Integration Tests job
4. Ensure the integration tests step runs both test suites under `cargo llvm-cov` (single step, as in commit `6987368`)

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Concurrent SQL and stream completion ordering | Integration | `tests/driver_manager_tests.rs` | `test_bulk_ingest_create` |
| Concurrent SQL and stream completion ordering | Integration | `tests/driver_manager_tests.rs` | `test_bulk_ingest_append` |
| Concurrent SQL and stream completion ordering | Integration | `tests/driver_manager_tests.rs` | `test_bulk_ingest_replace` |
| Concurrent SQL and stream completion ordering | Integration | `tests/driver_manager_tests.rs` | `test_bulk_ingest_create_append` |
| Stream error aborts import immediately | Integration | `tests/driver_manager_tests.rs` | (covered by existing error paths) |
| Autocommit enabled by default | Integration | `tests/driver_manager_tests.rs` | `test_autocommit_default` |
| Disable autocommit for explicit transactions | Integration | `tests/driver_manager_tests.rs` | `test_autocommit_toggle` |
| Commit transaction | Integration | `tests/driver_manager_tests.rs` | `test_transaction_commit` |
| Rollback transaction | Integration | `tests/driver_manager_tests.rs` | `test_transaction_rollback` |
| Toggle autocommit | Integration | `tests/driver_manager_tests.rs` | `test_autocommit_toggle` |
| Dotted table name without explicit schema | Integration | `tests/driver_manager_tests.rs` | `test_bulk_ingest_create` |
| Set session attributes command | Integration | `tests/driver_manager_tests.rs` | `test_transaction_commit` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| CSV Import (bulk ingest) | `cargo test --test driver_manager_tests test_bulk_ingest` | 4 tests pass (create, append, replace, create_append) |
| Transactions | `cargo test --test driver_manager_tests test_transaction` | 2 tests pass (commit, rollback) |
| All driver manager tests | `cargo test --test driver_manager_tests` | 29 tests pass, 0 failures |

### CI Pipeline (MANDATORY)

The fix is NOT verified until the GitHub Actions CI pipeline passes:

```bash
# Push and watch the pipeline
git push origin feature/0.7.0-adbc-compliance
gh run watch --exit-status
```

Expected: all jobs (Build, Lint, License Check, Unit Tests, Integration Tests) pass.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `cargo build --release --features ffi` | Exit 0 |
| Test (unit) | `cargo test --lib` | 0 failures |
| Test (driver manager) | `cargo test --test driver_manager_tests` | 0 failures |
| Lint | `cargo clippy --all-targets --all-features -- -W clippy::all` | 0 errors/warnings |
| Format | `cargo fmt --all -- --check` | No changes |
| CI Pipeline | `gh run watch --exit-status` | Exit 0 (green pipeline) |
