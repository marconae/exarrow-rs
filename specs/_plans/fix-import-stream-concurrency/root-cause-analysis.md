# Root Cause Analysis: CI Pipeline Hang

## Summary

The CI pipeline hangs indefinitely (~6 hours until GitHub Actions cancels) in the
**"Run integration tests with coverage"** step. The hang occurs specifically in the
**`driver_manager_tests`**, at the first `test_bulk_ingest_*` test.

## Evidence

### 1. The hang is in `driver_manager_tests`, NOT `integration_tests`

**Source:** CI run `22908260507` (commit `b2d82ab`), which split the two test suites
into separate steps.

| Step | Duration | Result |
|------|----------|--------|
| `cargo llvm-cov ... --test integration_tests` | 1m 40s | SUCCESS (46/46 pass) |
| `cargo test ... --test driver_manager_tests` | 51m (cancelled) | HANG |

### 2. The hang is NOT caused by `cargo-llvm-cov`

The same run (`22908260507`) ran `driver_manager_tests` with plain `cargo test`
(no coverage instrumentation). It still hung. This disproves the hypothesis in
commit `b2d82ab` ("OnceLock<Runtime> conflicts with cargo-llvm-cov's atexit handlers").

### 3. The last passing test before the hang

From the CI log archive:

```
test test_autocommit_toggle ... ok
[51 minutes of silence — no more output until cancellation]
```

The next test alphabetically is `test_bulk_ingest_append`. This is the **first test
that exercises the import protocol** via the FFI layer.

### 4. All 10 CI runs on this branch show the same pattern

| Run ID | Duration | Result |
|--------|----------|--------|
| 22926859672 | 6h 9m | cancelled |
| 22908260507 | 1h 1m | cancelled |
| 22895247585 | 5h 32m | cancelled |
| 22859327650 | 6h 9m | cancelled |
| 22851200929 | 3h 26m | cancelled |
| 22849857838 | 34m | cancelled (manual) |
| 22848566585 | 34m | cancelled (manual) |
| 22779831578 | 6h 9m | cancelled |
| 22779501338 | 4m | **failure** (lint, not hang) |
| 22772628030 | 6h 9m | cancelled |

## Root Cause

### The `current_thread` tokio runtime cannot drive concurrent I/O in the import protocol

The import protocol requires **two simultaneous I/O channels**:

1. **WebSocket** (port 8563): sends `IMPORT INTO ... FROM CSV AT 'http://...'` SQL
   and waits for the row-count response
2. **HTTP transport** (same port, separate TCP connection): receives HTTP GET from
   Exasol, then sends CSV data as chunked HTTP response

The code uses `tokio::select!` to poll both futures concurrently
(`src/import/csv.rs:839-852`). This works correctly on a **multi-threaded runtime**
because the I/O reactor runs on a background thread, waking futures as data arrives
on either socket.

On the **`current_thread` runtime** (`src/adbc_ffi.rs:85`), the I/O reactor is
only driven when `block_on` is actively polling. Inside the `select!`, both futures
yield (waiting for I/O), and the runtime's single-threaded reactor must process
events for **both** the WebSocket and the HTTP TCP socket. In practice, on Linux CI
runners, the `current_thread` reactor does not reliably wake both futures — the HTTP
socket's readiness notification is missed or starved, causing the `select!` to hang
indefinitely.

This explains why:
- **Simple queries work** (autocommit, DDL, DQL) — they only use the WebSocket
- **Import hangs** — it requires concurrent I/O on two different sockets
- **The hang is consistent** — it always occurs at the first bulk ingest test
- **Previous fixes didn't help** — they targeted the wrong component (tokio::spawn,
  llvm-cov, error handling) while the runtime type was the actual issue

### Contributing factor: `test_driver_manager_matches_direct_api` uses `#[tokio::test]`

This test (`tests/driver_manager_tests.rs`) creates its own tokio runtime via
`#[tokio::test]`, then calls FFI methods that internally call
`get_runtime().block_on(...)`. This is a **nested runtime** — tokio detects this
and panics ("Cannot start a runtime from within a runtime"). The panic crosses the
C ABI boundary via the ADBC driver manager FFI, causing **undefined behavior**.

This test runs alphabetically AFTER the bulk ingest tests, so it's not the primary
cause of the hang, but it would cause a crash/abort even if the bulk ingest issue
were fixed.

## What Was Tried (and Why It Failed)

| Commit | Theory | Fix | Why It Didn't Work |
|--------|--------|-----|--------------------|
| `1ca6157` | `select!` logic wrong + missing autocommit | Fixed select! to await SQL after stream succeeds | The `select!` logic was correct; the runtime type was the issue |
| `b2d82ab` | `OnceLock<Runtime>` + llvm-cov atexit conflict | Moved driver_manager_tests out of llvm-cov | Still hung — llvm-cov was not the cause |
| `6987368` | `tokio::spawn` + `block_on` = deadlock | Removed `tokio::spawn`, fixed error handling | `tokio::spawn` was already wrong on `current_thread`, but removing it didn't fix the concurrent I/O issue |

## Fix

### Primary: Change runtime from `current_thread` to `multi_thread`

In `src/adbc_ffi.rs:82-90`, change:

```rust
fn get_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime for ADBC FFI")
    })
}
```

With a multi-threaded runtime:
- The I/O reactor runs on a dedicated background thread
- Both WebSocket and HTTP sockets are serviced independently
- `block_on` parks the calling thread and waits; it does NOT drive the reactor
- `select!` works reliably because futures are woken by the background I/O thread

### Secondary: Fix `test_driver_manager_matches_direct_api`

Change from `#[tokio::test]` to `#[test]` with an explicit runtime for the async
portion, so the FFI `block_on` calls don't encounter a nested runtime context.

### Tertiary: Add CI timeout and import timeout

- Add `timeout-minutes: 30` to the Integration Tests job
- Add a configurable timeout to `handle_import_request` to prevent infinite waits
