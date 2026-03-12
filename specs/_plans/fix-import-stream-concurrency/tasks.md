# Tasks: fix-import-stream-concurrency

## Phase 1: Root Cause Analysis
- [x] 1.1 Identify which test suite hangs (driver_manager_tests, not integration_tests)
- [x] 1.2 Identify which specific test hangs (first test_bulk_ingest_* after test_autocommit_toggle)
- [x] 1.3 Eliminate cargo-llvm-cov as cause (proven by split-step run 22908260507)
- [x] 1.4 Identify root cause: current_thread runtime cannot drive concurrent I/O for import protocol
- [x] 1.5 Write root-cause-analysis.md

## Phase 2: Code Fix
- [x] 2.1 Change `get_runtime()` from `new_current_thread()` to `new_multi_thread().worker_threads(2)` in `src/adbc_ffi.rs:82-90`
- [x] 2.2 Fix `test_driver_manager_matches_direct_api` to use `#[test]` instead of `#[tokio::test]` (nested runtime UB)
- [x] 2.3 Run driver_manager_tests without coverage instrumentation (llvm-cov atexit handler conflicts with FFI static runtime)
- [x] 2.4 Add `timeout-minutes: 30` to Integration Tests job in `.github/workflows/ci.yml`

## Phase 3: Spec Deltas
- [x] 3.1 Apply spec delta: adbc-driver/transactions/spec.md
- [x] 3.2 Apply spec delta: import-export/csv-import/spec.md
- [x] 3.3 Apply spec delta: adbc-driver/bulk-ingestion/spec.md
- [x] 3.4 Apply spec delta: websocket-client/protocol/spec.md

## Phase 4: Verification
- [x] 4.1 `cargo build --release --features ffi` passes
- [x] 4.2 `cargo test --lib` passes (900 unit tests)
- [x] 4.3 `cargo clippy --all-targets --all-features -- -W clippy::all` passes (0 warnings)
- [x] 4.4 `cargo fmt --all -- --check` passes
- [ ] 4.5 Push and monitor CI pipeline via `gh run watch` — pipeline MUST pass (not just "no hang")
- [ ] 4.6 If pipeline fails: read logs, fix, push, loop until green
