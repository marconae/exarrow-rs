# Plan: fix-adbc-compliance-failures

## Summary

Fixes 42 failing ADBC Driver Foundry validation tests by implementing FFI-layer parameter binding (bind → execute flow), exposing CurrentCatalog as "EXA", using Exasol-provided parameter names in get_parameter_schema, and ensuring autocommit toggle works on established connections.

## Design

### Context

The ADBC Driver Foundry runs the `adbc_drivers_validation` test suite against exarrow-rs via the FFI cdylib. 42 tests fail across 5 root causes, all in `src/adbc_ffi.rs`:

1. **Parameter binding not wired through FFI execute paths** (28 bind tests + test_parameter_execute = 29 failures): `FfiStatement::execute_update()` and `FfiStatement::execute()` call `conn.execute_update(&sql)` / `conn.query(&sql)` directly, completely ignoring `self.bound_data` and `self.prepared`. The bound RecordBatch is never consumed.
2. **CurrentCatalog not exposed** (11 get_objects failures): `get_option_string` doesn't handle `OptionConnection::CurrentCatalog`, so validation tests that compare `driver.features.current_catalog` against returned catalog names get `None` vs `"EXA"`.
3. **Parameter schema field naming** (1 failure): `get_parameter_schema` generates `param_{i}` names but the Exasol prepared statement response includes actual column names that should be used.
4. **Autocommit toggle on established connections** (1 failure): `set_option(AutoCommit)` only calls `begin_transaction()` when `self.inner.is_some()` but doesn't call `ensure_connected()` first, so an established connection in error state fails.
5. **TIME type / table cleanup** (10 failures): Exasol doesn't support TIME; table-already-exists errors from missing teardown. These are foundry test-harness issues to be fixed via xfail/skip in the foundry repo's quirks config, not in exarrow-rs. See **Out-of-Scope: Foundry-Side Fixes** below.

- **Goals** — Fix all 32 failures attributable to exarrow-rs (categories 1-4). Document the 10 remaining failures (category 5) as foundry-side configuration issues.
- **Non-Goals** — Implementing batch-level parameter binding (multiple rows per execute). For now, iterate row-by-row over the bound RecordBatch.

### Decision

#### Architecture

All fixes target `src/adbc_ffi.rs`. No changes to the core library (`src/query/`, `src/adbc/`, `src/connection/`).

```
FfiStatement::bind(batch)
    └─► self.bound_data = Some(batch)

FfiStatement::execute_update() / execute()
    ├─► if self.bound_data is Some:
    │     ├─► prepare() if not already prepared
    │     ├─► for each row in bound RecordBatch:
    │     │     ├─► extract parameter values → Parameter enum
    │     │     └─► conn.execute_prepared_update(&prepared_stmt)
    │     └─► return aggregated row count / last result set
    └─► else: execute plain SQL (existing path)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Lazy prepare on bind+execute | `FfiStatement::execute_update/execute` | ADBC spec allows bind before prepare; driver auto-prepares |
| Arrow-to-Parameter conversion | New helper function | Convert Arrow array values to the `Parameter` enum per row |
| Eager connection establishment | `FfiConnection::set_option(AutoCommit)` | Ensure connection exists before toggling server-side autocommit |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Row-by-row execution of bound RecordBatch | Batch execute via Exasol protocol | Simpler; Exasol's prepared statement protocol supports single-row execution; batch can be added later |
| Auto-prepare on bind+execute | Require explicit prepare before bind | ADBC spec says bind can be called before prepare; auto-prepare is standard driver behavior |
| Return "EXA" for CurrentCatalog getter | Return NotImplemented | Validation tests require it; Exasol has exactly one catalog concept mapped to "EXA" |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| FFI parameter binding and execution | CHANGED | `prepared-statements/binding-and-execution/spec.md` |
| GetObjects current catalog | CHANGED | `adbc-driver/get-objects/spec.md` |
| Get parameter schema naming | CHANGED | `adbc-driver/get-parameter-schema/spec.md` |
| Transaction autocommit toggle | CHANGED | `adbc-driver/transactions/spec.md` |

## Implementation Tasks

1. **Implement Arrow-to-Parameter conversion helper** — Add a function that extracts a value from an Arrow array at a given row index and converts it to the `Parameter` enum. Handle all Arrow types that map to Exasol types (Int16/32/64, Float32/64, Utf8/LargeUtf8, Binary/LargeBinary, Boolean, Date32, Timestamp, Decimal128).

2. **Wire bound data through execute_update** — In `FfiStatement::execute_update()`, detect `self.bound_data`. If present: auto-prepare if needed, iterate rows, bind parameters via `PreparedStatement::bind()`, call `conn.execute_prepared_update()`, aggregate row counts.

3. **Wire bound data through execute (execute_query)** — In `FfiStatement::execute()`, detect `self.bound_data`. If present: auto-prepare if needed, iterate rows, bind parameters, call `conn.execute_prepared()`, collect result batches.

4. **Expose CurrentCatalog in get_option_string** — Add a `OptionConnection::CurrentCatalog` arm to `FfiConnection::get_option_string()` that returns `"EXA"`.

5. **Use Exasol parameter names in get_parameter_schema** — In `FfiStatement::get_parameter_schema()`, use the column name from the prepared statement metadata instead of `param_{i}`.

6. **Fix autocommit toggle to ensure connection** — In `FfiConnection::set_option(AutoCommit)`, replace the `if self.inner.is_some()` guard with unconditional `ensure_connected()` when transitioning states. This ensures the connection is established and the server-side autocommit attribute is toggled.

7. **Add regression integration tests** — Add new tests in `tests/driver_manager_tests.rs` (details below). These tests load the release cdylib and run against a live Exasol instance at `localhost:8563`.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1 (Arrow-to-Parameter helper) |
| Group B | Task 4 (CurrentCatalog), Task 5 (parameter names), Task 6 (autocommit) |
| Group C | Task 2 (execute_update binding), Task 3 (execute_query binding) |
| Group D | Task 7 (tests) |

Sequential dependencies:
- Group A → Group C (C depends on the conversion helper from A)
- Group B and Group C → Group D (tests depend on all fixes)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None identified | — | All changes are additions or modifications to existing code |

## Verification

### Scenario Coverage

All tests are integration tests in `tests/driver_manager_tests.rs`. They load `target/release/libexarrow_rs.dylib` (or `.so`) via the ADBC driver manager and connect to Exasol at `localhost:8563` (credentials `sys`/`exasol`).

| Scenario | Test Type | Test Location | Test Name | Description |
|----------|-----------|---------------|-----------|-------------|
| FFI bind and execute_update with parameters | Integration | `tests/driver_manager_tests.rs` | `test_bind_execute_update` | Creates a table, binds a RecordBatch with INT/VARCHAR values, executes INSERT via bind+execute_update, then SELECTs to verify rows were inserted correctly. |
| FFI bind and execute_query with parameters | Integration | `tests/driver_manager_tests.rs` | `test_bind_execute_query` | Prepares `SELECT ? + 1`, binds a RecordBatch with INT values, calls execute (execute_query), and verifies the result RecordBatch contains the expected computed values. |
| GetObjects CurrentCatalog option | Integration | `tests/driver_manager_tests.rs` | `test_get_current_catalog` | Calls `get_option_string(OptionConnection::CurrentCatalog)` and asserts it returns `"EXA"`. |
| Get parameter schema naming | Integration | `tests/driver_manager_tests.rs` | `test_parameter_schema_names` | Prepares `SELECT 1 + ?`, calls `get_parameter_schema()`, and asserts the field name is the Exasol-provided name (not `param_0`). Also asserts the field type is appropriate (not always `Utf8`). |
| Toggle autocommit with DML | Integration | `tests/driver_manager_tests.rs` | `test_autocommit_toggle_with_dml` | Toggles autocommit off, executes DML, commits, toggles autocommit back on, and verifies the data persists. This extends the existing `test_autocommit_toggle` which only checks the option value. |

**Existing tests that serve as regression guards:**

| Existing Test | Covers |
|---------------|--------|
| `test_autocommit_toggle` | Autocommit option get/set round-trip (already exists, unchanged) |
| `test_driver_manager_get_objects` | GetObjects at all depths with catalog="EXA" (already asserts catalog name) |
| `test_transaction_commit` | Commit flow with autocommit disabled |
| `test_transaction_rollback` | Rollback flow with autocommit disabled |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Parameter binding | `cargo build --release --features ffi && cargo test --test driver_manager_tests test_bind_execute_update test_bind_execute_query` | Both tests pass, rows inserted and queried via bound parameters |
| CurrentCatalog | `cargo test --test driver_manager_tests test_get_current_catalog` | Returns "EXA" |
| Parameter schema | `cargo test --test driver_manager_tests test_parameter_schema_names` | Field name from Exasol metadata, not `param_0` |
| Autocommit toggle | `cargo test --test driver_manager_tests test_autocommit_toggle_with_dml` | DML persists across autocommit toggle cycle |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `cargo build --release --features ffi` | Exit 0 |
| Unit Tests | `cargo test --lib` | 0 failures |
| Integration Tests | `cargo test --test integration_tests` | 0 failures |
| Driver Manager Tests | `cargo test --test driver_manager_tests` | 0 failures |
| Lint | `cargo clippy --all-targets --all-features -- -W clippy::all` | 0 warnings |
| Format | `cargo fmt --all -- --check` | No changes |

## Out-of-Scope: Foundry-Side Fixes

The following 10 failures are **not caused by exarrow-rs** and must be fixed in the ADBC Driver Foundry repo's `Exasol2025Quirks` configuration (`validation/tests/exasol.py`):

### 1. TIME type not supported by Exasol (4 failures)

Exasol does not support the SQL `TIME` data type. The foundry tests attempt `CREATE TABLE test_time (idx INT, res TIME(N))` which fails with `Feature not supported: SQL-Type TIME`.

| Test | Error |
|------|-------|
| `test_query[exasol:2025:type/bind/time_ms]` | `SQL-Type TIME` not supported |
| `test_query[exasol:2025:type/bind/time_ns]` | `SQL-Type TIME` not supported |
| `test_query[exasol:2025:type/bind/time_s]` | `SQL-Type TIME` not supported |
| `test_query[exasol:2025:type/bind/time_us]` | `SQL-Type TIME` not supported |

**Fix:** Add `time_ms`, `time_ns`, `time_s`, `time_us` to the quirks `xfail_queries` list, or mark them as unsupported in `DriverFeatures`.

### 2. Table-already-exists from missing teardown (6 failures)

The bind tests for `timestamp_ns`, `timestamp_s`, `timestamp_us`, `timestamptz_ns`, `timestamptz_s`, and `timestamptz_us` fail because a prior `timestamp_ms` or `timestamptz_ms` test created `test_timestamp` / `test_timestamptz` tables without dropping them afterward. Subsequent tests with different precisions try `CREATE TABLE` on the same name and get `object already exists`.

| Test | Error |
|------|-------|
| `test_query[exasol:2025:type/bind/timestamp_ns]` | `object TEST_TIMESTAMP already exists` |
| `test_query[exasol:2025:type/bind/timestamp_s]` | `object TEST_TIMESTAMP already exists` |
| `test_query[exasol:2025:type/bind/timestamp_us]` | `object TEST_TIMESTAMP already exists` |
| `test_query[exasol:2025:type/bind/timestamptz_ns]` | `object TEST_TIMESTAMPTZ already exists` |
| `test_query[exasol:2025:type/bind/timestamptz_s]` | `object TEST_TIMESTAMPTZ already exists` |
| `test_query[exasol:2025:type/bind/timestamptz_us]` | `object TEST_TIMESTAMPTZ already exists` |

**Fix:** The `Exasol2025Quirks.is_table_not_found()` method needs to detect the `42500` (object already exists) error and treat it as a retryable teardown-needed scenario, or the quirks `setup_queries` should use `CREATE OR REPLACE TABLE` / `DROP TABLE IF EXISTS` patterns. Alternatively, ensure the `teardown_queries` in the test config properly drop tables between tests.

### Summary for Foundry PR

These 10 failures require changes to `validation/tests/exasol.py` in the ADBC Driver Foundry repo:
1. Mark TIME bind tests as `xfail` (Exasol doesn't support TIME)
2. Fix table teardown for timestamp/timestamptz bind tests (use `DROP TABLE IF EXISTS` or `CREATE OR REPLACE TABLE` in setup)
