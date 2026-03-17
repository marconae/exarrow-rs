# Tasks: fix-adbc-compliance-failures

## Phase 2: Implementation (Group A - Arrow-to-Parameter helper)
- [x] 1.1 Implement Arrow-to-Parameter conversion helper function in src/adbc_ffi.rs

## Phase 2: Implementation (Group B - Small fixes, parallel)
- [x] 2.1 Expose CurrentCatalog in get_option_string (return "EXA")
- [x] 2.2 Use Exasol parameter names in get_parameter_schema
- [x] 2.3 Fix autocommit toggle to ensure connection before toggling

## Phase 2: Implementation (Group C - Bind+Execute wiring)
- [x] 3.1 Wire bound data through execute_update
- [x] 3.2 Wire bound data through execute (execute_query)

## Phase 2: Implementation (Group D - Tests)
- [x] 4.1 Add test_bind_execute_update integration test
- [x] 4.2 Add test_bind_execute_query integration test
- [x] 4.3 Add test_get_current_catalog integration test
- [x] 4.4 Add test_parameter_schema_names integration test
- [x] 4.5 Add test_autocommit_toggle_with_dml integration test

## Phase 3: Code Review
- [x] 5.0 Code review (fixed: pre-epoch timestamp bug, dead code, noisy comments)

## Phase 4: Verification
- [x] 5.1 Build release with FFI
- [x] 5.2 Run unit tests (905 passed)
- [x] 5.3 Run integration tests (46 passed)
- [x] 5.4 Run driver manager tests (40 passed)
- [x] 5.5 Run clippy lint (0 warnings)
- [x] 5.6 Run format check (clean)
