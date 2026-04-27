# Tasks: change-chacha20-legacy-encryption-spec

## Phase 2: Implementation (Group A — all parallel, disjoint files)
- [x] 2.1 Rewrite `specs/native-client/encryption/spec.md`: fix Feature summary and Background; apply DELTA:CHANGED to 5 scenarios; add 2 DELTA:NEW scenarios
- [x] 2.2 Update `specs/connection-management/native-auth/spec.md`: amend Background; apply DELTA:CHANGED to 1 scenario; add 1 DELTA:NEW scenario
- [x] 2.3 Update `specs/connection-management/transport-selection/spec.md`: amend Background; add 2 DELTA:NEW scenarios

## Phase 3: Verification
- [x] 3.1 Validate plan (`speq plan validate change-chacha20-legacy-encryption-spec`)
- [x] 3.2 Build (`cargo build`)
- [x] 3.3 Lint (`cargo clippy --all-targets --all-features -- -W clippy::all`)
- [x] 3.4 Format check (`cargo fmt --all -- --check`)
- [x] 3.5 Unit tests (`cargo test --lib`)
- [x] 3.6 Integration tests (`cargo test --test integration_tests`)
- [x] 3.7 Native protocol tests (`cargo test --test native_protocol_tests`)
- [ ] 3.7 Native protocol tests (`cargo test --test native_protocol_tests`)
