# Plan: change-chacha20-legacy-encryption-spec

## Summary

Correct the native-protocol specs to reflect that TLS and ChaCha20 are mutually exclusive — ChaCha20 is a legacy message-encryption fallback used only when TLS is disabled (pre-7.1 Exasol servers), matching the Exasol C++ reference driver and the implementation already in `src/transport/native/mod.rs`. Specs-only change; no code modifications.

## Design

Skipped — this is a documentation/spec accuracy fix for an already-correct implementation. No new design decisions.

### Context

The existing specs for `native-client/encryption` and `connection-management/native-auth` incorrectly state that TLS and ChaCha20 run simultaneously as concentric encryption layers on every native-protocol session. In reality:

- The Exasol C++ reference driver (`dwaSocket.cpp`) explicitly sets `usingSSL = false` whenever `useLegacyEncryption` is true — the two modes are mutually exclusive.
- Exasol R&D have confirmed: ChaCha20 was used for encrypted connections pre-7.1 and is now a legacy fallback. Since 7.1.0 all official drivers use TLS exclusively.
- exarrow-rs already implements this correctly at `src/transport/native/mod.rs:924`: `let use_chacha20 = !self.tls_active;`. The handshake at `src/transport/native/handshake.rs:120` also gates ChaCha20 key generation on the `use_chacha20` flag.

The specs therefore misrepresent a correct implementation. This plan rewrites the spec text so the specs match reality and future contributors do not "fix" the code to match a wrong spec.

- **Goals** — Accurate specs for TLS-vs-ChaCha20; clearly flag ChaCha20 as a pre-7.1 legacy fallback; document that Exasol 8 / protocol version 18+ uses TLS exclusively.
- **Non-Goals** — No code changes. No new test coverage (existing `native_connect_and_authenticate` covers the TLS-only path; existing `native_connect_no_tls` covers the legacy ChaCha20 path). No deprecation of the ChaCha20 code itself — it stays as the legacy fallback.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| native-client/encryption | CHANGED | `native-client/encryption/spec.md` |
| connection-management/native-auth | CHANGED | `connection-management/native-auth/spec.md` |
| connection-management/transport-selection | CHANGED | `connection-management/transport-selection/spec.md` |

## Implementation Tasks

1. Rewrite `specs/native-client/encryption/spec.md`: fix the Feature summary and Background to distinguish protocol v14 (ChaCha20 replaces RC4 in the legacy path) from protocol v18+ (TLS, no ChaCha20); rewrite the "TLS and ChaCha20 layering" scenario to state mutual exclusion; qualify the key-generation, RSA-encrypted key exchange, and encrypted send/receive scenarios with the TLS-absent precondition; add new scenarios "ChaCha20 used only when TLS is absent (legacy mode)" and "ChaCha20 skipped when TLS is active".
2. Update `specs/connection-management/native-auth/spec.md`: add the TLS-absent precondition to the "ChaCha20 key exchange during native protocol login" scenario and amend the Background so ChaCha20 is only mentioned as the non-TLS path; add a new scenario "ChaCha20 key exchange skipped when TLS is active".
3. Add to `specs/connection-management/transport-selection/spec.md` two new scenarios: "Native transport uses TLS exclusively against Exasol 7.1+ and Exasol 8" and "Native transport uses ChaCha20 legacy encryption when TLS is disabled".

No tasks are tagged `[expert]` — each task is a focused spec text rewrite with no algorithmic or concurrency concerns.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1, Task 2, Task 3 |

All three tasks touch disjoint spec files and can be applied concurrently. No sequential dependencies.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| — | — | No dead code. ChaCha20 code is retained intentionally as the legacy-encryption fallback, matching the C++ reference driver. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| native-client/encryption — ChaCha20 key generation | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_no_tls` |
| native-client/encryption — RSA-encrypted key exchange | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_no_tls` |
| native-client/encryption — Encrypted message sending | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_no_tls` |
| native-client/encryption — Encrypted message receiving | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_no_tls` |
| native-client/encryption — TLS and ChaCha20 are mutually exclusive | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_and_authenticate` |
| native-client/encryption — ChaCha20 used only when TLS is absent (legacy mode) | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_no_tls` |
| native-client/encryption — ChaCha20 skipped when TLS is active | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_and_authenticate` |
| connection-management/native-auth — ChaCha20 key exchange during native protocol login (legacy mode) | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_no_tls` |
| connection-management/native-auth — ChaCha20 key exchange skipped when TLS is active | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_and_authenticate` |
| connection-management/transport-selection — Native transport uses TLS exclusively against Exasol 7.1+ and Exasol 8 | Integration | `tests/native_protocol_tests.rs` | `test_native_connection` |
| connection-management/transport-selection — Native transport uses ChaCha20 legacy encryption when TLS is disabled | Integration | `tests/native_transport_smoke_test.rs` | `native_connect_no_tls` |

All scenarios are already covered by existing integration tests. No new tests required — the code these specs describe is unchanged.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| native-client/encryption (TLS path) | `cargo test --test native_transport_smoke_test native_connect_and_authenticate -- --nocapture` | Test passes. Session printed with `protocol_version > 0`. No ChaCha20 activation. |
| native-client/encryption (legacy path) | `cargo test --test native_transport_smoke_test native_connect_no_tls -- --nocapture` | Test passes (session printed) on servers that accept non-TLS connections; on Exasol 8 which rejects non-TLS, the test accepts a `ProtocolError` containing "public key" as the documented failure mode. |
| connection-management/native-auth | `cargo test --test native_protocol_tests test_native_connection` | Test passes against Exasol 8 over TLS without invoking ChaCha20 code paths. |
| connection-management/transport-selection | `cargo test --test native_protocol_tests test_default_native_transport` | Test passes; transport selection behavior unchanged by spec rewrite. |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Validate plan | `speq plan validate change-chacha20-legacy-encryption-spec` | Exit 0, no errors |
| Build | `cargo build` | Exit 0 |
| Test (lib) | `cargo test --lib` | 0 failures |
| Test (integration) | `cargo test --test integration_tests` | 0 failures |
| Test (native protocol) | `cargo test --test native_protocol_tests` | 0 failures |
| Lint | `cargo clippy --all-targets --all-features -- -W clippy::all` | 0 errors/warnings |
| Format | `cargo fmt --all -- --check` | No changes |
