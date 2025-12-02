# Agent Guidelines for exarrow-rs

Guidelines for AI coding agents working on this project.

## Project Overview

**exarrow-rs** is a community-maintained ADBC-compatible driver for Exasol with Apache Arrow support. It enables efficient database connectivity for analytical workloads using Rust.

## Architecture

```
Public API (adbc/)
    ↓
Session Management (connection/)
    ↓
Query Execution (query/)
    ↓
Transport Protocol (transport/)
    ↓
Data Conversion (arrow_conversion/)
    ↓
Type System (types/)
```

### Module Responsibilities

| Module | Purpose |
|--------|---------|
| `adbc/` | ADBC standard interface (Driver, Database, Connection, Statement) |
| `connection/` | Connection params, authentication, session state |
| `query/` | Statement execution, prepared statements, result sets |
| `transport/` | WebSocket protocol, message serialization |
| `arrow_conversion/` | JSON to Arrow columnar conversion |
| `types/` | Exasol to Arrow type mapping |
| `error.rs` | Domain-specific error types |
| `adbc_ffi.rs` | FFI bindings (feature-gated with `ffi`) |

## Coding Standards

### Naming

- Structs: `PascalCase`
- Functions/methods: `snake_case`
- Constants: `UPPER_SNAKE_CASE`
- Error variants: descriptive (`ConnectionFailed`, `AuthenticationFailed`)

### Error Handling

- Use `thiserror` for domain-specific errors
- Return `Result<T, ErrorType>` from all fallible functions
- Include context in error messages (host, port, indices)
- Validate inputs early, fail fast
- No panics in production code

### Async Patterns

- Built on Tokio runtime
- Use `async_trait` for async trait methods
- Use `tokio::sync::Mutex` for shared state
- Support timeouts via `tokio::time::timeout`

## Testing

### Test-Driven Development

1. Write failing test first
2. Verify it fails for the expected reason
3. Write minimal code to pass
4. Refactor while keeping tests green

### Test Organization

- Unit tests: inline `#[cfg(test)]` blocks
- Integration tests: `tests/` directory
- Test helpers: `tests/common/mod.rs`
- Naming: `test_<what>_<expected_behavior>`

### Running Tests

```bash
cargo test                                           # Unit tests
cargo test --test integration_tests -- --ignored     # Integration tests (requires Exasol)
cargo test --test driver_manager_tests -- --ignored  # FFI tests
```

### Integration Test Environment

```
EXASOL_HOST=localhost (default)
EXASOL_PORT=8563 (default)
EXASOL_USER=sys (default)
EXASOL_PASSWORD=exasol (default)
```

## Build Commands

```bash
cargo build                          # Debug build
cargo build --release --features ffi # Release with FFI
cargo clippy --all-targets           # Lint
cargo test --lib                     # Unit tests only
cargo doc --open                     # Documentation
```

## Key Patterns

### Connection String Format

```
exasol://[user[:password]@]host[:port][/schema][?params]
```

### Query Execution Flow

```rust
let driver = Driver::new();
let database = driver.open("exasol://user:pass@host:port")?;
let connection = database.connect().await?;
let results = connection.query("SELECT ...").await?;
connection.close().await?;
```

### Prepared Statements

```rust
let mut stmt = connection.create_statement("SELECT * FROM t WHERE id = ?").await?;
stmt.bind(0, value)?;
let results = stmt.execute().await?;
```

## Data Format

Exasol WebSocket API returns data in **column-major format**:
- `data[0]` = all values for column 0
- `data[1]` = all values for column 1

Arrow conversion handles this directly without transposition.

## Security Considerations

- TLS enabled by default
- RSA-encrypted password transmission
- Credentials never logged (use `safe_display_string`)
- Parameterized queries to prevent SQL injection

## When Adding Features

1. Identify the correct module based on responsibility
2. Write tests first
3. Follow existing patterns in that module
4. Update documentation if public API changes
5. Run `cargo clippy` and `cargo test` before committing

## Code Review Checklist

- [ ] Error handling is comprehensive
- [ ] Async operations have timeout support where appropriate
- [ ] No unwrap/expect in production paths
- [ ] Tests cover happy path and error cases
- [ ] Public APIs have doc comments
- [ ] No credentials in logs or error messages

## Git Workflow

- **Main Branch**: `main` (protected)
- **Feature Branches**: `feature/<description>` or `<username>/<description>`
- **Commit Conventions**: Use conventional commits format
    - `feat:` for new features
    - `fix:` for bug fixes
    - `perf:` for performance improvements
    - `refactor:` for code restructuring
    - `test:` for test additions/changes
    - `docs:` for documentation
    - `chore:` for maintenance tasks
- **Pull Requests**: Required for all changes to main