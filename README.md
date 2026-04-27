<div align="center">

![exarrow-rs logo](assets/exarrow-logo.svg)

[![Crates.io](https://img.shields.io/crates/v/exarrow-rs.svg)](https://crates.io/crates/exarrow-rs)
[![Documentation](https://docs.rs/exarrow-rs/badge.svg)](https://docs.rs/exarrow-rs)
[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)
[![CI](https://github.com/exasol-labs/exarrow-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/exasol-labs/exarrow-rs/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

ADBC-compatible driver for Exasol with Apache Arrow data format support.

</div>

---

## Add to your project

```bash
cargo add exarrow-rs
cargo add tokio --features rt-multi-thread,macros
```

## Quick Start

```rust
use exarrow_rs::adbc::Driver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = Driver::new();
    let database = driver.open("exasol://user:pwd@localhost:8563/my_schema")?;
    // The URI schema (/my_schema) is applied server-side automatically during connect().
    // No manual set_schema() call is needed.
    let mut connection = database.connect().await?;

    let results = connection.query("SELECT * FROM customers").await?;
    for batch in results {
        println!("Got {} rows", batch.num_rows());
    }

    connection.close().await?;
    Ok(())
}
```

---

## Transport

exarrow-rs uses the **native TCP protocol** by default — Exasol's binary wire protocol with direct Arrow conversion and no intermediate JSON serialization. No extra configuration is needed.

The WebSocket transport is available as an opt-in alternative for compatibility or testing:

```toml
[dependencies]
exarrow-rs = { version = "0.11", features = ["websocket"] }
```

```rust
let db = driver.open("exasol://user:pwd@host:8563?transport=websocket")?;
```

See [Transport Protocol](docs/setup-and-connect.md#transport-protocol) in the docs for feature flags and build options.

---

## Documentation

See [**docs/**](docs/index.md) for comprehensive documentation:

- [Setup & Connect](docs/setup-and-connect.md) - Docker setup, connection strings, parameters, TLS, and transport selection
- [Queries](docs/queries.md) - Query execution and transactions
- [Prepared Statements](docs/prepared-statements.md) - Parameter binding
- [Import / Export](docs/import-export.md) - Bulk data transfer
  - [Parallel Import](docs/import-export.md#parallel-import)
  - [Schema Inference](docs/import-export.md#auto-table-creation)
- [Type Mapping](docs/type-mapping.md) - Exasol to Arrow conversions
- [Driver Manager](docs/driver-manager.md) - ADBC integration ([Python](docs/driver-manager.md#python-adbc-driver-manager), [Polars](docs/driver-manager.md#python-polars), [Go](docs/driver-manager.md#other-languages), [Java](docs/driver-manager.md#other-languages))

---

## License

Community-supported. Licensed under [MIT](LICENSE).

---

<div align="center">

Build with Rust 🦀 and made with ❤️

Based on a prototype by [marconae](https://github.com/marconae), now maintained by [Exasol Labs 🧪](https://github.com/exasol-labs/).

</div>