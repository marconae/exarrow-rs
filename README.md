# exarrow-rs

[![Crates.io](https://img.shields.io/crates/v/exarrow-rs)](https://crates.io/crates/exarrow-rs)
[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)
[![CI](https://github.com/marconae/exarrow-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/marconae/exarrow-rs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/marconae/exarrow-rs/branch/main/graph/badge.svg)](https://codecov.io/gh/marconae/exarrow-rs)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

> ADBC-compatible driver for Exasol with Apache Arrow data format support.

*Community-maintained. Not officially supported by Exasol. This is a side-project of mine!*

## Overview

`exarrow-rs` is a Rust library that provides ADBC (Arrow Database Connectivity) compatible access to Exasol databases. It enables efficient data transfer using the Apache Arrow columnar format, making it ideal for analytical workloads and data science applications.

`exarrow-rs` uses WebSocket-based transport using [Exasol's native WebSocket API](https://github.com/exasol/websocket-api).

## Features

- **ADBC-Compatible Interface**: Standard database connectivity API
- **Apache Arrow Format**: Efficient columnar data representation
- **Async/Await**: Built on Tokio for concurrent operations
- **Secure**: TLS/SSL support, secure credential handling
- **Parameter Binding**: Type-safe parameterized queries
- **Metadata Queries**: Introspect catalogs, schemas, tables, and columns

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
exarrow-rs = "2.0"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Quick Start

### Basic Usage

```rust
use exarrow_rs::adbc::Driver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create driver and connect
    let driver = Driver::new();
    let database = driver.open("exasol://sys:exasol@localhost:8563/my_schema")?;
    let mut connection = database.connect().await?;

    // Execute a query (convenience method)
    let results = connection.query("SELECT * FROM customers WHERE age > 25").await?;

    // Process results as Arrow RecordBatches
    for batch in results {
        println!("Retrieved {} rows", batch.num_rows());
        println!("Schema: {:?}", batch.schema());
    }

    // Execute with parameters using Statement
    let mut stmt = connection.create_statement("SELECT * FROM orders WHERE customer_id = ?");
    stmt.bind(0, 12345)?;
    let results = connection.execute_statement(&stmt).await?;

    // Or use prepared statements for repeated execution
    let mut prepared = connection.prepare("SELECT * FROM orders WHERE status = ?").await?;
    prepared.bind(0, "pending")?;
    let results = connection.execute_prepared(&prepared).await?;
    connection.close_prepared(prepared).await?;

    // Transaction support
    connection.begin_transaction().await?;
    connection.execute_update("INSERT INTO logs VALUES (1, 'test')").await?;
    connection.commit().await?;

    connection.close().await?;
    Ok(())
}
```

### ADBC Driver Manager

Load the driver dynamically via the ADBC driver manager:

```rust
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_manager::ManagedDriver;

// Build the FFI library first: cargo build --release --features ffi

let mut driver = ManagedDriver::load_dynamic_from_filename(
    "target/release/libexarrow_rs.dylib", // .so on Linux
    Some(b"ExarrowDriverInit"),
    AdbcVersion::V110,
)?;

let opts = vec![(OptionDatabase::Uri, OptionValue::String(
    "exasol://sys:exasol@localhost:8563".to_string()
))];
let db = driver.new_database_with_opts(opts)?;
let mut conn = db.new_connection()?;

let mut stmt = conn.new_statement()?;
stmt.set_sql_query("SELECT 1")?;
let reader = stmt.execute()?;
```


## Connection String Format

```
exasol://[user[:password]@]host[:port][/schema][?param=value&...]
```

**Parameters:**
- `connection_timeout` - Connection timeout in seconds (default: 30)
- `query_timeout` - Query timeout in seconds (default: 300)
- `tls` - Enable TLS/SSL (default: true)

**Example:**
```
exasol://myuser:mypass@exasol.example.com:8563/production?connection_timeout=60&query_timeout=600
```

## Type Mapping

| Exasol Type              | Arrow Type                 | Notes                              |
|--------------------------|----------------------------|------------------------------------|
| `BOOLEAN`                | `Boolean`                  |                                    |
| `CHAR`, `VARCHAR`        | `Utf8`                     |                                    |
| `DECIMAL(p, s)`          | `Decimal128(p, s)`         | Precision 1-36                     |
| `DOUBLE`                 | `Float64`                  |                                    |
| `DATE`                   | `Date32`                   |                                    |
| `TIMESTAMP`              | `Timestamp(Microsecond)`   | Exasol: 0-9 fractional digits      |
| `INTERVAL YEAR TO MONTH` | `Interval(MonthDayNano)`   |                                    |
| `INTERVAL DAY TO SECOND` | `Interval(MonthDayNano)`   | 0-9 fractional seconds             |
| `GEOMETRY`               | `Binary`                   | WKB format                         |

## Examples

See the [examples/](examples/) directory:

- `basic_usage.rs` - Direct API usage
- `driver_manager_usage.rs` - ADBC driver manager integration

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Apache Arrow](https://arrow.apache.org/)
- Powered by [Tokio](https://tokio.rs/)
- Connects to [Exasol](https://www.exasol.com/)
