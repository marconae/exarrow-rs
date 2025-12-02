# exarrow-rs

[![Rust](https://img.shields.io/badge/rust-stable-brightgreen.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

> ADBC-compatible driver for Exasol with Apache Arrow data format support.

*Community-maintained. Not officially supported by Exasol.*

## Overview

`exarrow-rs` is a high-performance Rust library that provides ADBC (Arrow Database Connectivity) compatible access to Exasol databases. It enables efficient data transfer using the Apache Arrow columnar format, making it ideal for analytical workloads and data science applications.

* **Phase 1** (Current): WebSocket-based transport using Exasol's native WebSocket API
* **Phase 2** (Planned): Arrow-native gRPC protocol for even higher performance

## Features

- **ADBC-Compatible Interface**: Standard database connectivity API
- **Apache Arrow Format**: Efficient columnar data representation
- **Async/Await**: Built on Tokio for high-performance concurrent operations
- **Type Safety**: Comprehensive Rust type system integration
- **Secure**: TLS/SSL support, secure credential handling
- **Parameter Binding**: Type-safe parameterized queries
- **Metadata Queries**: Introspect catalogs, schemas, tables, and columns
- **Comprehensive Error Handling**: Detailed error messages with context

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
exarrow-rs = "0.1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

## Quick Start

```rust
use exarrow_rs::adbc::Driver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create driver and connect
    let driver = Driver::new();
    let database = driver.open("exasol://sys:exasol@localhost:8563/my_schema")?;
    let mut connection = database.connect().await?;

    // Execute a query
    let results = connection.query("SELECT * FROM customers WHERE age > 25").await?;

    // Process results as Arrow RecordBatches
    for batch in results {
        println!("Retrieved {} rows", batch.num_rows());
        println!("Schema: {:?}", batch.schema());
    }

    // Execute with parameters
    let mut stmt = connection.create_statement("SELECT * FROM orders WHERE customer_id = ?").await?;
    stmt.bind(0, 12345)?;
    let results = stmt.execute().await?;

    // Transaction support
    connection.begin_transaction().await?;
    connection.execute_update("INSERT INTO logs VALUES (1, 'test')").await?;
    connection.commit().await?;

    connection.close().await?;
    Ok(())
}
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

| Exasol Type | Arrow Type |
|-------------|------------|
| BOOLEAN | Boolean |
| CHAR, VARCHAR | Utf8 |
| DECIMAL(p≤38, s) | Decimal128 |
| DECIMAL(p>38, s) | Decimal256 |
| DOUBLE | Float64 |
| DATE | Date32 |
| TIMESTAMP | Timestamp(Microsecond) |
| INTERVAL YEAR TO MONTH | Interval(MonthDayNano) |
| INTERVAL DAY TO SECOND | Interval(MonthDayNano) |
| GEOMETRY | Binary |

## Examples

See the [examples/](examples/) directory for comprehensive usage examples:

- `basic_usage.rs` - Complete demonstration of all features
- `driver_manager_usage.rs` - Using the Driver Manager API

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with [Apache Arrow](https://arrow.apache.org/)
- Powered by [Tokio](https://tokio.rs/)
- Connects to [Exasol](https://www.exasol.com/)
