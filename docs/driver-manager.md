[Home](index.md) · [Setup & Connect](setup-and-connect.md) · [Queries](queries.md) · [Prepared Statements](prepared-statements.md) · [Import/Export](import-export.md) · [Types](type-mapping.md) · [Driver Manager](driver-manager.md)

---

# Driver Manager Integration

exarrow-rs can be used with ADBC driver managers via its C FFI interface.

## Building the Driver

Enable the `ffi` feature to build the C-compatible shared library. The `ffi` feature automatically includes the native TCP transport:

```bash
cargo build --release --features ffi
```

This produces a shared library:
- Linux: `target/release/libexarrow_rs.so`
- macOS: `target/release/libexarrow_rs.dylib`
- Windows: `target/release/exarrow_rs.dll`

The shared library uses the **native TCP protocol** by default. To also include the WebSocket transport in the binary (allowing runtime selection via `transport=websocket` in the connection URI):

```bash
cargo build --release --features 'ffi websocket'
```

The ADBC Driver Foundry and other downstream build systems should use `--features ffi`, which ensures the native protocol is always compiled in.

## Connection URI

The connection URI follows the format:

```
exasol://user:password@host:port?tls=true&validateservercertificate=0
```

TLS is enabled by default and required for production Exasol instances. The `validateservercertificate=0` option disables certificate validation and should only be used with the Exasol Docker container for local development and testing. In production, omit this parameter to enforce proper certificate validation:

```
exasol://user:password@host:port?tls=true
```

## Using with ADBC Driver Manager

### Python (adbc-driver-manager)

Install the dependencies:

```bash
pip install adbc-driver-manager pyarrow
```

Connect and query:

```python
import adbc_driver_manager.dbapi

conn = adbc_driver_manager.dbapi.connect(
    driver="/path/to/libexarrow_rs.so",
    entrypoint="ExarrowDriverInit",
    db_kwargs={"uri": "exasol://user:password@localhost:8563?tls=true&validateservercertificate=0"},
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM my_table")
table = cursor.fetch_arrow_table()
print(table)

cursor.close()
conn.close()
```

For DDL/DML statements (CREATE, INSERT, DROP, etc.) that do not return a result set, use the low-level statement API:

```python
import adbc_driver_manager._lib as adbc_lib

stmt = adbc_lib.AdbcStatement(conn.adbc_connection)
stmt.set_sql_query("CREATE SCHEMA my_schema")
stmt.execute_update()
stmt.close()
```

### Python (Polars)

Polars has native ADBC support, allowing direct queries into DataFrames:

```python
import polars as pl

# Query directly into a Polars DataFrame
df = pl.read_database_uri(
    query="SELECT * FROM my_table",
    uri="exasol://user:password@localhost:8563?tls=true&validateservercertificate=0",
    engine="adbc",
    engine_options={
        "driver": "/path/to/libexarrow_rs.so",
        "entrypoint": "ExarrowDriverInit",
    },
)

print(df)
```

For multiple queries, reuse the connection:

```python
import adbc_driver_manager.dbapi
import polars as pl

# Set up the connection once
conn = adbc_driver_manager.dbapi.connect(
    driver="/path/to/libexarrow_rs.so",
    entrypoint="ExarrowDriverInit",
    db_kwargs={"uri": "exasol://user:password@localhost:8563?tls=true&validateservercertificate=0"},
)

# Run multiple queries
df1 = pl.read_database(query="SELECT * FROM customers", connection=conn)
df2 = pl.read_database(query="SELECT * FROM orders", connection=conn)

# Join and analyze with Polars
result = df1.join(df2, on="customer_id").filter(pl.col("amount") > 100)
print(result)

conn.close()
```

### Rust (adbc-driver-manager)

Add the dependencies to your `Cargo.toml`:

```toml
[dependencies]
adbc_core = "0.1"
adbc_driver_manager = "0.1"
arrow = "53"
```

Load the driver, connect, and query:

```rust
use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{Connection, Database, Driver, Statement};
use adbc_driver_manager::ManagedDriver;
use arrow::array::RecordBatchReader;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load the shared library via the ADBC driver manager
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        "target/release/libexarrow_rs.so", // .dylib on macOS, .dll on Windows
        Some(b"ExarrowDriverInit"),
        AdbcVersion::V110,
    )?;

    // Create a database with the connection URI
    let uri = "exasol://user:password@localhost:8563?tls=true&validateservercertificate=0";
    let opts = vec![(OptionDatabase::Uri, OptionValue::String(uri.into()))];
    let db = driver.new_database_with_opts(opts)?;

    // Open a connection and execute a SELECT query
    let mut conn = db.new_connection()?;
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query("SELECT * FROM my_table")?;
    let mut reader = stmt.execute()?;

    for batch in reader.by_ref() {
        let batch = batch?;
        println!("{} rows x {} cols", batch.num_rows(), batch.num_columns());
    }

    // Execute DDL/DML (no result set)
    let mut stmt = conn.new_statement()?;
    stmt.set_sql_query("CREATE SCHEMA my_schema")?;
    stmt.execute_update()?;

    Ok(())
}
```

See [`examples/driver_manager_usage.rs`](https://github.com/exasol-labs/exarrow-rs/blob/main/examples/driver_manager_usage.rs) for a complete example with error handling, DDL/DML workflows, and driver info queries.

### Other Languages

The driver follows the ADBC specification and works with any ADBC driver manager:
- [adbc-driver-manager (Python)](https://arrow.apache.org/adbc/current/python/index.html)
- [Go ADBC](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc)
- [Java ADBC](https://arrow.apache.org/adbc/current/java/index.html)

## Examples

- **Python**: See [`examples/python/test_driver.py`](https://github.com/exasol-labs/exarrow-rs/blob/main/examples/python/test_driver.py) for a complete Python example with queries and DDL/DML workflows.
- **Rust**: See [`examples/driver_manager_usage.rs`](https://github.com/exasol-labs/exarrow-rs/blob/main/examples/driver_manager_usage.rs) for using the driver manager API from Rust.

## ADBC Functions

The FFI exports all standard ADBC entry points:

| Function | Description |
|----------|-------------|
| `ExarrowDriverInit` | Initialize the driver |
| `AdbcDatabaseNew` | Create a new database handle |
| `AdbcDatabaseSetOption` | Set database options (URI, etc.) |
| `AdbcDatabaseInit` | Initialize database with options |
| `AdbcConnectionNew` | Create a connection handle |
| `AdbcConnectionInit` | Open the connection |
| `AdbcStatementNew` | Create a statement handle |
| `AdbcStatementExecuteQuery` | Execute a query |
