[Home](index.md) · [Setup & Connect](setup-and-connect.md) · [Queries](queries.md) · [Prepared Statements](prepared-statements.md) · [Import/Export](import-export.md) · [Types](type-mapping.md) · [Driver Manager](driver-manager.md)

---

# Import / Export

High-performance bulk data transfer via HTTP transport. Supports streaming for large datasets.

## Which import path should I use?

The right path depends on your data source and how much schema control you need:

| Situation | Recommended path |
|-----------|-----------------|
| You already know your target schema and table structure | Connection methods + explicit DDL: create the table yourself, then call `connection.import_csv_from_files()` or `connection.import_parquet_from_files()` |
| You want the table created from file metadata | Parquet/CSV auto-create: pass `ParquetImportOptions::default().with_create_table_if_not_exists(true)` — the driver reads Parquet metadata and issues `CREATE TABLE` before importing |
| You already have Arrow `RecordBatch`es in memory | Arrow import via the connection method `connection.import_from_record_batch()` or the ADBC `Statement` API |
| You are calling from Python, Polars, or another language | The driver-manager path — see [Driver Manager](driver-manager.md) |

Use the connection convenience methods (`connection.import_*` and `connection.export_*`) as the default choice. They automatically propagate the connection's host and port into the HTTP transport, so you only need to set format-specific options. They also use whatever control transport you configured (native TCP by default, which gives the best throughput; WebSocket if you set `?transport=websocket` for proxy or compatibility reasons). Lower-level free functions and direct `HttpTransportClient` usage are available for advanced cases where you need full control over transport parameters, but they require you to supply host/port manually and are not the recommended starting point.

## Control Connection TLS vs HTTP Transport TLS

Exasol import and export use **two independent TLS layers**. Enabling TLS on the main control connection (native TCP or WebSocket) does **not** automatically enable TLS on the HTTP transport tunnel used for bulk data transfer. Each layer must be configured separately.

| Layer | What it protects | Where it is configured |
|-------|-----------------|------------------------|
| Control connection | Login, SQL, session | `?tls=true` / `?validateservercertificate=0` in the connection URI |
| HTTP transport tunnel | Bulk data (CSV / Parquet / Arrow) | `.use_tls(bool)` on the import/export options struct |

### Docker / local development

The Exasol Docker container uses a self-signed certificate for the control connection. Use `?validateservercertificate=0` in the URI to accept it. For the HTTP transport tunnel, use `.use_tls(false)` — Exasol Docker's ad-hoc per-session certificate handling for the data channel is separate from the control-channel certificate and does not integrate with standard TLS validation.

```rust
use exarrow_rs::adbc::Driver;
use exarrow_rs::import::CsvImportOptions;

// Local Docker / self-signed certificate
let driver = Driver::new();
let database = driver.open(
    "exasol://user:password@localhost:8563/my_schema?validateservercertificate=0"
)?;
let mut connection = database.connect().await?;

// HTTP transport does NOT use TLS against Docker:
let options = CsvImportOptions::default().use_tls(false);
connection.import_csv_from_files("my_table", &["data.csv"], options).await?;
```

### Exasol SaaS / production

In production and SaaS environments both layers should use TLS. The connection URI does not need `validateservercertificate=0` because the server presents a valid certificate. The HTTP transport uses `.use_tls(true)`, which is the correct setting for any Exasol host that supports TLS on the data channel.

```rust
use exarrow_rs::adbc::Driver;
use exarrow_rs::import::CsvImportOptions;

// Exasol SaaS / production (both layers TLS)
let driver = Driver::new();
let database = driver.open(
    "exasol://user:password@your-exasol-host.exasol.com:8563/my_schema"
)?;
let mut connection = database.connect().await?;

// HTTP transport uses TLS in production:
let options = CsvImportOptions::default().use_tls(true);
connection.import_csv_from_files("my_table", &["data.csv"], options).await?;
```

**Default**: `.use_tls(false)` is the Docker-safe default. Switch to `.use_tls(true)` for any production or SaaS Exasol host. The same `.use_tls(bool)` method is available on all six option builders: `CsvImportOptions`, `CsvExportOptions`, `ParquetImportOptions`, `ParquetExportOptions`, `ArrowImportOptions`, and `ArrowExportOptions`.

## Supported Formats

| Format    | Import | Export | Notes                         |
|-----------|--------|--------|-------------------------------|
| CSV       | Yes    | Yes    | Native Exasol format, fastest |
| Parquet   | Yes    | Yes    | Columnar with compression     |
| Arrow IPC | Yes    | Yes    | Direct RecordBatch transfer   |

## CSV Import

```rust
use exarrow_rs::import::CsvImportOptions;
use std::path::Path;

let options = CsvImportOptions::default()
    .column_separator(',')
    .skip_rows(1);  // Skip header row

let rows = connection.import_csv_from_file(
    "my_schema.my_table",
    Path::new("/path/to/data.csv"),
    options,
).await?;

println!("Imported {} rows", rows);
```

### CSV Options

| Option | Description |
|--------|-------------|
| `column_separator` | Field delimiter (default: `,`) |
| `skip_rows` | Number of header rows to skip |
| `row_separator` | Line ending (default: `\n`) |
| `null_string` | String representing NULL values |

## Parquet Import

```rust
use exarrow_rs::import::ParquetImportOptions;
use std::path::Path;

let rows = connection.import_from_parquet(
    "my_table",
    Path::new("/path/to/data.parquet"),
    ParquetImportOptions::default().with_batch_size(1024),
).await?;
```

### Auto Table Creation

Parquet imports can automatically create the target table by inferring the schema from the Parquet file metadata. This is useful when importing data without pre-defining the table structure.

```rust
use exarrow_rs::import::{ParquetImportOptions, ColumnNameMode};

let options = ParquetImportOptions::default()
    .with_create_table_if_not_exists(true)
    .column_name_mode(ColumnNameMode::Sanitize);

let rows = connection.import_from_parquet(
    "my_schema.new_table",  // Table will be created if it doesn't exist
    Path::new("/path/to/data.parquet"),
    options,
).await?;
```

The schema is inferred by reading only the Parquet metadata (not the full data), making it efficient even for large files.

### Column Name Modes

When auto-creating tables, column names can be handled in two ways:

| Mode | Behavior |
|------|----------|
| `Quoted` | Preserves original names exactly, wrapped in double quotes |
| `Sanitize` | Converts to uppercase, replaces invalid characters with `_`, quotes reserved words |

```rust
// Preserve exact column names (e.g., "customerId", "Order Date")
.column_name_mode(ColumnNameMode::Quoted)

// Sanitize for Exasol compatibility (e.g., CUSTOMERID, ORDER_DATE)
.column_name_mode(ColumnNameMode::Sanitize)
```

### Multi-File Schema Inference

When importing multiple Parquet files with auto table creation, the system computes a **union schema** that accommodates all files:

```rust
let files = vec![
    Path::new("/data/part-001.parquet"),
    Path::new("/data/part-002.parquet"),
];

let options = ParquetImportOptions::default()
    .with_create_table_if_not_exists(true);

let rows = connection.import_parquet_from_files(
    "combined_table",
    &files,
    options,
).await?;
```

Type widening rules when fields differ across files:
- Identical types remain unchanged
- `DECIMAL` types widen to max(precision), max(scale)
- `VARCHAR` types widen to max(size)
- `DECIMAL` + `DOUBLE` widens to `DOUBLE`
- Incompatible types fall back to `VARCHAR(2000000)`

## Parquet Export

```rust
use exarrow_rs::export::{ExportSource, ParquetExportOptions, ParquetCompression};
use std::path::Path;

let rows = connection.export_to_parquet(
    ExportSource::Table {
        schema: None,
        name: "my_table".into(),
        columns: vec![]
    },
    Path::new("/tmp/export.parquet"),
    ParquetExportOptions::default().with_compression(ParquetCompression::Snappy),
).await?;
```

### Export Sources

Export from tables or queries:

```rust
// Export entire table
ExportSource::Table { schema: None, name: "users".into(), columns: vec![] }

// Export specific columns
ExportSource::Table {
    schema: Some("prod".into()),
    name: "users".into(),
    columns: vec!["id".into(), "name".into()]
}

// Export query results
ExportSource::Query("SELECT * FROM users WHERE active = true".into())
```

## Parallel Import

For large datasets, import multiple files in parallel:

```rust
use exarrow_rs::import::ParallelImportOptions;

let files = vec![
    Path::new("/data/part-001.csv"),
    Path::new("/data/part-002.csv"),
    Path::new("/data/part-003.csv"),
];

let rows = connection.import_csv_parallel(
    "my_table",
    &files,
    CsvImportOptions::default(),
    ParallelImportOptions::default().with_max_connections(4),
).await?;
```

This uses multiple HTTP connections to Exasol for higher throughput.
