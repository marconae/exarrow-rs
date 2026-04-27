[Home](index.md) · [Setup & Connect](setup-and-connect.md) · [Queries](queries.md) · [Prepared Statements](prepared-statements.md) · [Import/Export](import-export.md) · [Types](type-mapping.md) · [Driver Manager](driver-manager.md)

---

# Setup & Connect

## Connection String Format

```
exasol://[user[:password]@]host[:port][/schema][?params]
```

### Examples

```
exasol://localhost:8563
exasol://user:password@localhost:8563
exasol://user:password@exasol.example.com:8563/my_schema
exasol://user:password@host:8563/schema?connection_timeout=60
```

## Docker Quickstart

For local development and testing, use the official [Exasol Docker image](https://hub.docker.com/r/exasol/docker-db):

```bash
docker run -d --name exasol-test -p 8563:8563 --privileged --shm-size=2g exasol/docker-db:latest
```

Default credentials: `sys` / `exasol`

Recommended connection string for Docker:

```
exasol://sys:exasol@localhost:8563?validateservercertificate=0
```

> [!NOTE]
> The `--privileged` flag is required by the Exasol Docker image. The container needs at least 2 GiB of RAM. The first startup takes a few minutes while the database initializes.

## Opening a Connection

```rust
use exarrow_rs::adbc::Driver;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let driver = Driver::new();
    let database = driver.open("exasol://user:password@localhost:8563/my_schema")?;
    // The schema in the URI (/my_schema) is opened server-side automatically during connect().
    // Unqualified queries can reference tables in my_schema without any additional setup.
    let mut connection = database.connect().await?;

    // Use the connection...

    connection.close().await?;
    Ok(())
}
```

If you need to switch schemas after connecting, call `connection.set_schema("other_schema").await?`. The `set_schema()` method is also available for connections opened without a URI schema.

## Parameters

All parameters are set via URL query string (`?key=value&key2=value2`).

| Parameter | Aliases | Default | Description |
|---|---|---|---|
| `transport` | — | `native` | Transport protocol: `native` (default) or `websocket` (requires `websocket` feature) |
| `tls` | `ssl`, `use_tls` | `true` | Enable TLS/SSL encryption |
| `validate_certificate` | `verify_certificate`, `validateservercertificate` | `true` | Validate the server's TLS certificate |
| `certificate_fingerprint` | `certificatefingerprint` | — | Pin connection to a specific server certificate (SHA-256 hex of DER cert) |
| `connection_timeout` | `timeout` | `30` | Connection timeout in seconds (max 300) |
| `query_timeout` | — | `300` | Query timeout in seconds |
| `idle_timeout` | — | `600` | Idle connection timeout in seconds |
| `client_name` | — | `exarrow-rs` | Client application name sent to server |
| `client_version` | — | crate version | Client version string sent to server |
| `user` / `username` | — | — | Alternative to `user:password@` in the URL |
| `password` / `pass` | — | — | Alternative to `:password@` in the URL |

### Boolean Parameter Values

Boolean parameters (`tls`, `validate_certificate`) accept these values (case-insensitive):

- **True:** `true`, `1`, `yes`, `on`
- **False:** `false`, `0`, `no`, `off`

### URL Encoding

Usernames and passwords containing special characters must be URL-encoded:

```
exasol://user%40example.com:p%40ssword@localhost:8563
```

### IPv6 Support

Use bracket syntax for IPv6 addresses:

```
exasol://user@[::1]:8563
exasol://user@[2001:db8::1]:9000/schema
```

## Schema and Session Behavior

When the connection URI includes a schema path (e.g., `/my_schema`), `connect()` automatically issues `OPEN SCHEMA my_schema` server-side before returning the connection. Unqualified queries — `SELECT * FROM my_table` rather than `SELECT * FROM my_schema.my_table` — will resolve against that schema immediately, without any manual setup step.

- **Auto-apply on connect**: `database.connect().await?` opens the schema stated in the URI. If the schema does not exist, `connect()` returns a `ConnectionError` rather than returning a connection whose schema state silently differs from the URI.
- **Switch schemas at runtime**: Call `connection.set_schema("other_schema").await?` at any point after connecting to change the active schema. This is useful when you need to access multiple schemas within the same session.
- **Session state accessors**: Use `connection.current_schema()` to read the currently open schema name, and `connection.session_id()` to retrieve the numeric session identifier assigned by the server.

## Session Attributes

Any unrecognized query parameter is forwarded as a session attribute to the Exasol server. Common attributes from the [Exasol WebSocket API](https://github.com/exasol/websocket-api):

| Attribute | Type | Description |
|---|---|---|
| `autocommit` | boolean | Auto-commit after each statement |
| `currentSchema` | string | Current schema name |
| `feedbackInterval` | number | Heartbeat interval during query execution (seconds) |
| `queryTimeout` | number | Server-side query timeout (seconds) |
| `resultSetMaxRows` | number | Max result set rows (0 = unlimited) |
| `snapshotTransactionsEnabled` | boolean | Enable snapshot transactions |
| `timestampUtcEnabled` | boolean | Enable UTC timestamp conversion |
| `numericCharacters` | string | Group/decimal separators (e.g. `.,`) |

The following attributes are read-only and cannot be set via the connection string: `compressionEnabled`, `openTransaction`, `dateFormat`, `dateLanguage`, `datetimeFormat`, `defaultLikeEscapeCharacter`, `timezone`, `timeZoneBehavior`.

## TLS Configuration

TLS is **enabled** by default, matching Exasol 7.1+ (which requires TLS on port 8563) and all official Exasol drivers.

**Docker / self-signed certificate** — disable certificate validation (TLS stays on):

```
exasol://user:password@host:8563?validateservercertificate=0
```

**Legacy pre-7.1 Exasol server** — disable TLS entirely:

```
exasol://user:password@host:8563?tls=false
```

> [!NOTE]
> The control connection and the HTTP transport tunnel for bulk import/export are configured independently. See [WebSocket TLS vs HTTP transport TLS](import-export.md#websocket-tls-vs-http-transport-tls) in the Import/Export docs for details and environment-specific examples.

## Timeouts

Configure connection and query timeouts via URL parameters:

```rust
// 60-second connection timeout, 10-minute query timeout
let database = driver.open(
    "exasol://user:password@host:8563?connection_timeout=60&query_timeout=600"
)?;
```

## Transport Protocol

exarrow-rs connects to Exasol using the **native TCP protocol** by default. The native protocol uses Exasol's binary wire format and parses result sets directly into Arrow with no intermediate JSON serialization, delivering significantly higher throughput than the WebSocket transport.

### Native Protocol (Default)

No configuration is needed. The native protocol is selected automatically when the `native` feature is enabled (which it is by default):

```
exasol://user:password@host:8563
```

The native protocol:
- Sends and receives Exasol's binary wire format over a TLS TCP connection
- Parses binary result sets in a single pass directly into Arrow arrays
- Uses ChaCha20 stream encryption on top of TLS for message-level security
- Is the recommended transport for all production use

### WebSocket Protocol (Alternative)

The WebSocket transport connects over the WebSocket protocol and exchanges JSON messages. It is provided as an opt-in alternative for compatibility or debugging.

Enable the `websocket` feature in your `Cargo.toml`:

```toml
[dependencies]
exarrow-rs = { version = "0.11", features = ["websocket"] }
```

Select WebSocket for a specific connection via the `transport` parameter:

```
exasol://user:password@host:8563?transport=websocket
```

### Feature Flags and Build Options

| Feature | Default | Included in |
|---------|---------|-------------|
| `native` | yes | default build, `--features ffi` |
| `websocket` | no | opt-in: `--features websocket` |

Build with both transports compiled in (transport selected at runtime via connection string):

```bash
cargo build --features websocket
```

Build a WebSocket-only binary (no native protocol):

```bash
cargo build --no-default-features --features websocket
```
