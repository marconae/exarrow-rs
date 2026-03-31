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
exasol://user:password@host:8563/schema?tls=false&connection_timeout=60
```

## Docker Quickstart

For local development and testing, use the official [Exasol Docker image](https://hub.docker.com/r/exasol/docker-db):

```bash
docker run -d --name exasol-test -p 8563:8563 --privileged --shm-size=2g exasol/docker-db:latest
```

Default credentials: `sys` / `exasol`

Recommended connection string for Docker:

```
exasol://sys:exasol@localhost:8563?tls=true&validateservercertificate=0
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
    let mut connection = database.connect().await?;

    // Use the connection...

    connection.close().await?;
    Ok(())
}
```

## Parameters

All parameters are set via URL query string (`?key=value&key2=value2`).

| Parameter | Aliases | Default | Description |
|---|---|---|---|
| `tls` | `ssl`, `use_tls` | `false` | Enable TLS/SSL encryption |
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

TLS is **disabled** by default. For production environments, enable TLS and keep certificate validation on:

```
exasol://user:password@host:8563?tls=true
```

For development with self-signed certificates, disable certificate validation:

```
exasol://user:password@host:8563?tls=true&validateservercertificate=0
```

## Timeouts

Configure connection and query timeouts via URL parameters:

```rust
// 60-second connection timeout, 10-minute query timeout
let database = driver.open(
    "exasol://user:password@host:8563?connection_timeout=60&query_timeout=600"
)?;
```
