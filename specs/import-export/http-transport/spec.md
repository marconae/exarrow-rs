# Feature: HTTP Transport

Specifies the HTTP transport layer for bulk data transfer using the EXA tunneling protocol, enabling firewall-friendly import and export operations through reverse-connection HTTP tunneling.

## Architecture

Exasol bulk import/export uses a **reverse-connection HTTP tunneling** pattern. The client
never opens an inbound listening port; instead, all communication flows over a single outbound
TCP connection that is "hijacked" into a bidirectional HTTP tunnel.

#### Components

**Client side (exarrow-rs):**
- **User Code / Callback Thread** — the application thread that produces (import) or consumes (export) data
- **HTTP Transport Thread** — starts an internal HTTP server, generates a temporary TLS certificate,
  and manages the tunnel. Communicates with User Code via an OS pipe.
- **WebSocket Connection** — the persistent control channel to Exasol for sending SQL commands

**Database side (Exasol):**
- **SQLProcess** — executes the IMPORT/EXPORT statement
- **Internal Port** (e.g. 35353) — the tunnel endpoint inside the Exasol cluster

#### Data Flow

```text
 Client                                              Exasol DB
 ──────                                              ─────────

 1. WebSocket connect ──────────────────────────────► DB Port
    (outbound TCP, unencrypted initially)

 2. HTTP Transport Thread
    - opens TCP to Exasol host:port
    - sends magic packet (0x02212102, 1, 1)
    - receives 24-byte response → internal address + port
    - generates temp RSA certificate
    - wraps connection with TLS (if enabled)            ◄── "Reverse Connection"
                                                            (DB's SQLProcess
 3. WebSocket sends IMPORT/EXPORT SQL:                       connects back through
    AT 'https://internal_addr:port'                          the tunnel to the
    PUBLIC KEY 'sha256//<base64>'                            client's HTTP handler)

 4. Exasol SQLProcess ──── HTTP GET (import) ────────► Client HTTP handler
                       ◄── HTTP response + data ──────

    OR

    Exasol SQLProcess ──── HTTP PUT + data ──────────► Client HTTP handler
                       ◄── HTTP 200 OK ───────────────

 5. Data flows between HTTP handler and User Code via OS pipe
```

#### Key Design Decisions

- **Firewall-friendly**: Only outbound TCP from client; no inbound ports needed
- **Temporary TLS certificates**: Client generates ad-hoc RSA cert per connection;
  SHA-256 fingerprint is passed in SQL PUBLIC KEY clause
- **Single tunnel per transfer**: Each import/export operation uses one TCP connection
  with one HTTP request/response cycle
- **Future: Public key requirement**: DB version >= 8.32 will require the public key
  from the temp certificate to be included in the EXA address and IMPORT/EXPORT FILE clause

**Source:** `http_transport.drawio` from exa-core-db documentation.

## Background

The client establishes an outbound TCP connection to Exasol and performs a magic packet handshake to create a bidirectional HTTP tunnel. All data transfer occurs through this single established connection, enabling firewall-friendly operation requiring only outbound connections. TLS encryption uses ad-hoc RSA certificates with SHA-256 fingerprints passed in SQL PUBLIC KEY clauses. The HTTP-transport TLS layer is independent of the main control-channel TLS layer (WebSocket or native TCP); applications configure HTTP-transport TLS per import/export operation via `use_tls(bool)` on the corresponding option builder.

## Scenarios

### Scenario: HTTP transport client connects to Exasol

* *GIVEN* an import or export operation has been requested
* *WHEN* client initiates import or export operation
* *THEN* client SHALL connect to Exasol host and port via TCP (outbound connection)
* *AND* connection SHALL be established without requiring inbound firewall rules

### Scenario: EXA tunneling handshake in client mode

* *GIVEN* a TCP connection to Exasol has been established
* *WHEN* client sends magic packet (0x02212102, 1, 1) as three little-endian i32 values
* *THEN* Exasol SHALL respond with 24-byte packet containing internal address
* *AND* internal address SHALL be parsed as: bytes 4-7 = port (i32 LE), bytes 8-23 = IP (null-padded string)

### Scenario: TLS encryption with ad-hoc certificates

* *GIVEN* TLS encryption is configured for the connection
* *WHEN* TLS encryption is enabled
* *THEN* client SHALL generate ad-hoc RSA certificate
* *AND* client SHALL wrap connection with TLS after magic packet exchange
* *AND* client SHALL compute SHA-256 fingerprint of DER-encoded public key
* *AND* fingerprint SHALL be formatted as `sha256//<base64>` for SQL PUBLIC KEY clause

### Scenario: Internal address used in SQL statements

* *GIVEN* the EXA tunneling handshake has completed successfully
* *WHEN* handshake completes successfully
* *THEN* client SHALL use internal address in IMPORT/EXPORT SQL AT clause
* *AND* SQL format SHALL be `AT 'http://internal_addr:port'` or `AT 'https://...'` with PUBLIC KEY

### Scenario: Firewall-friendly operation

* *GIVEN* the client operates behind NAT or firewall
* *WHEN* client operates behind NAT or firewall
* *THEN* only outbound TCP connections SHALL be required
* *AND* no inbound port forwarding or public IP SHALL be needed
* *AND* operation SHALL succeed with Exasol SaaS (cloud) instances

### Scenario: Import data flow (client sends data to Exasol)

* *GIVEN* an HTTP tunnel is established for an import operation
* *WHEN* IMPORT SQL is executed
* *THEN* Exasol SHALL send HTTP GET request through tunnel to fetch data
* *AND* client SHALL receive GET request and parse request line and headers
* *AND* client SHALL send HTTP response with data using chunked transfer encoding

### Scenario: Export data flow (Exasol sends data to client)

* *GIVEN* an HTTP tunnel is established for an export operation
* *WHEN* EXPORT SQL is executed
* *THEN* Exasol SHALL send HTTP PUT request through tunnel with data
* *AND* client SHALL receive PUT request with chunked transfer encoding
* *AND* client SHALL read data chunks until zero-length terminator

### Scenario: WebSocket TLS and HTTP transport TLS are independent

* *GIVEN* a connection has been established to Exasol with the main control channel (WebSocket or native TCP) using its own TLS configuration
* *WHEN* the application initiates an import or export operation that uses the HTTP transport tunnel
* *THEN* the HTTP-transport TLS setting (`use_tls(bool)` on the import/export options) SHALL be evaluated independently of the main control channel's TLS state
* *AND* the driver MUST NOT infer the HTTP-transport TLS setting from the control channel's TLS configuration
* *AND* the documented default for the HTTP-transport `use_tls` SHALL remain `false` because Exasol generates ad-hoc certificates for the tunnel that fail standard certificate validation in many client environments

### Scenario: HTTP transport TLS against Exasol Docker (self-signed certificate)

* *GIVEN* the application connects to a local `exasol/docker-db` instance with a control-channel connection string of the form `exasol://sys:exasol@localhost:8563/?validateservercertificate=0`
* *WHEN* the application performs an import or export through the HTTP transport tunnel
* *THEN* the application SHOULD set `use_tls(false)` on the import/export options
* *AND* the driver SHALL use a plain HTTP tunnel (no rustls wrap) so that the Exasol-side SQLProcess can connect back without certificate-validation failures from the ad-hoc cert

### Scenario: HTTP transport TLS against Exasol SaaS / production

* *GIVEN* the application connects to a managed or production Exasol cluster with TLS termination on the control channel and a trusted certificate chain
* *WHEN* the application performs an import or export through the HTTP transport tunnel
* *THEN* the application SHOULD set `use_tls(true)` on the import/export options
* *AND* the driver SHALL generate an ad-hoc RSA certificate, wrap the tunnel with rustls, and pass the SHA-256 fingerprint as `PUBLIC KEY 'sha256//<base64>'` in the IMPORT/EXPORT SQL `AT` clause
