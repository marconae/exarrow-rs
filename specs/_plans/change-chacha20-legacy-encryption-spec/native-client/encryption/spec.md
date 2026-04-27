# Feature: ChaCha20 Session Encryption

The native TCP protocol supports ChaCha20 stream encryption as a legacy message-encryption mode for connections that do not use TLS. When TLS is active (the default for Exasol 7.1+ and mandatory on Exasol 8), ChaCha20 is NOT used — TLS provides all transport and message confidentiality. When TLS is disabled (e.g. `tls=false` against pre-7.1 Exasol servers, matching the C++ reference driver's "legacy encryption" mode), the client generates random send and receive ChaCha20 keys, encrypts them with the server's RSA public key, transmits them during login, and encrypts all subsequent message payloads with ChaCha20.

## Background

ChaCha20 and TLS are MUTUALLY EXCLUSIVE in the native TCP protocol, matching the Exasol C++ reference driver (`useLegacyEncryption` mode in `dwaSocket.cpp`): when both are requested, SSL is disabled and ChaCha20 takes over; when TLS is active, ChaCha20 is never negotiated. Protocol version 14 replaced the older RC4 cipher with ChaCha20 in the non-TLS (legacy) encryption path — it did NOT mandate ChaCha20 for TLS connections. Protocol version 18+ (Exasol 7.1+) uses TLS exclusively and never exchanges ChaCha20 keys. When ChaCha20 is used, keys are 32 bytes, the key exchange happens during the login phase via `CMD_SET_ATTRIBUTES` with `ATTR_CLIENT_SEND_KEY`, `ATTR_CLIENT_RECEIVE_KEY`, and `ATTR_CLIENT_KEYS_LEN`, and all message payloads after the handshake are ChaCha20-encrypted (headers remain plaintext for framing).

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: ChaCha20 key generation

* *GIVEN* a native TCP connection has been established without TLS (legacy encryption mode)
* *AND* the native protocol login handshake has received the server's RSA public key
* *WHEN* preparing encryption keys
* *THEN* the system SHALL generate two cryptographically random 32-byte keys (send key and receive key)
* *AND* the system SHALL use a cryptographically secure random number generator
* *AND* the system MUST NOT generate ChaCha20 keys when TLS is active
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: RSA-encrypted key exchange

* *GIVEN* ChaCha20 send and receive keys have been generated (TLS is not active)
* *AND* the server's RSA public key and random phrase are available
* *WHEN* exchanging encryption keys with the server
* *THEN* the system SHALL RSA-encrypt each key using the server's public key via Exasol's raw RSA modular exponentiation scheme (no PKCS#1 padding)
* *AND* the system SHALL send `ATTR_CLIENT_SEND_KEY`, `ATTR_CLIENT_RECEIVE_KEY`, and `ATTR_CLIENT_KEYS_LEN` via `CMD_SET_ATTRIBUTES`
* *AND* the system MUST NOT send these attributes when TLS is active
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Encrypted message sending

* *GIVEN* ChaCha20 key exchange has completed successfully (legacy encryption mode)
* *WHEN* sending any message after the handshake
* *THEN* the system SHALL encrypt the message payload using ChaCha20 with the send key
* *AND* the message header SHALL remain unencrypted for framing purposes
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: Encrypted message receiving

* *GIVEN* ChaCha20 key exchange has completed successfully (legacy encryption mode)
* *WHEN* receiving any message after the handshake
* *THEN* the system SHALL decrypt the message payload using ChaCha20 with the receive key
* *AND* the system SHALL verify that decrypted data is valid before processing
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: TLS and ChaCha20 are mutually exclusive

* *GIVEN* a native TCP connection is being authenticated
* *WHEN* selecting the message-encryption mode for the session
* *THEN* the system SHALL use ChaCha20 if and only if TLS is NOT active on the transport
* *AND* the system MUST NOT run TLS and ChaCha20 simultaneously on the same connection
* *AND* when TLS is active the system MUST NOT generate, exchange, or apply ChaCha20 keys
* *AND* when TLS is inactive the system SHALL use ChaCha20 to encrypt all message payloads after the handshake
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: ChaCha20 used only when TLS is absent (legacy mode)

* *GIVEN* the connection string sets `tls=false` (legacy-encryption mode against a pre-7.1 Exasol server)
* *WHEN* the native protocol login handshake completes
* *THEN* the system SHALL perform the ChaCha20 key exchange during `CMD_SET_ATTRIBUTES`
* *AND* the system SHALL apply ChaCha20 encryption to all subsequent message payloads
* *AND* the system MUST NOT attempt any TLS negotiation on the underlying socket
<!-- /DELTA:NEW -->

<!-- DELTA:NEW -->
### Scenario: ChaCha20 skipped when TLS is active

* *GIVEN* a native TCP connection has completed TLS negotiation successfully
* *WHEN* the native protocol login handshake completes
* *THEN* the system MUST NOT generate ChaCha20 keys
* *AND* the system MUST NOT include `ATTR_CLIENT_SEND_KEY`, `ATTR_CLIENT_RECEIVE_KEY`, or `ATTR_CLIENT_KEYS_LEN` in `CMD_SET_ATTRIBUTES`
* *AND* all subsequent message payloads SHALL be transmitted in plaintext inside the TLS tunnel (no ChaCha20 layer)
<!-- /DELTA:NEW -->
