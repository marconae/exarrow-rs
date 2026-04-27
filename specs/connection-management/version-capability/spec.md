# Feature: Server Version Capability Detection

Provides a small utility that parses Exasol's `release_version` string captured during login and answers feature-availability questions (e.g. "does this server accept native Parquet import?") so that higher-level code paths can branch without issuing extra round-trips.

## Background

Every successful login — over both native TCP and WebSocket — populates `Session::server_info().release_version` with a dotted string such as `"7.1.0"`, `"8.27.1"`, or `"2025.1.11"`. The driver SHALL parse this string into a comparable `(major, minor, patch)` tuple at first use and cache it, then expose `supports_native_parquet_import()` and a generic `supports_at_least(major, minor, patch)` predicate. Parsing is lenient: trailing build/qualifier suffixes are ignored, and any string that fails to parse SHALL be treated as "below every threshold" so that the driver falls back to the legacy code path on unknown versions.

Native Parquet import is available from Exasol 2025.1.11 onward (matching the JDBC driver's `exaVersionInt >= 20250111` test gate). The threshold predicate uses lexicographic `(major, minor, patch)` comparison against the constant `(2025, 1, 11)`.

## Scenarios

### Scenario: Parse a valid release_version

* *GIVEN* a connected session whose `server_info().release_version` is `"2025.1.11"`
* *WHEN* the driver calls `parse_release_version` on that string
* *THEN* the returned tuple MUST be `(2025, 1, 11)`
* *AND* the parser MUST accept strings of the form `"<u32>.<u32>"` and `"<u32>.<u32>.<u32>"`
* *AND* the parser MUST tolerate trailing suffixes such as `"-rc1"`, `"+build.5"`, or `"_dev"` by stripping them before parsing the patch component

### Scenario: Reject an unparsable release_version

* *GIVEN* a `release_version` value that does not start with a numeric major component (e.g. `""`, `"unknown"`, or `"v1"`)
* *WHEN* the driver attempts to parse it
* *THEN* the parser MUST return `None`
* *AND* `supports_native_parquet_import()` MUST return `false` for that session
* *AND* `supports_at_least(major, minor, patch)` MUST return `false` for every threshold

### Scenario: Native Parquet import threshold

* *GIVEN* a parsed `(major, minor, patch)` tuple for the connected session
* *WHEN* `supports_native_parquet_import()` is called
* *THEN* it MUST return `true` if and only if `(major, minor, patch) >= (2025, 1, 11)` lexicographically
* *AND* the boundary cases MUST hold: `(2025,1,10)` yields `false`, `(2025,1,11)` yields `true`, `(2025,2,0)` yields `true`, `(2026,0,0)` yields `true`, `(7,1,0)` yields `false`, and `(8,31,0)` yields `false`

### Scenario: Capability check is in-process and round-trip-free

* *GIVEN* a connected session with a populated `server_info`
* *WHEN* any caller invokes `supports_native_parquet_import()` or `supports_at_least(major, minor, patch)`
* *THEN* the call MUST be synchronous and MUST NOT issue any network I/O
* *AND* the call MUST be safe to invoke repeatedly without measurable cost (parse result MAY be memoized)
