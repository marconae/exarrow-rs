[Home](index.md) · [Setup & Connect](setup-and-connect.md) · [Queries](queries.md) · [Prepared Statements](prepared-statements.md) · [Import/Export](import-export.md) · [Types](type-mapping.md) · [Driver Manager](driver-manager.md)

---

# Type Mapping

Type mappings below apply to the **native TCP transport** (the default). See [WebSocket transport differences](#websocket-transport-differences) if you use `transport=websocket`.

## Exasol to Arrow Type Conversions

| Exasol Type                      | Arrow Type                          | Notes                           |
|----------------------------------|-------------------------------------|---------------------------------|
| `BOOLEAN`                        | `Boolean`                           |                                 |
| `CHAR`, `VARCHAR`                | `Utf8`                              |                                 |
| `DECIMAL(p, s)`                  | `Decimal128(p, s)`                  | Precision 1–36                  |
| `DOUBLE`                         | `Float64`                           |                                 |
| `REAL`                           | `Float64`                           |                                 |
| `INTEGER`                        | `Int64`                             |                                 |
| `SMALLINT`                       | `Int32`                             |                                 |
| `DATE`                           | `Date32`                            |                                 |
| `TIMESTAMP`                      | `Timestamp(Microsecond, None)`      | 0–9 fractional digits           |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `Timestamp(Microsecond, "UTC")`     |                                 |
| `INTERVAL YEAR TO MONTH`         | `Utf8`                              | e.g. `"1-3"` (1 year 3 months)  |
| `INTERVAL DAY TO SECOND`         | `Utf8`                              | e.g. `"3 04:05:06.789"`         |
| `BINARY`                         | `Binary`                            |                                 |
| `GEOMETRY`                       | `Utf8`                              | WKT format                      |
| `HASHTYPE`                       | `Utf8`                              | Hex-encoded hash string         |

## Precision and Scale

### DECIMAL

Exasol supports `DECIMAL(p, s)` where:
- **Precision (p):** 1 to 36 digits
- **Scale (s):** 0 to p

Arrow `Decimal128` preserves the exact precision and scale.

### TIMESTAMP

Exasol timestamps support 0–9 fractional digits. Arrow stores timestamps at microsecond precision, which covers most use cases.

### INTERVAL

Both interval types are returned as `Utf8` strings preserving the Exasol string representation:
- `INTERVAL YEAR TO MONTH`: `"<years>-<months>"` (e.g. `"1-3"` for 1 year 3 months)
- `INTERVAL DAY TO SECOND`: `"<days> <HH>:<MM>:<SS>.<fractional>"` (e.g. `"3 04:05:06.789"`)

## GEOMETRY

Geometry values are returned in **WKT (Well-Known Text)** format as Arrow `Utf8` columns:

```
POINT (1.0 2.0)
LINESTRING (0 0, 1 1, 2 2)
POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0))
```

## HASHTYPE

Hash values (MD5, SHA-1, SHA-256, etc.) are returned as hex-encoded `Utf8` strings.

## NULL Handling

All types support NULL values. Arrow represents nulls via validity bitmaps, which is compatible with Exasol's NULL semantics.

## WebSocket Transport Differences

When using `transport=websocket`, the following types map differently from the native protocol:

| Exasol Type              | Native (default)         | WebSocket                  |
|--------------------------|--------------------------|----------------------------|
| `INTERVAL YEAR TO MONTH` | `Utf8`                   | `Interval(MonthDayNano)`   |
| `INTERVAL DAY TO SECOND` | `Utf8`                   | `Interval(MonthDayNano)`   |
| `GEOMETRY`               | `Utf8` (WKT)             | `Binary` (WKB)             |
