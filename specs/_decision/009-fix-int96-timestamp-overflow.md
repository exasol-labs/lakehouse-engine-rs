# Decisions: fix-int96-timestamp-overflow

## ADR: Coerce INT96 to microsecond, UTC, on read

**ID:** int96-coerce-microsecond-utc-on-read
**Plan:** `fix-int96-timestamp-overflow`
**Status:** Accepted

### Context

The scan bypasses the iceberg-rust reader, so it does not inherit that reader's INT96 fix. By default arrow-rs decodes Parquet INT96 as nanosecond timestamps, which cover only 1677 to 2262. A far-future INT96 value such as `9999-12-31 23:59:59` overflows on decode (#143).

### Decision

The scan decodes every Parquet INT96 timestamp as microsecond precision in UTC. This matches Iceberg Java and the Iceberg spec's microsecond timestamp types.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the nanosecond default | Rejected: it causes the overflow |
| Add a clamp or out-of-range fallback on top of decoding | Rejected: fix the root cause only |

### Consequences

INT96 sub-microsecond digits are truncated. Values above year 9999 remain unscannable and now fail at the Exasol emit boundary instead of at decode.
