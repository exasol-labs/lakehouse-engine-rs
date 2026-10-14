# Decisions: fix-timestamp-literal-precision

## ADR: Render timestamp literals at explicit microsecond precision via `arrow_cast`

**ID:** timestamp-literal-arrow-cast-microsecond
**Plan:** `fix-timestamp-literal-precision`
**Status:** Accepted

### Context

The scan types every Iceberg timestamp column as microsecond. The translator rendered a bare `TIMESTAMP '…'` literal, which DataFusion types as nanosecond. Unifying it with a microsecond column overflows for values above `2262-04-11` (#155). The Iceberg spec defines `timestamp` and `timestamptz` as microsecond.

### Decision

The translator renders timestamp literals through `arrow_cast` to microsecond precision. The UTC literal carries a `+00:00` offset in its value and the type label `"UTC"`, matching the scan's `timestamptz` mapping. The literal value is quoted and escaped like a string literal.

### Options Considered

| Option | Verdict |
|--------|---------|
| Bare `TIMESTAMP '…'` | Rejected: DataFusion types it as nanosecond, which causes the bug |
| `CAST(… AS TIMESTAMP)` | Rejected: the plain `TIMESTAMP` cast target is also nanosecond |

### Consequences

Far-future timestamp literals no longer overflow, so the CASE-WHEN clamp workaround for #155 works. The unescaped interpolation in the old UTC arm is closed. The emit-boundary failure above year 9999 remains tracked by #155.
