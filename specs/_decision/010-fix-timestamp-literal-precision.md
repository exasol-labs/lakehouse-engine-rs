# Decisions: fix-timestamp-literal-precision

## ADR: Render timestamp literals at explicit microsecond precision via `arrow_cast`

**ID:** timestamp-literal-arrow-cast-microsecond
**Plan:** `fix-timestamp-literal-precision`
**Status:** Accepted

### Context

The scan coerces every Iceberg timestamp column to `Timestamp(Microsecond, …)` (decisions
`fix-timestamptz-mapping` and `int96-coerce-microsecond-utc-on-read`). The translator instead
rendered a bare `TIMESTAMP '…'` literal, which DataFusion's SQL frontend types as
`Timestamp(Nanosecond)`. When `simplify_expressions` unifies that nanosecond literal with a
microsecond column, it constant-folds the literal to nanosecond and overflows for any value
above `2262-04-11` — even values well inside the microsecond column's own range (issue #155).
The Apache Iceberg spec defines `timestamp` and `timestamptz` as microsecond precision, so
rendering literals at microsecond precision aligns the translator with the precision the spec
defines and the engine's existing column-typing decisions already establish.

### Decision

Render both literals through DataFusion's `arrow_cast(<value>, <arrow-type-string>)`:
`literal_timestamp` → `arrow_cast('<value>', 'Timestamp(Microsecond, None)')`;
`literal_timestamp_utc` → `arrow_cast('<value>+00:00', 'Timestamp(Microsecond, Some("UTC"))')` — the
value string still carries a literal `+00:00` offset so it parses as UTC, but the cast's tz label
is `"UTC"` (not `"+00:00"`) to match the scan's `Timestamptz` Arrow mapping (`types/mapping.rs`)
and avoid a tz-label mismatch during type unification. Timestamp literals never use the bare
`TIMESTAMP '…'` form again, and the literal value is single-quoted and escaped exactly as a
string literal.

### Options Considered

| Option | Verdict |
|--------|---------|
| `arrow_cast(…, 'Timestamp(Microsecond, …)')` | ✓ Chosen — the only SQL-surface form that pins an explicit arrow type; verified always-registered under this workspace's DataFusion feature pin |
| Bare `TIMESTAMP '…'` | ✗ Rejected — the bug itself; DataFusion's SQL frontend defaults it to `Timestamp(Nanosecond)` |
| `CAST(… AS TIMESTAMP)` | ✗ Rejected — DataFusion's plain `TIMESTAMP` cast target is also nanosecond |

### Consequences

A far-future timestamp literal (e.g. year 9999) no longer overflows `simplify_expressions`
when unified with the scan's microsecond-typed columns, so the CASE-WHEN clamp workaround for
issue #155 works. The prior UTC arm's raw `TIMESTAMP '{raw}+00:00'` string interpolation — an
unescaped injection vector — is closed, since both arms now route the literal value through the
same quoting/escaping path as string literals. The emit-boundary year-> 9999 failure remains
unchanged and stays tracked by #155.
