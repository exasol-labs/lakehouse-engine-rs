# Plan: fix-212-timestamp-precision-collapse

## Summary

Keep a pushed-down `CAST(... AS TIMESTAMP(p))` at its declared fractional-seconds precision, so Exasol stops rejecting the pushdown with SQL error 04000 (issue #212). Precision must survive at three collapse points that ship together — the vs-expression CAST renderer, the adapter's EMITS-type derivation, and the scan's `exasol_type_to_arrow` emit-boundary coercion — because a `TIMESTAMP(p)` string any one point mishandles either fails Exasol's `Expected TIMESTAMP(6), but got TIMESTAMP(3)` type check or gets stringified at emit.

## Context

A result column Exasol types as `TIMESTAMP(p)` with `p != 3` (e.g. `CAST(c_ts AS TIMESTAMP(6))`) collapses to bare `TIMESTAMP` (Exasol's default `TIMESTAMP(3)`) at three independent points:

1. **EMITS declaration** — `crates/lakehouse-engine/src/adapter/pushdown/support.rs::exasol_type_from_json` reads a TIMESTAMP dataType's `withLocalTimeZone` flag but drops its precision, always emitting `"TIMESTAMP"`. This is the string Exasol's `EXPLAIN VIRTUAL` type check validates against.
2. **CAST rendering** — `crates/vs-expression/src/lib.rs::render_cast_target`'s `"TIMESTAMP"` arm renders bare `"TIMESTAMP"` in both the DataFusion and Exasol dialects.
3. **Emit-boundary coercion** — `crates/lakehouse-engine/src/types/mapping.rs::exasol_type_to_arrow` matches TIMESTAMP by exact string compare (`upper == "TIMESTAMP"`), with no parse arm for `TIMESTAMP(p)` (contrast `parse_decimal_args` for `DECIMAL(p,s)`). Once points 1-2 ship, this function receives `"TIMESTAMP(6)"`, falls through every arm, and returns `None`. The scan's `target_arrow_type` (`crates/lakehouse-engine/src/scan/emit.rs`) treats `None` as "route through the `Utf8`/VARCHAR string path" and casts the `Timestamp(Microsecond)` result column to a stringified timestamp — a value that mismatches the `TIMESTAMP(6)` EMITS declaration. This consumes the EMITS string on every emit path: plain row-scan projection, grouped-aggregate fan-out inner scans (`grouped_agg.rs`), and broadcast-join legs (`joins/planning.rs`).

The precision field is `fractionalSecondsPrecision`, not `precision`. This was verified against Exasol's `virtual-schema-common-java` data-type API doc and the reference fixture `pushdown_request_alltypes.json` (`C_TIMESTAMP_4` = `{"type":"TIMESTAMP","fractionalSecondsPrecision":7}`), and matches the repo's own committed test fixtures. The brief's `precision` assumption was uncaptured; see decision-log entry [1].

- **Goals** — a pushed-down TIMESTAMP CAST at any Exasol precision (0-9) passes Exasol's pushdown type check and returns correct values.
- **Non-Goals** — the raw-column `createVirtualSchema` TIMESTAMP schema declaration (`datafusion-scan/type-mapping`'s "Iceberg timestamptz maps to plain Exasol TIMESTAMP" scenario, always bare TIMESTAMP — untouched; this plan adds only the reverse `exasol_type_to_arrow` parse arm in the same feature), `TIMESTAMP WITH LOCAL TIME ZONE` (still declined), and any type other than TIMESTAMP.

## Design

### Decision

Read `fractionalSecondsPrecision` at the two producing collapse points, and teach the third (consuming) point to parse the resulting `TIMESTAMP(p)` string. Render the precision verbatim where the consumer's parser accepts 0-9 (Exasol: the EMITS clause and the Exasol-dialect wrapper SQL); snap it to the nearest DataFusion-supported unit where the parser is restricted; ignore `p` for the Arrow-target lookup (a single microsecond representation covers every precision).

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Verbatim precision | `exasol_type_from_json` EMITS; `render_cast_target` Exasol dialect | Exasol's parser accepts every precision 0-9 |
| Snap to nearest of `{0,3,6,9}` | `render_cast_target` DataFusion dialect | DataFusion 54 parses `TIMESTAMP(p)` only for `{0,3,6,9}`; 1,2,4,5,7,8 are parse errors |
| Parse `TIMESTAMP(p)`, drop `p`, map to `Timestamp(Microsecond, None)` | `exasol_type_to_arrow` emit-boundary coercion | Any precision collapses to the one Arrow representation on the way out, matching the way in; the declared `p` is Exasol's type-check concern only |
| Colocated pure helper | `snap` helper beside `render_cast_target` | Mirrors #211's `format_decimal_exasol_style` colocation precedent |
| WLTZ declines/short-circuits first | all three functions | Unchanged: WLTZ short-circuits before any precision logic; `exasol_type_from_json` still emits bare `TIMESTAMP WITH LOCAL TIME ZONE` (no `(p)`), so `exasol_type_to_arrow`'s existing WLTZ arm needs no change |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Read `fractionalSecondsPrecision` | `precision` (per brief) | Authoritative Exasol source + repo fixtures; `precision` would silently no-op |
| Snap to NEAREST for DataFusion | Ceil to next supported (always keep ≥ requested) | Honors the recorded design; sole down-snap `1→0` is an accepted, named trade-off |
| Absent precision → bare TIMESTAMP | Explicit `TIMESTAMP(3)` | Preserves existing behavior/tests; bare == Exasol `TIMESTAMP(3)` |
| `exasol_type_to_arrow` drops `p` (microsecond target for every precision) | Vary the Arrow `TimeUnit` by `p` | The project already collapses every TIMESTAMP precision to `Timestamp(Microsecond, None)` on the way in; matching it out keeps the round-trip consistent and avoids a coercion the emit path does not need |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| datafusion-scan/type-mapping | CHANGED | `datafusion-scan/type-mapping/spec.md` |

## Implementation Tasks

1. **vs-expression — CAST renderer (collapse point 2)**
   1. Add a colocated pure helper that snaps a precision to the nearest DataFusion-supported unit (`0→0,1→0,2→3,4→3,5→6,7→6,8→9`; `0/3/6/9` identity; clamp `>9` to 9), then rewrite `render_cast_target`'s `"TIMESTAMP"` arm: decline `withLocalTimeZone: true` first (unchanged); read `fractionalSecondsPrecision`; absent → bare `TIMESTAMP`; present → `TIMESTAMP(p)` verbatim for `Dialect::Exasol`, `TIMESTAMP(snap(p))` for `Dialect::DataFusion`. [expert]
   2. Update `renders_cast_timestamp_without_local_time_zone` (precision 3 present now renders `TIMESTAMP(3)`) and add unit tests: Exasol dialect 0/6/9 verbatim; DataFusion dialect 6 verbatim; DataFusion dialect 5 → `TIMESTAMP(6)`; absent → bare `TIMESTAMP` in both dialects; `withLocalTimeZone: true` still declines regardless of precision. [expert]

2. **adapter — EMITS type derivation (collapse point 1)**
   1. Rewrite `exasol_type_from_json`'s `"timestamp"` arm: keep the `withLocalTimeZone` precedence and its `TIMESTAMP WITH LOCAL TIME ZONE` output; otherwise read `fractionalSecondsPrecision` and render `TIMESTAMP(p)` when present, bare `TIMESTAMP` when absent.
   2. Add unit tests beside `exasol_type_from_json_reads_with_local_time_zone_flag`: precision 0/6/9 → `TIMESTAMP(p)`; absent → `TIMESTAMP`; `withLocalTimeZone: true` + precision → `TIMESTAMP WITH LOCAL TIME ZONE` (WLTZ precedence).

3. **scan — emit-boundary coercion (collapse point 3)**
   1. Teach `exasol_type_to_arrow` (`crates/lakehouse-engine/src/types/mapping.rs`) to parse a `TIMESTAMP(p)` string (in addition to the existing bare `"TIMESTAMP"` exact match) and return `Some(DataType::Timestamp(TimeUnit::Microsecond, None))` regardless of `p`. `TIMESTAMP WITH LOCAL TIME ZONE` needs no change — its WLTZ producer emits no `(p)` suffix (decision [3]).
   2. Add a unit test in the existing test module beside `exasol_type_to_arrow_reproduces_decimal_precision_binning` asserting `exasol_type_to_arrow("TIMESTAMP(6)")`, `("TIMESTAMP(0)")`, and `("TIMESTAMP(9)")` each equal `Some(DataType::Timestamp(TimeUnit::Microsecond, None))`. This test MUST fail on current exact-match-only code and pass after task 3.1.

4. **Verification** — run the checklist below; capture the repro end to end.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2 (`crates/vs-expression/src/lib.rs`) |
| Group B | 2.1, 2.2 (`crates/lakehouse-engine/src/adapter/pushdown/support.rs`) |
| Group C | 3.1, 3.2 (`crates/lakehouse-engine/src/types/mapping.rs`) |
| Group D | 4 |

Sequential dependencies: Groups A, B, and C touch three independent files with no shared state and run concurrently. Group D (Verification) runs after all three: its end-to-end manual repro only succeeds once all three collapse points land together — a `TIMESTAMP(p)` string produced by A and B must be consumed correctly by C at the emit boundary.

## Dead Code Removal

No code is removed. One existing test's expectation changes: `renders_cast_timestamp_without_local_time_zone` (`crates/vs-expression/src/lib.rs`) currently sends `fractionalSecondsPrecision: 3` and expects bare `TIMESTAMP`; after the fix a present precision of 3 renders `TIMESTAMP(3)`, so the assertion updates to `CAST("X" AS TIMESTAMP(3))` (task 1.2).

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| CAST to TIMESTAMP renders the declared fractional-seconds precision per SQL dialect | Unit | `crates/vs-expression/src/lib.rs` | `renders_cast_timestamp_precision_per_dialect` |
| CAST translates to DataFusion CAST syntax (regression: WLTZ still declines) | Unit | `crates/vs-expression/src/lib.rs` | `cast_to_unsupported_target_falls_back` |
| Projected CAST expression preserves the declared TIMESTAMP fractional-seconds precision in its EMITS type | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `exasol_type_from_json_reads_timestamp_fractional_seconds_precision` |
| A TIMESTAMP(p) EMITS string maps back to the microsecond Arrow timestamp | Unit | `crates/lakehouse-engine/src/types/mapping.rs` | `exasol_type_to_arrow_parses_timestamp_precision` |

All three scenarios are pure string-to-type computation with no I/O, so unit tests are the correct proof form.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Combined precision fix, identity precision (exercises all three points) | `scripts/capture-pushdown-payload.sh 'SELECT CAST(c_ts AS TIMESTAMP(6)) FROM {table}'` | `EXPLAIN VIRTUAL` succeeds (no 04000); echoed scan-spec JSON shows `"projection":[{"expr":"CAST(\"C_TS\" AS TIMESTAMP(6))"}]` and `"emit_exa_types":["TIMESTAMP(6)"]`; the `SELECT` RETURNS actual TIMESTAMP rows with correct microsecond values — NOT stringified timestamps (confirms `exasol_type_to_arrow` kept the column a timestamp at the emit boundary) |
| Combined precision fix, non-identity DataFusion snap (exercises the DataFusion snap + all three points) | `scripts/capture-pushdown-payload.sh 'SELECT CAST(c_ts AS TIMESTAMP(5)) FROM {table}'` | `EXPLAIN VIRTUAL` succeeds (no 04000); the DataFusion-dialect projection renders `TIMESTAMP(6)` (5 snaps to nearest supported); `"emit_exa_types":["TIMESTAMP(5)"]`; the `SELECT` RETURNS TIMESTAMP rows truncated to the declared `TIMESTAMP(5)` precision, confirming the up-snap-then-truncate round-trip |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
