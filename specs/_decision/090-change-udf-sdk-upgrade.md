# Decisions: change-udf-sdk-upgrade

## ADR: A timestamp is emitted at its source precision, clamped to what the engine can emit

**ID:** timestamp-emit-at-source-precision-clamped-by-engine
**Plan:** change-udf-sdk-upgrade
**Status:** Accepted

### Context

The SDK's `ExaType::Timestamp` now carries its fractional-second precision. Under the old precision-blind type, the scan's coercion target was fixed at microsecond for every declared `TIMESTAMP(p)`, which destroyed every nanosecond digit of an Iceberg `timestamp_ns` column. It also left Exasol 8.x relying on silent narrowing of an emitted microsecond value.

### Decision

The catalog declaration and the emit-boundary Arrow unit both follow the column's source width, clamped by what the running engine can emit: 3 only on Exasol 8.x, and 3, 6, or 9 on Exasol 2025.x and later. On the catalog path, the declared precision equals the emitted precision on both engine arms, so nothing truncates after the scan.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fixed microsecond target at every declared precision | Rejected: destroys nanosecond digits and leaves 8.x relying on unmeasured narrowing |

### Consequences

All three emit widths were measured live on Exasol 2025.1.16 and 8.29.13 with no failures and no rejected width. On 2025.x, the SLC accepted nanosecond into `TIMESTAMP(9)` and millisecond into `TIMESTAMP(3)`, and a `timestamp_ns` fixture kept two instants that differ below the microsecond. On 8.x, a nanosecond column loses its six digits, as the stated 8.x trade-off predicts.

## ADR: An inexpressible CAST precision is declined, not approximated

**ID:** cast-timestamp-precision-decline-not-approximate
**Plan:** change-udf-sdk-upgrade
**Status:** Accepted

### Context

For a projected `CAST(x AS TIMESTAMP(p))` with `p` outside `{0,3,6,9}`, the DataFusion dialect rendered the nearest supported precision, a silent approximation backed by an unmeasured claim that Exasol would truncate it back. This contradicted the rule that the rendered CAST target set must match Exasol's result exactly.

### Decision

The DataFusion dialect renders `p` in `{0,3,6,9}` verbatim and declines every other value, and `snap_timestamp_precision` is deleted. A declined node routes into adapter-written Exasol-dialect SQL, which renders every `p` in 0-9 verbatim, so Exasol computes the value natively. Every position a CAST can occupy already has this route.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the snap and verify the truncation live | Rejected: verifies a workaround instead of removing the approximation, and still breaks the exact-match rule |

### Consequences

Declining one select-list item routes the whole request to the qualified single-table wrapper, so Exasol computes every select-list item. The sharded fan-out, column projection narrowing, and scan-side WHERE are kept, and the per-shard `LIMIT` and bounded top-N are lost. For the exotic precisions `{1,2,4,5,7,8}`, exactness outweighs that cost. Live measurement on 2025.1.16 matched Exasol's own `CAST` results.
