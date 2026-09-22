# Decisions: change-udf-sdk-upgrade

## ADR: A timestamp is emitted at its source precision, clamped to what the engine can emit

**ID:** timestamp-emit-at-source-precision-clamped-by-engine
**Plan:** change-udf-sdk-upgrade
**Status:** Accepted

### Context

`exasol-udf-sdk` 0.28.1 made `ExaType::Timestamp` carry its declared fractional-second precision,
which `0.26.1` could not. Under the old, precision-blind type, the scan's Arrow coercion target was
fixed at `Timestamp(Microsecond, None)` for every declared `TIMESTAMP(p)`. That fixed target
destroyed every nanosecond digit of an Iceberg `timestamp_ns`/`timestamptz_ns` column through the
strict (`safe: false`) cast in `coerce_column`, an unrecorded gap distinct from the named Exasol
8.x millisecond-truncation trade-off. It also left the 8.x arm relying on Exasol silently narrowing
an emitted microsecond value rather than on a declared narrowing.

### Decision

The catalog declaration and the emit-boundary Arrow unit both follow the COLUMN's source width
(millisecond, microsecond, or nanosecond), clamped by what the running engine can emit: 3 only on
Exasol 8.x, and 3, 6, or 9 on Exasol 2025.x and later. On the catalog path, DECLARED therefore
equals EMITTED on both engine arms, so no component truncates after the scan.

### Options Considered

| Option | Verdict |
|--------|---------|
| Follow the column's source width, clamped by the engine's emit ceiling | ✓ Chosen — declared equals emitted on both engine arms; the round trip is exact by construction |
| Keep the fixed `Timestamp(Microsecond, None)` target at every declared precision | ✗ Rejected — destroys every nanosecond digit of an Iceberg `timestamp_ns` column, and leaves the 8.x arm an unmeasured reliance on Exasol narrowing a finer value |

### Consequences

All three emit widths were measured live against `exasol/docker-db:2025.1.16` and
`exasol/docker-db:8.29.13`, from a cleared `exa-data` volume on each leg: 15 test binaries, 0
failures on both, no SLC fingerprint mismatch, no width rejected.

On 2025.1.16, `SYS.EXA_ALL_COLUMNS` reports `TIMESTAMP(6)` for Iceberg `timestamp`/`timestamptz`
and `TIMESTAMP(9)` for `timestamp_ns`. The SLC accepted `Timestamp(Nanosecond, None)` into
`TIMESTAMP(9)` and `Timestamp(Millisecond, None)` into `TIMESTAMP(3)`, neither ever fed to it
before. A `timestamp_ns` v3 fixture, seeded through the `format-version` table property against
`apache/iceberg-rest-fixture:1.10.1` and iceberg-rust 0.10.0, carried two instants differing only
below the microsecond and both survived distinct, so the nanosecond evidence is end to end rather
than declaration-only.

On 8.29.13, the adapter declares all three timestamp columns bare `TIMESTAMP`,
`SYS.EXA_ALL_COLUMNS` reports each as `TIMESTAMP(3)`, and the SLC reports `precision: 3` for that
column, so `from_declared_digits` takes its `Millisecond` arm and DECLARED equals EMITTED there
too. A nanosecond column loses its six digits, exactly as the stated 8.x trade-off predicts.

## ADR: An inexpressible CAST precision is declined, not approximated

**ID:** cast-timestamp-precision-decline-not-approximate
**Plan:** change-udf-sdk-upgrade
**Status:** Accepted

### Context

For a projected `CAST(x AS TIMESTAMP(p))` with `p` outside `{0,3,6,9}`, `snap_timestamp_precision`
rendered the DataFusion side at the nearest member of that set instead of the requested value, a
silent approximation. It was paired with an unmeasured claim that the EMITS-declared Exasol column
would truncate the up-snapped value back to the requested `p`. The approximation also contradicted
this feature's own recorded rule that the rendered CAST target set must be exactly the set whose
DataFusion result matches Exasol's.

### Decision

`render_expression`'s DataFusion-dialect TIMESTAMP arm renders `p` in `{0,3,6,9}` verbatim and
declines every other value (`Err`/`None`); `snap_timestamp_precision` is deleted. A declined node
routes into SQL the adapter itself writes in the Exasol dialect, which renders `TIMESTAMP(p)`
verbatim for every `p` in 0-9, and Exasol computes the value natively there. Every position a CAST
node can occupy (select list, WHERE, GROUP BY, ORDER BY) already carries this route. No new route
is added.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline an inexpressible CAST precision in the DataFusion dialect | ✓ Chosen — removes a silent approximation and its unverified backing claim; the Exasol dialect already renders every `p` in 0-9 verbatim |
| Keep the snap and verify the up-snap truncation live | ✗ Rejected — verifies a workaround instead of removing the approximation, and still contradicts the feature's own exact-match rule |

### Consequences

Declining one select-list item widens the whole select list and routes the whole request to the
qualified single-table wrapper, so Exasol computes every select-list item, not only the declined
one. The sharded parallel fan-out, the referenced-column projection narrowing, and the scan-side
WHERE predicate are retained; the per-shard `LIMIT` and the bounded top-N are given up. For the six
exotic precisions `{1,2,4,5,7,8}`, exactness is judged the right side of that trade. Live measurement
on 2025.1.16 confirmed `CAST(ts AS TIMESTAMP(2))` renders as the qualified wrapper shape, with the
scan's own `EMITS` carrying the raw column rather than `TIMESTAMP(2)`, and the returned values match
Exasol's own `CAST` over the same literals in the same session.
