# Decisions: fix-212-timestamp-precision-collapse

## ADR: TIMESTAMP precision field is fractionalSecondsPrecision, not precision

**ID:** timestamp-precision-field-is-fractional-seconds-precision
**Plan:** fix-212-timestamp-precision-collapse
**Status:** Accepted

### Context

A pushed-down `CAST(... AS TIMESTAMP(p))` with p other than 3 collapsed to bare `TIMESTAMP` (default precision 3), and Exasol rejected it (issue #212). The brief named a `precision` field, but the capture script never records the input data-type descriptor, so that name was never observed.

### Decision

Both the adapter EMITS derivation and the CAST renderer read `fractionalSecondsPrecision` for TIMESTAMP precision.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read `precision` | Rejected: Exasol uses `precision` only for DECIMAL and INTERVAL, so the fix would be a silent no-op |
| `fractionalSecondsPrecision` with a `precision` fallback | Rejected: over-engineering, since Exasol never sends `precision` on TIMESTAMP |

### Consequences

Future TIMESTAMP-precision work reads `fractionalSecondsPrecision`, the field in Exasol's data-type API doc and the repo fixtures.

## ADR: DataFusion-dialect CAST rendering snaps TIMESTAMP precision to the nearest supported unit

**ID:** timestamp-precision-snap-nearest-datafusion-dialect
**Plan:** fix-212-timestamp-precision-collapse
**Status:** Accepted

### Context

DataFusion parses `TIMESTAMP(p)` casts only for p in 0, 3, 6, 9, and rejects other values. Exasol accepts 0 to 9.

### Decision

The DataFusion dialect snaps an unsupported precision to the nearest of 0, 3, 6, 9 and clamps values above 9 to 9. The Exasol dialect and the EMITS clause render p unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Round up to the next supported unit | Rejected: departs from the recorded "nearest" design without fixing a defect |

### Consequences

Up-snaps (2, 5, 8) give a finer DataFusion value that the EMITS-declared column truncates back to p. The one down-snap (1 to 0) drops the tenths digit, an accepted trade-off, since DataFusion cannot parse `TIMESTAMP(1)` and Iceberg stores microseconds.
