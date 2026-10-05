# Decisions: fix-212-timestamp-precision-collapse

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
