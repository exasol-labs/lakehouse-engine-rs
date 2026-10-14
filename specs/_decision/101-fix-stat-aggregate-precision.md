# Decisions: fix-stat-aggregate-precision

## ADR: Statistical aggregates merge per-shard means and squared deviations in double precision

**ID:** stat-aggregates-merge-mean-and-m2-in-double
**Plan:** fix-stat-aggregate-precision
**Status:** Accepted
**Supersedes:** stddev-variance-pushdown-via-count-sum-sum-sq-sufficient-statistics

### Context

A per-shard `SUM(x * x)` overflows integer columns, and the merge `sumsq - sum² / n` cancels to wrong or negative variances when values are large compared with their spread. DataFusion computes variance only in double precision.

### Decision

Each shard emits its count, its mean, and its sum of squared deviations from that mean. The wrapper merges them with Chan's parallel formula around a grand mean shifted by one shard's mean. Results may be less precise than native Exasol's exact DECIMAL computation, as `docs/semantic-differences.md` documents.

### Options Considered

| Option | Verdict |
|--------|---------|
| Shift each value by an exact DECIMAL reference before the double cast | Rejected: needs a custom DataFusion accumulator, a fourth partial column, and a DECIMAL merge in Exasol |
| Keep count, sum, and sum of squares in a wider type | Rejected: the cancellation in `sumsq - sum² / n` remains |

### Consequences

- A group whose values are all equal returns exactly `0`.
- Values that are large compared with their spread lose about one significant digit per power of ten of that ratio.
