# Decisions: fix-count-distinct-shard-cap

## ADR: Native-merge via Exasol's own COUNT(DISTINCT)

**ID:** count-distinct-native-merge
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

### Context

Cross-shard deduplication for `COUNT(DISTINCT col)` needs every distinct value, so its cost grows with cardinality and exceeds any fixed per-shard cap.

### Decision

Each shard streams one row per shard-local distinct value, and the outer query runs a plain `COUNT(DISTINCT)` over those rows. Exasol's aggregate engine performs the cross-shard deduplication.

### Options Considered

| Option | Verdict |
|--------|---------|
| Raise the fixed caps | Rejected: cost still grows with cardinality, the ceiling only moves |
| Fixed-width hash tokens | Rejected: cost still grows with cardinality |
| Value-hash shuffle | Rejected: every shard would scan every file, violating file-level no-overlap sharding |
| HyperLogLog or another mergeable sketch | Rejected: approximate, violates the exact-count requirement |

### Consequences

- Distinct cardinality is bounded only by Exasol's aggregate engine, not by a per-shard cap.

## ADR: The count stays byte-exact: approximate distinct is rejected

**ID:** count-distinct-exact-not-approximate
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

### Context

An approximate sketch such as HyperLogLog could scale `COUNT(DISTINCT)` without a cap.

### Decision

`COUNT(DISTINCT col)` stays exact. Sketch-based counting is out of scope.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fixed-size mergeable sketches (HLL) | Rejected: trades exactness for scale, against the project's correctness-first mission |

### Consequences

- Every scaling fix for this function preserves exact results.

