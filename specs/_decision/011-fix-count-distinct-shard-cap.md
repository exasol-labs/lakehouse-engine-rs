# Decisions: fix-count-distinct-shard-cap

## ADR: Native-merge via Exasol's own COUNT(DISTINCT)

**ID:** count-distinct-native-merge
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

### Context

`COUNT(DISTINCT col)` over a high-cardinality column failed with `ResourcesExhausted`. Each shard serialized its distinct values into a JSON array under a fixed byte and element cap, and a merge UDF unioned the arrays. Cross-shard dedup needs every distinct value, so the cost grows with cardinality and exceeds any fixed cap.

### Decision

Each shard streams one row per shard-local distinct value, and the outer wrapper runs a plain `COUNT(DISTINCT "V")` over those rows. Exasol's own aggregate engine performs the cross-shard dedup.

### Options Considered

| Option | Verdict |
|--------|---------|
| Raise the fixed caps | Rejected: cost still grows with cardinality, the ceiling only moves |
| Fixed-width hash tokens | Rejected: cost still grows with cardinality |
| Value-hash shuffle | Rejected: every shard would scan every file, violating file-level no-overlap sharding |
| HyperLogLog or another mergeable sketch | Rejected: approximate, violates the exact-count requirement |

### Consequences

The per-shard cap, its serialization code, the `AggKind::CountDistinct` variant, and the `LAKEHOUSE_DISTINCT_MERGE_COUNT` UDF are removed. Distinct cardinality is bounded only by Exasol's aggregate engine.

## ADR: The count stays byte-exact — approximate distinct is rejected

**ID:** count-distinct-exact-not-approximate
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

### Context

An approximate sketch (HyperLogLog, about 1-2% error) could scale `COUNT(DISTINCT)` past a fixed per-shard cap.

### Decision

`COUNT(DISTINCT col)` MUST remain exact. Sketch-based counting is out of scope.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fixed-size mergeable sketches (HLL) | Rejected: trades exactness for scale, against the project's correctness-first mission |

### Consequences

Every scaling fix for this function must preserve byte-exact results.

