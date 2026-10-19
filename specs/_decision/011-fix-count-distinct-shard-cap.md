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

## ADR: Case 2/3 routes to a qualified single-table wrapper

**ID:** count-distinct-case-2-3-qualified-wrapper
**Plan:** `fix-count-distinct-shard-cap`
**Status:** Accepted

### Context

Exasol never re-aggregates a declined pushdown. It runs the adapter's returned SQL as the final answer. A bare row scan returns raw columns where the request expects N aggregate columns, and Exasol rejects it at validation (`sqlCode 04000`, column-count mismatch, same bug class as #57). The scalar-subquery design failed for a different `04000`. Both failures have one cause: the adapter's SQL must produce the final result shape.

### Decision

Case 2/3 declines the fan-out and routes to a qualified single-table wrapper, the same pattern the grouped-aggregate decline already uses. The wrapper renders the exact single-group select list, including each `COUNT(DISTINCT)`, over a materialized sharded raw scan, so the adapter's SQL yields the one-row aggregate result. The single-group and grouped wrappers share one referenced-column helper for the inner-scan projection.

### Options Considered

| Option | Verdict |
|--------|---------|
| Bare row scan | Rejected: `04000` column-count mismatch |
| Per-distinct scalar subqueries | Rejected: `04000` emitting UDF in a scalar subquery |
| UNION-ALL of fan-outs plus outer grouping | Rejected: more complex than reusing the qualified-wrapper builder |

### Consequences

Case 2/3 returns the correct N-column aggregate shape for empty and non-empty results. The shared helper also closes #160, the grouped fallback's whole-table projection.

## ADR: Narrow Case 1 to bare-column arguments — expression-argument distinct always routes to the qualified wrapper

**ID:** count-distinct-case-1-bare-column-only
**Plan:** `fix-count-distinct-review-findings`
**Status:** Accepted

### Context

The fan-out declared its value column with the real Exasol type for a bare column but `VARCHAR(2000000)` for an expression argument. That relied on string-casting being injective on the expression's output type, which is not generally true: two distinct timestamps can print identically after truncation, so cross-shard dedup can undercount. The qualified wrapper already evaluates `COUNT(DISTINCT <expr>)` exactly over typed base columns with no cast.

### Decision

Case 1 applies only to a bare-column argument. A `COUNT(DISTINCT <expression>)`, alone or combined with other aggregates, always routes to the qualified single-table wrapper.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep the VARCHAR fan-out and document the injectivity assumption | Rejected: a documented assumption is not a proof, and no test covers every Arrow type's string cast |
| Keep the VARCHAR fan-out with an allowlist of string-castable types | Rejected: adds a maintained allowlist to preserve a shortcut the wrapper makes unnecessary |

### Consequences

Every `COUNT(DISTINCT <expression>)` is exact with no cast step. The fan-out value column always carries the column's real Exasol type, and the `VARCHAR(2000000)` arm is removed.
