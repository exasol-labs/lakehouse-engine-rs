# Decisions: refactor-pushdown-agg-dedup

## ADR: The single source of truth is a per-column descriptor, not a column count

**ID:** one-descriptor-owns-the-partial-column-set-not-a-count
**Plan:** refactor-pushdown-agg-dedup
**Status:** Accepted

### Context

The partial-aggregate column contract (how many columns each `AggKind` contributes, in what order and name, and what an empty shard puts in each) was encoded independently at five sites across the scan and adapter modules: the three sites the issue names plus `partial_emits_items` and `merge_select_items`. Nothing enforced agreement, and a disagreement is silent.

### Decision

`AggKind` exposes a per-column descriptor list, and all five contract sites are driven from it. The descriptor enumerates the ten distinct partial columns the contract admits.

### Options Considered

| Option | Verdict |
|--------|---------|
| A bare column count, as the issue suggests | Rejected: drives only `partial_row_from_batch`, and the other four sites would still match on `AggKind` |
| A slice of (name suffix, is-counter) tuples | Rejected: tuples of primitives cannot be matched exhaustively, so a new column would silently default instead of failing to compile |

### Consequences

Extending the contract means adding a case to an exhaustive match, which is a compile error at every renderer.
