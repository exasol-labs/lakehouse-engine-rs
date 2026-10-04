# Decisions: fix-196-select-list-predicate-pushdown

## ADR: Keep the pushable-node whitelist; do not invert it to "anything the translator renders"

**ID:** keep-select-list-pushable-node-whitelist
**Plan:** fix-196-select-list-predicate-pushdown
**Status:** Accepted

### Context

Select-list projection uses an explicit whitelist of node types safe to project per shard. It drifted from what the translator renders, so six advertised boolean predicate types widened the projection to the full base row (issue #196).

### Decision

The whitelist gains the six missing node types and stays an explicit list.

### Options Considered

| Option | Verdict |
|--------|---------|
| Delete the whitelist and push anything the translator renders | Rejected: the translator also renders aggregate nodes, which would be evaluated per shard and give wrong results for non-associative aggregates, and a non-decomposable aggregate does reach the row-scan path |

### Consequences

The whitelist is the single source of truth for what a row-scan projection may evaluate per shard. A new safe translator node type needs a deliberate whitelist edit.

## ADR: Route on the widening signal, not on a re-derived arity or type comparison

**ID:** route-widening-on-producer-signal
**Plan:** fix-196-select-list-predicate-pushdown
**Status:** Accepted

### Context

The row-scan path compared the projection's column count with the select-list arity to detect full-base-row widening. That misses a widened projection whose count equals the arity, which Exasol then rejects with `sqlCode 04000`. The check also did not run on the empty-result or broadcast-join paths.

### Decision

The projection builder returns the `needs_full_fallback` flag it already computes, and the dispatch, empty-result, and broadcast-join paths route on that flag. The count comparison is deleted.

### Options Considered

| Option | Verdict |
|--------|---------|
| Compare the projection against the column universe | Rejected: cannot tell a genuine `SELECT *` from the widening without guessing intent from the select list |
| Compare each EMITS type against `selectListDataTypes` | Rejected: type-string normalization differences cause false positives that send ordinary queries through a materializing wrapper |

### Consequences

Three call sites consume one signal instead of three partial re-derivations. Changing the tuple shape churns about 30 call sites, mostly test destructurings.
