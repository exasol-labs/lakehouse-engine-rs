# Decisions: fix-196-select-list-predicate-pushdown

## ADR: Keep the pushable-node whitelist; do not invert it to "anything the translator renders"

**ID:** keep-select-list-pushable-node-whitelist
**Plan:** fix-196-select-list-predicate-pushdown
**Status:** Accepted

### Context

`project_columns` dispatches each select-list item through an explicit whitelist of node
types safe to project positionally. The whitelist had drifted from the set
`render_expression_safe` actually renders, so six renderable, advertised boolean predicate
node types fell into the catch-all arm and widened the projection to the full base row
(issue #196). Deleting the whitelist and pushing anything the translator renders would make
that drift structurally impossible and is a smaller diff.

### Decision

Extend `project_columns`'s select-list `match item_type` arm list with the six missing node
types. Keep the whitelist an explicit list rather than inverting the check.

### Options Considered

| Option | Verdict |
|--------|---------|
| Extend the explicit whitelist with the six missing node types | ✓ Chosen — refuses aggregate and unknown nodes by construction |
| Delete the whitelist; push any node `render_expression_safe` renders, decline only on `None` | ✗ Rejected — `render_expression_inner` has a `function_aggregate` arm (`vs-expression/src/lib.rs:1149`); inverting the check would render an aggregate select item into a row-scan projection and evaluate it per shard, a silently wrong result for a non-associative aggregate. A non-decomposable aggregate legitimately reaches `RequestShape::RowScan`, so this path is reachable |

### Consequences

The whitelist stays the single source of truth for what a row-scan projection may evaluate
per shard, at the cost of needing a deliberate edit whenever the translator gains a new safe
node type — the cost this plan itself paid to close the six-type gap.

## ADR: Route on the widening signal, not on a re-derived arity or type comparison

**ID:** route-widening-on-producer-signal
**Plan:** fix-196-select-list-predicate-pushdown
**Status:** Accepted

### Context

A count comparison in `build_dispatch_sql`'s `RequestShape::RowScan` arm caught some
full-base-row widenings by comparing the derived projection's column count against the
select-list arity, routing a mismatch to the qualified single-table wrapper. The comparison
is blind to a widened projection whose column count coincides with the select-list arity —
verified live: a 10-column base table with a 10-item select list produced `sqlCode 04000`
"Data type mismatch in column number 10... Expected BOOLEAN, but got DECIMAL(20,0))" instead
of routing to the wrapper — and it ran on none of the empty-result or broadcast-join paths.

### Decision

Return the `needs_full_fallback` boolean `project_columns` already computes from
`project_columns` / `extract_projection` / `extract_join_projection` as a third tuple element,
and route all three consumers (the dispatch path, the empty-result path, and the
broadcast-join path) on that flag directly. Delete the `select_list_len != proj_cols.len()`
comparison rather than extending it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Pipe the producer's own `needs_full_fallback` flag out to every consumer | ✓ Chosen — exact and cannot rot; the producer already computes it |
| Detect the full-base-row shape by comparing the projection against the column universe | ✗ Rejected — cannot distinguish a genuine `SELECT *`, which produces a byte-identical projection, from the widening without inferring intent from the select-list JSON — the same guessing that produced the bug |
| Compare each derived EMITS type against the parallel `selectListDataTypes` entry | ✗ Rejected — risks false positives on type-string normalisation between the adapter's Iceberg-derived type and Exasol's declared type; a false positive routes ordinary queries through a materialising wrapper |

### Consequences

Three call sites (dispatch, empty-result, broadcast-join) now consume one boolean signal
instead of three independent, partially-blind re-derivations, at the cost of a wide but
mechanical call-site churn (~30 sites, almost all test destructurings) whenever the tuple's
shape changes.
