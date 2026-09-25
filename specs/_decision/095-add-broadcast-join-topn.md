# Decisions: add-broadcast-join-topn

## ADR: A join shard ranks each sort key by the value it emits, and the join scan owns that rule

**ID:** join-shard-ranks-by-emitted-value
**Plan:** add-broadcast-join-topn
**Status:** Accepted

### Context

A per-shard post-join top-N is correct only when each shard's local ordering agrees with the
Exasol-side wrapper's ranking of the merged rows. Three emitted values differ from the native
value the shard's own scan reads: a column the JSON rendering or the `CAST(... AS VARCHAR)`
fallback covers arrives as text, an emitted empty string arrives as NULL because Exasol's
VARCHAR domain has no empty string (`'' IS NULL` is TRUE, captured live), and an emitted `NaN`
in a `Float32`/`Float64` column arrives as NULL because the `emit_batch` path returns a silent
NULL for it (issue #246). If a shard ranked the native value instead, its cut could drop a row
that the wrapper's global top-N needs, returning wrong rows with no error. `arrow_type_to_tag`
maps nested and binary types to `utf8`, so the adapter cannot see which keys the scan renders
as JSON, ruling out a plan-time guard.

### Decision

A per-shard sort whose output a merge re-ranks ranks the value the merge sees, and the module
that renders the emitted value owns that rule. For the join scan, `build_join_sql` renders each
post-join sort key over the key column's emitted expression, the same `render_join_select_item`
output the select list uses, wrapped in `nullif(<expr>, '')` for a string key and in
`CASE WHEN isnan(<expr>) THEN NULL ELSE <expr> END` for a `Float32`/`Float64` key. Direction and
NULL placement render through `SortKey::render_ordered`, the shared direction/NULL-placement
seam.

### Options Considered

| Option | Verdict |
|--------|---------|
| Render post-join sort keys by the emitted value, owned by the join scan | ✓ Chosen — matches the wrapper's ranking of the rows it actually receives |
| Render the keys with `render_order_by_clause`, as the raw scan does | ✗ Rejected — ranks the native value, which diverges from the emitted value the wrapper ranks |
| A plan-time guard that withholds the per-shard bound for a JSON-fallback key, as `detect_topn` does | ✗ Rejected — `arrow_type_to_tag` collapses nested/binary types to `utf8`, hiding the JSON-rendered keys from the adapter; also leaves the empty-string divergence open |
| Withhold the per-shard bound for every string key | ✗ Rejected — a string column is a common sort key, and the emitted-value rule serves it correctly |

### Consequences

The rule assumes DataFusion's comparison of two strings agrees with Exasol's VARCHAR comparison.
For partition-value range/`BETWEEN` comparisons this is already verified live against a running
Exasol instance — both use byte/codepoint order (`specs/vs-adapter/direct-storage-hive-partitioning/spec.md`,
the partition-pruning scenario). What remains unverified is that the same equivalence extends from
partition-value strings to an arbitrary post-join sort key's string value (e.g. longer values, or
values under a non-default NLS/collation setting); the flat-scan top-N path makes the same
unverified extension. A future fix of issue #246 that changes the emitted `NaN` value changes this
rule with it. Every future per-shard sort, including a fix of the flat-scan path's ranking
divergence, follows this rule.
