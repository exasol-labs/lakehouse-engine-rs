# Decisions: add-broadcast-join-topn

## ADR: A join shard ranks each sort key by the value it emits, and the join scan owns that rule

**ID:** join-shard-ranks-by-emitted-value
**Plan:** add-broadcast-join-topn
**Status:** Accepted

### Context

A per-shard post-join top-N is correct only when each shard's ordering agrees with the Exasol wrapper's ranking of the merged rows. Three emitted values differ from the native value the scan reads. JSON or `CAST(... AS VARCHAR)` fallback columns arrive as text. An empty string arrives as NULL, because Exasol's VARCHAR has no empty string. A `NaN` in a `Float32` or `Float64` column arrives as NULL (issue #246). If a shard ranked the native value, its cut could drop a row the wrapper needs, and the query would return wrong rows without error. The adapter cannot see which keys render as JSON, because `arrow_type_to_tag` maps nested and binary types to `utf8`, so a plan-time guard is not possible.

### Decision

A per-shard sort whose output a merge re-ranks ranks the value the merge sees, and the module that renders the emitted value owns that rule. For the join scan, `build_join_sql` renders each post-join sort key over the key column's emitted expression, the same one the select list uses. A string key is wrapped in `nullif(<expr>, '')` and a float key in `CASE WHEN isnan(<expr>) THEN NULL ELSE <expr> END`. Direction and NULL placement render through the shared `SortKey::render_ordered`.

### Options Considered

| Option | Verdict |
|--------|---------|
| Render keys with `render_order_by_clause`, as the raw scan does | Rejected: ranks the native value, which differs from the one the wrapper ranks |
| Plan-time guard that withholds the per-shard bound for a JSON-fallback key | Rejected: nested and binary types are hidden behind `utf8`, and the empty-string divergence stays open |
| Withhold the per-shard bound for every string key | Rejected: string columns are common sort keys, and the emitted-value rule serves them correctly |

### Consequences

The rule assumes DataFusion's string comparison agrees with Exasol's VARCHAR comparison. That is verified live for partition values but not for arbitrary sort keys, the same gap the flat-scan top-N path has. A future fix of issue #246 or of the flat-scan ranking divergence follows this rule too.
