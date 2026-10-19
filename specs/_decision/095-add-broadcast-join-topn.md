# Decisions: add-broadcast-join-topn

## ADR: A join shard ranks each sort key by the value it emits, and the join scan owns that rule

**ID:** join-shard-ranks-by-emitted-value
**Plan:** add-broadcast-join-topn
**Status:** Accepted

### Context

A per-shard post-join top-N is correct only when each shard's ordering agrees with the Exasol wrapper's ranking of the merged rows. Three emitted values differ from the native value the scan reads: JSON or cast fallback columns arrive as text, an empty string arrives as NULL, and a float `NaN` arrives as NULL. A shard that ranked the native value could drop a row the wrapper needs and return wrong rows without error. The adapter cannot see which keys render as JSON, so a plan-time guard is not possible.

### Decision

A per-shard sort whose output a merge re-ranks ranks the value the merge sees, and the module that renders the emitted value owns that rule. The join scan sorts each post-join key over the same expression the select list emits. A string key maps an empty string to NULL, and a float key maps `NaN` to NULL. Direction and NULL placement render through the shared sort-key renderer.

### Options Considered

| Option | Verdict |
|--------|---------|
| Render keys with `render_order_by_clause`, as the raw scan does | Rejected: ranks the native value, which differs from the value the wrapper ranks |
| Plan-time guard that withholds the per-shard bound for a JSON-fallback key | Rejected: nested and binary types are hidden behind the string tag, and the empty-string divergence stays open |
| Withhold the per-shard bound for every string key | Rejected: string columns are common sort keys, and the emitted-value rule serves them correctly |

### Consequences

- The rule assumes DataFusion's string comparison agrees with Exasol's VARCHAR comparison. This is verified live for partition values but not for arbitrary sort keys, the same gap the flat-scan top-N path has.
- Any future fix of the `NaN` or flat-scan ranking divergence follows this rule.
