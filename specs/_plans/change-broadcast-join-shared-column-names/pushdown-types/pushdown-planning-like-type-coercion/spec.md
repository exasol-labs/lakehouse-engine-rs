# Feature: Pushdown Planning, LIKE Type Coercion

The adapter type-checks the subject of every pushed-down `LIKE` before rendering. A string subject passes through, a DATE subject is rewrapped in an explicit CAST-to-VARCHAR, and every other non-string subject declines. This runs on the single-table WHERE filter, the select-list projection, the broadcast join's combined WHERE filter, and the N-scan fallback's per-leg WHERE filter.

<!-- DELTA:CHANGED -->
## Background

* A LIKE `column` subject carries no `dataType` on the wire, and DataFusion does not coerce a non-string value to VARCHAR the way Exasol does. A non-string subject pushed unchanged fails the DataFusion scan. The adapter reads column Exasol types from `involvedTables[0].columns`.
* The dispatch lives in the adapter (`pushdown/support.rs`), because `vs-expression` is a syntactic translator with no column-type context.
* The rewrite or decline applies only to the JSON tree fed to the DataFusion renderer, not to the raw filter tree forwarded to Iceberg file pruning.
* A decline means different things at the two callers. In the WHERE filter, a non-string subject anywhere in the tree declines the whole top-level filter. The request then routes to the qualified single-table wrapper, which applies the original, un-rewritten predicate tree in its own `WHERE` (`pushdown/pushdown-declined-filter-self-apply`), so the predicate applies once. In the select-list, `project_columns` sets `needs_full_fallback` and projects the full base row. A select-list decline never omits a filter.
* The DATE rewrite renders rather than declines. It matches Exasol only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`). DataFusion's `CAST(Date32 AS VARCHAR)` is always ISO, and the pushdown request carries no session NLS format.
* A subject whose Exasol type resolves to a non-string type declines to avoid a scan failure. A subject whose name does not resolve declines fail-safe: `extract_all_column_types` drops entries missing `name` or `dataType` and reads only the first involved table, so a VARCHAR column can miss the lookup. That decline costs a pushdown and never a correct result.
* The guard walks the tree through the shared post-order rewrite primitive over the curated child-bearing fields: `expressions`, `arguments`, `results`, `expression`, `pattern`, `left`, `right`, `basis`. A LIKE inside a `function_scalar_case`, a comparison operand, or a scalar function's `arguments` is type-checked like a top-level one.
* The select-list pipeline runs the same guard as the WHERE surface, in one pass order: LIKE guard, string-function-argument guard, decimal-stringification rewriter.
* `predicate_like` is on `project_columns`' pushable select-list whitelist, so the guard pass before rendering keeps a select-list LIKE from failing the scan.
* `project_columns` has three callers: `extract_projection` (single-table), `render_broadcast_join` (against both legs' columns, keyed by (relation alias, column) in leg order), and `joins/mod.rs`'s empty-side path.
* The column-name lookup is case-normalized by the shared `column_exa_type` helper in `pushdown/support.rs`, which every type-rewrite guard calls.
* This feature owns the subject type dispatch. `pushdown-joins/pushdown-planning-join-filter-type-coercion` owns which column-type universe each join surface screens against.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A select-list LIKE over a non-string column widens the projection to the full base row

* *GIVEN* a `pushdown` request whose SELECT LIST carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in the projection's column universe is neither `VARCHAR(n)` nor `CHAR(n)` nor `DATE`, `DECIMAL(p,s)` including an Exasol integer's `DECIMAL(p,0)`, `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP`, or the column's name does not resolve in that universe at all
* *WHEN* the adapter resolves the select list into the scan-spec projection in `project_columns`
* *THEN* the subject dispatch SHALL decline the item, and the decline SHALL set the existing full-base-row fallback flag so the WHOLE select list widens to every column of the base row and Exasol post-processes the item itself
* *AND* the decline SHALL NOT omit a filter and SHALL NOT surface as an error out of `project_columns`, omitting the whole top-level filter is the WHERE-clause caller's meaning for a decline, NOT this caller's
* *AND* the query SHALL return Exasol's native result, and the DataFusion scan SHALL NOT fail with `There isn't a common type to coerce <Type> and Utf8 in LIKE expression`
* *AND* a LIKE nested inside a select-list item SHALL reach the same dispatch, inside a `function_scalar_case`, under a comparison predicate's `left` or `right`, or inside a `function_scalar`'s `arguments`, because the guard traverses the shared curated child-bearing field set rather than the item's top-level node only
* *AND* the same decline reached through the broadcast join's shared use of `project_columns` (`render_broadcast_join`, and `joins/mod.rs`'s empty-side path) SHALL widen to every leg's columns in leg order, keyed by (relation alias, column) on the broadcast path, with no error and no per-leg SQL change, because all three callers funnel through the one select-list pipeline
* *AND* a subject whose name does NOT resolve MAY lose a projection pushdown that renders as `Utf8 LIKE Utf8`, an accepted cost of the fail-safe that is slower but never wrong
<!-- /DELTA:CHANGED -->
