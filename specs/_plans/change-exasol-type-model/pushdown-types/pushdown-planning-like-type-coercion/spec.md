# Feature: Pushdown Planning, LIKE Type Coercion

The adapter type-checks the subject of every pushed-down `LIKE` before rendering. A string subject passes through, a DATE subject is rewrapped in an explicit CAST-to-VARCHAR, and every other non-string subject declines. This runs on the single-table WHERE filter, the select-list projection, the broadcast join's combined WHERE filter, and the N-scan fallback's per-leg WHERE filter.

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

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DATE`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the scan-spec filter SHALL carry the LIKE with its subject rendered as `CAST(<col> AS VARCHAR)`, so the LIKE matches against the `YYYY-MM-DD` string form of the DATE column
* *AND* file pruning SHALL keep the same data files it keeps for the same predicate without the CAST
* *AND* the emitted match semantics SHALL equal Exasol's implicit DATE-to-VARCHAR cast under the default `NLS_DATE_FORMAT` of `YYYY-MM-DD`, which is the ISO-8601 date text form both engines render for the Iceberg `date` primitive (calendar date without timezone, days from 1970-01-01)
* *AND* under a session that has altered `NLS_DATE_FORMAT` away from the `YYYY-MM-DD` default, the pushed-down match MAY diverge from native Exasol evaluation, because DataFusion's `CAST(Date32 AS VARCHAR)` is unconditionally ISO `YYYY-MM-DD` and the pushdown request carries no session NLS format for the adapter to reproduce, an accepted, accurately-scoped tracked exception, not a silent gap
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A select-list LIKE over a DATE column projects the CAST-to-VARCHAR form

* *GIVEN* a `pushdown` request whose SELECT LIST carries a `predicate_like` whose `expression` is a bare `column` node, for example `SELECT c_date LIKE '2024%' FROM …`
* *AND* the column's Exasol type in the request's involved-table columns is `DATE`
* *WHEN* the adapter resolves the select list into the scan-spec projection
* *THEN* the scan-spec projection SHALL carry the item as ONE positional rendered expression, `(CAST(<col> AS VARCHAR) LIKE <pattern>)`, the subject form a WHERE-clause DATE LIKE takes, NOT as the full-base-row fallback, so the projection stays one item per select-list item
* *AND* a `VARCHAR(n)` or `CHAR(n)` subject SHALL project its rendered fragment unchanged and keep its pushdown
* *AND* the DATE match semantics SHALL carry the same default-`NLS_DATE_FORMAT` fidelity, and the same altered-session-format tracked exception, that the WHERE-clause DATE scenario records, because the scan renders the same CAST
<!-- /DELTA:CHANGED -->
