# Feature: Pushdown Planning, LIKE Type Coercion

The adapter type-checks the subject of every pushed-down `LIKE` before rendering. A string subject passes through, a DATE subject is rewrapped in an explicit CAST-to-VARCHAR, and every other non-string subject declines. This runs on the single-table WHERE filter, the select-list projection, the broadcast join's combined WHERE filter, and the N-scan fallback's per-leg WHERE filter.

## Background

* A LIKE `column` subject carries no `dataType` on the wire, and DataFusion does not coerce a non-string value to VARCHAR the way Exasol does. A non-string subject pushed unchanged fails the DataFusion scan. The adapter reads column Exasol types from `involvedTables[0].columns`.
* The dispatch lives in the adapter (`pushdown/support.rs`), because `vs-expression` is a syntactic translator with no column-type context.
* The rewrite or decline applies only to the JSON tree fed to the DataFusion renderer, not to the raw filter tree forwarded to Iceberg file pruning.
* A decline means different things at the two callers. In the WHERE filter, a non-string subject anywhere in the tree declines the whole top-level filter. The request then routes to the qualified single-table wrapper, which applies the original, un-rewritten predicate tree in its own `WHERE` (`pushdown/pushdown-declined-filter-self-apply`), so the predicate applies once. In the select-list, `project_columns` sets `needs_full_fallback` and projects the full base row. A select-list decline never omits a filter.
* The DATE rewrite renders rather than declines. It matches Exasol only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`). DataFusion's `CAST(Date32 AS VARCHAR)` is always ISO, and the pushdown request carries no session NLS format.
* A subject whose Exasol type resolves to a non-string type declines to avoid a scan failure. A subject whose name does not resolve declines fail-safe: `extract_all_column_types` drops entries missing `name` or `dataType` and reads only the first involved table, so a VARCHAR column can miss the lookup. That decline costs a pushdown and never a correct result.
* The guard walks the tree through the shared post-order rewrite primitive (`pushdown/pushdown-module-structure`) over the curated child-bearing fields: `expressions`, `arguments`, `results`, `expression`, `pattern`, `left`, `right`, `basis`. A LIKE inside a `function_scalar_case`, a comparison operand, or a scalar function's `arguments` is type-checked like a top-level one.
* The select-list pipeline runs the same guard as the WHERE surface, in one pass order: LIKE guard, string-function-argument guard, decimal-stringification rewriter.
* `predicate_like` is on `project_columns`' pushable select-list whitelist, so the guard pass before rendering keeps a select-list LIKE from failing the scan.
* `project_columns` has three callers: `extract_projection` (single-table), `extract_join_projection` (against the union of both joined tables' columns), and `joins/mod.rs`'s empty-side path.
* The column-name lookup is case-normalized by the shared `column_exa_type` helper in `pushdown/support.rs`, which every type-rewrite guard calls.
* This feature owns the subject type dispatch. `pushdown-joins/pushdown-planning-join-filter-type-coercion` owns which column-type universe each join surface screens against.

## Scenarios

### Scenario: LIKE on a VARCHAR or CHAR column pushes down unchanged

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL leave the predicate subject unchanged, rendering `(<column> LIKE <pattern>)`
* *AND* the rendered filter SHALL be carried in the common spec, because a string subject needs no coercion
* *AND* this pass-through SHALL hold identically at all four surfaces that run the dispatch — the single-table WHERE filter, the select-list projection, the broadcast join's combined WHERE filter, and the N-scan fallback's per-leg WHERE filter (see `pushdown-joins/pushdown-planning-join-filter-type-coercion`, issue #215)

### Scenario: LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DATE`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL rewrap the subject as an explicit CAST-to-VARCHAR node (`function_scalar_cast` with target `{"type":"VARCHAR"}`) before rendering, so the emitted DataFusion SQL casts the DATE column to text and the LIKE matches against the `YYYY-MM-DD` string form
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to the DataFusion filter renderer, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the emitted match semantics SHALL equal Exasol's implicit DATE-to-VARCHAR cast under the default `NLS_DATE_FORMAT` of `YYYY-MM-DD`, which is the ISO-8601 date text form both engines render for the Iceberg `date` primitive (calendar date without timezone, days from 1970-01-01)
* *AND* under a session that has altered `NLS_DATE_FORMAT` away from the `YYYY-MM-DD` default, the pushed-down match MAY diverge from native Exasol evaluation, because DataFusion's `CAST(Date32 AS VARCHAR)` is unconditionally ISO `YYYY-MM-DD` and the pushdown request carries no session NLS format for the adapter to reproduce — an accepted, accurately-scoped tracked exception (#216), not a silent gap

### Scenario: LIKE on a DECIMAL column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DECIMAL(p,s)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (emit no `filter` in the common spec)
* *AND* the adapter SHALL NOT inject a CAST for the DECIMAL subject, because DataFusion's decimal-to-string formatting keeps trailing scale zeros that diverge from Exasol's trimmed formatting and would silently change which rows match — correct trimmed-decimal formatting is tracked in issue #211
* *AND* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL predicate tree as that wrapper's own `WHERE`, so Exasol evaluates the predicate over the returned rows
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: LIKE on an integer column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column is an Exasol integer, carried on the wire as `DECIMAL(p,0)` (Exasol has no distinct integer type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter exactly as for any other `DECIMAL(p,s)` subject, and SHALL self-apply the declined predicate in the qualified single-table wrapper's `WHERE`
* *AND* the adapter SHALL NOT inject a CAST for the integer subject
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query

### Scenario: LIKE on a non-column subject is left untouched

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is NOT a bare `column` node (for example a computed scalar expression)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL leave the node unchanged, applying neither the CAST rewrite nor the decline, because the subject's type is not resolvable from `involvedTables[0].columns`

### Scenario: LIKE on a bare column whose type cannot be resolved declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's name is NOT found in `involvedTables[0].columns` (a lookup miss — no resolvable Exasol type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (fail-safe), because it cannot prove the subject is a string and a non-string subject would hard-fail the DataFusion scan
* *AND* the adapter SHALL self-apply the declined predicate in the qualified single-table wrapper's `WHERE`, so the fail-safe costs a pushdown and never a correct result
* *AND* the name lookup SHALL be case-normalized by uppercasing the subject column name before matching, so a case-mismatched name resolves rather than spuriously declining
* *AND* that normalization SHALL be owned by exactly ONE helper, `column_exa_type` (`pushdown/support.rs`), which every type-rewrite guard calls rather than reimplementing

### Scenario: A nested non-string LIKE declines the entire enclosing filter

* *GIVEN* a `pushdown` request whose filter nests a non-string-column `predicate_like` at any position the curated child-bearing field set reaches — under a `predicate_and`, `predicate_or`, or `predicate_not`; under a comparison predicate's `left` or `right` operand; inside a `function_scalar`'s `arguments`; or inside a `function_scalar_case`'s `basis`, `arguments`, or `results`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter, not only the offending conjunct, and SHALL self-apply that whole filter in the qualified single-table wrapper's `WHERE` — REPLACING the recorded "mirroring the all-or-nothing untranslatable-predicate backstop", whose named backstop does not exist
* *AND* a DATE-column LIKE nested at any of those positions SHALL instead be rewritten in place to its CAST-to-VARCHAR form while the surrounding tree is preserved
* *AND* at a nesting position outside the three junction node types, a subject whose Exasol type RESOLVES to a non-string type SHALL have its decline replace the pre-change behavior of pushing the predicate down and hard-failing the DataFusion scan, so a filter shape that previously returned no result at all now returns the correct result
* *AND* at such a position a subject whose name does NOT resolve in `involvedTables[0].columns` MAY instead lose a pushdown that previously rendered as-is and succeeded, because an unresolved name does not prove a non-string column — the fail-safe decline trades that pushdown for evaluation in the wrapper's `WHERE`, an accepted cost that SHALL NOT be recorded as a fixed hard failure

### Scenario: A LIKE nested inside a CASE expression is type-guarded

* *GIVEN* a `pushdown` request whose filter nests a `predicate_like` inside a `function_scalar_case` — for example `WHERE CASE WHEN <col> LIKE '1%' THEN 1 ELSE 0 END = 1`, where the LIKE sits in the searched CASE's `arguments` and the CASE itself sits under the comparison predicate's `left`
* *AND* the LIKE subject is a bare `column` node whose Exasol type in `involvedTables[0].columns` is not `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL apply the same subject dispatch it applies to a top-level LIKE — declining pushdown of the whole top-level filter for a DECIMAL, integer, DOUBLE, BOOLEAN, or TIMESTAMP subject — rather than hard-failing the DataFusion scan as the junction-only traversal did (`There isn't a common type to coerce <Type> and Utf8 in LIKE expression`)
* *AND* a subject whose name does NOT resolve in `involvedTables[0].columns` SHALL decline at this position under the same fail-safe rule, carrying the pushdown-loss trade recorded in the "A nested non-string LIKE declines the entire enclosing filter" scenario rather than the fixed-hard-failure contrast above
* *AND* a DATE-column LIKE at that position SHALL instead be rewritten in place to `CAST(<col> AS VARCHAR)`, leaving the enclosing CASE's `basis`, `arguments`, and `results` structure otherwise unchanged

### Scenario: A select-list LIKE over a DATE column projects the CAST-to-VARCHAR form

* *GIVEN* a `pushdown` request whose SELECT LIST carries a `predicate_like` whose `expression` is a bare `column` node — for example `SELECT c_date LIKE '2024%' FROM …`
* *AND* the column's Exasol type in the projection's column universe is `DATE`
* *WHEN* the adapter resolves the select list into the scan-spec projection in `project_columns`
* *THEN* the adapter SHALL run the same subject dispatch on the item that it runs on a WHERE-clause LIKE, rewrapping the subject as a `function_scalar_cast` node with target `{"type":"VARCHAR"}` before rendering
* *AND* the item SHALL project as ONE positional rendered expression carrying `(CAST(<col> AS VARCHAR) LIKE <pattern>)`, NOT as the full-base-row fallback, so the projection stays one item per select-list item
* *AND* a `VARCHAR(n)` or `CHAR(n)` subject SHALL project its rendered fragment unchanged and keep its pushdown
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to the projection renderer, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the DATE match semantics SHALL carry the same default-`NLS_DATE_FORMAT` fidelity, and the same altered-session-format tracked exception (#216), that the WHERE-clause DATE scenario records, because the identical CAST node is emitted

### Scenario: A select-list LIKE over a non-string column widens the projection to the full base row

* *GIVEN* a `pushdown` request whose SELECT LIST carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in the projection's column universe is neither `VARCHAR(n)` nor `CHAR(n)` nor `DATE` — `DECIMAL(p,s)` including an Exasol integer's `DECIMAL(p,0)`, `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP` — or the column's name does not resolve in that universe at all
* *WHEN* the adapter resolves the select list into the scan-spec projection in `project_columns`
* *THEN* the subject dispatch SHALL decline the item, and the decline SHALL set the existing full-base-row fallback flag so the WHOLE select list widens to every column of the base row and Exasol post-processes the item itself
* *AND* the decline SHALL NOT omit a filter and SHALL NOT surface as an error out of `project_columns` — omitting the whole top-level filter is the WHERE-clause caller's meaning for a decline, NOT this caller's
* *AND* the query SHALL return Exasol's native result, and the DataFusion scan SHALL NOT fail with `There isn't a common type to coerce <Type> and Utf8 in LIKE expression` (issue #219)
* *AND* a LIKE nested inside a select-list item SHALL reach the same dispatch — inside a `function_scalar_case`, under a comparison predicate's `left` or `right`, or inside a `function_scalar`'s `arguments` — because the guard traverses the shared curated child-bearing field set rather than the item's top-level node only
* *AND* the same decline reached through the broadcast join's shared use of `project_columns` (`extract_join_projection`, and `joins/mod.rs`'s empty-side path) SHALL widen to the disjoint UNION of every involved table's columns, with no error and no per-leg SQL change, because all three callers funnel through the one select-list pipeline
* *AND* a subject whose name does NOT resolve MAY lose a projection pushdown that renders as `Utf8 LIKE Utf8`, an accepted cost of the fail-safe that is slower but never wrong
