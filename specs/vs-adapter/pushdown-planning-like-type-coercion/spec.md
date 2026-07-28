# Feature: Pushdown Planning — LIKE Type Coercion

Makes pushed-down `LIKE` and `REGEXP_LIKE` predicates type-aware in the single-table pushdown
path (see `vs-adapter/pushdown-planning`'s "Filter predicate is pushed into the scan spec"
scenario, which this feature specializes). A LIKE predicate's `column` subject never carries a
`dataType` on the wire, and DataFusion performs no implicit non-string-to-VARCHAR coercion the
way Exasol does — so a pushed-down LIKE over a non-string column previously hard-failed the
DataFusion scan at execution time. This feature dispatches on the column's Exasol type, read from
`involvedTables[0].columns`, before rendering the filter: string subjects pass through unchanged,
DATE subjects are rewrapped in an explicit CAST-to-VARCHAR, and every other non-string subject
declines pushdown of the whole top-level filter so Exasol evaluates it natively. This is a
single-table scan-spec-filter-path fix only; the broadcast-join per-leg filter path
(`crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`) and the SELECT-list/
projection path have the same latent gap and are tracked separately (issues #215 and #219).

## Background

* A LIKE predicate's `column` subject carries no `dataType` on the wire; column Exasol types are read from `involvedTables[0].columns`.
* The type dispatch and rewrite happen in the adapter (`pushdown/support.rs`), not in `vs-expression`, because `vs-expression` is a pure syntactic JSON-to-SQL translator with no external column-type context and is shared with a sibling VS-adapter project.
* The rewrite (or decline) applies ONLY to the JSON tree fed to the DataFusion filter renderer; the raw filter tree forwarded to Iceberg file pruning is left untouched.
* A non-string LIKE subject anywhere in the filter tree declines the WHOLE top-level filter (not just the offending conjunct), mirroring the existing all-or-nothing untranslatable-predicate correctness backstop — Exasol then evaluates the entire predicate natively.
* The DATE CAST-to-VARCHAR is Exasol-faithful only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`); a session with an altered NLS date format is an accepted, tracked exception (#216), not a silent gap.
* This delta widens the guard's TRAVERSAL inside the filter tree it already processes. Its WIRING is unchanged — still the single-table WHERE-clause chain only — so no scenario about which surfaces the guard covers changes, and issues #215 and #219 stay open and separately tracked.
* The guard walks the filter tree through the shared post-order rewrite primitive (`vs-adapter/pushdown-module-structure`), over the same curated child-bearing field set as the string-function argument guard and the decimal-stringification rewriter: the array fields `expressions` / `arguments` / `results` and the single-child fields `expression` / `pattern` / `left` / `right` / `basis`. It no longer recurses through `predicate_and` / `predicate_or` / `predicate_not` alone.
* The junction-only traversal left a documented, untreated blind spot: a `LIKE` reachable only through a non-junction node — inside a `function_scalar_case`, under a comparison predicate's operand, or inside a scalar function's `arguments` — was never type-checked and rendered as-is, so a non-string subject hard-failed the DataFusion scan. The widened traversal closes it.
* The widened reach changes behavior at a nesting position the junction-only traversal did not reach, and it has TWO sub-cases that must not be conflated. Where the subject's Exasol type RESOLVES to a non-string type, the pre-change render hard-failed the DataFusion scan, so the decline replaces a query that returned no result at all. Where the subject's name does NOT resolve, no such guarantee holds: `extract_all_column_types` drops any `involvedTables[0].columns` entry missing `name` or `dataType` and reads the first involved table only, so a genuinely VARCHAR column can miss the lookup — that shape rendered as `Utf8 LIKE Utf8` and SUCCEEDED before this change, and the fail-safe decline now trades a working pushdown for correct native Exasol evaluation.
* Both sub-cases are accepted, correctness first: a decline is always correct because Exasol evaluates the predicate natively. The second is a lost pushdown (slower, never wrong), not a fixed crash, and is recorded as such rather than folded into the first.
* The primitive visits a `predicate_like` node's OWN `expression` and `pattern` children before the LIKE dispatch runs on that node. This is inert: the per-node dispatch acts only on `predicate_like` / `predicate_like_regexp` node types, so a bare `column` subject and a literal pattern pass through the child visit untouched and reach the subject dispatch unchanged.

## Scenarios

### Scenario: LIKE on a VARCHAR or CHAR column pushes down unchanged

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL leave the predicate subject unchanged, rendering `(<column> LIKE <pattern>)` exactly as before this change
* *AND* the rendered filter SHALL be carried in the common spec, because a string subject needs no coercion

### Scenario: LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DATE`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL rewrap the subject as an explicit CAST-to-VARCHAR node (`function_scalar_cast` with target `{"type":"VARCHAR"}`) before rendering, so the emitted DataFusion SQL casts the DATE column to text and the LIKE matches against the `YYYY-MM-DD` string form
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to the DataFusion filter renderer, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the emitted match semantics SHALL equal Exasol's implicit DATE-to-VARCHAR cast under the default `NLS_DATE_FORMAT` of `YYYY-MM-DD`, which is the ISO-8601 date text form both engines render for the Iceberg `date` primitive (calendar date without timezone, days from 1970-01-01)
* *AND* under a session that has altered `NLS_DATE_FORMAT` away from the `YYYY-MM-DD` default, the pushed-down match MAY diverge from native Exasol evaluation, because DataFusion's `CAST(Date32 AS VARCHAR)` is unconditionally ISO `YYYY-MM-DD` and the pushdown request carries no session NLS format for the adapter to reproduce — an accepted, accurately-scoped tracked exception (#216), not a silent gap

### Scenario: LIKE on a DECIMAL column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DECIMAL(p,s)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (emit no `filter` in the common spec), so Exasol evaluates the entire predicate natively
* *AND* the adapter SHALL NOT inject a CAST for the DECIMAL subject, because DataFusion's decimal-to-string formatting keeps trailing scale zeros that diverge from Exasol's trimmed formatting and would silently change which rows match — correct trimmed-decimal formatting is tracked in issue #211
* *AND* the returned scan-driving SQL SHALL remain valid with the filter omitted, exercising the existing untranslatable-predicate correctness backstop

### Scenario: LIKE on an integer column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column is an Exasol integer, carried on the wire as `DECIMAL(p,0)` (Exasol has no distinct integer type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter exactly as for any other `DECIMAL(p,s)` subject, so Exasol evaluates the predicate natively
* *AND* the adapter SHALL NOT inject a CAST for the integer subject

### Scenario: LIKE on a non-column subject is left untouched

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is NOT a bare `column` node (for example a computed scalar expression)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL leave the node unchanged, applying neither the CAST rewrite nor the decline, because the subject's type is not resolvable from `involvedTables[0].columns`
* *AND* the adapter SHALL preserve the pre-existing behavior for non-column LIKE subjects, which remains outside this change's scope

### Scenario: LIKE on a bare column whose type cannot be resolved declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's name is NOT found in `involvedTables[0].columns` (a lookup miss — no resolvable Exasol type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (fail-safe), because it cannot prove the subject is a string and a non-string subject would hard-fail the DataFusion scan
* *AND* the name lookup SHALL be case-normalized by uppercasing the subject column name before matching, mirroring `extract_all_column_types`'s existing uppercasing of column names (`support.rs:411`), so a case-mismatched name resolves rather than spuriously declining

### Scenario: A nested non-string LIKE declines the entire enclosing filter

* *GIVEN* a `pushdown` request whose filter nests a non-string-column `predicate_like` at any position the curated child-bearing field set reaches — under a `predicate_and`, `predicate_or`, or `predicate_not`; under a comparison predicate's `left` or `right` operand; inside a `function_scalar`'s `arguments`; or inside a `function_scalar_case`'s `basis`, `arguments`, or `results`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter, not only the offending conjunct, mirroring the all-or-nothing untranslatable-predicate backstop
* *AND* a DATE-column LIKE nested at any of those positions SHALL instead be rewritten in place to its CAST-to-VARCHAR form while the surrounding tree is preserved
* *AND* at a nesting position outside the three junction node types, a subject whose Exasol type RESOLVES to a non-string type SHALL have its decline replace the pre-change behavior of pushing the predicate down and hard-failing the DataFusion scan, so a filter shape that previously returned no result at all now returns the correct result from native Exasol evaluation
* *AND* at such a position a subject whose name does NOT resolve in `involvedTables[0].columns` MAY instead lose a pushdown that previously rendered as-is and succeeded, because an unresolved name does not prove a non-string column — the fail-safe decline trades that pushdown for correct native Exasol evaluation, an accepted cost that SHALL NOT be recorded as a fixed hard failure

### Scenario: A LIKE nested inside a CASE expression is type-guarded

* *GIVEN* a `pushdown` request whose filter nests a `predicate_like` inside a `function_scalar_case` — for example `WHERE CASE WHEN <col> LIKE '1%' THEN 1 ELSE 0 END = 1`, where the LIKE sits in the searched CASE's `arguments` and the CASE itself sits under the comparison predicate's `left`
* *AND* the LIKE subject is a bare `column` node whose Exasol type in `involvedTables[0].columns` is not `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL apply the same subject dispatch it applies to a top-level LIKE — declining pushdown of the whole top-level filter for a DECIMAL, integer, DOUBLE, BOOLEAN, or TIMESTAMP subject — rather than hard-failing the DataFusion scan as the junction-only traversal did (`There isn't a common type to coerce <Type> and Utf8 in LIKE expression`)
* *AND* a subject whose name does NOT resolve in `involvedTables[0].columns` SHALL decline at this position under the same fail-safe rule, carrying the pushdown-loss trade recorded in the "A nested non-string LIKE declines the entire enclosing filter" scenario rather than the fixed-hard-failure contrast above
* *AND* a DATE-column LIKE at that position SHALL instead be rewritten in place to `CAST(<col> AS VARCHAR)`, leaving the enclosing CASE's `basis`, `arguments`, and `results` structure otherwise unchanged
