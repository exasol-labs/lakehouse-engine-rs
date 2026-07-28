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

* This delta widens the guard's TRAVERSAL inside the filter tree it already processes. Its WIRING is unchanged — still the single-table WHERE-clause chain only — so no scenario about which surfaces the guard covers changes, and issues #215 and #219 stay open and separately tracked.
* The guard walks the filter tree through the shared post-order rewrite primitive (`vs-adapter/pushdown-module-structure`), over the same curated child-bearing field set as the string-function argument guard and the decimal-stringification rewriter: the array fields `expressions` / `arguments` / `results` and the single-child fields `expression` / `pattern` / `left` / `right` / `basis`. It no longer recurses through `predicate_and` / `predicate_or` / `predicate_not` alone.
* The junction-only traversal left a documented, untreated blind spot: a `LIKE` reachable only through a non-junction node — inside a `function_scalar_case`, under a comparison predicate's operand, or inside a scalar function's `arguments` — was never type-checked and rendered as-is, so a non-string subject hard-failed the DataFusion scan. The blind spot was recorded only in this feature's code documentation, because the issue that carried it (#207) is closed. The widened traversal closes it.
* The widened reach changes behavior at a nesting position the junction-only traversal did not reach, and it has TWO sub-cases that must not be conflated. Where the subject's Exasol type RESOLVES to a non-string type, the pre-change render hard-failed the DataFusion scan, so the decline replaces a query that returned no result at all. Where the subject's name does NOT resolve, no such guarantee holds: `extract_all_column_types` drops any `involvedTables[0].columns` entry missing `name` or `dataType` and reads the first involved table only, so a genuinely VARCHAR column can miss the lookup — that shape rendered as `Utf8 LIKE Utf8` and SUCCEEDED before this change, and the fail-safe decline now trades a working pushdown for correct native Exasol evaluation.
* Both sub-cases are accepted, correctness first: a decline is always correct because Exasol evaluates the predicate natively. The second is a lost pushdown (slower, never wrong), not a fixed crash, and is recorded as such rather than folded into the first.
* The primitive visits a `predicate_like` node's OWN `expression` and `pattern` children before the LIKE dispatch runs on that node. This is inert: the per-node dispatch acts only on `predicate_like` / `predicate_like_regexp` node types, so a bare `column` subject and a literal pattern pass through the child visit untouched and reach the subject dispatch unchanged.
* The rewrite (or decline) still applies ONLY to the JSON tree fed to the DataFusion filter renderer; the raw filter tree forwarded to Iceberg file pruning is left untouched, so no file-pruning decision changes.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: A nested non-string LIKE declines the entire enclosing filter

* *GIVEN* a `pushdown` request whose filter nests a non-string-column `predicate_like` at any position the curated child-bearing field set reaches — under a `predicate_and`, `predicate_or`, or `predicate_not`; under a comparison predicate's `left` or `right` operand; inside a `function_scalar`'s `arguments`; or inside a `function_scalar_case`'s `basis`, `arguments`, or `results`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter, not only the offending conjunct, mirroring the all-or-nothing untranslatable-predicate backstop
* *AND* a DATE-column LIKE nested at any of those positions SHALL instead be rewritten in place to its CAST-to-VARCHAR form while the surrounding tree is preserved
* *AND* at a nesting position outside the three junction node types, a subject whose Exasol type RESOLVES to a non-string type SHALL have its decline replace the pre-change behavior of pushing the predicate down and hard-failing the DataFusion scan, so a filter shape that previously returned no result at all now returns the correct result from native Exasol evaluation
* *AND* at such a position a subject whose name does NOT resolve in `involvedTables[0].columns` MAY instead lose a pushdown that previously rendered as-is and succeeded, because an unresolved name does not prove a non-string column — the fail-safe decline trades that pushdown for correct native Exasol evaluation, an accepted cost that SHALL NOT be recorded as a fixed hard failure
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A LIKE nested inside a CASE expression is type-guarded

* *GIVEN* a `pushdown` request whose filter nests a `predicate_like` inside a `function_scalar_case` — for example `WHERE CASE WHEN <col> LIKE '1%' THEN 1 ELSE 0 END = 1`, where the LIKE sits in the searched CASE's `arguments` and the CASE itself sits under the comparison predicate's `left`
* *AND* the LIKE subject is a bare `column` node whose Exasol type in `involvedTables[0].columns` is not `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL apply the same subject dispatch it applies to a top-level LIKE — declining pushdown of the whole top-level filter for a DECIMAL, integer, DOUBLE, BOOLEAN, or TIMESTAMP subject — rather than hard-failing the DataFusion scan as the junction-only traversal did (`There isn't a common type to coerce <Type> and Utf8 in LIKE expression`)
* *AND* a subject whose name does NOT resolve in `involvedTables[0].columns` SHALL decline at this position under the same fail-safe rule, carrying the pushdown-loss trade recorded in the "A nested non-string LIKE declines the entire enclosing filter" scenario rather than the fixed-hard-failure contrast above
* *AND* a DATE-column LIKE at that position SHALL instead be rewritten in place to `CAST(<col> AS VARCHAR)`, leaving the enclosing CASE's `basis`, `arguments`, and `results` structure otherwise unchanged
<!-- /DELTA:NEW -->
