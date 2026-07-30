# Feature: Pushdown Planning — LIKE Type Coercion

Makes pushed-down `LIKE` and `REGEXP_LIKE` predicates type-aware on both render surfaces that
carry a pushed expression tree: the single-table WHERE-clause filter and the select-list
projection (`project_columns`, shared with the broadcast join). A LIKE predicate's `column`
subject never carries a `dataType` on the wire, and DataFusion performs no implicit
non-string-to-VARCHAR coercion the way Exasol does — so a pushed-down LIKE over a non-string
column hard-failed the DataFusion scan at execution time. This feature dispatches on the column's
Exasol type, read from `involvedTables[0].columns`, before rendering: string subjects pass through
unchanged, DATE subjects are rewrapped in an explicit CAST-to-VARCHAR, and every other non-string
subject declines. What a decline MEANS belongs to the caller, and the two surfaces differ: the
WHERE-clause caller routes the request to the qualified single-table wrapper, which applies the
declined predicate as Exasol SQL in its own `WHERE`, while the select-list caller widens the
projection to the full base row so Exasol post-processes the item itself.

## Background

* This delta SUPERSEDES the preceding Background bullet "A non-string LIKE subject anywhere in the filter tree declines the WHOLE top-level filter (not just the offending conjunct), mirroring the existing all-or-nothing untranslatable-predicate correctness backstop — Exasol then evaluates the entire predicate natively." The all-or-nothing decline scope is unchanged and still correct. The claim about what happens NEXT was wrong and is corrected, not merely superseded: there is no Exasol-side backstop for a predicate whose capability the adapter advertised, so a declined filter must be applied by the adapter's own returned SQL. It now routes the request to the qualified single-table wrapper, which renders the ORIGINAL (un-rewritten) predicate tree as that wrapper's `WHERE`. See `vs-adapter/pushdown-declined-filter-self-apply`.
* This delta SUPERSEDES the preceding Background bullet "Both sub-cases are accepted, correctness first: a decline is always correct because Exasol evaluates the predicate natively. The second is a lost pushdown (slower, never wrong), not a fixed crash, and is recorded as such rather than folded into the first." A decline is correct because the ADAPTER evaluates the predicate in its own outer `WHERE` — not because Exasol re-evaluates it. Both sub-cases remain accepted and the cost is unchanged in kind: the predicate leaves the node-local scan and is evaluated by Exasol over the returned rows, slower but never wrong.
* This delta SUPERSEDES the preceding Background bullet "The broadcast-join PER-LEG filter path (`crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`) is a third render surface that remains unguarded — the one open exception, tracked by issue #215." That surface stays unguarded and #215 stays open, but its BLOCKING dependency is removed: the join paths now apply a declined filter themselves (broadcast declines to the N-scan fallback; an N-scan side-local conjunct that declines becomes a residual conjunct in the outer wrapper's `WHERE`), so wiring the guard there no longer rests on a false backstop.
* The DATE-subject CAST rewrite is unaffected. It renders rather than declines, so it never reaches the self-application path.
* The decline path applies the predicate ONCE. The type-rewritten tree is not pushed into the scan at all, and the wrapper's `WHERE` renders the ORIGINAL request tree, so Exasol's own implicit coercions apply exactly as they would to the un-delegated query.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: LIKE on a DECIMAL column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DECIMAL(p,s)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (emit no `filter` in the common spec)
* *AND* the adapter SHALL NOT inject a CAST for the DECIMAL subject, because DataFusion's decimal-to-string formatting keeps trailing scale zeros that diverge from Exasol's trimmed formatting and would silently change which rows match — correct trimmed-decimal formatting is tracked in issue #211
* *AND* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL predicate tree as that wrapper's own `WHERE`, so the predicate is evaluated by Exasol over the returned rows — REPLACING the recorded clause "the returned scan-driving SQL SHALL remain valid with the filter omitted, exercising the existing untranslatable-predicate correctness backstop", which relied on a backstop that does not exist
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query, rather than the unfiltered row set the omission returned
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIKE on an integer column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column is an Exasol integer, carried on the wire as `DECIMAL(p,0)` (Exasol has no distinct integer type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter exactly as for any other `DECIMAL(p,s)` subject, and SHALL self-apply the declined predicate in the qualified single-table wrapper's `WHERE` — REPLACING the recorded "so Exasol evaluates the predicate natively", which assumed an Exasol-side re-check that does not occur
* *AND* the adapter SHALL NOT inject a CAST for the integer subject
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIKE on a bare column whose type cannot be resolved declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's name is NOT found in `involvedTables[0].columns` (a lookup miss — no resolvable Exasol type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (fail-safe), because it cannot prove the subject is a string and a non-string subject would hard-fail the DataFusion scan
* *AND* the adapter SHALL self-apply the declined predicate in the qualified single-table wrapper's `WHERE`, so the fail-safe costs a pushdown and never a correct result
* *AND* the name lookup SHALL be case-normalized by uppercasing the subject column name before matching, so a case-mismatched name resolves rather than spuriously declining
* *AND* that normalization SHALL be owned by exactly ONE helper, `column_exa_type` (`pushdown/support.rs`), which every type-rewrite guard calls rather than reimplementing — so this clause names an owner instead of asserting that the guard MIRRORS one. NO line-number citation SHALL be recorded for it
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A nested non-string LIKE declines the entire enclosing filter

* *GIVEN* a `pushdown` request whose filter nests a non-string-column `predicate_like` at any position the curated child-bearing field set reaches — under a `predicate_and`, `predicate_or`, or `predicate_not`; under a comparison predicate's `left` or `right` operand; inside a `function_scalar`'s `arguments`; or inside a `function_scalar_case`'s `basis`, `arguments`, or `results`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter, not only the offending conjunct, and SHALL self-apply that whole filter in the qualified single-table wrapper's `WHERE` — REPLACING the recorded "mirroring the all-or-nothing untranslatable-predicate backstop", whose named backstop does not exist
* *AND* a DATE-column LIKE nested at any of those positions SHALL instead be rewritten in place to its CAST-to-VARCHAR form while the surrounding tree is preserved
* *AND* at a nesting position outside the three junction node types, a subject whose Exasol type RESOLVES to a non-string type SHALL have its decline replace the pre-change behavior of pushing the predicate down and hard-failing the DataFusion scan, so a filter shape that previously returned no result at all now returns the correct result
* *AND* at such a position a subject whose name does NOT resolve in `involvedTables[0].columns` MAY instead lose a pushdown that previously rendered as-is and succeeded, because an unresolved name does not prove a non-string column — the fail-safe decline trades that pushdown for evaluation in the wrapper's `WHERE`, an accepted cost that SHALL NOT be recorded as a fixed hard failure
<!-- /DELTA:CHANGED -->
