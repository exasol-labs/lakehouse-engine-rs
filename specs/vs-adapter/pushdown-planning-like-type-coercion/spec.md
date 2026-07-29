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
WHERE-clause caller omits the whole top-level filter so Exasol evaluates the predicate natively,
while the select-list caller widens the projection to the full base row so Exasol post-processes
the item itself. The broadcast-join PER-LEG filter path
(`crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`) is a third render surface
that remains unguarded — the one open exception, tracked by issue #215.

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
* This delta SUPERSEDES the preceding Background bullet "This delta widens the guard's TRAVERSAL inside the filter tree it already processes. Its WIRING is unchanged — still the single-table WHERE-clause chain only — so no scenario about which surfaces the guard covers changes, and issues #215 and #219 stay open and separately tracked." The guard's WIRING is what this delta changes: the select-list pipeline now runs it, closing issue #219. Issue #215 remains the one open surface.
* The select-list wiring reuses `like_subject_type_guard` and `guard_like_subject` verbatim. Neither the per-node dispatch table nor the traversal changes; only the select-list surface's pass list does (`vs-adapter/pushdown-module-structure`). No `vs-expression` code changes, because column Exasol types come from `involvedTables`, not from the wire nodes the stateless translator sees.
* `predicate_like` and `predicate_like_regexp` were already on `project_columns`' pushable select-list node whitelist, so rendering was never the gap — the missing type-guard pass before rendering was.
* The select-list surface runs the LIKE-subject pass FIRST, in the same order the WHERE-clause surface already ran it (LIKE guard, then string-function-argument guard, then decimal-stringification rewriter). Equalizing the two pass lists is what lets ONE pipeline function serve both surfaces, so one ordering rule governs both by construction rather than by convention (`vs-adapter/pushdown-module-structure`).
* A decline means different things at the two callers, and the pipeline names neither meaning. `handle_pushdown` omits the whole top-level filter; `project_columns` sets its existing `needs_full_fallback` flag and projects the full base row. That caller-agnostic contract is precisely what allows a single shared pipeline; the select-list scenarios below MUST NOT be read as "decline the whole filter" — no filter is involved.
* `project_columns` has THREE callers — `extract_projection` (single-table), `extract_join_projection` (`joins/rendering.rs`, against the disjoint union of both joined tables' columns), and `joins/mod.rs`'s empty-side path — so wiring the guard into the one shared select-list pipeline reaches the broadcast-join SELECT list as well as the single-table projection, with no per-leg SQL change.
* The select-list decline can lose a projection pushdown that previously rendered and SUCCEEDED, in exactly one shape: a subject whose name does not resolve. `extract_all_column_types` drops any `involvedTables[0].columns` entry missing `name` or `dataType` and reads the first involved table only, so a genuinely VARCHAR column can miss the lookup and render as `Utf8 LIKE Utf8`. The fail-safe trades that pushdown for correct native Exasol evaluation — slower, never wrong — and is the same accepted cost the WHERE-clause path already records.
* Apache Iceberg spec check: NOT implicated as a data-type-mapping question, and grounded rather than assumed. The spec's Primitive Types table defines `date` as "Calendar date without timezone or time", `decimal(P,S)` as "Fixed-point decimal; precision P, scale S" with "Scale is fixed, precision must be 38 or less", `string` as "Arbitrary-length character sequences" / "Encoded with UTF-8", `boolean` as "True or false", `double` as "64-bit IEEE 754 floating point", and `timestamp` as "Timestamp, microsecond precision, without timezone". The spec mandates no text or display form for any of them. The divergence this guard handles is therefore Exasol-versus-DataFusion SQL-dialect rendering, not an Iceberg schema or type-mapping deviation — the same determination `vs-adapter/pushdown-planning-string-fn-type-coercion` records, and the reason declining (rather than casting) is the only branch that cannot silently change a result.

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

### Scenario: A select-list LIKE over a DATE column projects the CAST-to-VARCHAR form

* *GIVEN* a `pushdown` request whose SELECT LIST carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node — for example `SELECT c_date LIKE '2024%' FROM …`
* *AND* the column's Exasol type in the projection's column universe is `DATE`
* *WHEN* the adapter resolves the select list into the scan-spec projection in `project_columns`
* *THEN* the adapter SHALL run the same subject dispatch on the item that it runs on a WHERE-clause LIKE, rewrapping the subject as a `function_scalar_cast` node with target `{"type":"VARCHAR"}` before rendering
* *AND* the item SHALL project as ONE positional rendered expression carrying `(CAST(<col> AS VARCHAR) LIKE <pattern>)`, NOT as the full-base-row fallback, so the projection stays one item per select-list item
* *AND* a `VARCHAR(n)` or `CHAR(n)` subject SHALL project byte-identically to its pre-change rendered fragment, so the already-working string case keeps its pushdown
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to the projection renderer, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the DATE match semantics SHALL carry the same default-`NLS_DATE_FORMAT` fidelity, and the same altered-session-format tracked exception (#216), that the WHERE-clause DATE scenario records, because the identical CAST node is emitted

### Scenario: A select-list LIKE over a non-string column widens the projection to the full base row

* *GIVEN* a `pushdown` request whose SELECT LIST carries a `predicate_like` or `predicate_like_regexp` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in the projection's column universe is neither `VARCHAR(n)` nor `CHAR(n)` nor `DATE` — `DECIMAL(p,s)` including an Exasol integer's `DECIMAL(p,0)`, `DOUBLE PRECISION`, `BOOLEAN`, or `TIMESTAMP` — or the column's name does not resolve in that universe at all
* *WHEN* the adapter resolves the select list into the scan-spec projection in `project_columns`
* *THEN* the subject dispatch SHALL decline the item, and the decline SHALL set the existing full-base-row fallback flag so the WHOLE select list widens to every column of the base row and Exasol post-processes the item itself
* *AND* the decline SHALL NOT omit a filter and SHALL NOT surface as an error out of `project_columns` — omitting the whole top-level filter is the WHERE-clause caller's meaning for a decline, NOT this caller's
* *AND* the widened projection SHALL replace the pre-change behavior of rendering the item and hard-failing the DataFusion scan with `There isn't a common type to coerce <Type> and Utf8 in LIKE expression`, so a query that previously returned no result at all now returns Exasol's native result (issue #219)
* *AND* a LIKE nested inside a select-list item SHALL reach the same dispatch — inside a `function_scalar_case`, under a comparison predicate's `left` or `right`, or inside a `function_scalar`'s `arguments` — because the guard traverses the shared curated child-bearing field set rather than the item's top-level node only
* *AND* the same decline reached through the broadcast join's shared use of `project_columns` (`extract_join_projection`, and `joins/mod.rs`'s empty-side path) SHALL widen to the disjoint UNION of every involved table's columns, with no error and no per-leg SQL change, because all three callers funnel through the one select-list pipeline
* *AND* a subject whose name does NOT resolve MAY thereby lose a projection pushdown that previously rendered as `Utf8 LIKE Utf8` and succeeded, an accepted cost of the fail-safe that is slower but never wrong and SHALL NOT be recorded as a fixed hard failure
