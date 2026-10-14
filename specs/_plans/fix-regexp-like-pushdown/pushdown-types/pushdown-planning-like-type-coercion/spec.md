<!-- DELTA:CHANGED -->
# Feature: Pushdown Planning — LIKE Type Coercion

Makes pushed-down `LIKE` predicates type-aware on the render surfaces that
carry a pushed expression tree: the single-table WHERE-clause filter, the select-list
projection (`project_columns`, shared with the broadcast join), the broadcast join's combined
WHERE filter, and the N-scan fallback's per-leg WHERE filter (`pushdown-joins/pushdown-planning-join-filter-type-coercion`, issue #215). A LIKE predicate's `column`
subject never carries a `dataType` on the wire, and DataFusion performs no implicit
non-string-to-VARCHAR coercion the way Exasol does, so a LIKE over a non-string column pushed
unchanged fails the DataFusion scan at execution time. This feature dispatches on the column's
Exasol type, read from `involvedTables[0].columns`, before rendering: string subjects pass through
unchanged, DATE subjects are rewrapped in an explicit CAST-to-VARCHAR, and every other non-string
subject declines. What a decline MEANS belongs to the caller, and the two surfaces differ: the
WHERE-clause caller routes the request to the qualified single-table wrapper, which applies the
declined predicate as Exasol SQL in its own `WHERE`, while the select-list caller widens the
projection to the full base row so Exasol post-processes the item itself.
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
## Background

* A LIKE predicate's `column` subject carries no `dataType` on the wire; column Exasol types are read from `involvedTables[0].columns`.
* The type dispatch and rewrite happen in the adapter (`pushdown/support.rs`), not in `vs-expression`, because `vs-expression` is a pure syntactic JSON-to-SQL translator with no external column-type context and is shared with a sibling VS-adapter project.
* The rewrite (or decline) applies ONLY to the JSON tree fed to the DataFusion filter renderer; the raw filter tree forwarded to Iceberg file pruning is left untouched.
* A non-string LIKE subject anywhere in the filter tree declines the WHOLE top-level filter, not just the offending conjunct. Exasol does not re-apply a predicate whose capability the adapter advertised, so the adapter applies a declined filter in its own returned SQL: the request routes to the qualified single-table wrapper, which renders the ORIGINAL (un-rewritten) predicate tree as that wrapper's `WHERE` (see `pushdown/pushdown-declined-filter-self-apply`). The type-rewritten tree is not pushed into the scan at all, so the predicate is applied ONCE and Exasol's own implicit coercions apply exactly as they would to the un-delegated query.
* The DATE-subject CAST rewrite renders rather than declines, so it never reaches the self-application path.
* The DATE CAST-to-VARCHAR is Exasol-faithful only under the default `NLS_DATE_FORMAT` (`YYYY-MM-DD`); a session with an altered NLS date format is an accepted, tracked exception (#216), not a silent gap.
* The guard walks the filter tree through the shared post-order rewrite primitive (`pushdown/pushdown-module-structure`), over the same curated child-bearing field set as the string-function argument guard and the decimal-stringification rewriter: the array fields `expressions` / `arguments` / `results` and the single-child fields `expression` / `pattern` / `left` / `right` / `basis`. A `LIKE` reachable only through a non-junction node — inside a `function_scalar_case`, under a comparison predicate's operand, or inside a scalar function's `arguments` — is therefore type-checked like a top-level one.
* The primitive visits a `predicate_like` node's OWN `expression` and `pattern` children before the LIKE dispatch runs on that node. This is inert: the per-node dispatch acts only on `predicate_like` nodes, so a bare `column` subject and a literal pattern pass through the child visit untouched and reach the subject dispatch unchanged.
* A decline has two sub-cases that must not be conflated. A subject whose Exasol type RESOLVES to a non-string type would fail the DataFusion scan if pushed, so it declines to avoid that failure. A subject whose name does NOT resolve declines fail-safe, and there no such guarantee holds: `extract_all_column_types` drops any `involvedTables[0].columns` entry missing `name` or `dataType` and reads the first involved table only, so a genuinely VARCHAR column can miss the lookup, and a pushdown as `Utf8 LIKE Utf8` is valid there. That decline trades a working pushdown for evaluation by Exasol over the returned rows: slower, never wrong. Both sub-cases hold on the WHERE-clause and the select-list surface.
* The select-list pipeline runs the same guard as the WHERE-clause surface (`like_subject_type_guard` and `guard_like_subject`), in the same pass order (LIKE guard, then string-function-argument guard, then decimal-stringification rewriter), so ONE pipeline function serves both surfaces and one ordering rule governs both by construction (`pushdown/pushdown-module-structure`, issue #219). No `vs-expression` code takes part, because column Exasol types come from `involvedTables`, not from the wire nodes the stateless translator sees.
* `predicate_like` is on `project_columns`' pushable select-list node whitelist, so the type-guard pass before rendering is what keeps a select-list LIKE from failing the scan.
* A decline means different things at the two callers, and the pipeline names neither meaning. `handle_pushdown` omits the whole top-level filter; `project_columns` sets its existing `needs_full_fallback` flag and projects the full base row. That caller-agnostic contract is what allows a single shared pipeline; the select-list scenarios below MUST NOT be read as "decline the whole filter" — no filter is involved.
* `project_columns` has THREE callers — `extract_projection` (single-table), `extract_join_projection` (`joins/rendering.rs`, against the disjoint union of both joined tables' columns), and `joins/mod.rs`'s empty-side path — so the guard in the one shared select-list pipeline reaches the broadcast-join SELECT list as well as the single-table projection, with no per-leg SQL change.
* The subject-name lookup is case-normalized by the shared `column_exa_type` helper in `pushdown/support.rs`, which every type-rewrite guard calls. A case-mismatched subject name resolves rather than spuriously declining, and a genuine miss declines fail-safe. `pushdown/pushdown-module-structure` owns the helper's contract, and this feature does not restate it.
* The Unicode-versus-ASCII fold divergence between the two `col_types` builders produces no join-path miss for any column name the adapter can declare. This feature reads `involvedTables[0].columns` through `extract_all_column_types`, the Unicode-folding path; `pushdown/pushdown-module-structure` records the divergence and the issue tracking its removal.
* Four surfaces carry a pushed expression tree and run the subject dispatch: the single-table WHERE-clause filter, the select-list projection, and, owned by `pushdown-joins/pushdown-planning-join-filter-type-coercion` (issue #215), the broadcast join's combined WHERE filter and the N-scan fallback's per-leg WHERE filter. This feature owns the subject TYPE DISPATCH itself — pass through a string subject, rewrap a DATE subject as CAST-to-VARCHAR, decline everything else — shared verbatim by all four surfaces; the join feature owns which column-type universe each join surface screens against and what a decline means there.
* Apache Iceberg spec check: NOT implicated as a data-type-mapping question, and grounded rather than assumed. The spec's Primitive Types table defines `date` as "Calendar date without timezone or time", `decimal(P,S)` as "Fixed-point decimal; precision P, scale S" with "Scale is fixed, precision must be 38 or less", `string` as "Arbitrary-length character sequences" / "Encoded with UTF-8", `boolean` as "True or false", `double` as "64-bit IEEE 754 floating point", and `timestamp` as "Timestamp, microsecond precision, without timezone". The spec mandates no text or display form for any of them. The divergence this guard handles is therefore Exasol-versus-DataFusion SQL-dialect rendering, not an Iceberg schema or type-mapping deviation — the same determination `pushdown-types/pushdown-planning-string-fn-type-coercion` records, and the reason declining (rather than casting) is the only branch that cannot silently change a result.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: LIKE on a VARCHAR or CHAR column pushes down unchanged

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `VARCHAR(n)` or `CHAR(n)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL leave the predicate subject unchanged, rendering `(<column> LIKE <pattern>)`
* *AND* the rendered filter SHALL be carried in the common spec, because a string subject needs no coercion
* *AND* this pass-through SHALL hold identically at all four surfaces that run the dispatch — the single-table WHERE filter, the select-list projection, the broadcast join's combined WHERE filter, and the N-scan fallback's per-leg WHERE filter (see `pushdown-joins/pushdown-planning-join-filter-type-coercion`, issue #215)
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIKE on a DATE column pushes down wrapped in CAST-to-VARCHAR

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DATE`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL rewrap the subject as an explicit CAST-to-VARCHAR node (`function_scalar_cast` with target `{"type":"VARCHAR"}`) before rendering, so the emitted DataFusion SQL casts the DATE column to text and the LIKE matches against the `YYYY-MM-DD` string form
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to the DataFusion filter renderer, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the emitted match semantics SHALL equal Exasol's implicit DATE-to-VARCHAR cast under the default `NLS_DATE_FORMAT` of `YYYY-MM-DD`, which is the ISO-8601 date text form both engines render for the Iceberg `date` primitive (calendar date without timezone, days from 1970-01-01)
* *AND* under a session that has altered `NLS_DATE_FORMAT` away from the `YYYY-MM-DD` default, the pushed-down match MAY diverge from native Exasol evaluation, because DataFusion's `CAST(Date32 AS VARCHAR)` is unconditionally ISO `YYYY-MM-DD` and the pushdown request carries no session NLS format for the adapter to reproduce — an accepted, accurately-scoped tracked exception (#216), not a silent gap
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIKE on a DECIMAL column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's Exasol type in `involvedTables[0].columns` is `DECIMAL(p,s)`
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (emit no `filter` in the common spec)
* *AND* the adapter SHALL NOT inject a CAST for the DECIMAL subject, because DataFusion's decimal-to-string formatting keeps trailing scale zeros that diverge from Exasol's trimmed formatting and would silently change which rows match — correct trimmed-decimal formatting is tracked in issue #211
* *AND* the adapter SHALL route the request to the qualified single-table wrapper and render the ORIGINAL predicate tree as that wrapper's own `WHERE`, so Exasol evaluates the predicate over the returned rows
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIKE on an integer column declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column is an Exasol integer, carried on the wire as `DECIMAL(p,0)` (Exasol has no distinct integer type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the whole top-level filter exactly as for any other `DECIMAL(p,s)` subject, and SHALL self-apply the declined predicate in the qualified single-table wrapper's `WHERE`
* *AND* the adapter SHALL NOT inject a CAST for the integer subject
* *AND* the returned rows SHALL equal native Exasol evaluation of the same query
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIKE on a non-column subject is left untouched

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is NOT a bare `column` node (for example a computed scalar expression)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL leave the node unchanged, applying neither the CAST rewrite nor the decline, because the subject's type is not resolvable from `involvedTables[0].columns`
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: LIKE on a bare column whose type cannot be resolved declines the whole filter

* *GIVEN* a `pushdown` request whose filter carries a `predicate_like` whose `expression` is a bare `column` node
* *AND* the column's name is NOT found in `involvedTables[0].columns` (a lookup miss — no resolvable Exasol type)
* *WHEN* the adapter builds the DataFusion scan-spec filter
* *THEN* the adapter SHALL decline pushdown of the WHOLE top-level filter (fail-safe), because it cannot prove the subject is a string and a non-string subject would hard-fail the DataFusion scan
* *AND* the adapter SHALL self-apply the declined predicate in the qualified single-table wrapper's `WHERE`, so the fail-safe costs a pushdown and never a correct result
* *AND* the name lookup SHALL be case-normalized by uppercasing the subject column name before matching, so a case-mismatched name resolves rather than spuriously declining
* *AND* that normalization SHALL be owned by exactly ONE helper, `column_exa_type` (`pushdown/support.rs`), which every type-rewrite guard calls rather than reimplementing
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
### Scenario: A select-list LIKE over a DATE column projects the CAST-to-VARCHAR form

* *GIVEN* a `pushdown` request whose SELECT LIST carries a `predicate_like` whose `expression` is a bare `column` node — for example `SELECT c_date LIKE '2024%' FROM …`
* *AND* the column's Exasol type in the projection's column universe is `DATE`
* *WHEN* the adapter resolves the select list into the scan-spec projection in `project_columns`
* *THEN* the adapter SHALL run the same subject dispatch on the item that it runs on a WHERE-clause LIKE, rewrapping the subject as a `function_scalar_cast` node with target `{"type":"VARCHAR"}` before rendering
* *AND* the item SHALL project as ONE positional rendered expression carrying `(CAST(<col> AS VARCHAR) LIKE <pattern>)`, NOT as the full-base-row fallback, so the projection stays one item per select-list item
* *AND* a `VARCHAR(n)` or `CHAR(n)` subject SHALL project its rendered fragment unchanged and keep its pushdown
* *AND* the rewrite SHALL apply ONLY to the JSON tree fed to the projection renderer, leaving the raw filter tree forwarded to Iceberg file pruning unchanged
* *AND* the DATE match semantics SHALL carry the same default-`NLS_DATE_FORMAT` fidelity, and the same altered-session-format tracked exception (#216), that the WHERE-clause DATE scenario records, because the identical CAST node is emitted
<!-- /DELTA:CHANGED -->

<!-- DELTA:CHANGED -->
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
<!-- /DELTA:CHANGED -->
