# Plan Review Findings: change-pushdown-keep-more-in-scan

## Summary
- Axes checked: 6/6
- Total findings: 16 (Blockers: 6, Findings: 10)
- Intent Fidelity blockers: 0

Ways this plan can fail:

1. Open PR #624 types `SECOND` and keeps its split, and it supersedes the same ADR. Whichever PR merges second breaks the other's tests, spec, and ADR. Routed to `[HIDDEN_DEPENDENCY]` BLOCKER.
2. The converted-literal check counts markers per request, but the planner renders and discards fragments and writes one spec per N-scan leg. Valid queries then fail with an adapter error. Routed to `[AMBIGUOUS_REQUIREMENT]` BLOCKER.
3. The WHERE split contradicts recorded specs that say a declined filter leaves the fan-out without a filter, and task 2.2 changes the SQL of requests whose filter renders. Routed to two `[REQUIREMENT_CONFLICT]` BLOCKERs.

## Intent Fidelity

#### [SCOPE_REDUCTION] FINDINGS
- Location: decision-log.md [2]; plan.md § Impact ("A text literal compared with a TIMESTAMP, DOUBLE, or scaled DECIMAL still runs in Exasol"); split-aggregate-types delta, scenario "A date-part function takes its fixed native type" ("and `EXTRACT` SHALL take no fixed type"); plan.md § Dependencies ("The implementing commit then carries `Closes #615`")
- Issue: Item 5 asks to "Let Exasol convert constant text before the scan" and to give "`YEAR`, `MONTH`, and the other date-part functions fixed result types". The interview answer chose "constant text keeps the scan filter" without a type restriction. The plan narrows the first sub-item to DATE and excludes `EXTRACT(YEAR|MONTH|DAY|HOUR|MINUTE FROM x)` from the second. The DATE narrowing fits the "minimal" request and the issue's own example (`d = '2024-01-02'`), and decision [2] gives a technical reason for TIMESTAMP (the floor needs the literal's digits at plan time). The plan states no reason for excluding `EXTRACT`. No follow-up records either remainder, and `Closes #615` closes the only issue that tracks them.
- Fix: In plan.md § Context, add an open-question bullet: "Text literals compared with a TIMESTAMP, DOUBLE, or scaled DECIMAL still decline (#TBD); the user confirms the DATE-only scope." In decision-log.md [3] Consequences, state why `EXTRACT(YEAR|MONTH|DAY|HOUR|MINUTE FROM x)` gets no fixed type, or add it to the scenario, task 1.2, and tests 1.3 to 1.5. If it stays out, list it in the same open-question bullet with (#TBD).

## Feasibility

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Context (open-question bullet on one-argument `SECOND`), § Dependencies, § Parallelization row A; decision-log.md [3]; split-aggregate-types delta, Background bullet 3 and the last AND step of "A date-part function takes its fixed native type"; tasks 1.1, 1.3, 1.4, 1.5
- Issue: Open PR #624 (Closes #603) changes the same feature, ADR, and code, and the plan does not model it.
  - #624 types `SECOND(x)` and `EXTRACT(SECOND FROM x)` over `TIMESTAMP(p)` as `DECIMAL(2+p, p)` and adds the scenario "An unprojected SECOND group key takes the type of its argument's precision" with "the request SHALL keep the split path".
  - This plan's delta says "`SECOND` ... and `EXTRACT` SHALL take no fixed type, so an unprojected key over them declines the split". Task 1.3 asserts `SECOND` returns `None`, task 1.4 asserts "`GROUP BY SECOND(ts)` routes to the wrapper", and task 1.5 adds `GROUP BY SECOND(C_TS)` with `PathCheck::Wrapper`. All of them fail once #624 merges.
  - #624 adds the ADR `copy-typing-rule-only-when-exact-and-version-stable`, which supersedes `split-aggregate-columns-take-exact-types-or-decline`, and its Consequences state: "a new rule that meets the policy needs no new ADR". Decision [3] supersedes the same ADR with `Promotes to ADR: yes`, which duplicates #624's policy (`/speq:adr-rules` rule 5).
  - #624 rewrites Background bullets 3 and 5 of `pushdown-planning-split-aggregate-types`. This delta's DELTA:CHANGED Background replaces both, so recording it after #624 deletes #624's `SECOND` rule.
  - #624's policy requires a copied rule to hold "on every supported Exasol version", and architecture § Constraints says "The engine supports Exasol 8.29.x and later". Task 1.1 measures only the local Exasol.
  - The open question "(#TBD)" on `SECOND` returning `Int32` is #603, which #624 fixes.
- Fix:
  - In plan.md § Dependencies, add "PR #624 merges before group A starts", and add `PR #624 merged` to group A's Depends on.
  - Rebase the split-aggregate-types delta on #624's text. Keep #624's `SECOND` sentence and version list in Background bullet 3, append the date-part rule, and delete the last AND step about `SECOND` and `EXTRACT`.
  - In decision-log.md [3], set `Promotes to ADR: no`, delete the `Supersedes:` line, and state in Rationale that `copy-typing-rule-only-when-exact-and-version-stable` covers the rule.
  - Delete the `SECOND` assertions from tasks 1.3 and 1.4 and the `GROUP BY SECOND(C_TS)` row from task 1.5.
  - Replace the § Context open-question bullet with "One-argument `SECOND` is typed by PR #624 (#603)."
  - Extend task 1.1 to measure the six types on Exasol 8.29.x and on the current version, and name both versions in the Background bullet.

#### [UNSTATED_ASSUMPTION] BLOCKER
Also tagged `[AMBIGUOUS_REQUIREMENT]`.
- Location: plan.md task 3.3 ("The translator counts the markers its renders write for one request. One public function turns a serialized common spec and that count into the Exasol SQL ... returns an error ... when the spec does not hold exactly the counted well-formed markers"), task 3.4; decision-log.md [1]
- Issue: The check assumes that every marker a render writes reaches exactly one serialized spec. The planner does not work that way:
  - Task 2.1's `split_where` renders the whole filter first and then each conjunct. `render_scan_expression` runs the typed pass and then `render_expression`, which can still decline, for example on two-argument `SECOND`. So `WHERE C_DATE = '01.01.2024' AND SECOND(C_TS, 3) > 1` writes the DATE marker twice, and the spec holds it once.
  - `render_broadcast_join` renders the condition and the filter, then returns `Ok(None)` on a widened projection, and the N-scan renders them again.
  - `project_columns` discards rendered items when it widens to the full row.
  - An N-scan join serializes one common spec per leg (`build_side_fan_out_sql`), and each holds only part of the request's markers.

  The function therefore returns an error for valid queries that the new scenario says must succeed. `ScanTranslator` is `Copy` and holds only `&ColumnTypes`, so a counter stored by value splits across copies. `build_fan_out_inner` has no count parameter, and each of its callers would have to pass one: row scan, aggregate, `COUNT(DISTINCT)`, grouped, top-N, wrapper, broadcast, side fan-outs, and `tests/it/pushdown_public_surface.rs`. Task 3.4 does not list that work.
- Fix:
  - Rewrite task 3.3 without the per-request count. The marker delimits itself: a start character, the literal hex-encoded, and an end character, using characters that `serde_json` does not escape.
  - The common-argument function converts every well-formed marker in the serialized spec. It returns the plain string literal when the spec holds no marker character, and it returns an error when a marker character is outside a well-formed marker.
  - Keep the request-JSON guard.
  - If a count must stay, attach it to each rendered `SqlFragment`, sum only the fragments that a spec stores, check it per spec, and say so in tasks 3.3 and 3.4.
  - Add unit tests to task 3.7: a DATE text-literal conjunct next to a conjunct that `render_expression` declines, a broadcast attempt that falls through to the N-scan, and an N-scan join with a DATE text literal in each leg's filter.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: scan-text-conversion delta, new scenario, AND step "a literal that does not parse SHALL fail the query ... whenever a shard scans, also when no row would reach the comparison"; plan.md tasks 3.1 and 3.8
- Issue: The step assumes that the error fires only when a shard scans. Exasol may fold the constant concatenation when it compiles the dispatch SQL. When pruning removes every file, it is not stated whether the dispatch SQL still contains `LAKEHOUSE_SCAN`. The planning notes record that native `IN` converts each item without catching the error, while `=` catches it and parses per row. Native and adapter outcomes can therefore differ in both directions: on an empty table or a fully pruned file list, and between `IN` and `=`. Task 3.1 measures only native Exasol. Task 3.1 also checks `0001-01-01` and `9999-12-31` but no literal before 1582-10-15. PR #626 measured that Exasol uses the Julian calendar before that date, and the scenario claims parity for every literal.
- Fix: In task 3.1, also measure the adapter's dispatch SQL for an unparseable literal with `=` and with `IN`, on an empty table and on a filter that prunes every file, and compare a literal before 1582-10-15 with native Exasol. Replace "whenever a shard scans" with the measured condition. Add those cases to the error cases of task 3.8 and to the `docs/semantic-differences.md` section of task 3.9.

#### [NFR_IGNORED] FINDINGS
- Location: plan.md task 3.3 ("the function returns an error naming the cause")
- Issue: The serialized common spec carries the CONNECTION name or a sealed credential envelope. `ScanSpec::from_parts_json` documents "Errors never echo the inputs (the common blob carries credentials)", and architecture § Constraints says "Errors never contain credentials". Task 3.3 does not forbid the new error from quoting the spec.
- Fix: In task 3.3, state that the error names the cause without quoting the serialized spec or the literal's surroundings. In task 3.6, add an assertion that the error text holds no substring of the spec.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: recorded spec `pushdown-aggregates/pushdown-planning-aggregate-extensions`, scenario "A declined WHERE filter under an aggregate request is applied ahead of the aggregate"; this plan's declined-filter delta, new scenario "A single-table WHERE filter splits at AND between the scan and the wrapper"
- Issue: The recorded scenario states "the per-shard fan-out SHALL carry no aggregate, no group keys, and no filter". The new scenario states that the fan-out's scan spec "SHALL carry the conjunction of the rendered conjuncts as its `filter`" for "an aggregate, a `GROUP BY`". Both apply to an aggregate request with one declined and one rendered conjunct. The plan has no delta for that feature.
- Fix: Add `specs/_plans/change-pushdown-keep-more-in-scan/pushdown-aggregates/pushdown-planning-aggregate-extensions/spec.md` with a DELTA:CHANGED block for that scenario. The AND step reads: "the per-shard fan-out SHALL carry no aggregate and no group keys, and its `filter` SHALL hold only the conjuncts the DataFusion-bound render renders (`pushdown/pushdown-declined-filter-self-apply`)". Add the feature to plan.md § Features, and map the scenario in § Scenario Coverage to `a_partly_declined_filter_keeps_its_rendered_conjuncts_in_the_scan`.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md task 2.2 ("compute the fan-out projection (`referenced_columns`) from the wrapper's clauses only"); pushdown-planning delta, scenario "A declined WHERE filter routes the single-table request to the qualified wrapper", last AND step; recorded spec `pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback`, Background bullet 1 ("projects only the referenced columns: group keys, select-list aggregate arguments, filter columns, ...")
- Issue: `qualified_single_table_fallback_pushdown` also serves the `fallback()` closure in `mod.rs`. That closure passes a rendered `filter` with no declined half, for grouped and `COUNT(DISTINCT)` requests that cannot split. `referenced_columns` walks every clause, including `filter`, so these fan-outs project the filter's columns today. Task 2.2 drops those columns for every caller. A request whose filter renders but that takes the wrapper fallback then changes its SQL. That contradicts this plan's "a request whose filter renders ... SHALL take its existing dispatch shape with the emitted SQL byte-identical to its pre-change output" and the recorded Background bullet. No task regenerates the affected goldens.
- Fix: Reword task 2.2 so that the projection is computed over the request with `filter` replaced by the declined half, and only when a declined half is present. The `fallback()` closure keeps today's projection. Add a unit test to task 2.4: a grouped wrapper fallback with a rendering filter emits byte-identical SQL. If the narrowing should apply to both callers instead, add a DELTA:CHANGED for the grouped-agg-wrapper-fallback Background bullet, remove that shape from the byte-identical step, and add a task that regenerates the changed goldens.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md task 3.3 ("a marker that carries the original literal", "writes `DATE '<YYYY-MM-DD>'`, or `NULL` when the conversion returns NULL"); task 3.6
- Issue: The marker passes through `serde_json` when the spec is serialized, and the function reads the serialized text. `serde_json` escapes `"`, `\`, and control characters, so a literal holding one of them reaches Exasol in its escaped form and the conversion error quotes the wrong text. A control-character marker is escaped too, and the function would not find it. "writes ... `NULL`" can mean the text `NULL` or an SQL NULL. Exasol's `||` treats an SQL NULL as an empty string, which leaves the DataFusion SQL with a missing operand.
- Fix: In task 3.3, state that the payload is encoded so that JSON serialization leaves it unchanged, and that the NULL branch writes the four characters `NULL` into the spec text. Add cases to task 3.6 for a literal holding `"`, `\`, and a control character, and an assertion that the NULL branch yields the text `NULL`.

#### [COMPLETENESS_GAP] FINDINGS
- Location: scan-text-conversion delta, new scenario, AND step "in the WHERE filter, a select item, a GROUP BY key, an aggregate argument, and a broadcast join condition or filter"; scan-text-conversion delta Background bullet 4 ("each N-scan leg filter"); declined-filter delta, new scenario, AND step "for a row scan, an aggregate, a `GROUP BY`, a `COUNT(DISTINCT)`, and an `ORDER BY` with `LIMIT`"; plan.md tasks 2.5 and 3.8
- Issue: The E2E case table of task 3.8 has no GROUP BY key or aggregate argument that compares a DATE with a text literal, no N-scan leg filter, and no DATE text literal next to a declined conjunct. The last case is where groups B and C interact. Task 2.5 proves result parity only for a row scan and `COUNT(*)`. The `GROUP BY`, `COUNT(DISTINCT)`, and top-N shapes have only SQL-shape unit tests. `specs/testing.md` § Correctness oracles requires that a shape the adapter advertises is proved against a live Exasol.
- Fix: Add to task 3.8: `GROUP BY CASE C_DATE WHEN '<text>' THEN 1 ELSE 0 END`, `SUM(CASE WHEN C_DATE = '<text>' THEN 1 ELSE 0 END)`, an N-scan join whose leg filter holds `C_DATE = '<text>'`, and `C_DATE = '<text>' AND C_DECIMAL_A LIKE '1%'` with `Has(SCAN_FILTER)`. Add `GROUP BY`, `COUNT(DISTINCT ID)`, and `ORDER BY ID LIMIT 1` variants of the same WHERE to task 2.5.

#### [IMPLEMENTATION_DETAIL] FINDINGS
- Location: declined-filter delta, Background bullet 2 ("The single-table path and the broadcast-join path screen the WHERE filter through `ScanTranslator::split_where`, and the N-scan legs through `screen_leg_filter`. Both split a filter that does not render whole into its top-level `AND` conjuncts and screen each one.")
- Issue: No scenario step depends on the two function names. The second sentence states behavior that the new scenario already owns, and it is inaccurate. `screen_leg_filter` splits every leg filter, not only one that "does not render whole". The broadcast path keeps no split, because task 2.3 declines broadcast whenever a declined half exists.
- Fix: Replace bullet 2 with: "A conjunct declines when the scan translator declines it (`pushdown-types/pushdown-planning-scan-text-conversion`)." Leave the splitting behavior to the scenarios.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: declined-filter delta, new scenario, THEN step ("render each conjunct for DataFusion once")
- Issue: A test cannot observe how often the adapter renders a conjunct. Task 2.1 renders the whole filter first and then each conjunct, so each conjunct is rendered twice when the whole filter declines.
- Fix: Replace the clause with an observable outcome: "and apply each conjunct exactly once, in the scan or in the wrapper's `WHERE`".

## Task Breakdown

#### [TASK_GRANULARITY] FINDINGS
- Location: plan.md task 3.3 (1,077 characters, nine sentences)
- Issue: One `[expert]` task holds five units that are verified separately: the marker render in the DataFusion dialect, the marker bookkeeping, the common-argument builder, the quoting and NULL branch, and the request-JSON guard with its error. A reviewer cannot check them as one unit, and task 3.6 tests them as one block.
- Fix: Split task 3.3 into 3.3a (the DataFusion-dialect render of a converted-literal node, tested in `lib_tests.rs`) and 3.3b (the common-argument builder with quoting, the NULL branch, the guard, and the error, tested in the new module's `_tests.rs`). Map the task 3.6 cases to 3.3b.

## Design Depth

#### [ADR_CONFLICT] BLOCKER
- Location: decision-log.md [1] ("This conforms to `scan-never-converts-session-formatted-text` ... That ADR lists this option as deferred", `Promotes to ADR: no`); scan-text-conversion delta, new scenario ("a broadcast join condition or filter, which keeps its broadcast plan"); `specs/_decision/587-refactor-typed-scan-translator.md`
- Issue: The accepted ADR decides that the translator "declines any conversion between text and these types, explicit or implicit, so Exasol performs it". It lists "Let Exasol convert constant text before the scan" as "Deferred to a follow-up". Its Consequences state: "A filter such as `date_col = '2024-01-01'` runs in Exasol, prunes no files, and in a join loses the broadcast plan." This plan's typed pass stops declining the implicit conversion of a DATE text literal, takes the deferred option, and keeps the broadcast plan. The scan still parses no text, but the ADR's Decision and its Consequence are contradicted, and no entry supersedes them. Decision [1] also meets `/speq:adr-rules` rule 2: it affects several components (the vs-expression renderer, the pushdown-planner's dispatch SQL, and the data flow into `LAKEHOUSE_SCAN`), and it rejects plausible alternatives (a DataFusion placeholder and more `LAKEHOUSE_SCAN` arguments).
- Fix: In decision-log.md [1]:
  - Set `Promotes to ADR: yes` and add `Supersedes: scan-never-converts-session-formatted-text`.
  - Restate the Decision: the scan converts no session-formatted text; the translator declines every such conversion except a text literal compared with a DATE, which Exasol converts in the dispatch SQL before the scan.
  - In Rationale, name criteria 1 and 4 and the ADR search result.
  - Add the superseded ADR's rejected options (`exa_to_varchar`, reading the session's NLS settings) to Alternatives.
  - Add an `Architecture:` line that names § Data Flow and § Components of the delta.

#### [ARCHITECTURE_DRIFT] BLOCKER
- Location: architecture.md (changes only § Data Flow); decision-log.md [1] ("One `vs-expression` module owns how the renderer marks a converted literal and how the serialized spec becomes Exasol SQL, so the pushdown-planner does not know the encoding"); plan.md task 3.3
- Issue: `specs/architecture.md` § Components describes vs-expression as translating "VS expression JSON into DataFusion SQL and Exasol SQL fragments", with "owns: the Exasol SQL type and its SQL and `dataType` JSON renderings". It describes the pushdown-planner with "owns: dispatch SQL". The plan gives vs-expression a new owned encoding and makes it build the Exasol SQL of the dispatch SQL's `common` argument. The delta does not change § Components.
- Fix: Add a `<!-- DELTA:CHANGED -->` § Components block to architecture.md that copies the full current section. On the vs-expression line, add the converted-literal marker and the Exasol SQL of a marked `common` argument to its responsibilities and its owns list. If the fix of the `[UNSTATED_ASSUMPTION]` BLOCKER moves the builder into the pushdown-planner, record that on the pushdown-planner line instead. Name § Components in decision [1]'s `Architecture:` line.

#### [INFORMATION_COUPLING] FINDINGS
- Location: plan.md task 2.1 ("Reuse `flatten_conjuncts`, `conjoin`, and `conjoin_scan_filters` from `joins/rendering.rs` by widening their visibility, and leave `screen_leg_filter` unchanged"); task 3.3 ("byte-identical to `sql_string_literal`")
- Issue:
  - After task 2.1, two functions split a filter into rendered and declined conjuncts. `split_where` renders each conjunct through `render_scan_filter`, and `screen_leg_filter` through `ScanTranslator::expression`. The per-conjunct rule therefore lives in two places that can drift.
  - `joins/rendering.rs` already imports `ScanTranslator` from `support.rs`. Importing its helpers back into `support.rs` creates a dependency cycle between `support` and `joins::rendering`.
  - Task 3.3 makes vs-expression reproduce `sql_string_literal` (`crates/lakehouse-engine/src/exasol.rs`) byte for byte, so one quoting rule lives in two crates.
- Fix: In task 2.1, move `flatten_conjuncts` and `conjoin` into `support.rs` next to `split_where`, and make `joins/rendering.rs` import them. Add one per-conjunct screen method on `ScanTranslator` that both `split_where` and `screen_leg_filter` call. In task 3.3, move Exasol string-literal quoting into vs-expression and make `sql_string_literal` call it, or keep the plain-literal branch in the engine.

## Prose Quality

#### [PROSE_UNCLEAR] FINDINGS
- Location: decision-log.md [1]; plan.md tasks 3.2, 3.3, 3.7; architecture.md § Data Flow, new line
- Issue: One concept has four names: "converted-literal node" (task 3.2), "marker" (task 3.3), "converted literal" (decision [1]), and "converted DATE literal" (plan.md § Context). "Marker" is never defined, which breaks `/speq:writing-guardrails` rule 4. One sentence in task 3.3 does not parse on one read: "A marker character from anywhere but the render never becomes Exasol SQL."
- Fix: Define "converted-literal marker" once in decision-log.md [1], for example "the text the DataFusion-dialect renderer writes in place of a DATE text literal, which carries the literal for Exasol to convert", and use that term in plan.md and architecture.md. Replace the quoted sentence with: "The adapter converts no literal in a request whose JSON holds a marker character."
