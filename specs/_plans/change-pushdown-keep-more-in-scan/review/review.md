# Plan Review Findings: change-pushdown-keep-more-in-scan

## Summary
- Axes checked: 6/6
- Total findings: 16 (Blockers: 1, Findings: 15)
- Intent Fidelity blockers: 0

## Failure Modes

1. The implementer follows the task list, and three existing tests that encode the whole-WHERE decline fail: one E2E pruning case and two unit tests. Under time pressure, the pruning case is loosened to `Scan`, and the only test that proves pruning for a split filter loses its outer-WHERE check. Routed to `[HIDDEN_DEPENDENCY]` (BLOCKER).
2. A date-part type is wrong on one engine flavor, for example Exasol Nano 2026.2 or `HOUR` over a DATE. No test asserts the declared group-key type, so a type that is too wide passes locally, and a version-specific type fails only the CI Nano leg. Routed to `[HIDDEN_DEPENDENCY]` and `[TRACEABILITY_GAP]`.
3. The new `ScanWhere` variant is wired wrong at one consumer, for example the broadcast join keeps the rendered half instead of returning `Ok(None)`. No test sends a partly declined filter through a broadcast-eligible join, so the join returns extra rows. Routed to `[TRACEABILITY_GAP]` and `[INFORMATION_COUPLING]`.

## Intent Fidelity

`[INTENT_DRIFT]`: no objection, axis checked. #615 item 5 (`gh issue view 615`, OPEN) has four bullets. The plan delivers bullets 2 (WHERE split), 3 (date-part types), and 4 (DataFusion 55.2.0 lacks apache/datafusion#22906, confirmed by `cargo search datafusion`, so the plan adds the regression guard instead of deleting the floor). Bullet 1 is dropped by the user's decision in `## Interview` Q5.

`[SCOPE_CREEP]`: no objection, axis checked. Every task traces to one of the three groups or to the `AGENTS.md` docs rule. The empty-`selectList` rewrite in two scenarios aligns them with the recorded Background on current `main` (`OutputContract::AnyOneColumn`, `support.rs:886`).

`[SCOPE_REDUCTION]`: no objection, axis checked. Group C removal is user-authorized. The `Closes #615` / `Refs #615` open question keeps the issue honest.

Group C leftovers: none found. No marker, conversion task, `architecture.md`, `pushdown-types/` delta, or `docs/semantic-differences.md` task remains. Decision numbering holds only [1]. plan.md § Impact says `docs/semantic-differences.md` stays. The remaining DATE-text sentences (plan.md § Context bullets 2 and 9, § Out of scope bullet 1, `## Interview` Q1 and Q5) explain the drop, which is correct.

## Feasibility

`[NFR_IGNORED]`: no objection, axis checked. Performance improves (the scan filters rows that reached Exasol before). Security and migration are untouched: no property, CONNECTION field, credential path, or capability changes. File pruning keeps reading the raw `filter_json_raw` (`mod.rs:153` comment).

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: plan.md § Implementation Tasks group 2, § Dead Code Removal, § Verification > Checklist
- Issue: Three existing tests assert that a WHERE with one declined conjunct and one rendered conjunct declines whole. Task 2.1 makes each assertion false, and the plan names none of them, so the checklist rows "Unit tests: 0 failures" and "E2E on Exasol 2025: 0 failures" fail as written. `specs/testing.md` § Regression guards requires each such test to be deleted or deliberately changed, never weakened.
  - `crates/lakehouse-e2e/tests/common/pruning_cases.rs:364`: `Case::new("S LIKE 'xaa' ESCAPE 'a' AND SECOND(TS, 3) >= 0", OuterWhere)`. `Placement::OuterWhere` holds only when the plan has `LHS_T0` and no `"filter":"` (`pruning_cases.rs:43`). `S LIKE 'xaa' ESCAPE 'a'` alone is a `Scan` case (line 363), so after the split the scan spec carries it and the case fails on Iceberg and Delta.
  - `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs:1681` `nested_like_decline_routes_to_wrapper_where`: filter `STATUS = 'OPEN' AND AMOUNT LIKE '9%'`. The helper `declined_filter_wrapper_sql` panics unless `split_where` returns `ScanWhere::Declined`, and the test asserts "the wrapper WHERE must carry the WHOLE declined filter, both conjuncts".
  - `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs:890` `pruning_input_unchanged_when_df_render_declines`: filter `id > 5 AND SECOND(ts, 3) … AND LENGTH(amount) > 5`, asserts `ScanWhere::Declined(&filter)`. `id > 5` renders, so the new variant comes back.
- Fix: Add a task to group 2, after 2.3: (a) in `pruning_cases.rs`, add a `Placement::Split` variant that holds when the plan has both `LHS_T0` and `"filter":"`, and set it on the line-364 case, keeping its scanned-file list `["a1", "a2", "b1"]`; map the pruning step of the changed scenario "A declined WHERE filter routes the single-table request to the qualified wrapper" to this case. (b) Delete `nested_like_decline_routes_to_wrapper_where`, because the 2.5 E2E test proves the split. (c) Rewrite `pruning_input_unchanged_when_df_render_declines` to assert that the new variant's declined half holds the original, un-rewritten `SECOND` and `LENGTH` conjuncts, which is the property the test name states. Add `pruning_cases.rs`, `support_tests.rs`, and `pushdown_tests.rs` to group B's Knowledge, and list (b) in § Dead Code Removal.

#### [EFFORT_MISESTIMATION] FINDINGS
- Location: plan.md § 1.2, § 2.1, § Parallelization row B
- Issue: Four code edits that the task lines need are unstated.
  - (a) `ResultRule` (`scalar_functions.rs:30`) has no variant that carries a fixed exact type. Task 1.2 must add one and its arm in `ScalarFunction::result_type` (`scan_typing.rs:335`).
  - (b) `ScanWhere::declined()` returns `Option<&'a Json>` (`support.rs:716`). It cannot borrow the owned conjunction that task 2.1 puts in the new variant, so its signature changes, and `qualified_single_table_fallback_pushdown` (`joins/sql_builders.rs:804`) follows.
  - (c) `joins/broadcast_policy.rs:7` imports `flatten_conjuncts` from `rendering`. The move in 2.1 edits that file, which group B's Knowledge omits.
  - (d) Task 2.1 does not say what `split_where` returns when every conjunct renders alone but the whole filter declined.
- Fix: In 1.2, add "add a `ResultRule` variant that holds a fixed exact type, and its arm in `result_type`". In 2.1, add "change `ScanWhere::declined()` to borrow from `&self`", "update the `flatten_conjuncts` import in `joins/broadcast_policy.rs`", and "when every conjunct renders, return `ScanWhere::Pushed` with the conjunction". Add `joins/broadcast_policy.rs` to group B's Knowledge.

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § 1.1, § Verification > Checklist
- Issue: CI runs the local E2E suite on three images (`.github/workflows/ci.yml:341-353`): `exasol/docker-db:2025.1.16`, `exasol/docker-db:8.29.13`, and `exasol/nano:2026.2.0-nano.8` on arm64. Task 1.1 measures the date-part types only on 8.29.13 and 2025.1.16, and the checklist omits Nano. The new `SPLIT_AGGREGATE_CASES` rows run on Nano in CI. The earlier review raised this, and the plan still lacks it.
- Fix: Add `exasol/nano:2026.2.0-nano.8` to the measurements of 1.1. Add a checklist row "E2E on Exasol Nano | `make e2e-down; EXASOL_IMAGE=exasol/nano:2026.2.0-nano.8 make e2e-up test-e2e` | 0 failures".

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Context; declined-filter-self-apply delta, scenario "A single-table WHERE filter splits at AND between the scan and the wrapper", fourth AND step
- Issue: The scenario promises a result equal to native Exasol for every shape. After the split, the scan evaluates a rendered conjunct over rows that a declined conjunct would exclude, so a rendered conjunct that can raise an error in DataFusion, such as an integer division by zero, now runs on those rows. Before the change, the whole WHERE ran in Exasol for such a filter. Exasol applies an unadvertised conjunct after the scan in the same way, so the plan likely accepts this, but it does not say so.
- Fix: Add one sentence to plan.md § Context: "A rendered conjunct runs in the scan over rows that a declined conjunct excludes, as for a conjunct whose capability the adapter does not advertise." If a live check shows native Exasol succeeds where the split fails for realistic data, add the case to `docs/semantic-differences.md` in task 2.6.

## Requirement Quality

Every delta builds on the current recorded text. The unmarked Backgrounds of the pushdown-planning and aggregate-extensions deltas match the recorded text exactly (`diff`). `speq plan validate change-pushdown-keep-more-in-scan` passes with three AND-count warnings. `pushdown-types/pushdown-planning-scan-text-conversion` holds the scenario "A digits-only literal wider than the integer's precision runs in Exasol", which backs plan.md § Impact bullet 3.

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: split-aggregate-types delta § Background (DELTA:CHANGED); recorded `specs/pushdown-aggregates/pushdown-planning-split-aggregate-types/spec.md` lines 62 and 91
- Issue: The changed Background no longer names "the typed pass". Two unchanged scenarios still depend on it: "An unprojected group key with a derived exact type keeps the split" ("that the typed pass gives an exact type (Background)") and "An unprojected SECOND group key takes the type of its argument's precision" ("in the typed scan pass"). After the merge, both steps cite a term the spec no longer defines. The earlier review raised this, and the delta still has it.
- Fix: In the split-aggregate-types delta, add DELTA:CHANGED blocks for those two scenarios, copied from the recorded text. Replace "that the typed pass gives an exact type (Background)" with "that has an exact type (Background)", and "has the exact type `TIMESTAMP(p)` in the typed scan pass" with "has the exact type `TIMESTAMP(p)` (Background)".

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: declined-filter-self-apply delta § Background bullet 2; recorded scenarios "A declined filter is applied before aggregation, grouping, and truncation" and "An N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct"
- Issue: The changed Background drops "A filter declines when the scan translator declines it" and defines only "A conjunct declines when the scan cannot compute it". Two unchanged scenarios still use the dropped terms: "whose filter declines" (aggregation scenario GIVEN) and "the scan translator renders it" (N-scan scenario, second and fourth AND steps). After the merge, "a filter declines" can mean "every conjunct" or "any conjunct". The deltas also use a second name for the same concept: "the DataFusion-bound render declines".
- Fix: Rewrite bullet 2 of the declined-filter delta Background as: "A conjunct declines when the scan translator, which decides what the scan computes, cannot render it (`pushdown-types/pushdown-planning-scan-text-conversion`). A filter declines when at least one of its top-level conjuncts declines. Exasol drops a `WHERE` that folds to `TRUE`, so it never sends a trivially-true filter."

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: declined-filter-self-apply delta, scenario "A declined filter does not widen the wrapper's projection", THEN step
- Issue: "SHALL emit only the columns that the request's select list, the wrapper's other clauses, and the declined conjuncts name, and no column because a conjunct declined" contradicts itself on a literal reading. The columns that the declined conjuncts name are emitted precisely because those conjuncts declined. "The wrapper's other clauses" also leaves open which clauses count.
- Fix: Replace the THEN step with: "the wrapper's fan-out SHALL emit only the columns that the request's select list, GROUP BY, HAVING, and ORDER BY, and the declined conjuncts name".

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: pushdown-planning delta, scenario "Filter predicate is pushed into the scan spec", THEN step; scenario "A declined WHERE filter routes the single-table request to the qualified wrapper", last AND step
- Issue: (a) "SHALL translate a filter that does not render whole one top-level `AND` conjunct at a time" reads first as "render whole one top-level AND conjunct". The earlier review raised this, and the text is unchanged. (b) "byte-identical to its pre-change output" has no referent once the plan is recorded, so no test can be written from it. `pushdown-joins/pushdown-planning-join-fallback` line 65 already states the same requirement testably.
- Fix: (a) Rewrite the THEN step as "the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, and, when the whole filter does not render, SHALL translate it one top-level `AND` conjunct at a time". (b) Replace "with the emitted SQL byte-identical to its pre-change output" with "and SHALL emit the SQL that the golden-SQL fixtures pin for that shape".

#### [IMPLEMENTATION_DETAIL] FINDINGS
- Location: pushdown-planning delta § Background (unmarked); recorded `specs/pushdown/pushdown-planning/spec.md` line 18
- Issue: The CHANGED scenario "Filter predicate is pushed into the scan spec" drops line 38, the only scenario step that names `iceberg::expr::Predicate`. The Background sentence "Scenario clauses naming `iceberg::expr::Predicate` describe the Iceberg arm" then serves no scenario, and it names a Rust type. The earlier review raised this.
- Fix: Mark the pushdown-planning delta's `## Background` as DELTA:CHANGED and delete that sentence. Per `.speq/plan-hook.md`, also drop the sentences that name internal structure in the copied block: the `UdfContext::node_count()` / `dispatch` / tokio bullet, and "Every planning path reads these values from it". If that rewrite is too large for this plan, add one plan.md § Out of scope sentence naming the orphaned Background sentence.

#### [REQUIREMENT_CONFLICT] FINDINGS
- Location: recorded `specs/pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback/spec.md` § Background bullet 2; recorded `specs/pushdown-types/pushdown-planning-scan-text-conversion/spec.md` § Background bullet 6
- Issue: Two recorded Backgrounds keep stating the whole-filter route. The grouped wrapper fallback says the inner raw scan projects "filter columns", but after task 2.2 a column that only a rendered conjunct names is not projected, as the new scenario states. The scan-text-conversion Background says "a WHERE filter runs in the qualified wrapper", but after the change only the declined conjuncts run there.
- Fix: Add a delta for each feature with a DELTA:CHANGED Background: replace "filter columns" with "the columns of the filter conjuncts the wrapper applies", and replace "a WHERE filter runs in the qualified wrapper" with "a WHERE conjunct runs in the qualified wrapper's `WHERE`, and the other conjuncts stay in the scan". If the hook's rewrite of those Backgrounds is too large, add one plan.md § Out of scope sentence per stale phrase.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § 2.6; `docs/capabilities.md` § Scalar functions, line 88
- Issue: Task 2.6 fixes two sentences, but a third one still describes the removed behavior: "The scan computes these functions when DataFusion can render the whole filter or select list that contains them." After the split, a function in one renderable `AND` operand runs in the scan even when another operand declines.
- Fix: Add to 2.6: in § Scalar functions, replace "when DataFusion can render the whole filter or select list that contains them" with "when DataFusion can render the WHERE condition or the whole select list that contains them". Add the sentence to § Dead Code Removal's doc-text row.

## Task Breakdown

`[CLUSTER_INCOHERENCE]`: no objection, axis checked. Group A shares one spec delta and `scan_typing.rs`/`scalar_functions.rs`. Group B shares three deltas and the `adapter/pushdown/` modules. Group D stands alone. The A-to-B dependency is a file conflict on `capability.rs` and `docs/capabilities.md`, and the plan states it.

#### [TRACEABILITY_GAP] FINDINGS
- Location: plan.md § 1.3, § 2.5, § Verification > Scenario Coverage
- Issue: Two THEN steps have no test that fails when they break.
  - (a) "`YEAR` SHALL take `DECIMAL(4,0)`, and `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK` SHALL take `DECIMAL(2,0)`". An unprojected key shows its type only in the `EMITS` clause. `PathCheck::Split` and the row oracle pass with any wider type, because `comparable_outcome` compares only the output columns. Only `MAX(DAY(C_DATE)) + 1` exposes a type. The earlier review raised this.
  - (b) The last AND step of "A single-table WHERE filter splits at AND between the scan and the wrapper" (a broadcast-eligible join whose filter holds a declined conjunct takes the N-scan fallback). Every existing broadcast decline test (`join.rs:1101`, `join.rs:1190`) uses one declined conjunct, so none sends the new variant through `render_broadcast_join_emitting`.
- Fix: (a) In 1.3, beside each `GROUP BY` date-part row, add a row with the same query and `PathCheck::Has(r#""GK_0" DECIMAL(4,0)"#)` for `YEAR`, and `DECIMAL(2,0)` for `MONTH`, `WEEK`, `HOUR`, and `EXTRACT(MINUTE …)`. (b) Add a task to group 2: in `join.rs`, a case over the fact and dimension join with `WHERE SECOND(o.O_ORDERDATE, 3) = 1 AND o.O_CUSTKEY > 0` that returns 0 rows, matches native, and takes `PathCheck::NScan(2)`. Map the step to that test in § Scenario Coverage.

#### [TASK_GRANULARITY] FINDINGS
- Location: plan.md § 2.4
- Issue: Task 2.4 says its golden is "generated before 2.1", but it is numbered after 2.1 to 2.3. An implementer who works in order generates the golden from post-change output, and it then guards nothing. The earlier review raised this.
- Fix: Renumber 2.4 as 2.1, shift 2.1 to 2.3 to 2.2 to 2.4, and update the references to those numbers in plan.md.

## Design Depth

`[ADR_OVERPROMOTION]`: no objection, axis checked. No entry promotes. Decision [1] and both review-finding entries say `Promotes to ADR: no`.

`[ARCHITECTURE_DRIFT]`: no objection, axis checked. No `yes` entry exists. plan.md § Context says "Architecture: no change" with a reason. `specs/architecture.md` states no whole-filter rule: its only related line is § Constraints line 94 (one scan translator), and the shared per-conjunct screen on `ScanTranslator` keeps that.

`[ADR_CONFLICT]`: no objection, axis checked. `no-exasol-side-fallback-for-an-advertised-capability`: every declined conjunct is still applied in the wrapper. `screen-renderability-at-the-render-consumer-not-in-the-partition-classifier`: the screen stays at the render consumer, and pruning keeps every conjunct. `typed-pass-infers-exasol-types`: one rule per function, exact only after the live check of 1.1. `exact-types-follow-the-exasol-version`: the E2E checklist runs Exasol 8 and 2025. `scan-never-converts-session-formatted-text`: group C is gone.

#### [INFORMATION_COUPLING] FINDINGS
- Location: plan.md § 2.1, § 2.2
- Issue: The new `ScanWhere` variant holds a rendered half and a declined half, which is what `Pushed` and `Declined` each hold alone. Each consumer must now know that two variants route to the wrapper: the early return in `build_dispatch_sql` (`mod.rs:214`), the broadcast arm (`joins/sql_builders.rs:97-101`), and both accessors. The rule "recompute the emission over the declined half, for this variant only" lives in `build_dispatch_sql`, although `qualified_single_table_fallback_pushdown` already reads `scan_where` to place both halves. `.speq/plan-hook.md` asks to prefer widening an existing type and to give one line of reason when choosing the design with more moving parts. The plan gives none.
- Fix: In 2.1, widen `ScanWhere` into one value with two optional halves (rendered scan SQL, declined tree) and route on "a declined half is present" in `build_dispatch_sql` and the broadcast join. Or keep the variant and add one line to 2.1 that says why it beats widening. In 2.2, compute the split emission inside `qualified_single_table_fallback_pushdown` from `scan_where`, so one function owns which columns the wrapper's fan-out emits.

## Prose Quality

No em dashes in the plan artifacts. plan.md § Summary leads with the result.

#### [PROSE_UNCLEAR] FINDINGS
- Location: plan.md § Context bullet 3; decision-log.md § Interview Q3; plan.md § Verification > Manual Testing row 1
- Issue: (a) "Neither item-5 behavior of this plan and no #461 regression guard is on `main`" mixes "neither" with "and no", so it needs a second read. (b) Interview Q3 cites "`specs/testing.md` § Regression guards, line 99". Line 99 sits in § Correctness oracles, which plan.md § Context cites correctly. (c) Row 1 expects the scan spec's `"filter":"` to hold `"ID" > 1`. The filter is a JSON string inside the scan call, so its quotes appear escaped as `\"ID\" > 1`, and a literal search for `"ID" > 1` finds nothing.
- Fix: (a) Rewrite as "`main` has neither item-5 behavior of this plan nor a #461 regression guard." (b) Replace "§ Regression guards, line 99" with "§ Correctness oracles". (c) Change the expected output to "the scan spec's `"filter":"` holds `\"ID\" > 1`".

#### [PROSE_BLOAT] FINDINGS
- Location: plan.md § 2.5
- Issue: Task 2.5 is one bullet of about 180 words. It mixes the case list, two plan assertions, two special cases, three scenario-line edits, and a unit-test edit, so a reader cannot check it off in parts.
- Fix: Split 2.5 into sub-bullets: (1) the test and its case list; (2) the `EXPLAIN VIRTUAL` assertions, with the `SELECT C_VARCHAR` and `SELECT RANDOM()` special cases; (3) the `/// Scenario:` lines added to the three E2E tests and removed from `filter_in_common_arg`.
