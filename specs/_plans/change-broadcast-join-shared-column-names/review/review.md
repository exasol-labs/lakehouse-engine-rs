# Plan Review Findings: change-broadcast-join-shared-column-names

## Summary
- Axes checked: 6/6
- Total findings: 8 (Blockers: 1, Findings: 7)
- Intent Fidelity blockers: 0

## Round 1 Findings

All 14 findings of `review/review-1.md` are resolved. None is re-raised. Three of the fixes introduced new defects, which are raised below as new findings.

- `[HIDDEN_DEPENDENCY]` BLOCKER: resolved. Task 3.4 keeps the N-scan half of the binary-filter test on a `.with_join_broadcast_max_bytes("1")` schema. Task 2.3 deletes the `SHARED_COL` block and names both `testdata/dispatch_golden/*_broadcast_join.sql` files. The Dead Code Removal "Comment" row lists `scan.rs:379`, `:389`, and `direct_storage.rs:852`. New defect in the `colliding_columns_…` instruction: see Feasibility.
- `[UNSTATED_ASSUMPTION]` (byte tie): resolved in the NEW scenario step and Decision [2]. New defect in the test meant to prove it: see the Feasibility BLOCKER.
- `[EFFORT_MISESTIMATION]`: resolved. Task 2.2 parses the window keys from the `JoinLegs::qualify` output and detects `NoLeg` after `qualify`. Task 1.3 adds the four scan test files. The four adapter test files the round-1 finding named (`adapter_tests.rs`, `pushdown_tests.rs`, `joins_tests.rs`, `planning_tests.rs`) hold only Exasol request JSON (`"join_type": "inner"`) and no `JoinSpec` literal, as the interview answer states.
- `[UNSTATED_ASSUMPTION]` (Delta protocol): resolved by the Context bullet that quotes Reader Requirements for Column Mapping.
- `[UNSTATED_ASSUMPTION]` (Decision [2] overclaim): resolved.
- `[REQUIREMENT_CONFLICT]` BLOCKER: resolved by three new deltas. A grep of the recorded library for "disjoint", "overlapping column", "bare-name", and "self-join" finds no other live scenario that still states the guard.
- `[AMBIGUOUS_REQUIREMENT]`: resolved for the three named scenarios. The same "two-table" wording remains in the fallback scenario this plan now changes: see Requirement Quality.
- `[COMPLETENESS_GAP]` (ORDER BY on the other leg): resolved by the scenario step and `order_by_on_other_legs_same_named_column_declines_broadcast`.
- `[TRACEABILITY_GAP]` (three findings): resolved. Task 3.2 runs the self-join tests on both schemas, task 3.1 adds the `score` column with queries (d1) to (d5), and tasks 3.1 and 3.2 add the tags.
- `[INFORMATION_COUPLING]`: resolved by `relation_alias` in `scan/spec.rs` and by dropping the empty-projection default. New fixture consequence: see Feasibility.
- `[PROSE_UNCLEAR]`, `[PROSE_BLOAT]`: resolved.

## Why This Plan May Fail

- The self-join test that should prove leg pruning picks the dimension cannot pass as written. Task 3.2 says its one-sided filter "sits on the second occurrence", but the existing test filters `a.O_ORDERKEY`, the first occurrence. Routed to `[UNSTATED_ASSUMPTION]` BLOCKER.
- Dropping the scan's empty-projection default breaks fixtures that rely on it. `join_scan_tests.rs::post_join_spec` builds on `minimal_spec()`, whose projection is empty. Routed to `[EFFORT_MISESTIMATION]`.
- Queries (d4) and (d5) compare a `LIMIT 3` result with fixed expected rows, but nothing requires the new `LABELS.score` values to be distinct. A tie at the third row makes the result nondeterministic. Routed to `[UNSTATED_ASSUMPTION]`.

## Intent Fidelity

[no objection — axis checked: every interview answer maps to a plan element. "Broadcast self-joins too" maps to the NEW scenario's self-join GIVEN, query (b), and task 3.2's `VS_NAME` runs. "`<alias>_<column>`" applies to output names only (task 1.1 output-name rule, task 1.2 `AS "<alias>_<column>"`). "tuple with the alias name and the column name" maps to the qualified reference and the (relation alias, column) universe in tasks 1.1, 1.2, and 2.1. "F/D is not fine" maps to Decision [2] and `relation_alias`. The `NoLeg` answer maps to the NEW scenario step and the `unattributable_decline` reuse in task 2.2. "any column comparison check should be qualified" maps to the NEW scenario step, the inventories in tasks 1.2, 2.1, and 2.2, and the five comparison-class unit tests. "we should aim at having test that excercise edge cases" maps to § Verification and queries (d1) to (d5); (d4) and (d5) serve that answer and are not scope creep. Broadcast over more than two legs stays in Out of scope, as agreed.]

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks 3.2
- Issue: Task 3.2 states: "The one-sided filter sits on the second occurrence and prunes at least one of that leg's files, so on `VS_NAME` that test also asserts that the join block's dimension alias is `LHS_T1`." The existing test `e2e_self_join_with_one_sided_filter_matches_single_node` (`crates/lakehouse-e2e/tests/e2e_local/join.rs:1632`) filters the first occurrence: `WHERE a.O_ORDERKEY <= {threshold}`, with `threshold = DIM_CUSTOMER_ROWS` (5). `fact_orders` holds keys 1 to 5 and 6 to 10 in two files (`crates/lakehouse-e2e/tests/common/seed.rs:866-874`), so the filter prunes the second file from leg 0 only. Leg 0 then has fewer bytes and becomes the dimension, `LHS_T0`. The `LHS_T1` assertion fails, so the E2E checklist step cannot pass. Changing the assertion to `LHS_T0` would pass, but leg 0 also wins the byte tie-break, so the test could not tell pruning from the tie-break. The user's correction ("the planning my yield differrent set of parquet files to scan per leg") would then have no test that proves it.
- Fix: In plan.md task 3.2, replace "The one-sided filter sits on the second occurrence and prunes at least one of that leg's files" with "Move the one-sided filter to the second occurrence, `WHERE b.O_ORDERKEY <= {threshold}`, which prunes `fact_orders`' second file from leg 1 only, and derive the expected rows by filtering `b`'s order keys". Keep the assertion that the join block's dimension alias is `LHS_T1` on `VS_NAME`.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Implementation Tasks 2.3, the `colliding_columns_render_qualified_unified_wrapper_without_error` sentence
- Issue: Task 2.3 says to "replace the `render_broadcast_join(...).is_none()` assertion with the reason the test now relies on: a broadcast threshold below the smaller side, so the request takes the N-scan wrapper". The test (`crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs:421`) calls `render_broadcast_join` and `build_n_scan_join_sql` directly. Neither function takes a threshold. `select_broadcast_sides` applies it inside `plan_join`, which this test never calls. No assertion in this test can state that reason, so the instruction cannot be executed as written.
- Fix: In task 2.3, replace that sentence with: "In `colliding_columns_render_qualified_unified_wrapper_without_error`, delete the `disjoint_schema_guard` assertion and the `render_broadcast_join(...).is_none()` assertion, and keep its `build_n_scan_join_sql` assertions, which are the test's purpose."

#### [EFFORT_MISESTIMATION] FINDINGS
- Location: plan.md § Implementation Tasks 1.2 and 1.3
- Issue: Task 1.2 makes `build_join_sql` fail a join spec whose projection is empty. `crates/lakehouse-engine/src/scan/join_scan_tests.rs::post_join_spec` builds on `minimal_spec()` (`scan/test_support_tests.rs:97`), whose projection is the default empty list. Every test that calls `post_join_spec` (6 call sites) therefore relies on the default that task 1.2 removes. Task 1.3 says to "move" these fixtures "to qualified projection items", which reads as rewriting items that already exist. The same tests also read result columns, whose names become `<alias>_<column>`.
- Fix: In task 1.3, state that `post_join_spec` gains an explicit qualified projection over both relations, because it carries none today, and that each `join_scan_tests.rs` test that reads a result column by name reads its `<alias>_<column>` output name.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Implementation Tasks 3.1, queries (d4) and (d5)
- Issue: (d4) `ORDER BY l.SCORE DESC LIMIT 3` and (d5) `ORDER BY l.SCORE LIMIT 3` compare their rows with seed-derived rows. Task 3.1 requires the new `LABELS.score` values to "differ from every `EVENTS.score`" and to "sort differently from them", but not to differ from each other. If two rows tie at the third position, the selected `e.SCORE` value depends on shard and merge order, and the comparison can fail with no defect present.
- Fix: In task 3.1, require the 20 `LABELS.score` values to be distinct, for example `format!("score-{:02}", 21 - id)`, which sorts in the opposite order to `EVENTS.score` (`5.0 * id`).

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] FINDINGS
- Location: `pushdown-joins/pushdown-planning-join-fallback/spec.md` § Scenario "A join outside the broadcast contract is declined safely" (WHEN step, first and second AND steps), and the same delta's unmarked Feature description
- Issue: The CHANGED scenario keeps "the join is not a single broadcast-eligible two-table inner equi-join, …, spans more than two involved tables", "(N ≥ 2 involved tables, the two-table case being N = 2)", and "spanning more than two involved tables … SHALL by itself NEVER be a reason to return an error". The description keeps "outside the two-table broadcast contract". A self-join that meets the broadcast contract has one involved table and two legs. Read literally, it "is not a … two-table inner equi-join", so the THEN step "SHALL NOT emit a broadcast plan" applies to it, which contradicts the NEW scenario "Legs that share column names are planned as a broadcast fan-out". A three-leg self-join spans one involved table, so "spans more than two involved tables" does not name it, although `plan_join` checks the leg count (`join.tables.len() == 2`). Round 1 fixed the same wording in `pushdown-planning-join`. This plan's CHANGED copy of the fallback scenario keeps it.
- Fix: In that CHANGED scenario, replace "two-table inner equi-join" with "two-leg inner equi-join", replace both occurrences of "more than two involved tables" with "more than two legs", and replace "(N ≥ 2 involved tables, the two-table case being N = 2)" with "(N ≥ 2 legs, the two-leg case being N = 2)". Mark the Feature description `DELTA:CHANGED` and replace "the two-table broadcast contract" with "the two-leg broadcast contract".

[no objection — axis checked for the remaining deltas: a diff of every CHANGED block against its recorded text shows only the intended edits, and the REMOVED self-join scenario is a verbatim copy whose title no test carries. On the planner's question: the LIKE (12 bullets) and string-function (15 bullets) Backgrounds exceed 5 bullets, but the length does not matter for this plan. The plan changes one bullet in each. Each changed bullet restates a THEN step of the CHANGED scenario in the same delta (the `extract_join_projection` universe "keyed by (relation alias, column) in leg order"), so it is redundant, not `[IMPLEMENTATION_DETAIL]`. The other 11 and 14 bullets are recorded LIKE and string-function facts that this plan does not introduce. Trimming them would add unrelated work to a join plan, against `.speq/plan-hook.md` § Minimal plans. The other CHANGED Backgrounds (`pushdown-planning-join`, `pushdown-planning-join-fallback-self-join`, `pushdown-planning-join-filter-type-coercion`) stay at 5 bullets.]

## Task Breakdown

#### [TRACEABILITY_GAP] FINDINGS
- Location: plan.md § Scenario Coverage (string-function row); § Implementation Tasks
- Issue: § Scenario Coverage maps "A non-coercible resolvable column type in a select-list string function falls back to the full base row" to `support_tests.rs::selectlist_string_fn_over_double_falls_back_to_full_row`. That test carries no `/// Scenario:` line (`crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs:3384`), and no task adds one. AGENTS.md: "A test implementing a spec scenario carries one `/// Scenario: <title>` line per scenario".
- Fix: In task 2.3, add: "Add `/// Scenario: A non-coercible resolvable column type in a select-list string function falls back to the full base row` to `support_tests.rs::selectlist_string_fn_over_double_falls_back_to_full_row`."

#### [TRACEABILITY_GAP] FINDINGS
- Location: plan.md § Dead Code Removal, "Comment" row; § Implementation Tasks 3.1
- Issue: `crates/lakehouse-e2e/tests/common/e2e_harness.rs:605` documents `has_two_scan_wrapper` and `has_n_scan_wrapper` with "The `LHS_T*` aliases appear only in the generated N-scan wrapper, never in a native retry or the broadcast path." After this plan the broadcast pushdown SQL carries `LHS_T0` and `LHS_T1` in the scan spec's condition, projection items, join block, and `EMITS` names. The helper stays correct, because it matches the subquery alias form `AS "LHS_T{i}"`, which the broadcast SQL never emits. Query (d5) and the `VS_NAME_LOW` runs of task 3.2 rely on that distinction, but the comment states a reason that is no longer true. AGENTS.md: "Update or delete comments when behavior changes."
- Fix: Add the `e2e_harness.rs` comment on `has_two_scan_wrapper` to the Dead Code Removal "Comment" row. In task 3.1, rewrite it to: "Only the N-scan wrapper emits the subquery alias form `AS "LHS_T{i}"`. The broadcast path names `LHS_T*` relations inside its scan spec, never as a subquery alias."

Cluster coherence: [no objection — axis checked: groups A, B, and C still run in sequence. Group A shares the `scan-execution-join` delta and `scan/` modules. Group B shares the seven adapter deltas and `adapter/pushdown/`. Task 1.1's mechanical adapter edits are declared and ordered before group B. Task 3.4 joins group C, which already owns `e2e_local/scan.rs`.]

## Design Depth

[no objection — axis checked: the alias format and the output-name rule now share one owner, `scan/spec.rs`, and `JoinLegs::leg_alias` calls `relation_alias`, so the injectivity invariant has one home. The scan reads both aliases from the join block and formats none. ADR `raw-scan-projection-gets-an-explicit-projectionitem-tag-instead-of-a-syntactic-heuristic` requires that identifiers and rendered SQL crossing the adapter-to-scan boundary be tagged separately and that a column stay a bare JSON string. The qualified reference keeps the relation and the name in separate fields, and an unqualified one stays a bare string, so the plan conforms. ADRs `route-non-emittable-selectlist-to-qualified-wrapper` (positional `04000` check), `join-shard-ranks-by-emitted-value`, `join-post-limit-lives-in-join-block`, and `broadcast-join-builds-one-hash-table` are not contradicted. The consequence bullet of `per-side-predicate-pushdown-for-joins-by-tablename-conjunct-attribution` ("the broadcast path keeps the native alias") describes the state before the recorded alias stripping and is not that ADR's Decision. Both Design Decisions entries say `Promotes to ADR: no`, so `[ADR_OVERPROMOTION]` does not apply. `specs/architecture.md` lists no `JoinSpec` fields, and no component, boundary, or dependency changes, so "Architecture: no change" holds.]

## Prose Quality

#### [PROSE_BLOAT] FINDINGS
- Location: plan.md § Implementation Tasks 3.1
- Issue: "Use a new two-column table instead if an existing test reads all of `LABELS`' columns." leaves the implementer a check that the plan can settle now. No test reads all of `LABELS`' columns. `e2e_pushdown_scans_table_from_involved_tables` selects `id, label`, and `e2e_pushdown_resolves_files_once_multi_table` selects `a.id, b.label` (`crates/lakehouse-e2e/tests/e2e_local/scan.rs`). `seed_table` also drops and recreates a persisted table whose schema changed (`seed.rs::empty_table_to_fill`), so an existing Docker warehouse picks up the new column.
- Fix: In task 3.1, delete that sentence.

[no objection — axis checked for the rest of plan.md and decision-log.md: the Summary has one claim per sentence, Impact bullet 2 describes the projection objects correctly, the Context interview pointer is one sentence, and no artifact uses an em dash. `/speq:writing-guardrails` exempts Gherkin and Background bullets. The Interview section quotes the user verbatim and is coordinator-owned.]
