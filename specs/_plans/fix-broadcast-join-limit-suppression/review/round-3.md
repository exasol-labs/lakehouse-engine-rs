# Plan Review Findings: fix-broadcast-join-limit-suppression (round 3)

## Summary
- Axes checked: 6/6
- Total findings: 3 (Blockers: 1, Advisory: 2)
- Intent Fidelity blockers: 0

## Prior-Round Blocker Recheck

This round reviews a revision that reconciles the plan with PR #314 (issue #312). The mandate requires confirming the round-1 and round-2 resolutions are not silently reopened. All hold.

- **Resolved, intact:** R2-B1 (`[REQUIREMENT_CONFLICT]` — recorded fallback Background bullet withdrawn by prose). `pushdown-planning-join-fallback/spec.md:7-9` still carries the `<!-- DELTA:CHANGED -->` block opening "This bullet SUPERSEDES the preceding Background bullet 'Every LIMIT-carrying join reaches this wrapper, never broadcast. …'". The join delta's `DELTA:CHANGED` Background bullet (`pushdown-planning-join/spec.md:7-9`) keeps its `SUPERSEDES` pointer (round-2 A6). The revision did not edit the three spec deltas, so both markers are untouched.
- **Resolved, intact:** R2-B2 (`[REQUIREMENT_CONFLICT]` — structural pairing not delivered). Decision [12] `JoinSpec.post_join_limit` is OPERATIVE and ADR-promoting; decisions [1] and [4] stay marked SUPERSEDED; task 2 still adds the field (`spec.rs:459`), the nine `JoinSpec` literals, and the moved read site (`join_scan.rs:205` → `join.post_join_limit`); fallback delta clauses 2-3 (`:38-39`) and scan delta Background (`:8-9`) still state the type-level guarantee. The `[plan-revision]` #314 entry explicitly re-affirms decision [12] unchanged.
- **Resolved, intact:** round-1 B1/B3/B4/B5/B6 mechanisms (marker fixes, wrapper-guard `debug_assert!` + `None` fallback, classify-before-render ordering at `joins/mod.rs:189`, the two ordering-arm tests, the tasks-1/2 merge). None was reopened; the revision touched only `#314`-affected prose and two operative line numbers.

## Reconciliation Fidelity (primary focus)

Verified against the current tree. The code-surface reconciliation is accurate:

- `unbounded_result_sets()` is gone; `capped_result_sets(max_rows)` is at `exasol_ws.rs:159`; the harness default is uncapped (`result_set_max_rows: 0`, `exasol_ws.rs:98`). No plan text still relies on a "default capped connection" — the two surviving `unbounded_result_sets` mentions (plan.md:102, :127) both correctly state it was removed by #314.
- The deletion of `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan` is captured in Task 5 (`:169-201` test+doc, `:165-167` orphaned header) and § Dead Code Removal (`:165-201`), consistent with the real file (header `:165-167`, doc `:169-177`, test `:178-201`). The user's DELETE decision is honored, not an invert.
- The `capped_result_sets` doc-comment rewrite (`exasol_ws.rs:144-158`) is assigned to Task 6; the `e2e_join_test.rs:107-116` comment rewrite to Task 5. Both line ranges are correct on the tree.
- Both claimed `src/` line-number fixes are correct: `join_scan.rs:146` is the join binding (`:131` is a doc-comment line); `scan/mod.rs:264` is the join-routing predicate (`:263` is blank). Spot-checks beyond them all match exactly — the nine `JoinSpec` literals, the nine `join_requires_exasol_postprocessing` call sites in `sql_builders_tests.rs` (task 3's enumeration), `support.rs:96` (fifth parameter of `build_scan_driving_sql`), `scan_join_test.rs:211`/`:216` (`join_spec` helper + limit param), `test_support_tests.rs:86`, `raw_scan.rs:411`, `spec.rs:459`/`:678-680`.

The gap is in references the reconciliation did **not** scope: #314 also planted the reversed claim (and the symbol this plan deletes) into the **recorded** spec library and a second test file. The `[plan-revision]` re-validation covered references *inside* plan.md/decision-log.md/the three deltas only. See R3-B1 and A1.

## Intent Fidelity

[no objection — axis checked: the revision faithfully operationalizes the round-3 ask. The user directed DELETE (not invert) of `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan`; Task 5 and § Dead Code Removal delete it and rely on the already-present positive replacement `e2e_broadcast_join_bare_limit_stays_broadcast_and_truncates`. No problem is reinterpreted, no scope is added or dropped beyond #314 reconciliation. INTENT blockers: 0.]

## Feasibility

#### [EFFORT_MISESTIMATION] ADVISORY (A2)
- Location: decision-log.md § "[plan-revision] Reconciled with PR #314" (the full-re-validation paragraph) and plan.md § Implementation Tasks task 2
- Issue: the re-validation asserts "`sql_builders.rs:665-723` (Serena confirms the 59-line span including the doc comment)" for `build_broadcast_join_sql`, and task 2 calls it "one 59-line function (`joins/sql_builders.rs:665-723`)". On the current tree the function's doc comment starts at `:666`, the `fn` signature is at `:681`, and the body ends at `:727` (the next function `qualified_join_group_by` is at `:729`); `:665` is the blank line after the previous function and `:723` falls mid-body. The span is neither 59 lines nor bounded at 665-723. This is non-operative — task 2 mandates Serena `find_symbol`/`replace_symbol_body`, which resolves by name, and the operative edit anchor (the `JoinSpec` literal at `:693`) is exactly correct — but the re-validation's specific line claim is inaccurate, contradicting its "everything else clean" conclusion.
- Fix: In decision-log.md § "[plan-revision] Reconciled with PR #314" and plan.md task 2, correct the `build_broadcast_join_sql` reference to its actual bounds (doc comment `:666`, `fn` at `:681`, body end `:727`), or drop the exact line span and name the symbol alone, since task 2 navigates it by Serena.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER (R3-B1)
- Location: recorded `specs/e2e-harness/e2e-harness/spec.md:29-33`, recorded `specs/e2e-harness/lakekeeper-e2e-harness/spec.md:57`, and recorded `specs/vs-adapter/pushdown-planning-selectlist-expressions/spec.md:108`; plan.md § Features (three deltas, no e2e-harness delta) and § Dead Code Removal (deletes `join_requires_exasol_postprocessing`)
- Issue: this plan reverses "any pushed `LIMIT` disqualifies broadcast" and deletes the function `join_requires_exasol_postprocessing`, but three live recorded specs still assert that reversed claim and/or name the deleted symbol, and the reconciliation added no delta for any of them. (a) `e2e-harness/e2e-harness/spec.md:29-33` (Background prose) states "a broadcast-eligible inner equi-join disqualifies broadcast pushdown under ANY pushed limit (`join_requires_exasol_postprocessing`), so a capped connection silently moves a join test onto the unaccelerated two-scan fallback". (b) `lakekeeper-e2e-harness/spec.md:57` (Background prose) states "For a join, ANY pushed `limit` disqualifies broadcast pushdown via `join_requires_exasol_postprocessing` and falls back to the unaccelerated two-scan (`LHS_T0`/`LHS_T1`) wrapper". Both the claim and the symbol are falsified once a bare `LIMIT` stays broadcast. After recording, the library carries `vs-adapter/pushdown-planning-join` ("a bare `LIMIT` … served by the broadcast path") next to these two contradicting statements, with no `DELTA:CHANGED` telling a future reader which wins — the exact defect class rounds 1 (B1) and 2 (R2-B1) blocked on, now at locations #314 introduced. (c) `pushdown-planning-selectlist-expressions/spec.md:108` (recorded SHALL clause) names "`join_requires_exasol_postprocessing` returning true" as the mechanism routing an aggregate to the fallback; the behavior it describes survives this plan, but the named symbol will not exist, leaving a recorded spec citing a deleted function. (The `_recorded/012-…` and `_decision/001-…` hits are frozen archives and are correctly left alone, matching this log's own convention.)
- Fix: Add spec deltas under `specs/_plans/fix-broadcast-join-limit-suppression/e2e-harness/e2e-harness/spec.md` and `.../e2e-harness/lakekeeper-e2e-harness/spec.md`, each with a `<!-- DELTA:CHANGED -->` Background bullet in the repo's explicit-pointer form — open with `This bullet SUPERSEDES the preceding Background bullet "<quoted opening>"` (as round-2 A6 established) — that keeps the true half (a declared cap still reaches the adapter as a pushdown `limit`, and the harness stays uncapped so plan-shape tests are not silently perturbed) and drops the "any limit disqualifies broadcast / `join_requires_exasol_postprocessing`" claim, replacing it with the post-#307 reality (a bare `LIMIT` and a bare-projected-column `ORDER BY` now stay broadcast; only the four surviving forcing conditions, offset-without-order, and unrenderable/unprojected sort keys force the fallback, per `vs-adapter/pushdown-planning-join`). For `pushdown-planning-selectlist-expressions/spec.md:108`, add a `DELTA:CHANGED` copy of that scenario whose clause names the replacement (`classify_join_window` returning `ExasolPostProcessed`) instead of the deleted `join_requires_exasol_postprocessing`, behavior unchanged. Add the two new features to plan.md § Features. If the user instead elects to defer the e2e-harness spec cleanup to a follow-up, record that as an explicit decision-log entry citing an issue — a silent gap is not acceptable for a class blocked twice already.

#### [COMPLETENESS_GAP] ADVISORY (A1)
- Location: `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs:94-102` (doc comment on `declared_cap_truncates_returned_row_count`, added by #314); plan.md § Implementation Tasks tasks 5 and 6, § Dead Code Removal
- Issue: this doc comment names the deleted symbol `join_requires_exasol_postprocessing` (`:95`) and cross-references the deleted test `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan` (`:100-102`) as proof of "the plan-shape consequence any pushed limit carries" — the reversed claim. The plan rewrites the analogous comment at `e2e_join_test.rs:107-116` but names no task for `e2e_harness_row_cap_test.rs`. After Task 5 deletes the test, this comment names a nonexistent test and a deleted symbol and states behavior this plan reverses. It does not break compilation, so it is advisory, but it is the same reconciliation defect the plan handles for its twin comment.
- Fix: In plan.md Task 5 (or Task 6's doc-comment list), add `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs`'s `:94-102` doc comment: drop the `join_requires_exasol_postprocessing` citation and the `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan` cross-reference, and restate the plan-shape note per post-#307 behavior (a bare `LIMIT` stays broadcast; only the surviving forcing conditions fall back).

## Task Breakdown

[no objection — axis checked: the round-2 same-file serialization notes (tasks 1-and-2 sequential; the Group C caveat) are intact, and the reconciliation added no task that violates its parallelization group. The R3-B1 fix adds `DELTA:CHANGED` bullets that /speq:record merges with no new implementing task, because tasks 1-6 already delete the symbol and implement the reversed behavior.]

## Design Depth

[no objection — axis checked: the revision changed no module, interface, or boundary. Decision [12]'s `JoinSpec.post_join_limit` type-level home of the post-join cap — round-2's certified design — is unchanged, and the classifier/builder two-site-one-outcome split is untouched.]

## Prose Quality

[no objection — axis checked: the reconciliation prose in § Context, § Impact, Task 5, Task 6, and the `[plan-revision]` decision-log entry leads with its conclusion and names its actor. The `[plan-revision]` #314 entry is long but is decision-log Finding prose that front-loads outcome ("two operative drifts fixed, everything else clean") before enumerating. Round 1's three prose advisories (§ Summary sentence length, the multi-site restatement of the pre-join/post-join asymmetry, the duplicated OFFSET counterexample) stand unactioned by design and are not reopened.]
