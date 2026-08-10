# Plan Review Findings: fix-broadcast-join-limit-suppression (round 4)

## Summary
- Axes checked: 6/6
- Total findings: 0 (Blockers: 0, Advisory: 0)
- Intent Fidelity blockers: 0

This is the final confirming round of the #314-reconciliation cycle. The round-3 BLOCKER
(R3-B1) and both advisories (A1, A2) are resolved. No new finding survives verification.
The plan is approvable.

## Round-3 Blocker Recheck

- **Resolved: [REQUIREMENT_CONFLICT] R3-B1** (recorded specs assert the reversed claim / name the
  deleted `join_requires_exasol_postprocessing`). Three delta files were added and all target the
  correct recorded content:
  - **`e2e-harness/e2e-harness/spec.md`** — one `<!-- DELTA:CHANGED -->` Background bullet opening
    `This bullet SUPERSEDES the preceding Background bullet "The harness sends Exasol's own`
    `` `resultSetMaxRows` default (`0`, no limit) unless a call site declares a cap. …" ``. The
    quoted opening is an exact prefix of the sole recorded bullet at `specs/e2e-harness/e2e-harness/spec.md:24-34`
    (the false "disqualifies broadcast pushdown under ANY pushed limit (`join_requires_exasol_postprocessing`)"
    claim sits mid-bullet at :30-31, so replacing the whole bullet removes it). The replacement keeps
    the true half (uncapped default; a declared cap reaches the adapter as a pushdown `limit`) and
    states the post-#307 reality.
  - **`e2e-harness/lakekeeper-e2e-harness/spec.md`** — one `<!-- DELTA:CHANGED -->` Background bullet
    whose SUPERSEDES quote is an exact prefix of the sole recorded bullet at
    `specs/e2e-harness/lakekeeper-e2e-harness/spec.md:61` (the false "For a join, ANY pushed `limit`
    disqualifies broadcast pushdown via `join_requires_exasol_postprocessing`" claim sits mid-bullet,
    removed by the whole-bullet replace).
  - **`vs-adapter/pushdown-planning-selectlist-expressions/spec.md`** — one `<!-- DELTA:CHANGED -->`
    scenario "A widened derived projection routes to a native wrapper on every path", matched to the
    recorded scenario by heading. Byte-compared line-for-line against recorded `spec.md:98-109`: every
    clause is identical except the single :108 clause, which swaps `` `join_requires_exasol_postprocessing`
    returning true `` → `` `classify_join_window` returning `ExasolPostProcessed` ``. Behavior unchanged.
  - **No fourth live location.** `grep` across `specs/` (excluding `_plans/`, `_recorded/`, `_decision/`)
    for `join_requires_exasol_postprocessing`, `disqualif`, and `any pushed` returns exactly the three
    bullets/clause above and nothing else — so no un-superseded contradiction survives a hypothetical
    record. The frozen `_recorded/` and `_decision/` archives are correctly left alone.
  - **Structure valid.** All three deltas carry `# Feature:`, a description line, the correct headers,
    paired open/close markers, and the "reproduced verbatim and UNMARKED as required structural context"
    note over the unmarked context (matching recorded delta 012's pattern). `speq plan validate
    fix-broadcast-join-limit-suppression` passes, validating all 6 deltas; the only warnings are AND-step
    counts inherited verbatim from the faithfully-copied recorded scenarios (pre-existing, not introduced).
    The three features are added to plan.md § Features pointing at real delta paths.
- **Resolved: [COMPLETENESS_GAP] A1** (twin doc comment names the deleted symbol and deleted test).
  plan.md Task 6 (`plan.md:133`) and § Dead Code Removal (`plan.md:158`) now both cover
  `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs:94-102`, instructing to drop the
  `join_requires_exasol_postprocessing` citation and the `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan`
  cross-reference and restate the plan-shape note per post-#307 behavior. Verified against the current
  file: the deleted symbol is at :95 and the deleted-test cross-reference at :100-102, exactly as cited.
- **Resolved: [EFFORT_MISESTIMATION] A2** (inaccurate `59-line` / `:665-723` span). The span and the
  "59-line" descriptor are dropped from both operative locations — plan.md task 2 (`plan.md:120`) and the
  `[plan-revision]` #314 entry (`decision-log.md:226`) — which now name the symbol alone and note it is
  navigated by Serena. The verified-correct `JoinSpec` literal anchor at `:693` is retained. The only
  surviving `665`/`59-line` strings are the round-1 HISTORY finding (`decision-log.md:177`, correctly
  frozen per the log's as-of-review convention) and the A2 finding-record itself (`:245-246`).

**Prior resolutions confirmed intact (not reopened).** R2-B1: the `<!-- DELTA:CHANGED -->` +
`SUPERSEDES` Background bullets on the join delta (`pushdown-planning-join/spec.md:7-9`) and the fallback
delta (`pushdown-planning-join-fallback/spec.md:7-9`) are untouched. R2-B2 / decision [12]:
`JoinSpec.post_join_limit` is present and unchanged in all three original deltas — join (scenario clauses
`:50-51`, `:64`), fallback (`:38`), and scan-execution-join (Background `:8-14`, scenario `:22-27`).

## Intent Fidelity

[no objection — axis checked: the fix operationalizes the round-3 ask and the interview decision faithfully.
The user directed a full re-review and DELETE (not invert) of `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan`;
the plan deletes it (task 5, § Dead Code Removal `:159`) and the R3-B1 reconciliation deltas are the
recorded-library cleanup a full re-review requires (a recorded spec citing a deleted symbol is a hard
inconsistency at record time). No problem is reinterpreted; no scope added or dropped. INTENT blockers: 0.]

## Feasibility

[no objection — axis checked: the three new deltas need no code beyond tasks 1-6, add no dependency,
assumption, or NFR. The one feasibility risk in a reconciliation delta — that the recorder appends
instead of replaces — is closed: each SUPERSEDES quote is a unique exact prefix of its recorded bullet
(grep-confirmed uniqueness), and the selectlist scenario matches by heading, per the recorder behavior
the 005 precedent established and rounds 1-3 merged successfully.]

## Requirement Quality

[no objection — axis checked: this is where R3-B1 lived and it is resolved. The three deltas are internally
consistent with the three original deltas — all state the same surviving forcing set (four postprocessing
conditions + a `limit` offset with no `orderBy` + an unrenderable/unprojected sort key), matching
`pushdown-planning-join/spec.md:8` and its decline scenario (`:75-83`) and the fallback delta's enumerated
set (`:12`). The selectlist copy is byte-faithful but for the mandated symbol swap. No `REQUIREMENT_CONFLICT`
survives a hypothetical record; grep confirms no fourth un-superseded location.]

## Task Breakdown

[no objection — axis checked: the three reconciliation deltas carry no implementing task, and that is
correct — they rename one symbol in a spec clause and reconcile harness Background prose for behavior
tasks 1-6 already deliver (task 1 renames `join_requires_exasol_postprocessing` → `classify_join_window`;
the aggregate-over-join fallback the selectlist clause describes is covered by `aggregate_over_join_*`
tests in tasks 1 and 3). Not a `TRACEABILITY_GAP`: documentation reconciliation for already-planned code.
The round-2 serialization notes (tasks 1-and-2 sequential; task 4 after task 2) are untouched.]

## Design Depth

[no objection — axis checked: the fix changed no module, interface, or boundary. Decision [12]'s
`JoinSpec.post_join_limit` type-level home of the post-join cap and the classifier/builder two-site-one-outcome
split are unchanged.]

## Prose Quality

[no objection — axis checked: the plan.md § Features paragraph (`:99`) leads with its conclusion, is terse,
and accurately states "each carries one `DELTA:CHANGED` block" (verified: one per delta). The new delta
Background bullets and Gherkin clauses are writing-guardrails-exempt (Background bullets / scenarios). The
decision-log R3-B1/A1/A2 entries are Finding prose that front-loads the finding before the direction change,
consistent with the log's established style. One non-operative descriptor: Task 6's "keep the true
delivered-row-count half (`:88-93`, unchanged)" is loose — the sentence at file :92-95 straddles the true
framing and the false disqualification claim — but the task is a full doc-comment rewrite whose load-bearing
instructions (drop the :95 symbol and :100-102 cross-reference, restate per post-#307) are pinpointed
correctly, so no rewrite risk remains. Not raised as a finding.]
