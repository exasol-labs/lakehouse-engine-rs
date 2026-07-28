# Plan Review Findings: refactor-pushdown-collect-walk-dedup (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 3 (Blockers: 0, Advisory: 3)
- Intent Fidelity blockers: 0

Ledger correction: round-1's Summary block reported `Advisory: 5 / Total findings: 7`. The round-1
document contains **4** advisories and 2 blockers — total 6. The counts in that Summary were wrong;
the findings themselves were not. Reconcile the orchestrator's ledger to 2 BLOCKER + 4 ADVISORY for
round 1.

## Round-1 Blocker Recheck

- **Resolved: [AMBIGUOUS_REQUIREMENT] The `Json::Array` structural gate expected 0 where a correct
  implementation leaves 2.** Fixed in substance, not reworded, and not loosened into
  unfalsifiability — the point I was asked to press hardest. `plan.md` § Manual Testing row now reads
  expected `2`, "down from 4 — exactly the two arms that MUST survive untouched:
  `annotate_columns_with_alias`'s rebuild arm (`:79`, issue #257's territory) and
  `referenced_side_columns`'s `selectList` match (`:293`). `0` here means the implementer edited
  out-of-scope code". A second row carries the function-scoped check. Task 5.3 restates it per
  function and adds "goes from four occurrences to two, never to zero". Spec delta Background bullet
  16 records the surviving arms.
  Falsifiability verified by executing both gates against the current unmigrated tree: row 210
  returns `4` (≠ `2` → fails on a not-yet-done migration; would return `3` if only one collector were
  migrated), and row 211 (`grep -A 12 'fn collect_column_tables\|fn collect_side_column_names' … |
  grep -c 'Json::'`) returns `2` (≠ `0` → fails), catching the `Json::Object` arm of each collector
  at window offsets 8 and 21. Both gates therefore still fail on an incomplete migration and pass
  only on the intended end state. Logged as `decision-log.md` § Review Findings `[1] [plan-review]`.

- **Resolved: [TRACEABILITY_GAP] Two named golden anchors for `collect_all_column_names` cannot fail
  if the primitive is wrong.** `plan.md` § Scenario Coverage now names only
  `group_by_fallback_matches_golden` and `multi_count_distinct_decline_matches_golden`, states they
  are "the complete `dispatch_golden` coverage for this collector", and states the exclusion
  explicitly ("The empty-result and partial/merge grouped goldens are deliberately NOT named"). The
  spec delta's final AND now reads "the two `dispatch_golden` decline-wrapper assertions — the
  declined `GROUP BY` fallback and the multi/mixed `COUNT(DISTINCT)` decline, whose committed goldens
  both carry a narrowed inner-scan `projection`", replacing the "grouped-aggregate" phrase that
  pointed at `grouped_aggregate_matches_golden`.
  The planner's supporting claims re-verified independently rather than accepted: `base_col_types()`
  (`dispatch_golden.rs:26`) is exactly four columns — `REGION`, `NAME`, `AMOUNT`, `ID`;
  `testdata/dispatch_golden/group_by_fallback.sql` carries `"projection":["REGION","NAME"]` and
  `testdata/dispatch_golden/multi_count_distinct_decline.sql` carries `"projection":["NAME","ID"]`,
  so both are genuinely narrowed from that universe; and
  `testdata/dispatch_golden/empty_group_by_wrapper.sql` is `SELECT CAST(NULL AS VARCHAR(2000000)),
  CAST(NULL AS DECIMAL(30,4)) FROM DUAL WHERE 1=0` — no inner scan, no projection, confirming it
  could never have failed. Logged as `decision-log.md` § Review Findings `[2] [plan-review]`.

Both round-1 advisories the coordinator asked to be actioned are also genuinely resolved:

- **[COMPLETENESS_GAP]** — new `plan.md` task 3.5 extends the fixture with "one `column` object
  carrying a child object that is itself a `column` node, and assert the callback fires for BOTH",
  and names the implementation it excludes: "an implementation written as `if column { f(map) } else
  { recurse }` passes every other case in the fixture and every existing golden". Verified that the
  assertion bites: under `if/else` the outer `column` fires and the nested one never does, so
  "fires for BOTH" fails. The invariant is now pinned.
- **[AMBIGUOUS_REQUIREMENT] on the 7th/8th AND** — the scenario is down to 6 AND steps (validator
  confirms 6/5/5). The 7th AND's content sits in Background bullet 15 with its reason; the 8th AND's
  content sits in Background bullet 16 as a scope bullet. 6 matches the largest in-feature precedent
  (`One classifier decides the request shape for both the dispatch and empty-result paths`, 6 ANDs).
- **Self-correction accepted.** Grepped `plan.md` and `decision-log.md` for `AND step`, `AND-step`,
  `8 AND`, and `classifier decides`: no hits. The incorrect 8-AND claim was never in the artifacts,
  so nothing needs correcting there.

## Intent Fidelity

[no objection — axis checked: the revision touched only verification wording, one new test subtask,
and two Background bullets. Scope is unchanged: `plan.md` § Non-Goals (line 22) still excludes the
#257 rewrite walker, the `prop_parsed<T>`/`note_parsed<T>` framework, any `Visitor` trait or typed
AST, both descoped transform walks, the three `support` type-rewrite guards, folding
`resolve_s3_max_connections`, and unifying the case-folding divergence. Task 4.6 still forbids
touching `annotate_columns_with_alias`, `strip_table_alias`, and the three guards — and the revised
task 5.3 now reinforces that boundary rather than eroding it, which is the opposite of the round-1
defect. No new work item appeared.]

## Feasibility

[no objection — axis checked: no new external dependency, ordering constraint, or unstated
assumption entered with the revision. Task 3.5 adds fixture lines to a test task already scoped to
`support.rs`, so Group A's file disjointness (`adapter/mod.rs` + `adapter/connection.rs` versus
`adapter/pushdown/support.rs`) is untouched. Row 211's `-A 12` window is positional and would miss a
stray traversal arm sitting more than 12 lines below the `fn` line, but row 210's `4 → 2` count and
the diff cover that residue, so the pair is sufficient — noted, not raised. Re-verified in passing
that `cargo test -p lakehouse-engine golden_` selects exactly four tests
(`golden_broadcast_join_sql_unchanged`, `golden_n_scan_join_sql_unchanged`,
`golden_grouped_qualified_fallback_sql_unchanged`, and
`golden_ineligible_decline_message_unchanged` in `joins/mod.rs`), so "All 4 join golden assertions"
is accurate.]

## Requirement Quality

#### [PROSE_UNCLEAR] ADVISORY
- Location: `vs-adapter/pushdown-module-structure/spec.md` § Background, bullet 16
- Issue: the new bullet pins a source-line count into a document that outlives this plan:
  "`joins/rendering.rs` therefore still holds two `Json::Array` arms after this extraction". Issue
  #177's own Descoped section schedules `annotate_columns_with_alias` to move onto issue #257's
  primitive once it lands ("this issue's two transform walks reuse it if/when #257 lands"), which
  drops that count from two to one and makes the recorded Background false. `plan.md`'s equivalent
  wording is harmless because the plan is archived at record time; the spec Background is not. The
  scope statement in the bullet's first half is correct and worth keeping — only the count rots.
- Fix: In `vs-adapter/pushdown-module-structure/spec.md` § Background bullet 16, delete the clause
  "`joins/rendering.rs` therefore still holds two `Json::Array` arms after this extraction," and keep
  the rest as "The rewrite-shaped walks are untouched — `annotate_columns_with_alias`,
  `strip_table_alias`, and the three `support` type-rewrite guards keep their own recursion, none of
  which is a column-collecting traversal." Leave the `4 → 2` count in `plan.md`, where it is a
  one-shot implementation gate.

## Task Breakdown

#### [PROSE_UNCLEAR] ADVISORY (re-raised from round 1, unfixed)
- Location: `plan.md` § Implementation Tasks task 3 subtask 3 (line 131) and `decision-log.md`
  § Design Decisions [2] (line 40) — both still read "the six affected tests"
- Issue: unchanged from round 1 and re-verified against the current artifacts. "Six" contradicts the
  plan's own Verification table, which names ten resolver tests, and matches no reading of the code:
  the fourteen listed call sites sit in ten test functions, of which exactly two
  (`df_target_partitions_uses_supplied_value` at `mod.rs:1829`,
  `df_threads_per_udf_uses_supplied_value` at `:1935`) round-trip through `build_adapter_notes` and
  so actually characterize `vs-adapter/create-virtual-schema-adapter-notes-resources`. The
  implementer cannot tell how many tests to expect to touch. Honest severity is ADVISORY — the task
  is still executable from the fourteen enumerated line numbers — so it is re-raised, not escalated.
- Fix: In `plan.md` task 2.3 and `decision-log.md` [2], replace "the six affected tests" with "the
  ten affected tests", and name `df_target_partitions_uses_supplied_value` and
  `df_threads_per_udf_uses_supplied_value` as the two that characterize
  `vs-adapter/create-virtual-schema-adapter-notes-resources` via `build_adapter_notes`.

## Design Depth

[no objection — axis checked: no module, interface, or boundary changed in the revision. The
primitive's shape, `pub(super)` placement in `support`, callback signature, wrapper deletions, and
the `resolve_df_fixed_count` key parameter are all unchanged from the round-1 artifacts, where they
were certified. The two new Background bullets and the two relocated AND clauses move prose between
sections; they move no design decision. Moving the #257-separation rationale from a THEN/AND clause
into Background is the correct direction — a coordination constraint belongs in context, not in the
verifiable obligations of a scenario.]

## Prose Quality

#### [PROSE_BLOAT] ADVISORY (re-raised from round 1, unfixed)
- Location: `plan.md` § Summary (line 5), § Context (line 19 closing sentence), § Design/Context
  (line 34 closing sentence), § Verification (line 201 closing sentence); `decision-log.md` [1],
  [3], [6], [7], [9]
- Issue: unchanged from round 1 and re-verified line by line against the current artifacts. The
  Summary's opening sentence still runs 62 words against the 25-word cap. All eight instances of
  prose defending the artifact rather than stating the decision still stand verbatim: "This
  conclusion is stated so a reviewer can check the call rather than infer it from an omission"
  (`plan.md:19`); "A future caller that genuinely needs non-column nodes can widen the primitive
  then, with a real use case in hand" (`plan.md:34`, repeated in `decision-log.md` [1]); "a refactor
  whose scenarios needed new behavioral tests would not be a refactor" (`plan.md:201`); "recorded
  here so it is not read as an oversight" ([1]); "a future planner must not re-litigate it after both
  primitives land" ([3]); "Recorded because the divergence looks like an inconsistency a later reader
  would 'fix'" ([6]); "Both boundaries are recorded so a later planner does not re-open them" ([7]);
  and [9], whose entire Rationale restates `plan.md:19`. The 25-word sentence cap and the ban on
  filler and unrequested hedging are `/speq:writing-guardrails` rules, not reviewer taste — which the
  planner concedes. Severity stays ADVISORY: none of it makes a requirement non-actionable.
- Fix: Split `plan.md` line 5 into two sentences of at most 25 words each. Delete the closing
  sentence of `plan.md` lines 19, 34, and 201, and the quoted closing sentence of `decision-log.md`
  [1], [3], [6], and [7]. Reduce `decision-log.md` [9] to its Decision bullet plus a one-sentence
  Rationale, or drop the entry and keep only `plan.md` line 19's first three sentences.
