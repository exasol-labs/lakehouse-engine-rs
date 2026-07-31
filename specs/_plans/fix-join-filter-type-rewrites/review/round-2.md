# Plan Review Findings: fix-join-filter-type-rewrites (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 3 (Blockers: 0, Advisory: 3)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

All four round-1 BLOCKERs are resolved. Each was verified against the revised artifacts and the
underlying source, not against the `[plan-review]` decision-log claim.

- **Resolved: [COMPLETENESS_GAP] `type_screened_leg_filter` screened the wrong tree for
  renderability.** Task 2.2 now reads "Partition `side_local`'s conjuncts on BOTH conditions applied
  to the REWRITTEN conjunct — `apply_type_rewrites(c, col_types)` is `Some(rw)` AND
  `datafusion_renderable(&rw)`" and "Fail closed in BOTH directions". Task 2.1 gains the failing case
  "a conjunct whose REWRITE the type pipeline ACCEPTS but the DataFusion dialect CANNOT render lands
  in the DECLINED half in RAW form (never dropped from both halves)", mapped in § Scenario Coverage as
  `type_screened_leg_filter_declines_type_accepted_but_unrenderable_rewrite`. The invariant is
  restated normatively, with an explicit "SHALL become residual / MUST NOT be omitted from both"
  clause, in all three places demanded: `pushdown-planning-join-fallback` (Background bullet 2 plus
  scenario clauses at lines 67, 72, 78), `pushdown-declined-filter-self-apply` scenario 2 (lines 50,
  53, 54), and the new `pushdown-planning-join-filter-type-coercion` feature (Background bullet at
  lines 58-64 plus scenario clauses 108, 109). plan.md § Patterns gains the "Screen the REWRITTEN
  tree, not just the raw one" row. Verified against source: the mirrored arm exists verbatim at
  `support.rs:1092` (`(Some(raw), Some(tree)) if !datafusion_renderable(tree) => (None, Some(raw))`);
  the drop site is confirmed at `sql_builders.rs:589-591`; `datafusion_renderable` is already imported
  in `joins/rendering.rs:7-9`, and `apply_type_rewrites` (`support.rs:1074`, `pub(super)`) is visible
  there, so the fix needs no new plumbing.
- **Resolved: [REQUIREMENT_CONFLICT] the Design § Context coverage claim.** plan.md now reads "are
  the only JOIN WHERE-filter surfaces with no column-type awareness" (line 21-22) and a following
  paragraph states "This plan does NOT make type-rewrite coverage complete. Three pushed expression
  surfaces stay unwired and keep their exposure: the grouped-aggregate render path, the
  aggregate-argument render path, and #223 slice 3's GROUP-BY-only DECIMAL keys". That matches
  `pushdown-planning-string-fn-type-coercion`'s own out-of-scope bullet and § Non-Goals; no artifact
  now implies coverage is complete. The paragraph split at the "The two JOIN WHERE-filter sites"
  boundary also landed.
- **Resolved: [TRACEABILITY_GAP] #223 slice 2 had no live verification — option (a) taken, and it
  works.** Tasks 3.5 (seed one scale ≥ 2 DECIMAL column on `fact_orders`) and 3.6 (live E2E
  `e2e_join_decimal_stringification_matches_native_at_both_surfaces`) are real tasks, with the
  Integration row in § Scenario Coverage, the § Manual Testing row, the Group C entry, the 3.5 → 3.6
  ordering, and § [10] revised to "FIVE Docker-Exasol E2E tests are mandatory deliverables". The
  fixture change is executable and the test is genuinely observable — all four load-bearing facts
  check out: `LENGTH` is a governed decimal stringifier (`support.rs:825`, `fn_name == "CONCAT" ||
  fn_name == "LENGTH"`), `FN_LENGTH` is an advertised capability (`capabilities.rs:99`) so Exasol
  really delegates the predicate, the `fact_orders` Iceberg schema builder and `make_orders_batch`
  exist at `tests/common/seed.rs:1014-1019` and `1057-1076`, and the same file already carries a
  `Decimal128Array::from(...).with_precision_and_scale(p, s)` template (lines 2731-2750). The
  additivity claim also holds: `seed_star_schema` runs for the whole E2E suite via `seed_events`
  (`seed.rs:130`), but the only cross-file `fact_orders` consumers name columns explicitly
  (`e2e_capability_test.rs:125`) or use `COUNT(*)` (`e2e_scan_test.rs:1459`) — no `SELECT *`, no
  column-count assertion — and one extra column on a 10-row table cannot approach
  `DEFAULT_JOIN_BROADCAST_MAX_BYTES` (128 MiB, `adapter/mod.rs:94`), so existing broadcast
  eligibility does not flip. Task 3.6 does exercise both surfaces as claimed: at `VS_NAME` the
  stringification is a rewrite that keeps the broadcast plan, at `VS_NAME_LOW` it is side-local to
  `fact_orders` and reaches that leg rewritten.
- **Resolved: [TRACEABILITY_GAP] row-equality claims mapped only to unit tests — option (b) for the
  shared-name scenario, and the asymmetry is justified in the spec text, not only in chat.** The
  shared-column-name scenario (new feature, lines 122-128) no longer carries "the returned rows SHALL
  equal native Exasol evaluation"; its final clause is now the MUST-NOT-screen-against-a-combined-
  universe clause. The justifying Background bullet is in the spec itself (lines 65-70): "pinned at
  the PARTITION level only — it makes no claim about what a live query returns, unlike every other
  scenario here… the claim is about WHICH column-type universe the screen consults, which is pure
  planning-time computation". § Verification repeats it and decision [11] records it. The two
  `pushdown-declined-filter-self-apply` join scenarios keep their row-equality clauses and now carry
  Integration rows (§ Scenario Coverage lines 260, 262) against
  `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` and
  `e2e_n_scan_like_on_decimal_side_column_applied_in_outer_where`. I audited every scenario in all
  seven deltas for an orphaned live-result claim: the four remaining "returned rows SHALL equal native
  Exasol evaluation" clauses (new feature scenarios 1 and 3; self-apply scenarios 1 and 2) each have
  an Integration row, and the two remaining unit-only scenarios (N-scan rewrite reaches its leg;
  byte-identical SQL) claim only planning-time output. No untraceable live-result claim remains.

`speq plan validate fix-join-filter-type-rewrites` passes on the revised tree (7 deltas validated;
only pre-existing AND-step-count warnings, all of which predate this round).

## Intent Fidelity

[no objection — axis checked: the revision adds no work outside the wiring. Tasks 3.5/3.6 are the
review's own prescribed option (a) and are verification of in-scope behavior, traceable to decision
[1]'s justification, not new feature scope. Blocker 4's fix NARROWS one spec claim rather than
dropping scope — the behavior stays specified at the partition level and the same residual route
keeps its live coverage. `grep -rn "Closes\|Fixes #"` over the plan directory still returns only
`Closes #215` plus the two MUST-NOT rows forbidding `Closes #223` / `Closes #228`; decision [8] is
unchanged on scoping.]

## Feasibility

[no objection — axis checked: every symbol and fixture the revision newly names exists and has the
shape the task assumes. `cols_per_side` that task 2.4 indexes is already built at
`sql_builders.rs:393-396` (`involved_table_columns` per side), so no unstated construction step
hides in it. `involved_table_columns` returns `Vec<(String, String)>` (`joins/planning.rs:376`),
which is exactly `join_col_types`'s declared return type and exactly what `extract_join_projection`'s
`combined` is today (`joins/rendering.rs:28-29`) and what `classify_where_filter` takes
(`support.rs:1085-1088`) — so advisory 5's single-owner refactor is a drop-in with no shape mismatch
and no behavior change. The round-1 ADVISORY fixes for the #228 soft dependency (§ Dependencies plus
decision [8]), the `NLS_DATE_FORMAT` qualification (§ Impact DATE row plus task 3.2), and the
unresolvable-column decline row (§ Impact sixth row plus the following paragraph) all landed.]

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `plan.md` task 3.6; § Manual Testing, row "pushdown-planning-decimal-string-format (live
  row equality)"
- Issue: task 3.6 never says where the ground truth comes from, and the decimal case is the one test
  in the plan where that matters. Tasks 3.1-3.3 compare against "the ground-truth filtered set", which
  a test can compute in Rust from the seeded values because the predicate is an ordinary `LIKE`/date
  bound. Task 3.6's predicate is `LENGTH(<scale ≥ 2 DECIMAL col>) > n`, whose expected row set depends
  on Exasol's DECIMAL-to-VARCHAR trimming rule — the very behavior under test. An implementer who
  computes the expected set by replicating the trim in Rust produces a test asserting that the adapter
  agrees with the test author's model of Exasol, not with Exasol. That is the assumption class #279
  disproved and the explicit reason decision [10] promoted this case from unit to live in the first
  place, so the gap partially undoes Blocker 3's fix. The task also does not pin `n`; if `n` does not
  fall between the trimmed length and the full-scale length (4 and 7 for `2912.00`), the pre-fix and
  post-fix row sets coincide and the test passes vacuously against the very divergence it exists to
  catch.
- Fix: In `plan.md` task 3.6, name the ground-truth source explicitly: derive the expected row set
  FROM Exasol (a native Exasol table loaded with the same decimal values, or an Exasol-side scalar
  query pinning the trimmed length) rather than from a Rust-side reimplementation of the trim. Add
  that the test MUST pin `n` strictly between the Exasol-trimmed and DataFusion full-scale stringified
  lengths of the seeded values, and MUST assert the row sets differ pre-fix — stating the concrete
  values chosen in task 3.5 so the two lengths are computable from the plan. Mirror the "derive the
  expectation from Exasol, not from Rust" instruction in the § Manual Testing row's Expected Output.

## Requirement Quality

[no objection — axis checked: the Blocker-1 invariant is restated consistently across all four
locations and does not contradict the broadcast delta, which reaches the same rule from the other
direction (`pushdown-planning-join` clause 39 renders "the translator over the pipeline's REWRITTEN
tree" and clause 41 declines when "the translator cannot express a node in the tree"). The new
feature's six scenarios remain individually testable with concrete GIVEN types and falsifiable THENs.
No new conflict was introduced: `pushdown-planning-string-fn-type-coercion`'s unwired-surface bullet,
`pushdown-planning-like-type-coercion`'s FOUR-surface count, and plan.md § Design § Context's
three-unwired-surfaces paragraph now agree. The residual-renderability premise the widened trigger
set rests on still holds for all three guards — the decimal rewriter never declines, and a
type-declined LIKE or >2-argument INSTR/LOCATE renders in the Exasol dialect by construction.]

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY
- Location: `plan.md` task 2.1; § Scenario Coverage, row
  "pushdown-declined-filter-self-apply / An N-scan side-local conjunct whose DataFusion render
  declines becomes a residual conjunct | Unit | … | `type_screened_leg_filter_partition_is_total_and_fails_closed`"
- Issue: Blocker 1's fix made the whole-tree fail-closed arm bidirectional, and it is now stated
  normatively in four clauses across three deltas — "if a side's re-formed accepted-conjunct tree does
  not itself survive the pipeline, OR survives but is not DataFusion-renderable, that side's ENTIRE
  side-local set SHALL become residual" (`pushdown-planning-join-fallback` line 78,
  `pushdown-declined-filter-self-apply` line 54, new feature line 109, plan.md § Patterns "Fail
  closed"). No task asks for a test of it. Task 2.1 enumerates seven unit cases and none is the
  whole-tree arm; the per-conjunct unrenderable-rewrite case it does add is a different clause. The
  mapped test name promises the coverage anyway (`…_and_fails_closed`), so an implementer reads the
  name as a requirement and finds no input that reaches the arm. The arm is in fact unreachable by the
  plan's own reasoning: `pushdown-planning-join-fallback` concedes "either must hold, since each of its
  conjuncts satisfied both, but nothing in the type system forbids it", and decision [6]'s rewrite-once
  equivalence argument — all three passes are per-node post-order traversals and `predicate_and` is not
  a governed node — proves the re-formed tree cannot fail once every conjunct passed. The outcome is a
  named test with no constructible input, in a plan whose § Verification prose otherwise holds every
  clause to a mapped test.
- Fix: In `plan.md` § Scenario Coverage, rename the mapped unit test to drop the unreachable promise
  (e.g. `type_screened_leg_filter_partition_is_total_and_disjoint`), and add one clause to
  `decision-log.md` § [5] recording that the whole-tree fail-closed arm is DEFENSIVE and unreachable
  under decision [6]'s post-order-traversal equivalence argument, so it is deliberately asserted in the
  specs but carries no test. If instead the arm is to be tested, add the case to task 2.1 with its
  concrete construction (name the input that makes the re-formed tree fail while every conjunct passes)
  rather than leaving the implementer to find one.

## Design Depth

[no objection — axis checked: the revision adds no abstraction. Advisory 5's fix removes one, giving
the broadcast type universe a single owner (`join_col_types`, task 1.2, decision [12]) and deleting
`extract_join_projection`'s duplicate `combined` derivation — the leakage is closed by extraction, not
by a new layer, and the return type matches so nothing widens. `type_screened_leg_filter`'s interface
is unchanged by Blocker 1's fix: the added condition tightens the existing predicate inside the
function rather than surfacing a new parameter or a second return, so the "one call yields a total,
disjoint, fail-closed, per-side-correct partition" depth argument still holds. No new module, boundary,
dependency direction, or configuration parameter. Decision [6] remains the plan's only tactical
shortcut and still names its ceiling and a function-local upgrade path.]

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: `plan.md` § Summary; § Design § Context (paragraph at lines 29-31)
- Issue: two guardrail slips, one of them introduced by this round's edit. (1) § Summary now carries
  FOUR sentences across two paragraphs against the hard two-sentence cap. Round-1's fix correctly split
  the over-long opener into two sentences, but the two sentences that followed it were left in place,
  so the split traded a length violation for a count violation. (2) § Design § Context's new
  unwired-surfaces paragraph lists three items — "the grouped-aggregate render path, the
  aggregate-argument render path, and #223 slice 3's GROUP-BY-only DECIMAL keys" — then closes with the
  parenthetical "(#223 slices 1 and 3)", which reads as a gloss on the list it follows even though
  slice 1 (computed-expression arguments) is not one of the three items named. The reader cannot tell
  on one pass whether slice 1 is a fourth unwired surface or a cross-reference.
- Fix: In `plan.md` § Summary, cut to two sentences: keep "Wire `apply_type_rewrites` into both join
  WHERE-filter render sites: `render_broadcast_join`'s combined filter and the N-scan fallback's
  per-leg filter." as sentence one, and fold the failure modes and the #285 safety note into one
  second sentence at or under 25 words, moving whatever does not fit into § Design § Context, which
  already states both. In § Design § Context, delete the trailing "(#223 slices 1 and 3)" and instead
  name slice 1 as its own list item with its surface ("a non-bare-column stringification argument,
  #223 slice 1"), so the list is exhaustive and the parenthetical is unnecessary.

[Also checked, no objection: the round-1 PROSE_BLOAT items all landed — § Impact's performance
paragraph now leads with the conclusion ("each decline costs exactly one thing"), and task 3.3 reads
"`VS_NAME_LOW`, whose lowered `join_broadcast_max_bytes` forces the N-scan fallback", which matches
`with_join_broadcast_max_bytes("1")` at `tests/e2e_join_test.rs:91`. The new normative clauses use
RFC-2119 keywords correctly and name their actor. Background bullets and Gherkin steps are outside the
governed set, so the new feature's long Background is not a finding.]
