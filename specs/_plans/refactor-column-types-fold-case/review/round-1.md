# Plan Review Findings: refactor-column-types-fold-case (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 9 (Blockers: 6, Advisory: 3)
- Intent Fidelity blockers: 0

## Premortem

Six months out, three ways this landed badly:

1. **A wider projection that was actually a dropped column.** The delta records the new
   `involved_table_columns` (Unicode) × `collect_side_column_names` (ASCII) pairing as bounded — "the
   failure mode is a wider projection, never a dropped column or a wrong result". That bound is false
   for the mixed case. The permanent library now carries a safety claim that does not hold, and the
   next author who breaks the upstream premise trusts it. → `[UNSTATED_ASSUMPTION]` F1.
2. **The library says the folds still diverge.** Three recorded Background bullets of the same feature,
   plus one of `datafusion-scan/type-mapping-module-structure`, still assert two folds and a
   still-open `fold_case`. Nothing in the delta supersedes them, so `speq record` merges a spec that
   contradicts the shipped code — the exact stale-citation defect this feature exists to delete. →
   `[COMPLETENESS_GAP]` R1, `[REQUIREMENT_CONFLICT]` R2, R4.
3. **The unification silently re-created the leakage #265 refused.** `vs-adapter/pushdown-col-types-consolidation`
   rejected unifying on the grounds that the fold's harmlessness "would rest on a normalization
   `resolve_table_schema` owns". The plan does the unification and never confronts that judgment, so
   the reversal is undocumented and the next reader cannot tell which ruling is current. →
   `[INFORMATION_LEAKAGE]` D1.

## Intent Fidelity

[no objection — axis checked: the plan removes exactly `fold_case` and nothing else. § Non-Goals
matches issue #270's scope (no wrapper deletion, no signature change, no collect-walk fold touch, no
reshaping of the surviving selection parameter), and § Verification adds no test, per the interview
answer recorded in `decision-log.md` § Interview. The three items the planner added beyond the brief
(stale doc comment, the new cross-fold pairing, the E2E premise) are each a direct consequence of the
removal, not gold-plating.]

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER

- Location: `vs-adapter/pushdown-col-types-consolidation/spec.md` § Background (DELTA:NEW bullet 1) and § Scenario clause 33; `decision-log.md` § [4]
- Issue: the recorded failure-mode bound is false. All three artifacts state "`referenced_side_columns`'
  empty-narrowing fallback keeps `full_cols`, so the failure mode is a wider projection — never a
  dropped column or a wrong result". The fallback only fires when narrowing yields NOTHING
  (`crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs:316-325`):
  `let narrowed = full_cols.iter().filter(|(name, _)| names.contains(name))…; if narrowed.is_empty() { full_cols.to_vec() } else { narrowed }`.
  Take a side whose `full_cols` (post-change: Unicode-folded) is `[STRASSE, ID]` and a `selectList`
  naming both columns, so `collect_side_column_names` (ASCII-folded) yields `{STRAßE, ID}`. `narrowed`
  is `[ID]` — non-empty, so the fallback does NOT fire, and the `ß`-bearing column is dropped from the
  fan-out leg while the outer wrapper still renders a reference to it. That is a dropped column and an
  erroring or wrong-result wrapper, not a wider projection. Before the change both sides folded ASCII
  and the column matched, so this is a bound the change itself introduces. The premise (no `ß` reaches
  either side) makes it unreachable — but the bound is the stated safety net for the premise failing,
  and it is being written into the permanent library as fact.
- Fix: In `vs-adapter/pushdown-col-types-consolidation/spec.md`, rewrite the failure-mode sentence in
  both the DELTA:NEW Background bullet 1 and § Scenario clause 33 to state the real bound: a
  disagreement drops the diverging column from the narrowed leg whenever any OTHER column of that side
  matches, and only falls back to `full_cols` when NO column matches — so the failure mode is a dropped
  column in the mixed case and a wider projection only in the all-miss case. Replace the
  "never a dropped column or a wrong result" claim with the upstream-normalization premise as the sole
  reason the pairing is safe. Apply the same correction to `decision-log.md` § [4] Rationale.

#### [UNSTATED_ASSUMPTION] ADVISORY

- Location: `plan.md` § Verification → Manual Testing, row 2
- Issue: `cargo test --test e2e_non_ascii_identifier_test -- --nocapture` cannot run the test it names.
  `crates/lakehouse-engine/tests/e2e_non_ascii_identifier_test.rs:11` is `#![cfg(feature = "exasol-e2e")]`,
  so without the feature the binary compiles to zero tests and exits 0. The row is the gate for the
  premise the whole plan rests on, and as written it passes vacuously. `Makefile:78` runs it correctly
  with `--features exasol-e2e`.
- Fix: In `plan.md` § Manual Testing row 2, change the command to
  `cargo test --features exasol-e2e --test e2e_non_ascii_identifier_test -- --test-threads=1 --nocapture`.

## Requirement Quality

#### [COMPLETENESS_GAP] BLOCKER

- Location: `vs-adapter/pushdown-col-types-consolidation/spec.md` § Background (DELTA:CHANGED block)
- Issue: the CHANGED block supplies two bullets against a recorded Background of 20, names none of the
  bullets it supersedes, and leaves three that become false. The recorded file
  (`specs/vs-adapter/pushdown-col-types-consolidation/spec.md`) still asserts, with no delta covering
  it: bullet 13 — "the guards' Unicode-folding lookup can never miss a name **the ASCII-folding
  join-side builder** produced"; bullet 16 — "It is preserved byte-for-byte here for a design reason
  … **A follow-up issue tracks removing `column_types`' `fold_case` parameter** as dead flexibility";
  bullet 20 — "each wrapper here supplies a different table selection **and a different case-fold
  function**", which is the recorded justification for the pass-through exception this plan's own
  § Scenario clause 28 depends on. The parent delta made supersession explicit
  ("This delta SUPERSEDES the preceding Background bullet '<quoted text>'"); this delta does not, so
  `recorder-agent` has no way to match CHANGED bullet 2 ("ONE `col_types` fold test remains") to
  recorded bullet 17, and the three stale bullets survive the merge.
- Fix: In `vs-adapter/pushdown-col-types-consolidation/spec.md` § Background, prefix each DELTA:CHANGED
  bullet with an explicit `This bullet SUPERSEDES the preceding Background bullet "<first 12 words>"`
  sentence naming recorded bullets 10 and 17. Add DELTA:CHANGED bullets superseding recorded bullets
  13, 16, and 20: restate 13's closing clause without "the ASCII-folding join-side builder"; restate
  16 without the preserved-for-a-design-reason and follow-up-issue sentences (see D1); restate 20 so
  the partial-application exception rests on the table selection alone.

#### [REQUIREMENT_CONFLICT] BLOCKER

- Location: `plan.md` § Features (single-row table); missing delta for `datafusion-scan/type-mapping-module-structure`
- Issue: the recorded Background of a second feature is contradicted with no delta.
  `specs/datafusion-scan/type-mapping-module-structure/spec.md:14` states: "The two `col_types`
  wrappers this delta's relocation scenario retains are that case: `extract_all_column_types` and
  `involved_table_columns` **each supply their own table selection and case fold** to
  `support::column_types`." That bullet is the recorded carrier of the pass-through-deletion
  EXCEPTION — the rule under which both wrappers survive — and after this change neither wrapper
  supplies a case fold. `plan.md` § Features lists one CHANGED feature, so nothing corrects it.
- Fix: Add a `DELTA:CHANGED` Background bullet to a new
  `specs/_plans/refactor-column-types-fold-case/datafusion-scan/type-mapping-module-structure/spec.md`
  superseding that bullet's last sentence, so the exception's example reads "each supply their own
  table selection to `support::column_types`", and state that the exception's rule (a wrapper supplying
  ANY argument the shared helper does not choose is a partial application, not a pass-through) is
  unchanged. Add the feature as a second CHANGED row in `plan.md` § Features.

#### [COMPLETENESS_GAP] BLOCKER

- Location: `vs-adapter/pushdown-col-types-consolidation/spec.md` § Scenario clause 32; `decision-log.md` § [3]; `plan.md` § Implementation Tasks
- Issue: the comment-carrier enumeration claims exhaustiveness and is wrong. Clause 32: "THREE
  carriers exist and all three are in scope". FIVE sentences in the code assert the removed
  divergence. Two are unlisted, and one of them is in a file no task touches:
  (a) `crates/lakehouse-engine/src/adapter/pushdown/support.rs:671-673` — `column_exa_type`'s doc
  comment: "`involved_table_columns`' ASCII-folded keys agree for every column name the adapter can
  declare". After the change `involved_table_columns` produces no ASCII-folded keys at all. This
  sentence was itself added by the parent plan's review fix (`_recorded/004…/tasks.md` 4.2), so it is
  the newest carrier and the one most likely to survive.
  (b) `crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs:359-360` — the closing
  paragraph: "A partial application of `support::column_types`, supplying the find-by-name selection
  **and the ASCII-only fold this side has always applied**." Clause 32 directs the implementer to
  keep "its find-by-name selection rationale intact", which is this very sentence — so a literal
  reading preserves the false fold claim.
- Fix: In `vs-adapter/pushdown-col-types-consolidation/spec.md` § Scenario clause 32, change "THREE
  carriers" to FIVE and add the two above with quotes: `column_exa_type`'s doc sentence SHALL be
  reworded so the agreement claim names the single fold both builders now apply (keeping the
  `resolve_table_schema` premise and the `type`-tag paragraph intact), and
  `involved_table_columns`' closing partial-application sentence SHALL drop "and the ASCII-only fold
  this side has always applied" while keeping the find-by-name selection clause. Add the
  `column_exa_type` reword to `plan.md` § Implementation Tasks (it is in `support.rs`, so it belongs
  with the task that already edits that file) and to `plan.md` § Dead Code Removal. Record the
  corrected count in `decision-log.md` § [3].

#### [REQUIREMENT_CONFLICT] ADVISORY

- Location: missing delta for `vs-adapter/pushdown-planning-like-type-coercion`
- Issue: `specs/vs-adapter/pushdown-planning-like-type-coercion/spec.md:44` asserts "The
  Unicode-versus-ASCII fold divergence between the two `col_types` builders … `vs-adapter/pushdown-module-structure`
  records the divergence, the live capture showing it unreachable, and the issue tracking removal of
  the `fold_case` parameter that preserves it." After this change the divergence and the parameter are
  gone and issue #270 is closed. (The bullet's cross-reference is already stale — #265 moved that
  content to `vs-adapter/pushdown-col-types-consolidation` — so this is a pre-existing carrier this
  change makes worse, not one it creates.)
- Fix: Either add a `DELTA:CHANGED` Background bullet under a new
  `specs/_plans/refactor-column-types-fold-case/vs-adapter/pushdown-planning-like-type-coercion/spec.md`
  restating bullet 44 as "both `col_types` builders now fold with the Unicode `to_uppercase`; this
  scenario reads `involvedTables[0].columns` through `extract_all_column_types` exactly as before",
  or record in `decision-log.md` an explicit decision to leave it to a separate cleanup with the
  reason stated.

## Task Breakdown

#### [TASK_GRANULARITY] BLOCKER

- Location: `plan.md` § Parallelization
- Issue: the table is wrong in two ways, both against this repo's own convention
  (`_recorded/004…/plan.md` § Parallelization uses `Task 1 → Task 2` for a chain and one row per
  independent branch, and states "Only one task may hold `support.rs` at a time").
  (a) "Group A | Tasks 1, 2, 3" lists three tasks as one parallel group while the prose directly
  below says "Tasks 1-3 must land as one compiling change … the crate does not compile between them".
  Sequencing them does not fix it either: tasks 1 and 2 each leave the crate non-compiling, so neither
  can run its own build/test gate. They are one work unit, not three.
  (b) Group B (task 4) is declared independent of Group A, but task 4 edits
  `crates/lakehouse-engine/src/adapter/pushdown/support.rs` (the doc comment at ~line 6100) and task 1
  edits the same 6,100-line file at lines 442-453. Two concurrent writers on that file is the merge
  hazard the parent plan called out by name.
- Fix: In `plan.md`, merge § Implementation Tasks 1, 2, and 3 into ONE task (keeping every listed
  edit as a bullet of it) and renumber the remainder. Replace § Parallelization with a single
  sequential chain — merged task → doc-comment reword → verification gate — and delete the
  "Task 4 … is independent" sentence, stating instead that every task before the gate edits
  `support.rs` and so must run one at a time.

## Design Depth

#### [INFORMATION_LEAKAGE] BLOCKER

- Location: `plan.md` § Design → Patterns (line 38); `decision-log.md` § [1]; `vs-adapter/pushdown-col-types-consolidation/spec.md` § Background
- Issue: the recorded feature rejected exactly this unification on leakage grounds, and the plan
  neither supersedes nor answers that ruling. Recorded bullet 16: unifying is refused because
  "making either builder's fold depend on `resolve_table_schema`'s uppercasing would put one module's
  decision inside another module's body, **which is the information leakage this plan exists to
  remove**" (`specs/_decision/042-refactor-col-types-guard-dedup.md:158` records the same rejection
  in its alternatives table). Removing `fold_case` is settled by the user, so the question is not
  whether — it is that the plan operationalizes it while leaving the opposite ruling standing in the
  library. `plan.md:38` then waives the design diagnostic outright ("The design diagnostic in
  `/speq:design-philosophy` is not run per-question here"), skipping the one row that bears on this:
  "Would changing how a module works internally force an edit anywhere outside it?" — after
  unification, `column_types`' behavior-preservation rests on a normalization owned by
  `resolve_table_schema`, which `column_types` cannot see. `decision-log.md` § [1] argues only
  `to_uppercase` versus `to_ascii_uppercase` and never mentions the leakage objection it overrides.
- Fix: Add a `decision-log.md` design decision that names the recorded leakage objection verbatim,
  states why it no longer decides — the surviving fold is chosen to match the CONSUMER
  (`column_exa_type`), which is inside the same module, so the builder no longer encodes a foreign
  module's decision; the dependency on `resolve_table_schema` is a behavior-preservation premise
  guarded by an E2E test, not a fold selection rule — and marks it `Promotes to ADR: no`. Supersede
  recorded bullet 16 in the delta with a bullet carrying that reasoning (see R1). In `plan.md` § Design,
  replace line 38 with the one diagnostic row that applies and its answer.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY

- Location: `plan.md` § Design → Patterns, lines 38 and 102
- Issue: two sentences do process commentary rather than stating the change. Line 38 — "The design
  diagnostic in `/speq:design-philosophy` is not run per-question here: this change introduces no
  module, interface, or boundary. It removes a parameter, which that skill's General- vs
  Special-Purpose rule names directly." — argues about which review step applies instead of making a
  design claim, and is the sentence D1 objects to. Line 102 — "The E2E entry is the load-bearing one:
  it is what makes the unification safe, by proving no `ß`-bearing name reaches either fold." —
  overstates the evidence: the test asserts the served name of ONE seeded Iceberg column; that no
  `ß`-bearing name reaches either fold follows from `resolve_table_schema` being the sole declaration
  site, which the test does not establish.
- Fix: In `plan.md`, replace line 38 per D1's fix. Rewrite line 102's second sentence as: "The E2E
  entry is the standing guard on the premise: it asserts an Iceberg `straße` column is served as
  `STRASSE`, so a `resolve_table_schema` change that stopped Unicode-uppercasing declared names fails
  a test before any fold could matter."
