# Plan Review Findings: refactor-column-types-fold-case (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 6 (Blockers: 2, Advisory: 4)
- Intent Fidelity blockers: 0

## Round-1 Blocker Recheck

- **Resolved: [UNSTATED_ASSUMPTION] F1 — false failure-mode bound.** Verified against
  `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs:316-325`: the fallback is
  `if narrowed.is_empty() { full_cols.to_vec() } else { narrowed }`, so a mixed side keeps the
  non-empty subset. All three artifacts now state that bound correctly — delta § Background
  DELTA:NEW bullet 1 ("the failure mode if that premise ever broke is a DROPPED column rather than
  a wider projection … Only the ALL-MISS case widens a projection"), § Scenario clause 36 (with an
  explicit `MUST NOT be recorded as "a wider projection, never a dropped column"`), and
  `decision-log.md` § [4]. The `[STRASSE, ID]` × `{STRAßE, ID}` worked example is carried in all
  three.
- **Resolved: [COMPLETENESS_GAP] R1 — CHANGED block superseded no recorded bullet.** All five
  CHANGED bullets now open with `This bullet SUPERSEDES the preceding Background bullet "<quote>"`.
  Each quote matches the recorded text verbatim and the five targets are recorded lines 10, 13, 16,
  17, and 20 of `specs/vs-adapter/pushdown-col-types-consolidation/spec.md` — exactly the set round 1
  named. No recorded Background bullet asserting two folds, a preserved-by-design divergence, or a
  per-wrapper case fold survives the merge.
- **Resolved: [REQUIREMENT_CONFLICT] R2 — second feature contradicted with no delta.**
  `specs/_plans/refactor-column-types-fold-case/datafusion-scan/type-mapping-module-structure/spec.md`
  exists, supersedes the pass-through-exception bullet's closing sentence with the RULE restated and
  only the example corrected, and `plan.md` § Features carries it as a second CHANGED row.
  `speq plan validate refactor-column-types-fold-case` passes on both deltas. (This fix introduces
  B1 below.)
- **Resolved: [COMPLETENESS_GAP] R4 — comment-carrier enumeration wrong.** § Scenario clause 35 now
  reads "FIVE carriers exist and all five are in scope" and quotes each. Independently confirmed by
  grepping every `to_ascii_uppercase` / ASCII-fold mention in `crates/`: the five are
  `support.rs:442-449`, `joins/planning.rs:356`, `joins/planning.rs:359-360`, `support.rs:671-673`,
  and `support.rs:6100-6103`. The three remaining hits (`joins/rendering.rs:255`,
  `support.rs:1310-1321`, `joins/rendering.rs:901-906`) all describe the collect-walk divergence,
  which clause 37 correctly excludes; the fourth (`joins/planning.rs:774-792`) is deleted with the
  test. No sixth carrier exists. All five map to a task: carriers 1-3 to task 1, carriers 4-5 to
  task 2.
- **Resolved: [TASK_GRANULARITY] — tasks 1-3 not a parallel group, task 4 not independent.**
  § Implementation Tasks is now three tasks, the former 1-3 merged into one with each edit as a
  bullet. § Parallelization is a single row, `Task 1 → Task 2 → Task 3`, and the independence claim
  is replaced by "Every task before the gate edits … `support.rs`, so they MUST run one at a time."
  Cited line ranges verified: task 1 at `support.rs:442-475` and `joins/planning.rs:356-360,774-796`,
  task 2 at `support.rs:671-673` and `support.rs:6100-6103`.
- **Not resolved (partially): [INFORMATION_LEAKAGE] D1 — recorded refusal to unify left standing.**
  The spec-library half is resolved: `decision-log.md` § [6] quotes the objection verbatim and
  matches `specs/vs-adapter/pushdown-col-types-consolidation/spec.md:16` and
  `specs/_decision/042-refactor-col-types-guard-dedup.md:158` word for word, delta Background
  bullet 3 supersedes recorded bullet 16 carrying that reasoning, and `plan.md:38` replaces the
  diagnostic waiver with the one applicable diagnostic row and its answer. The
  permanent-decision half is NOT resolved — see B2. Round 1's own `Fix:` text specified
  `Promotes to ADR: no`, which is what makes the residue reachable; the planner followed the
  instruction, and the instruction was wrong.

Both round-1 advisories were also addressed: `plan.md` § Manual Testing row 2 now reads
`cargo test --features exasol-e2e --test e2e_non_ascii_identifier_test -- --test-threads=1
--nocapture`, and the `pushdown-planning-like-type-coercion` staleness is deferred in
`decision-log.md` § [7] — which is one of the two options round 1's `Fix:` authorized, so the
deviation is adequately recorded, not skipped. § [7]'s reasoning is sound on the substance (two of
that bullet's three claims were already stale before this change) and the behavior claim is
correct: that feature reads `involvedTables[0].columns` through `extract_all_column_types`, the
surviving Unicode path.

## Intent Fidelity

[no objection — axis checked: the plan still removes exactly `fold_case`. § Non-Goals is unchanged
and intact (no signature change, no wrapper deletion, no new test, no collect-walk fold touch, no
reshaping of the surviving selection parameter); § Scenario clauses 31, 32, 34, and 37 each fence one
of them normatively. § Verification adds no test, per the interview answer. The one item added beyond
round 1's instructions — the cross-reference retarget in the second delta — is raised as B1 on
requirement-conflict grounds rather than as scope creep, because the delta had to restate that
scenario regardless.]

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY

- Location: `plan.md` § Design → Context; `vs-adapter/pushdown-col-types-consolidation/spec.md`
  § Background
- Issue: no normative Iceberg section is quoted anywhere in this plan, and CLAUDE.md § Iceberg
  specification compliance makes that a MUST: "Any feature planned via `/speq:plan` that touches
  scanning, pushdown, or schema/type handling MUST be checked against the Apache Iceberg table spec
  … quote the relevant normative section, don't rely on memory." This plan touches both pushdown and
  schema/type handling, and the omission is not cosmetic here: the entire safety argument rests on
  `resolve_table_schema` mapping every Iceberg field through `f.name.to_uppercase()`
  (`file_resolution.rs:640`). That fold is a lossy, case-destroying normalization of an Iceberg
  schema field `name`, applied because Exasol folds unquoted identifiers to uppercase. The plan
  makes it load-bearing for a NEW claim — byte-identity of both wrappers, plus the safety of the one
  new cross-fold pairing in delta bullet 1 — while naming it only as "the adapter's existing
  normalization", never as a deliberate Exasol-driven deviation from Iceberg's own field-name
  handling. CLAUDE.md requires exactly that naming: "A deviation driven by an Exasol target-type
  limitation … is not a gap — but it must still be named as a deliberate trade-off in the spec, not
  left unstated."
- Fix: In `plan.md` § Design → Context, add one sentence quoting the Apache Iceberg table spec's
  normative statement on schema field `name` and case-sensitive column resolution (from
  https://iceberg.apache.org/spec/, verified against the live page — do not quote from memory). In
  `vs-adapter/pushdown-col-types-consolidation/spec.md` § Background, add one DELTA:NEW bullet
  naming `resolve_table_schema`'s `to_uppercase` as a deliberate trade-off forced by Exasol's
  unquoted-identifier fold, citing that section, and stating that two Iceberg fields differing only
  in case collide under it. If an issue already tracks that collision, cite it inline in the
  `(#nnn)` style; if none does, state in `decision-log.md` that no such issue is filed and why.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER

- Location: `datafusion-scan/type-mapping-module-structure/spec.md` § Scenarios, the relocation
  scenario's wrapper clause (delta line 23); `decision-log.md` § [7]
- Issue: the new delta retargets the builder-contract owner in a scenario clause but leaves a
  Background bullet of the SAME recorded feature naming the old owner, so the merged feature
  contradicts itself and its own `MUST NOT` is violated by a sibling bullet. Delta clause 23 adds:
  "That owner name SHALL be `vs-adapter/pushdown-col-types-consolidation` and MUST NOT stay
  `vs-adapter/pushdown-module-structure`". Recorded
  `specs/datafusion-scan/type-mapping-module-structure/spec.md:21`, which no delta bullet
  supersedes, closes with: "The merge itself is specified by `vs-adapter/pushdown-module-structure`,
  which owns the two functions' file set; this feature records only that its own fence is lifted."
  The retarget is correct on the merits — recorded line 63 did name the stale owner, and the
  builder's contract does live in `vs-adapter/pushdown-col-types-consolidation` — so the defect is
  the half-correction, not the correction. Before this delta the feature was consistently stale;
  after it, one file asserts both owners, which is worse for the next reader than either state.
  (Recorded line 19, "The rewiring itself is specified by `vs-adapter/pushdown-module-structure`,
  which owns the guards' file set", is NOT affected — guard rewiring genuinely is that feature's.)
- Fix: In `datafusion-scan/type-mapping-module-structure/spec.md` § Background, add a third
  `DELTA:CHANGED` bullet opening `This bullet SUPERSEDES the preceding Background bullet "This delta
  also SUPERSEDES a clause of the relocation scenario, \"*AND* `extract_all_column_types`
  (`support.rs`) and `involved_table_columns` (`joins/planning.rs`) MUST NOT be merged …\""` that
  restates the bullet with its closing sentence changed to name
  `vs-adapter/pushdown-col-types-consolidation` as the specifier of the merge and the owner of the
  two functions' contract, keeping the fence-lifted statement and the "MUST NOT in the permanent
  library that the shipped code violates" reasoning unchanged. Then correct the count in delta
  Background bullet 2 from "exactly ONE scenario clause besides the bullet above" to name both
  superseded bullets. In `decision-log.md` § [7], add one sentence stating the criterion that
  separates the two cross-reference cases — this feature's file is already being edited for the
  pass-through example R2 required, whereas `vs-adapter/pushdown-planning-like-type-coercion` would
  need a new delta file for a reference this change does not touch.

#### [COMPLETENESS_GAP] ADVISORY

- Location: `vs-adapter/pushdown-col-types-consolidation/spec.md` § Scenarios (the DELTA:CHANGED
  scenario replacing the recorded merged-builder scenario)
- Issue: the delta replaces the recorded scenario in full and drops two clauses that remain true
  after the change, so both constraints leave the permanent library unremarked. Recorded
  `specs/vs-adapter/pushdown-col-types-consolidation/spec.md:52`: "*AND* the builder SHALL be
  declared `pub(super)` in `support`, which already reaches `pushdown` and its `joins::planning`
  descendant, so NO production item's visibility widens and no join-module `use` path widens" — the
  new scenario pins both WRAPPERS' visibility in clause 30 but says nothing about the builder's, so
  after the merge nothing forbids widening `column_types`. Recorded line 54, the
  `use crate::types::mapping::exasol_type_from_json` deletion clause, is likewise dropped; it is the
  weaker loss, since the import is already gone, but the compile-enforced-proof rationale goes with
  it. Neither clause is one this plan changes.
- Fix: In `vs-adapter/pushdown-col-types-consolidation/spec.md` § Scenarios, add one AND clause after
  clause 30 restating recorded clause 52's `pub(super)`-in-`support` requirement and its
  no-visibility-widening consequence verbatim, and one AND clause restating recorded clause 54's
  requirement that `joins/planning.rs` carry no `use crate::types::mapping::exasol_type_from_json`.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY

- Location: `plan.md` § Features (row 2), § Verification → Scenario Coverage, § Verification →
  Manual Testing
- Issue: `datafusion-scan/type-mapping-module-structure` is listed as CHANGED but appears in no other
  section of `plan.md`. All four § Scenario Coverage rows and all four § Manual Testing rows name
  `vs-adapter/pushdown-col-types-consolidation`, and no § Implementation Tasks bullet touches the
  second feature. An implementer reading the plan has no gate proving that delta landed, and cannot
  tell whether the omission is deliberate or an oversight — the second delta is spec-only, but the
  plan never says so.
- Fix: In `plan.md` § Verification, add one § Manual Testing row for
  `datafusion-scan/type-mapping-module-structure` with command
  `speq plan validate refactor-column-types-fold-case` and expected output "validation passes; both
  delta specs listed", and add one sentence under § Scenario Coverage stating that the second delta
  changes one Background bullet and one cross-reference clause with no code change, so its gate is
  delta validation rather than a test.

## Design Depth

#### [REQUIREMENT_CONFLICT] BLOCKER

- Location: `decision-log.md` § [6] (`Promotes to ADR: no`)
- Issue: two `Status: Accepted` ADRs in `specs/_decision/042-refactor-col-types-guard-dedup.md` are
  falsified by this change and nothing will supersede either, because the entry that records the
  reversal is marked `Promotes to ADR: no`. `/speq:spec-merge` emits a `**Supersedes:**` pointer only
  for entries marked `yes` and "MUST NOT edit any other file in `specs/_decision/`", so a `no` entry
  reaches `_recorded/` and never touches the permanent decision record. The two survivors:
  (a) `column-types-builder-separate-selection-and-fold-params` (line 35), Decision:
  "`column_types(request, select_table, fold_case)`. `extract_all_column_types` passes a first-table
  selector plus `str::to_uppercase`; `involved_table_columns` passes a find-by-name selector plus
  `str::to_ascii_uppercase`" — a signature the shipped code no longer has, with "Unify the fold for
  both callers | ✗ Rejected" standing in its options table.
  (b) `col-types-fold-divergence-unreachable-design-preserved` (line 133), Decision: "Keep both folds
  byte-for-byte and keep the characterization test, on new grounds" — with "Unify the folds now that
  no reachable input distinguishes them | ✗ Rejected" standing in its options table. This is the
  unresolved half of round-1 D1: the objection was answered in the spec library and left standing in
  the decision library. § [6]'s own header claims otherwise — "Supersedes the recorded decision …
  (`specs/_decision/042-refactor-col-types-guard-dedup.md`)" — which `Promotes to ADR: no` makes
  unachievable, and it names the target by Decision text rather than by ADR title or ID slug, so the
  recorder could not resolve it even if flipped.
- Fix: In `decision-log.md` § [6], change `Promotes to ADR:` from `no` to `yes` and rewrite the
  supersession header to name the target ADR's ID slug exactly:
  `col-types-fold-divergence-unreachable-design-preserved`. Add a new entry § [8] "The builder's
  two-parameter shape is superseded by the one-parameter shape", also `Promotes to ADR: yes`, naming
  ID slug `column-types-builder-separate-selection-and-fold-params`, whose Decision and Rationale
  state that `column_types` now takes `(request, select_table)` and that the options table's
  rejection of unifying the fold is discharged by the exhaustive fixed-point sweep plus
  `resolve_table_schema`'s normalization, cross-referencing § [1] and § [6] rather than restating
  them.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY

- Location: `decision-log.md` § [6] Rationale
- Issue: the Rationale is one ~11-line paragraph carrying five distinct ideas (why the question is
  narrow, the in-module-consumer argument, the premise-versus-rule argument, the pure-refactor
  discharge, the retained standard), against `/speq:writing-guardrails`' "paragraphs at 3–7 lines"
  and "One idea per paragraph. Split a paragraph that carries two." Several sentences exceed the
  25-word cap. This is the entry a reviewer reads to check D1, so density costs the most here.
- Fix: In `decision-log.md` § [6], split the Rationale into four paragraphs — the framing sentence;
  the FIRST (in-module consumer) argument; the SECOND (behavior-preservation premise) argument; and
  the pure-refactor discharge plus the retained unreachable-input-domain standard — and break any
  sentence over 25 words in two. Change no claim.
