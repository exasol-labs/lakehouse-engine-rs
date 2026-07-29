# Plan Review Findings: refactor-col-types-guard-dedup (round 9)

## Summary
- Axes checked: 6/6
- Total findings: 5 (Blockers: 0, Advisory: 5)
- Intent Fidelity blockers: 0

## Scope of this round

Revision review round 2 of the 2-round cycle. Round 1 of this cycle is `review/round-8.md` (2 BLOCKERs,
4 ADVISORY). Effort is spent, in order, on: independently re-deriving both round-8 BLOCKER fixes against
the tree rather than against `decision-log.md`'s claim; then a bounded fresh pass for defects the fixes
themselves introduced. Rounds 1-7 are not re-litigated. Round-8's four ADVISORY findings are not
re-opened except where a fix physically touched the same content — which happened twice, noted below.

## Round-8 Blocker Recheck

- **Resolved: [AMBIGUOUS_REQUIREMENT] BLOCKER 1 — the `straße` LIKE clause could not observe the
  mechanism it asserted.** Verified at every site the Fix line named, and the fix's new mechanism was
  re-derived from the code rather than accepted.
  - Delta GIVEN (`vs-adapter/create-virtual-schema/spec.md:75`) now pins the column: "that column an
    Iceberg `string` column whose seeded values carry distinguishable prefixes, alongside an `id`
    column". The `long`/`DECIMAL(20,0)` false-by-construction path the finding named is closed.
  - Round-8's clause 81 is split. `:81` keeps the row subset alone. `:82` is the new discriminator and
    names its own limitation inline: "A declined filter returns the identical row set, so this
    generated-SQL assertion, not the row subset, is what discriminates a resolved lookup from a
    fail-safe decline."
  - plan.md task 7 carries the fifth assertion with every citation checked against the tree:
    `explain_virtual_sql` is at `tests/common/e2e_harness.rs:282` exactly; `CommonScanSpec::filter` is
    `#[serde(default, skip_serializing_if = "Option::is_none")]` at `src/scan/spec.rs:610-611`;
    `assert_filter_pushed_down` spans `e2e_capability_test.rs:1258-1269` (doc comment through `fn`
    line) and asserts `pushed_sql.contains("\"filter\":\"")`; `support.rs:682-686` is exactly the
    four-line rationale comment plus the `_ => None` arm that declines DECIMAL — a more accurate span
    than the Fix line's own `684-686`.
  - **The discriminator survives two attacks the Fix line did not anticipate, so the fix is sound
    rather than merely compliant.** (a) `explain_virtual_sql`'s doc comment says it flattens "the
    generated scan-driving plan plus Exasol's echoed pushdown request", which would defeat a naive
    substring match — but the echoed request's `filter` is object-valued, so only the scan spec's
    string-valued `filter` can satisfy the `"filter":"` pattern the existing helper relies on. (b) The
    assertion needs the filter to name `STRASSE` rather than the Iceberg `straße`;
    `build_alias_items`' own doc comment (`src/scan/sql_support.rs`) states the inner `"col" AS "COL"`
    wrap exists "so projection/filter expressions resolved against uppercase identifiers find a match
    regardless of the Parquet field casing", and `quote_ident` supplies the quoting. The assertion is
    satisfiable, not false by construction.
  - § Manual Testing gains the `EXPLAIN VIRTUAL … WHERE strasse LIKE 'a%'` row (`plan.md:197`) with the
    same falsifiability reason. § Verification row 9 (`plan.md:177`) names both assertions and states
    "The row-subset assertion alone proves nothing about the guards". § Verification's red-phase
    paragraph (`plan.md:180`) was updated to "its five assertions" — a propagation the Fix line did not
    demand and the planner found on its own.

- **Resolved: [REQUIREMENT_CONFLICT] BLOCKER 2 — the delta's self-description did not match the block
  it authors.** Confirmed by diff, not by reading the claim.
  - Extracted the recorded scenario's steps from `specs/vs-adapter/create-virtual-schema/spec.md` and
    the delta's from `spec.md:58-70`, then `comm`'d them. Recorded steps present in the delta: 7 of 7,
    byte-for-byte, with `comm -23` empty. Delta total: 10. Added: exactly 3, and exactly the three the
    finding named (the field-name fold at `:66`, the single-owner clause at `:67`, the `ß`-expansion
    trade-off at `:68`). So "ADDS THREE clauses … AMENDS NO recorded clause" is now true of the file.
  - Bullet 44 rewritten as demanded. Bullet 45 replaced: "This delta SUPERSEDES nothing." It quotes the
    recorded declaration clause verbatim, states that clause "stands unaltered here", and states the
    seven-verbatim-steps fact. The false supersession is gone, so a recorder no longer has to resolve a
    supersession whose target is unchanged.
  - Count correction propagated to all four sites the Fix line named: plan.md § Features (`:101` "adds
    three clauses to one scenario and adds one scenario … and amends no recorded clause"), decision
    [14]'s Decision line (`decision-log.md:106` "adding three clauses … no recorded clause is
    amended"), § Verification row 8 relabelled "(added clauses)" (`plan.md:176`), and § Non-Goals
    bullet 4 (`plan.md:20`). Swept for survivors: no `amends one clause` claim about
    `create-virtual-schema` remains anywhere, and the four remaining "(amended clause)" labels all sit
    on features that genuinely amend.
  - Both fixes are logged as `[plan-review]` entries at `decision-log.md:399-409`, and each entry's
    Direction change matches what the files actually say.

- `speq feature validate`: 0 errors library-wide; warnings only, all pre-existing AND-step counts.

## Intent Fidelity

No objection — axis checked. Neither fix touched scope. The user's two verbatim instructions are still
discharged by the same artifacts round 8 certified, and BLOCKER 1's fix strengthens the second one: "be
sure that it works" is now gated on evidence that the LIKE was pushed, not on a row set a decline
reproduces. No `[SCOPE_CREEP]` — the fifth assertion and the `string`-column constraint were both
demanded by the round-8 Fix lines, and the `Iceberg string` pin narrows the fixture rather than widening
task 7. No `[SCOPE_REDUCTION]` — nothing was dropped to satisfy either fix; the row-subset assertion was
retained alongside the new one rather than replaced by it.

## Feasibility

No objection — axis checked. Every new claim the two fixes introduced was re-derived against the tree:
`e2e_harness.rs:282`, `scan/spec.rs:610-611`, `e2e_capability_test.rs:1258-1269`, `support.rs:682-686`,
and `build_alias_items` in `scan/sql_support.rs`. `query_columns` (`exasol_ws.rs:158`) returns
column-major `Vec<Vec<Value>>` and `query_scalar_i64` (`:138`) returns an i64, so task 7's claim that
"the first four" assertions run through those two helpers holds — including the projected-values
assertion, which needs row data rather than metadata. Round-8 ADVISORY 1 (delta bullet 50's scan-side
mechanism) and ADVISORY 2 (the own-namespace `MUST`) were left unapplied and their content was not
touched by either fix, so they are not re-opened here; both remain report-only. Worth noting for the
human reader only: BLOCKER 1's fix pins the fixture column to Iceberg `string`, which happens to keep
the new scenario clear of the `list`-typed JSON-fallback gap ADVISORY 1 described.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: plan.md § Consequences, the `classify_exa_type` and `ExaTypeClass` stay `pub` row
  (`plan.md:82`): "No recorded clause forbids the narrowing — this plan's delta already amends three
  clauses of that same feature, so the API-count clause is amendable too."
- Issue: the count contradicts the three other places that state it, and the fix made the wrong number
  actively confusable. The feature is `datafusion-scan/type-mapping-module-structure`, whose delta
  amends FOUR clauses across two scenarios — stated as four in decision [9]'s verbatim twin of this
  same argument (`decision-log.md:73`, "when this plan's own delta already amends four clauses of that
  same feature"), in decision [11]'s title (`:83`), and in plan.md § Features (`:103`). Three is the
  classifier scenario's subset, not the feature's total, and the sentence says "of that same feature".
  The number itself may predate this revision, but BLOCKER 2's fix propagated "three clauses" into four
  new places in plan.md and the decision log as the `create-virtual-schema` added-clause count — so
  this row now reads as if it were citing that delta rather than the type-mapping one. Consequence is
  bounded: the argument holds at either count, so nothing normative breaks.
- Fix: In plan.md § Consequences, change the `pub` row's "already amends three clauses of that same
  feature" to "already amends four clauses of that same feature", matching decision [9], decision [11]
  and § Features. Change nothing else in the row.

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Verification → Scenario Coverage row 8 (`plan.md:176`) versus the delta's added
  clause at `vs-adapter/create-virtual-schema/spec.md:67` ("*AND* that fold SHALL be owned by exactly
  ONE site, `resolve_table_schema` … and no other code path SHALL declare a differently-cased name").
- Issue: the row now has three added clauses to cover and cites assertions that reach two of them. Its
  Test Name cell names only "The `SYS.EXA_ALL_TABLES` and `SYS.EXA_ALL_COLUMNS` assertions, which pin
  both declared identifiers as `STRASSE` — the `ß`-to-`SS` expansion the added clauses record", which
  covers the fold clause (`:66`) and the expansion clause (`:68`). Nothing in § Verification or
  § Manual Testing gates the single-owner `SHALL`: a live catalog read showing `STRASSE` is consistent
  with one declaring site but cannot show there is only one. The underlying fact IS verified — delta
  Background bullet 46 and decision [12] both cite `adapter/mod.rs:551`/`:576` and the single caller at
  `adapter/mod.rs:255` — so this is missing bookkeeping on a structural clause, not an untested
  behavior. It surfaced only because BLOCKER 2's fix made the three-clause count explicit; round 8
  certified traceability against a one-amended-clause description.
- Fix: In plan.md § Verification → Scenario Coverage row 8, extend the Test Name cell to state that the
  single-owner clause is covered structurally rather than by assertion, citing `adapter/mod.rs:551` and
  `:576` as the one declaring site and `adapter/mod.rs:255` as its one caller. Add one § Manual Testing
  `create-virtual-schema` row whose command greps the crate for column-name declaration sites — e.g.
  `grep -rn '"name": ' crates/lakehouse-engine/src/adapter/mod.rs` — with the expected count and the
  falsifying baseline stated, matching the falsifiable-baseline style rows 2, 3, 7, 8 and 12 already
  use. Add no task; task 7 is not widened.

## Task Breakdown

No objection — axis checked. Neither fix moved a task boundary. Task 7's file set is unchanged
(`tests/common/seed.rs`, one new `tests/` binary, `Makefile`), so § Parallelization's claim that it may
run parallel to the `support.rs` chain and task 4 still holds in fact — the fifth assertion adds a call
to an existing helper in `tests/common/e2e_harness.rs`, which it reads rather than edits. The task 3
predecessor edge and the task 6 successor edge are unchanged and still stated in both directions.
tasks.md still matches plan.md: seven implementation entries, 2.1 checked, 2.7 grouped with the
parallel branch; its 2.7 summary never enumerated the assertion count, so the four-to-five change left
it accurate rather than stale. Swept for a stale "four assertions" claim across plan.md, tasks.md and
decision-log.md: none survives. Task 7 remains one verifiable unit — one fixture, one binary, one
Makefile line — so the fifth assertion does not warrant a `[TASK_GRANULARITY]` split.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: `vs-adapter/create-virtual-schema` delta, new clause at `spec.md:82` ("*AND* the
  adapter-GENERATED pushdown SQL for that same `LIKE` query SHALL carry the predicate over
  `"STRASSE"` …"), against the placement argument in plan.md § Consequences row 11 (`plan.md:81`) and
  decision [14] (`decision-log.md:104-108`).
- Issue: the fix put a pushdown-outcome `SHALL` inside a feature that owns name declaration, and the
  plan's own placement rationale no longer covers it. § Consequences row 11 justifies siting the
  scenario in `create-virtual-schema` because "The property is that a declared identifier survives the
  `createVirtualSchema` round trip and stays queryable" and because `pushdown-module-structure` "owns
  no live identifier behavior". Clause 82 asserts something else: that a LIKE filter is PUSHED rather
  than declined. Whether a resolvable-name LIKE pushes is owned by
  `vs-adapter/pushdown-planning-like-type-coercion`, whose recorded clauses frame the opposite outcome
  as permitted — `spec.md:65` ("SHALL decline pushdown of the WHOLE top-level filter (emit no `filter`
  in the common spec)"), `:29` ("a decline is always correct"), and `:131` ("an accepted cost of the
  fail-safe that is slower but never wrong and SHALL NOT be recorded as a fixed hard failure"). There
  is no logical contradiction — clause 82 asserts this name DOES resolve, which the recorded clauses
  permit — but two features now carry a normative statement about one pushdown decision, with nothing
  enforcing their agreement. That is the dual-ownership pattern this plan exists to remove, and the
  pattern round 4 already caught once at the spec layer when the relocation clause restated the shared
  builder's contract. Raised as ADVISORY, not BLOCKER: the round-8 Fix line demanded this clause, and
  an unfalsifiable clause was the worse defect — the trade is correct, only the ownership seam is
  unstated.
- Fix: In `specs/_plans/refactor-col-types-guard-dedup/vs-adapter/create-virtual-schema/spec.md`, add
  one sentence to clause 82 (or to the Background bullet block at `:44-53`) stating the boundary: this
  clause uses the generated pushdown SQL as the OBSERVATION CHANNEL for the declared name's
  resolvability and does not restate or constrain LIKE pushdown policy, which
  `vs-adapter/pushdown-planning-like-type-coercion` owns — including that feature's recorded position
  that a decline is always correct. Add the same one-sentence boundary to plan.md § Consequences row 11,
  whose current rationale covers the scenario's identifier property but not its pushdown clause. Change
  no assertion and do not move the scenario.

Otherwise no objection — axis checked. Neither fix adds a module, interface, boundary or test-only
cross-module item. The design surface certified in rounds 7 and 8 is untouched: two seams held apart by
the recorded non-optional `&str` contract, both builder wrappers retained as partial applications,
`adapter` → `types` dependency direction intact. Task 7 still adds test assets only.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Impact paragraph 3 sentence 3 (`plan.md:111`) and § Non-Goals bullet 4 sentence 2
  (`plan.md:20`) — both sentences BLOCKER 2's fix edited.
- Issue: the count correction pushed two governed sentences further over the 25-word cap rather than
  leaving them where it found them. Measured word by word. § Impact paragraph 3 sentence 3 now runs 26
  words ("Task 7 adds the permanent E2E coverage a `straße`-named table and column deserve, plus the
  three `create-virtual-schema` clauses stating the column-name fold the library never documented.");
  singular-to-plural took it from 24 to 26. § Non-Goals bullet 4 sentence 2 now runs 33 words ("Task 7
  adds three `vs-adapter/create-virtual-schema` clauses stating that column names are declared through
  the same full-Unicode `to_uppercase` as table names, plus a live E2E scenario proving a `straße`-named
  table and column stay queryable."), up from 31. § Impact is the section an approving architect reads
  first and § Non-Goals is the scope contract, which is why rounds 2, 5, 6, 7 and 8 each spent a
  finding on these two sections. Round-8 ADVISORY 4 was not applied at all: § Impact paragraph 3
  sentence 2 still runs 53 words, and § Impact paragraph 2 sentence 3 and § Non-Goals bullet 3
  sentence 3 are unchanged. Delta Background bullets 44 and 45 are not governed prose and are not
  counted here, whatever their length.
- Fix: In plan.md § Impact paragraph 3, split sentence 3 at "plus": state that task 7 adds the
  permanent E2E coverage for a `straße`-named table and column, then that it adds the three
  `create-virtual-schema` clauses stating the column-name fold the library never documented. In
  § Non-Goals bullet 4, split sentence 2 at "plus" the same way. Then apply round-8 ADVISORY 4's Fix
  line verbatim to § Impact paragraph 3 sentence 2, § Impact paragraph 2 sentence 3 and § Non-Goals
  bullet 3 sentence 3, which remain unapplied. Keep every claim; add none.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: plan.md § Non-Goals bullet 4 (`plan.md:20`), opening sentence: "Documenting or changing the
  `ß`-to-`SS` expansion itself."
- Issue: round-8 ADVISORY 3, unresolved, and re-raised only because BLOCKER 2's fix edited this bullet's
  next sentence without touching the contradiction one clause above it. The Non-Goal's heading declares
  documenting the expansion out of scope; the delta's added clause 68 is an explicit `SHALL` to document
  it ("the full-Unicode fold's one-to-many expansions SHALL be recorded as a deliberate Exasol-target
  trade-off rather than left unstated"), and the bullet's own body now says task 7 adds three clauses
  doing exactly that. A reader consulting § Non-Goals first cannot rely on it. Severity is unchanged
  from round 8 — the intended distinction (changing the expansion is out, recording it is in) is
  recoverable from the body.
- Fix: In plan.md § Non-Goals bullet 4, change the opening sentence from "Documenting or changing the
  `ß`-to-`SS` expansion itself." to "Changing the `ß`-to-`SS` expansion, or adding a collision check
  for it." Keep the rest of the bullet. Change nothing in the delta.
