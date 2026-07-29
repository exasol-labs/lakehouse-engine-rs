# Plan Review Findings: refactor-col-types-guard-dedup (round 8)

## Summary
- Axes checked: 6/6
- Total findings: 6 (Blockers: 2, Advisory: 4)
- Intent Fidelity blockers: 0

## Scope of this round

Revision review after the task 3 live gate refuted the plan's reachability premise. Effort is weighted on
the changed surface: plan.md § Impact / § Non-Goals / § Consequences / § Patterns / task 4 / new task 7,
decision-log decisions [12]-[14] and the two `[live-gate]` entries, the rewritten
`vs-adapter/pushdown-module-structure` captured-evidence bullets, and the NEW
`vs-adapter/create-virtual-schema` delta. The untouched 90% is re-read for revision damage only, not
re-litigated. Rounds 1-7 are untouched.

## Premortem

Three failure stories, each routed into the taxonomy:

1. **Six months out, a user's `straße`-named column silently loses LIKE pushdown and nobody notices.**
   The plan's new E2E test passed the whole time, because a declined filter returns the identical row
   set — the test asserted rows, not the pushed SQL. → Requirement Quality, BLOCKER 1.
2. **A recorder merges the `create-virtual-schema` delta and cannot tell what it changed.** Its own
   Background says it supersedes a clause that the delta carries verbatim, and counts one amended clause
   where three are added. → Requirement Quality, BLOCKER 2.
3. **A `straße`-named LIST column returns Arrow-display text where the type-mapping contract promises
   JSON.** The delta certified "the scan side is unaffected" from a mechanism description that does not
   match the code. → Feasibility, ADVISORY 1.

## Revision-claim verification (independent, against the tree)

Every load-bearing new claim was re-derived rather than taken from the planner's report.

- **Root cause CONFIRMED.** `resolve_table_schema` folds at `file_resolution.rs:640`
  (`(f.name.to_uppercase(), exasol_ty)`), with the in-code reason at `:637-639`. Declaration at `:610`.
  So the plan's, decision [12]'s and delta bullet 80's citation set is accurate.
- **Fold-agreement sweep CONFIRMED, and correctly framed.** Compiled and ran the exact claim: over all
  1,112,064 Unicode scalar values `c`, `x = c.to_string().to_uppercase()` satisfies `x.to_uppercase() == x`
  AND `x.to_ascii_uppercase() == x`, zero exceptions. Both second folds are applied to the FIRST fold's
  output, so the multi-char `ß`→`SS` case is compared correctly — the review angle's specific concern does
  not bite. The count matches `0x110000` minus 2048 surrogates. Generalization to strings holds because
  Rust's `str::to_uppercase` maps per scalar (its only contextual case, final sigma, is in `to_lowercase`).
  Delta bullet 82's wording states exactly this and does not overclaim.
- **Capture (1)'s withdrawal is correctly framed as inconclusive, not disproved.** Bullet 83 says it is
  "withdrawn as evidence that Exasol normalizes an adapter-declared name" and that the observation "is
  equally consistent with Exasol echoing the declared name verbatim" — which is the accurate epistemic
  status, since every name it read was already uppercase ASCII. Capture (2) is correctly retained as
  still-true background about Exasol's NATIVE identifier fold, and bullet 84 names the one surviving
  residual (Exasol not de-normalizing an already-uppercase declared name). No softer "inconclusive"
  re-framing is needed — the delta already uses it. No objection on review angle 3.
- **Decision [13]'s rejection of alternative (b) CONFIRMED, not merely asserted.** Grepped for non-ASCII
  characters in string literals across `support.rs`, `joins/planning.rs`, `grouped_agg.rs`, `topn.rs`,
  `joins/sql_builders.rs`, `joins/rendering.rs`: every hit is an em-dash/ellipsis/arrow inside an assertion
  message, none is a column name. Task 1's test takes literal `col_types` slices and calls neither builder.
  So task 4's test really is the only assertion that would fail on a silent unification, and dropping it
  really would let task 5 pass the whole suite. Task 4's retained-but-reframed test is justified, and
  plan.md task 4, clause 128 and delta bullet 86 all state honestly that the literal is CONSTRUCTED and not
  delivered. No objection on review angle 2.
- **All five falsifiable grep baselines CONFIRMED against the tree:** `grep -cF 'find(|(n, _)| *n == name)'`
  → 3; the four-alternative predicate ERE over `adapter/` → 7 lines; `not yet wired|intended first consumer`
  → 1; `support.rs:411` → 1; `e2e_non_ascii_identifier_test` in Makefile → 0.
- **Task 7's infrastructure claims CONFIRMED.** `Makefile:77-78` enumerates `--test` binaries explicitly, so
  the Makefile line really is required and its `grep -c … Makefile` gate is falsifiable (0 today).
  `E2E_NAMESPACE` is at `seed.rs:66` exactly. `create_and_append` (`seed.rs:296`) takes `namespace` and
  `table_name` as `&str` and its `create_and_append_files` body creates the namespace when absent
  (`seed.rs:361-366`), so a brand-new namespace needs no extra plumbing. `VsProps::new(vs_name, namespace)`
  (`e2e_harness.rs:186`), `create_virtual_schema` (`:224`), `query_scalar_i64` (`exasol_ws.rs:138`) and
  `query_columns` (`:158`) all exist as named. `e2e_capture_pushdown.rs:26` declares
  `const VS_NAME = "MY_LAKEHOUSE"` and `:55-58` substitutes `{table}`, as § Manual Testing row 4 states.
  Review angle 6 is satisfied: the Makefile step is specified AND gated.
- **`flatten_table_name` CONFIRMED** at `adapter/tables.rs:29` with its `to_uppercase` at `:42`.
- **Recorded Background preserved.** All 9 recorded `create-virtual-schema` Background bullets are carried
  verbatim into the new delta (19 total); `comm` shows zero dropped. The amended scenario carries all 7
  recorded steps verbatim. The round-3 dropped-Background defect class did not recur.
- `speq feature validate`: 0 errors library-wide; warnings only, all pre-existing AND-step counts.

## Intent Fidelity

No objection — axis checked. The revision serves the user's two verbatim instructions rather than
substituting for them. "Take what we learned into account" is discharged by decisions [12]-[14], the
`[live-gate]` entries, and the corrected § Impact. "I would make sure that we have a e2e with a table or
column named `straße` and be sure that it works" is discharged by task 7, which seeds BOTH a `straße` table
name and a `straße` column name and drives them through a live `createVirtualSchema` — not a unit
approximation. Neither the new E2E task nor the fifth delta is `[SCOPE_CREEP]`: both trace directly to that
sentence, and CLAUDE.md's never-a-silent-gap rule requires the casing behavior the capture surfaced be
recorded rather than left unstated. § Non-Goals correctly fences what the addition does NOT do (no fold
change, no collision check) — though its heading contradicts its body, raised as ADVISORY 2. The original
"full scope now" interview answer still governs tasks 1, 2 and 5 unchanged.

## Feasibility

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `vs-adapter/create-virtual-schema` delta, Background bullet at `spec.md:50` ("The scan side is
  unaffected and needs no delta: `datafusion-scan/scan-execution-field-id-projection` already maps
  projection names back to the Parquet field casing case-insensitively, which is why an uppercased
  declaration still reaches the right Iceberg field.")
- Issue: the stated mechanism is not the actual one, and the conclusion has a narrow exception the bullet
  denies. The projection does not resolve case-insensitively — `build_alias_items`
  (`scan/sql_support.rs:15-27`) emits `"straße" AS "STRASSE"` using the SAME full-Unicode `to_uppercase`, and
  the outer projection then references the uppercase alias (`raw_scan.rs:369`). That path works, but for a
  different reason than the bullet gives. The ONE genuinely case-insensitive lookup on that path is the
  JSON-fallback decision at `raw_scan.rs:362-366`, which compares `col_name.to_lowercase()` against
  `f.name().to_lowercase()`. `"STRASSE".to_lowercase()` is `strasse` and `"straße".to_lowercase()` is
  `straße`, so the `find` misses and `unwrap_or(false)` silently decides "no cast needed". Failure scenario:
  an Iceberg table with a `straße`-named `list<int>` column is declared `VARCHAR(2000000)` by
  `resolve_table_schema`, but the scan omits the `CAST(… AS VARCHAR)`, so the List array reaches
  `convert.rs:152-156`'s display-string backstop instead of the JSON form the `datafusion-scan/type-mapping`
  contract promises — the same column named `strasse` gets the JSON form. Bullet 49 enumerates the `ß`
  expansion's consequences as exactly two (queryable only as `STRASSE`; unchecked duplicate-name collision)
  and this is a third. The plan's own standard is that an unnamed divergence is indistinguishable from a
  regression (bullet 85).
- Fix: In `specs/_plans/refactor-col-types-guard-dedup/vs-adapter/create-virtual-schema/spec.md`, replace
  the bullet at line 50 with two sentences. First, state the real mechanism: the scan wraps the listing table
  in an inner `SELECT "<parquet name>" AS "<UPPERCASE>"` built by `build_alias_items`
  (`scan/sql_support.rs`) using the SAME full-Unicode `to_uppercase`, so the uppercased declaration resolves
  by alias rather than by case-insensitive comparison. Second, name the one exception: the JSON-fallback
  decision at `scan/raw_scan.rs` matches on `to_lowercase()` forms, which a `ß`-expanded name cannot satisfy,
  so a `ß`-named column of an Exasol-incompatible Arrow type skips its `CAST(… AS VARCHAR)` and falls to the
  display-string backstop — a pre-existing gap this plan does not fix, filed as its own GitHub issue and
  cited inline in that bullet per CLAUDE.md's tracked-exception pattern. Add the same exception to bullet 49's
  trade-off enumeration. Do not change the fold and do not widen task 7.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `vs-adapter/create-virtual-schema` delta, Background bullet at `spec.md:53` ("The added
  scenario's fixture MUST live in its OWN Iceberg namespace, not in `e2e_lakehouse`. Every existing E2E
  virtual schema is created over `e2e_lakehouse`, so a table added there would appear in each of those
  suites' enumerations and could churn assertions this plan promises to leave untouched."); same reasoning in
  decision [14] alternative (c) and plan.md task 7.
- Issue: a `MUST` bound for the permanent library rests on an unverified "could". Half the reasoning IS
  verified and correct — reusing `typed_distinct_probe` would churn `e2e_capability_test.rs:817`
  (`assert_eq!(cols.len(), 10)`), which is why task 3's edit had to be reverted. The other half, that adding
  a NEW table to `e2e_lakehouse` would churn enumerations, is not: the only table-enumeration assertion in the
  suite is `e2e_scan_test.rs::e2e_create_vs_enumerates_namespace_tables` (`:2875-2899`), and it asserts
  `table_names.contains(&"EVENTS")` / `contains(&"LABELS")` — membership, not an exact set — so a third table
  would not churn it. The separate namespace is still defensible isolation, but the recorded justification
  overstates a risk the tree does not show, and the cost it buys (a whole extra E2E binary with its own SLC
  install, `.so` upload, script creation and VS creation) is real. This is the defect class round 6 raised
  against clause 121: a normative obligation attached to a contingent, unverified fact.
- Fix: In that bullet, replace "would appear in each of those suites' enumerations and could churn assertions
  this plan promises to leave untouched" with the verified statement plus the reason that actually holds:
  every existing E2E virtual schema is created over `e2e_lakehouse`, so a table added there enters every one
  of those schemas and every one of their `createVirtualSchema` `loadTable` round trips; the one existing
  table-enumeration assertion (`e2e_scan_test.rs`) tests membership rather than an exact set, so the isolation
  is chosen to keep the new fixture invisible to unrelated suites, not because a named assertion would break.
  Apply the identical correction to decision [14] alternative (c) and to plan.md task 7's "load-bearing"
  sentence. Keep the separate namespace and keep the `typed_distinct_probe` prohibition, whose reason IS
  verified — cite `e2e_capability_test.rs:817` for it.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] BLOCKER
- Location: `vs-adapter/create-virtual-schema` delta, new scenario "A non-ASCII Iceberg table and column name
  stay queryable end to end", clause at `spec.md:81` ("*AND* a `LIKE` predicate over that column SHALL return
  the correct subset of rows, so the type-rewrite guards resolve the column's Exasol type from a `col_types`
  entry whose name came through this fold — the one pushdown path whose `col_types` lookup issue #265
  consolidates"); same claim in plan.md task 7 and in § Verification → Scenario Coverage row 9.
- Issue: the assertion cannot observe the mechanism the clause states, so the requirement is not verifiable as
  written. A `col_types` lookup miss makes `guard_like_subject` return `None`, which declines the WHOLE
  top-level filter — and the recorded spec states three times that a decline is *correct*:
  `specs/vs-adapter/pushdown-planning-like-type-coercion/spec.md:23` ("Exasol then evaluates the entire
  predicate natively"), `:29` ("a decline is always correct"), and `:37` ("slower, never wrong"). So "returns
  the correct subset of rows" holds identically whether the guard resolved `VARCHAR` and pushed the LIKE down,
  or missed the lookup and declined it. The clause therefore passes under exactly the failure it exists to
  catch, and the plan's one link between its `straße` E2E coverage and its own guards is unfalsifiable. This
  is the defect class rounds 3, 4 and 5 each opened their BLOCKER on — a gate whose channel cannot discriminate
  the hypotheses — now recurring in a NEW clause bound for the permanent library. A second, compounding gap:
  neither task 7 nor the scenario names the `straße` column's Iceberg TYPE, and only a `string` column makes
  the clause satisfiable at all — a `long` column is declared `DECIMAL(20,0)`, which `guard_like_subject`
  declines BY DESIGN (`support.rs:684-686`), so the mechanism the clause asserts would be false by
  construction while the row-subset assertion still passed.
- Fix: In `specs/_plans/refactor-col-types-guard-dedup/vs-adapter/create-virtual-schema/spec.md`, amend the
  GIVEN step at line 75 to state that the `straße`-named column is an Iceberg `string` column carrying values
  with distinguishable prefixes, alongside the `id` column. Then split clause 81 into two: keep the row-subset
  assertion, and add a clause requiring that the adapter-GENERATED pushdown SQL for that `LIKE` query carry the
  predicate over `"STRASSE"` — proving the filter was pushed rather than declined, which is the only reading
  that distinguishes a resolved `col_types` lookup from a fail-safe decline. In plan.md task 7, add the
  corresponding assertion: capture the generated SQL with the existing `explain_virtual_sql` helper
  (`crates/lakehouse-engine/tests/common/e2e_harness.rs:282`) for the `LIKE` query and assert its scan-spec
  `filter` names the LIKE over `STRASSE`; state that a declined filter omits `filter` from the scan spec, which
  is what makes the assertion falsifiable. Add a § Manual Testing row for it. Update § Verification row 9 to
  name both the row-subset and the generated-SQL assertions.

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: `vs-adapter/create-virtual-schema` delta, Background bullets at `spec.md:44` ("This delta amends
  ONE clause of ONE scenario and adds ONE scenario") and `spec.md:45` ("This delta SUPERSEDES the clause
  '*AND* the adapter SHALL return a JSON response describing one virtual table per discovered Iceberg
  table — …'. … The amended form adds the field-name half and changes nothing else in it."); restated in
  plan.md § Features ("It amends one clause and adds one scenario"), decision [14] ("amending one clause of
  the enumeration scenario"), and § Verification → Scenario Coverage row 8's "(amended clause)" label.
- Issue: the delta's self-description does not match the block it authors, and its supersession bullet names a
  clause the delta retains verbatim. Diffed step by step against
  `specs/vs-adapter/create-virtual-schema/spec.md`: all SEVEN recorded steps of that scenario appear in the
  delta byte-for-byte, including the exact clause bullet 45 declares superseded. The delta ADDS three new
  `*AND*` clauses (the field-name fold, the single-owner clause, the `ß`-expansion trade-off) — so it amends
  ZERO clauses and adds THREE. Bullet 45's "The amended form adds the field-name half and changes nothing else
  in it" describes a merged clause that does not exist anywhere in the delta. Consequences are concrete: a
  recorder following bullet 45 must resolve a supersession whose target is unchanged, and the permanent library
  would carry a Background bullet asserting a supersession that never happened — the same drift decision [11]
  argues a delta exists to prevent, and the same self-inconsistency round 2's BLOCKER 2 and round 4's
  supersession-marker finding each caught in the sibling deltas.
- Fix: In `specs/_plans/refactor-col-types-guard-dedup/vs-adapter/create-virtual-schema/spec.md`, rewrite
  bullet 44 to read that the delta ADDS THREE clauses to one scenario and adds one scenario, amending no
  recorded clause. Replace bullet 45 entirely: delete the false supersession and state instead that the
  recorded scenario is complete about TABLE-name casing and silent about COLUMN-name casing, that every
  recorded step is carried verbatim, and that the three added clauses supply the field-name half without
  altering any existing step — keeping this delta's existing verbatim-quote style for the recorded clause it
  leaves standing. Apply the same count correction to plan.md § Features (the `create-virtual-schema`
  paragraph), to decision [14]'s Decision line, and to § Verification → Scenario Coverage row 8, whose
  "(amended clause)" label becomes "(added clauses)".

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: plan.md § Non-Goals, bullet 4 ("Documenting or changing the `ß`-to-`SS` expansion itself. Task 7
  adds a `vs-adapter/create-virtual-schema` clause stating that column names are declared through the same
  full-Unicode `to_uppercase` as table names, plus a live E2E scenario … the expansion is named as a
  deliberate Exasol-target trade-off.")
- Issue: the Non-Goal's heading declares documenting the expansion out of scope, and its own body plus the
  delta then document it normatively. Delta clause 68 is an explicit `SHALL`: "the full-Unicode fold's
  one-to-many expansions SHALL be recorded as a deliberate Exasol-target trade-off rather than left
  unstated: `ß` becomes `SS` …". Background bullet 49 does the same in prose. § Non-Goals is the scope
  contract a reader consults first, so a Non-Goal contradicted by a `SHALL` in the same plan is a scope
  statement that cannot be relied on. The intended distinction — changing the expansion is out, recording it
  is in — is recoverable from the body but not from the heading.
- Fix: In plan.md § Non-Goals, change that bullet's opening sentence from "Documenting or changing the
  `ß`-to-`SS` expansion itself." to "Changing the `ß`-to-`SS` expansion, or adding a collision check for it."
  Keep the rest of the bullet, which already states that task 7 records the expansion as a deliberate
  trade-off. Change nothing in the delta.

## Task Breakdown

No objection — axis checked. Task 7 is correctly placed and correctly fenced. Its file set
(`tests/common/seed.rs`, one new `tests/` binary, `Makefile`) intersects none of the `support.rs` writer set
(tasks 1, 2, 5) nor task 4's `joins/planning.rs`, so § Parallelization's "may run parallel to the support.rs
chain and task 4" holds in fact. Its predecessor edge on task 3 is real (the capture supplies the `STRASSE`
served form its assertions pin) and its successor edge into task 6 is real and stated in both directions
(task 6 "Must follow task 7 as well as task 5"; task 7 "It is NOT a predecessor of task 5"). tasks.md matches
plan.md: seven implementation entries, task 3 checked, `2.7` grouped with the parallel branch. Traceability is
complete — the new delta's amended scenario and new scenario each have a § Verification → Scenario Coverage
row and a § Manual Testing row, and every one of task 7's four assertions appears in the scenario. No task
implements nothing in scope. Task 7 is a single verifiable unit at one fixture, one binary, one Makefile line,
so no `[TASK_GRANULARITY]` split is warranted despite its length.

## Design Depth

No objection — axis checked. The revision adds no module, interface or boundary: task 7 adds test assets only,
and the new delta records existing behavior rather than relocating a decision. The design surface certified in
round 7 is unchanged — two seams held apart by the recorded non-optional `&str` contract, both builder wrappers
retained as partial applications, no test-only cross-module item, `adapter` → `types` dependency direction
intact. Decision [12]'s reason for keeping `fold_case` after the refutation is the correct one on this axis and
strengthens rather than weakens: unifying the folds would make `column_types`' correctness rest on
`resolve_table_schema`'s uppercasing, putting one module's decision inside another module's body — the
information leakage the plan exists to remove. The `fold_case` parameter is nonetheless a configuration
parameter the module declined to decide, so tracking its removal by issue rather than defending it is the right
disposition, and decision [4]'s "preserved divergence with a known end date" framing survives the rescope.
Task 7's own-namespace/own-binary isolation is more machinery than the alternative, but its cost is bounded and
its reason partly verified — raised as ADVISORY 2 on the recorded justification, not as a design objection.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Impact paragraph 2 sentence 3, § Impact paragraph 3 sentence 2, and § Non-Goals bullet 3
  sentence 3.
- Issue: the rewritten governed prose breaches the 25-word sentence cap in the two sections rounds 2, 5, 6 and
  7 each spent a finding enforcing, and § Impact is the section an approving architect reads first. Measured
  word by word: § Impact paragraph 3 sentence 2 runs 53 words ("Task 4's characterization test is retained on a
  constructed literal, because it is the only assertion in the repository that distinguishes the two folds and
  therefore the only guard against task 5's merge unifying them silently; its follow-up issue is rescoped from a
  correctness fix to removing `column_types`' `fold_case` parameter as dead flexibility."). § Impact paragraph 2
  sentence 3 runs 29 words ("This crate uppercases each Iceberg field name itself … where the two folds provably
  agree."). § Non-Goals bullet 3 sentence 3 runs 32 words ("The task 3 live capture proved the divergence
  unobservable through the adapter, so a follow-up issue tracks removing …"). Round 7 measured the pre-revision
  § Impact paragraph 2 at 24/21/24/24, so this is a regression introduced by the revision, not a chronic
  survivor. One separate stale-record nit in the same class, not raised as its own finding: the round-5
  decision-log prose entry still describes a § Summary shape two later rounds replaced ("The section stays at two
  sentences plus the byte-identity line"), which is round 7's unapplied ADVISORY 2 plus the revision's own
  § Summary edit.
- Fix: In plan.md § Impact paragraph 3, split sentence 2 at its semicolon into two sentences and split the
  first half at "because": state that task 4's test is retained on a constructed literal, then that it is the
  only assertion in the repository distinguishing the two folds and therefore the only guard on task 5's merge,
  then that its follow-up issue is rescoped to removing `fold_case` as dead flexibility. In § Impact paragraph
  2, split sentence 3 at "so": state the crate's own uppercasing with its citation, then that every name
  reaching a builder is already Unicode-uppercased where both folds agree. In § Non-Goals bullet 3, split
  sentence 3 at "so". Keep every claim; add none. Then apply round 7's still-unapplied ADVISORY 2 fix verbatim
  to the round-5 decision-log prose entry, updating its forward pointer to the round-6 entry.
