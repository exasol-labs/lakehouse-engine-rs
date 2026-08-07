# Plan Review Findings: fix-e2e-harness-undeclared-limit (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 12 (Blockers: 6, Advisory: 6)
- Intent Fidelity blockers: 2

## Premortem

Three ways this plan fails six months out:

1. **The flip lands, `/speq:record` merges the delta, and the permanent library carries a lie.**
   `specs/e2e-harness/lakekeeper-e2e-harness/spec.md:61` documents the 10000-row default and the
   `unbounded_result_sets()` opt-out as recorded behavior. The plan deletes both and files no delta
   for that feature. → B1.
2. **Phase 1 stalls on day one.** The measurement needs a connection declaring a cap of `5`. No
   such API exists before Phase 3, and the field is private. → B2.
3. **The truncation "fix" ships with a test that never could have failed.** 30,000 × 100-byte rows
   is ~3 MB against a 64 MiB `numBytes`; one fetch response returns the lot. → B3.

## Intent Fidelity

Certified positively where it holds: Phase 4 is genuinely open-ended — the phase header states
"open-ended by construction", task 1.4 is labelled "a prediction, not a bound", and task 4.12
re-runs both suites end to end after every per-binary fix. That structure honours "fix everything
unmasked" rather than substituting a fixed list. The `fetch_result_columns` work is **not** scope
creep: the defect is real (verified at `crates/lakehouse-engine/tests/common/exasol_ws.rs:177-216`
— one `fetch` at `startPosition: 0`, return value never compared against `numRows`), it is the same
file and the same defect class as the change, and no production code is touched. Its *gating*
rationale is what fails, under B3. The live measurement is correctly deferred to `/speq:implement`:
task 1.2 is `[expert]`, the Rules block says "Verify against the live stack, never from
documentation or code inspection", and plan.md § Implementation Tasks says "Nothing about the
result is pre-decided in this plan." No fabricated results anywhere.

#### [SCOPE_REDUCTION] BLOCKER
- Location: tasks.md § Phase 4, loop step 3(c); plan.md § Design → Non-Goals
- Issue: the plan authorizes exactly the deferral the user foreclosed. Step 3(c) reads "the test
  exposes a product defect the cap was hiding — record it, fix it if it is in this plan's scope,
  and raise an issue if it is not", and plan.md § Non-Goals forecloses the only route to fixing
  one: "changing adapter, scan, or pushdown production code". The interview answer was categorical:
  "if the live capture or the full E2E run surfaces failures outside those seven, they are still in
  scope and must be fixed here, not deferred to a follow-up issue." As written an implementer can
  lawfully leave an unmasked failure unfixed and file an issue instead.
- Fix: In tasks.md § Phase 4, rewrite loop step 3(c) to read that a production defect unmasked by
  the flip is fixed in this plan, with the single named exception of #307's already-tracked
  limit-disqualifies-broadcast behaviour, and that any other candidate deferral is escalated to the
  user for a decision rather than filed as a follow-up issue. In plan.md § Design → Non-Goals,
  narrow "changing adapter, scan, or pushdown production code" to "changing adapter, scan, or
  pushdown production code *except where a Phase 4 failure proves a defect the cap was hiding*",
  and cross-reference the revised step 3(c).

#### [INTENT_DRIFT] BLOCKER
- Location: decision-log.md § Interview, lines 17-20
- Issue: the sibling project appears in `decision-log.md`, which the user forbade by name. The
  recorded Q reads "How should the plan handle the issue's note about a sibling project's mirrored
  harness file?" and the A reads "Ignore that part of the issue entirely. Do not mention the
  sibling project anywhere in `plan.md`, `decision-log.md`, or the spec deltas." The instruction
  was "must not appear anywhere in plan.md, decision-log.md, or spec deltas"; the transcript
  entry is inside one of the three named files. `plan.md` and the spec delta are clean, and
  `tasks.md:19`'s "sibling `*_tests.rs` file" is the unrelated test-layout rule.
- Fix: In decision-log.md § Interview, replace the second Q/A pair with a formulation that names
  no sibling project — e.g. Q: "How should the plan handle the issue's final *Investigation not
  done* bullet?" A: "Out of scope for this plan; it is not addressed in any artifact." Change
  nothing else.

## Feasibility

Certified: `cloud_e2e_test`'s five `connect_redacting` sites all issue bounded statements
(`SELECT * … LIMIT 10` at :463, `COUNT(*)` at :480 and :523, `DESCRIBE` at :537, an aggregate at
:547, `LIMIT 5` at :613 and :854), so removing the cap carries no result-set blow-up risk against
shared SaaS staging — a real hazard the plan is not exposed to. Task 5.1's premise checks out:
`Makefile:79-80` carries an explicit `--test` list for `test-e2e`. Task 5.1/5.2's helpers exist —
`explain_virtual_sql` at `crates/lakehouse-engine/tests/common/e2e_harness.rs:285`,
`typed_distinct_probe` at 12 rows (`crates/lakehouse-engine/tests/common/seed.rs:2475`, `:2506`).

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: tasks.md § Phase 1, tasks 1.1 and 1.2; plan.md § Parallelization
- Issue: Phase 1 cannot run before Phase 3, inverting the plan's stated ordering. Task 1.1 says
  "Until task 3.1 lands, express the cap by calling the existing `ExaConn` knob", but the only
  existing knob is `unbounded_result_sets()`
  (`crates/lakehouse-engine/tests/common/exasol_ws.rs:142`), whose whole body is
  `self.result_set_max_rows = 0;` — it can express `0` and nothing else. `result_set_max_rows: u32`
  is a private field (`:20`, no `pub`), so the capture binary cannot set it directly either. Task
  1.2 requires the capped run to use "a small value distinguishable from any SQL `LIMIT` in the
  statement", i.e. some `n` ∉ {0, 10000}. No such value is expressible until task 3.1 adds
  `capped_result_sets(n)`. plan.md § Parallelization nonetheless asserts "Group A → Group C" and
  § Implementation Tasks calls Phase 1 the step that "Gates everything downstream."
- Fix: Split task 3.1 in tasks.md. Add a new task 1.0 that introduces
  `ExaConn::capped_result_sets(max_rows: u32)` **while leaving the `connect_inner` default at
  `10000` and `unbounded_result_sets` in place**, and make tasks 1.1/1.2 depend on it. Reduce task
  3.1 to the two remaining changes: flip `connect_inner` to `0` and delete
  `unbounded_result_sets`. In plan.md § Parallelization, add "Task 1.0 → Group A" to the
  sequential-dependency list, and in § Implementation Tasks amend phase 1's description to state
  that the declarable-cap knob lands first.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Verification → Scenario Coverage (the note beginning "The fetch-completeness
  test lives in `e2e_count_distinct_test`"); decision-log.md § Design Decisions [4]; tasks.md task 2.1
- Issue: the entire Phase-2-before-Phase-3 gate rests on a byte-math assumption the plan never
  states and never measures, and the arithmetic points the other way. plan.md calls
  `high_card_probe` "the only fixture large enough to exercise a multi-response fetch" and the spec
  delta's third scenario requires "a seeded table whose row count exceeds what a single Exasol
  `fetch` response returns". But `HIGH_CARD_ROWS = 30_000`
  (`crates/lakehouse-engine/tests/common/seed.rs:2267`) at "~100 bytes each" (`:2259`, `:2378`) is
  roughly 3 MB, against the harness's `numBytes: 67108864` — 64 MiB. One response almost certainly
  returns all 30,000 rows, which means task 2.1's "Write the failing test first" cannot fail first,
  the new scenario's GIVEN is unsatisfiable with this fixture, and decision [4]'s claim that
  "flipping first would trade a visible undeclared limit for an invisible client-side truncation"
  is unsupported — no existing fixture comes within an order of magnitude of 64 MiB. This is
  exactly the class of claim CLAUDE.md § Verification discipline forbids asserting without a live
  run.
- Fix: In tasks.md § Phase 1, add a task 1.6 that measures rows-per-`fetch`-response against the
  live Docker stack for an uncapped `high_card_probe` scan and records the figure in
  `injection-surface.md`. Then rewrite task 2.1 to force chunking deterministically rather than
  hoping for it — have the test drive `fetch_result_columns` with a `numBytes` small enough that
  the measured result set spans several responses (parameterise `numBytes` in task 2.2's rewrite),
  and state the expected response count in the task. In plan.md § Verification → Scenario
  Coverage, replace "the only fixture large enough to exercise a multi-response fetch" with the
  measured basis. In decision-log.md [4], restate the rationale on what the measurement shows: if
  truncation is unreachable with present fixtures, say so and reclassify Phase 2 as hardening that
  still precedes the flip, rather than claiming the flip makes truncation reachable.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: tasks.md task 4.10; tasks.md task 3.4
- Issue: the plan's compile-only verification for the binaries it cannot execute proves nothing,
  and its post-deletion compile gate misses two features. Task 4.10 instructs "compile it under
  `--features exasol-e2e`" for `e2e_azure_test` — but that file's first line is
  `#![cfg(feature = "azure-e2e")]` (`crates/lakehouse-engine/tests/e2e_azure_test.rs:37`), and no
  `required-features` entry exists in `crates/lakehouse-engine/Cargo.toml`, so
  `--features exasol-e2e` compiles an **empty** binary and exits 0. An implementer would report a
  green compile that type-checked none of the three `exa_conn()` sites or the `connect_redacting`
  site at `:649`. Task 3.4 compounds it: "A stale reference to the deleted method breaks only
  under `--features exasol-e2e`" is false — task 3.2 deletes a
  `.unbounded_result_sets()` call at `e2e_lakekeeper_test.rs:884`, and that file is
  `#![cfg(feature = "lakekeeper-e2e")]` (`:37`). The prescribed gate cannot catch a mistake in the
  very deletion it exists to verify.
- Fix: In tasks.md task 4.10, change the compile instruction to `--features azure-e2e`. In task
  4.11, keep `--features cloud-e2e` (correct). In task 3.4, replace "compile the E2E features
  explicitly" with the four explicit invocations —
  `cargo clippy --all-targets --features exasol-e2e`, `… --features lakekeeper-e2e`,
  `… --features azure-e2e`, `… --features cloud-e2e` — and delete the false claim that a stale
  reference "breaks only under `--features exasol-e2e`". Add a sentence to tasks.md § Rules
  stating that each E2E binary is gated by its own crate-root `#![cfg(feature = …)]`, so compiling
  under the wrong feature yields an empty binary and a meaningless exit 0.

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER
- Location: plan.md § Features (single row, `e2e-harness/e2e-harness`); missing delta for
  `e2e-harness/lakekeeper-e2e-harness`
- Issue: a recorded spec this plan falsifies gets no delta. `specs/e2e-harness/lakekeeper-e2e-harness/spec.md:61`
  is a Background bullet reading "**The shared test harness's hardcoded `resultSetMaxRows: 10000`
  on every `execute()` call is a test-harness artifact that suppresses broadcast eligibility…**"
  and closing "The suite's dedicated connection for this scenario opts out via a SCOPED
  `ExaConn::unbounded_result_sets()` builder method, so the join genuinely reaches the broadcast
  path when its rows are fetched". After this plan: the hardcoded 10000 is gone, the named method
  is deleted (plan.md § Dead Code Removal), and the exact call site it describes is deleted (task
  3.2, `e2e_lakekeeper_test.rs:884`). That bullet is load-bearing for the recorded scenario
  "A two-table broadcast join over a vended-credential warehouse returns correct rows" (`:119`).
  `/speq:record` would leave the permanent library asserting a default that no longer exists and
  naming a symbol that no longer compiles. The planner's own census flagged the `:884` opt-out as
  the site the issue missed, but did not follow it to the spec that documents it.
- Fix: Author `specs/_plans/fix-e2e-harness-undeclared-limit/e2e-harness/lakekeeper-e2e-harness/spec.md`
  as a delta that rewrites the Background bullet at `:61` under `<!-- DELTA:CHANGED -->`: state
  that the harness declares no row cap by default, that a cap declared via `capped_result_sets`
  reaches the adapter as a pushdown `limit` and suppresses broadcast eligibility, and that this
  scenario's connection therefore needs no opt-out. Add a row for
  `e2e-harness/lakekeeper-e2e-harness` (status CHANGED) to plan.md § Features. Add a task to
  tasks.md § Phase 3 that lands this delta alongside task 3.2's deletion of the `:884` call.
  Re-check `specs/e2e-harness/cloud-e2e-harness/spec.md:21` and `:85` for the same exposure — both
  name `common/exasol_ws::ExaConn` but neither mentions the cap, so those two lines need no change;
  confirm that in the decision log.

#### [COMPLETENESS_GAP] ADVISORY
- Location: tasks.md task 2.2; spec delta § Scenarios, third scenario
- Issue: task 2.2 fixes only the `resultSetHandle` path and leaves the inline path able to truncate
  silently. `fetch_result_columns` opens with
  `if let Some(data) = result_set["data"].as_array() { return data… }` — an early return that never
  consults `numRows`. Exasol's `execute` response can carry a partial `data` prefix
  (`numRowsInMessage < numRows`) alongside a handle; the current branch order returns the prefix.
  The spec scenario's normative "the helper SHALL return exactly the row count the result-set
  metadata reports in `numRows`" is therefore stronger than the task that implements it.
- Fix: Extend tasks.md task 2.2 to cover the inline branch: take the inline `data` as the first
  accumulated chunk rather than as a complete answer, compare the accumulated row count against
  `numRows` on every path, and fall through to the fetch loop whenever rows remain and a
  `resultSetHandle` is present. State that the early return is removed, not preserved.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks, phase 1 ("Gates everything downstream. Nothing about
  the result is pre-decided in this plan."); spec delta § Scenarios, second scenario
- Issue: nothing downstream branches on the Phase 1 result, and one outcome is in fact pre-decided.
  The spec delta already asserts, as a normative SHALL, that a declared cap of `n` on a bare
  projection produces `limit` `n` — the very thing task 1.2 is meant to establish. The prediction
  is well-evidenced (issue #312: "A bare select evidently does"), so it will most likely hold; but
  the plan defines no action if it does not, which makes "gates everything downstream" decorative.
- Fix: Add a step to tasks.md task 1.3: if any measured shape contradicts the spec delta's second
  scenario, stop before Phase 3 and report the contradiction for re-planning rather than proceeding.
  Name that stop condition in plan.md § Implementation Tasks under phase 1.

## Task Breakdown

Certified: traceability is complete in both directions. All three new scenarios have implementing
tasks (5.2, 5.3, 2.1) and both new Background bullets have tasks (3.1, 2.2); no task implements
anything outside the delta. Phase 4 covers all eleven `ExaConn`-constructing test binaries — an
independent count confirms `exa_conn()` appears only in `e2e_scan_test` (54), `e2e_capability_test`
(69), `e2e_join_test` (19), `e2e_count_distinct_test` (12), `e2e_positional_deletes_test` (9),
`e2e_refresh_test` (7), `e2e_lakekeeper_test` (8), `e2e_azure_test` (3), `e2e_int96_timestamp_test`
(2), `e2e_non_ascii_identifier_test` (1), `e2e_capture_pushdown` (1), with `cloud_e2e_test` reaching
`ExaConn` only through `connect_redacting` — and `e2e_capture_pushdown` is covered by Phase 1
instead. All eight binaries in `Makefile:80`'s `test-e2e` list appear as 4.1-4.8.

#### [TASK_GRANULARITY] ADVISORY
- Location: tasks.md tasks 4.1 and 4.2
- Issue: one checkbox each for "fix every unmasked failure" across a 54-site and a 69-site binary.
  Neither is verifiable as a unit, and neither leaves a record of how many assertions were touched
  — which matters more than usual here, because the user accepted unbounded remediation and will
  want to see what it cost. Open-endedness is the correct shape; the missing part is the trail.
- Fix: Add a line to tasks.md § Phase 4's shared loop instructing the implementer to append a
  nested checkbox to the owning task for each newly-failing test as it is discovered
  (`- [ ] 4.2.a <test name> — resolved via (a|b|c)`), so the task list grows to match the real
  failure set instead of hiding it behind one checkbox.

#### [TRACEABILITY_GAP] ADVISORY
- Location: plan.md § Context → Measured census, row "`exa_conn()` call sites | 186"
- Issue: the census figure is wrong and disagrees with the plan's own task list. tasks.md's
  per-binary counts (4.1-4.10 plus `e2e_capture_pushdown`'s one site) sum to 185, and an
  independent count over the eleven binaries also gives 185. The table is presented as the plan's
  evidence of rigour — "Verified with Serena and repository search, not from the issue's
  approximations" — and it lands on the same number the issue approximated, off by one.
- Fix: Change the count in plan.md § Context → Measured census from 186 to 185, and add the
  per-binary breakdown already present in tasks.md so the two artifacts reconcile on their face.

## Design Depth

Certified: the opt-out → opt-in inversion is the right direction and is the substance of the fix —
a cap becomes visible where used and absent where not. The knob survives YAGNI on genuine grounds:
the Phase 5 regression test cannot assert "a declared cap becomes a pushdown limit" without a way
to declare one, and the capture-tool env var answers the issue's own complaint that the comparison
is unreproducible. No boundary violation — the change is confined to test code, and decision [6]'s
reasoning for skipping the Iceberg-spec check is sound as stated: CLAUDE.md scopes that rule to
features "that touch scanning, pushdown, or schema/type handling", and this plan touches a
test-only WebSocket client, its call sites, one diagnostic binary, and docs. Recording the
non-application explicitly, rather than skipping it silently, is the right handling.

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: tasks.md task 3.3; plan.md § Dead Code Removal (final paragraph)
- Issue: the comment being deleted carries two facts, and only one of them gets a new home. The
  comment at `crates/lakehouse-engine/tests/e2e_join_test.rs:113-117` reads "Same connection
  settings as `e2e_broadcast_join_result_correct` below, so the two tests pin ONE plan: the default
  10000-row cap reaches the adapter as a pushdown `limit`…". The cap clause dies with the flip; the
  lead clause — these two tests MUST use identical connection settings or the shape assertion and
  the correctness assertion describe different plans — survives intact and is exactly the kind of
  pair invariant a future editor breaks by adding `capped_result_sets` to one of them.
  `capped_result_sets`' doc comment cannot own it (it is a property of a test pair, not of the
  cap), and task 3.3's "say so in the commit body" is not a durable home. plan.md § Dead Code
  Removal then generalises the loss: "Any per-test comment discovered during phase 4 that explains
  the injected cap is dead for the same reason and MUST be removed rather than reworded."
- Fix: In tasks.md task 3.3, instruct the implementer to delete only the cap clause and keep a
  one-line comment stating the surviving invariant — that `e2e_broadcast_join_pushdown_shape` and
  `e2e_broadcast_join_result_correct` must share identical connection settings so they pin one
  plan. In plan.md § Dead Code Removal, qualify the final paragraph: a per-test comment's
  cap-explaining clause is removed, and any residual invariant it states is preserved rather than
  discarded with it.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: plan.md § Summary, second sentence
- Issue: 35 words against the 25-word guardrail, carrying four coordinated clauses ("Measure which
  pushdown shapes… flip the default… fix the latent single-`fetch` truncation… and repair every
  E2E assertion…"). The first sentence lands the BLUF cleanly; the second reads as the phase list
  compressed into one line.
- Fix: Split plan.md § Summary's second sentence in two, keeping the two-sentence Summary cap by
  folding the mechanism into the first sentence — e.g. "Stop the E2E WebSocket harness from
  attaching an invented `resultSetMaxRows: 10000` to every statement, so an E2E assertion
  describes the request it runs. Measure the injection surface live, default to Exasol's own `0`,
  fix the single-`fetch` truncation the cap was hiding, and repair every assertion the flip
  unmasks."
