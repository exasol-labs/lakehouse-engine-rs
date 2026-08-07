# Plan Review Findings: fix-e2e-harness-undeclared-limit (round 2)

## Summary
- Axes checked: 6/6
- Total findings: 5 (Blockers: 1, Advisory: 4)
- Intent Fidelity blockers: 1

## Round-1 Blocker Recheck

- **Partially resolved: [SCOPE_REDUCTION] Phase 4 authorized deferring any unmasked defect.** The
  escape route is gone. `tasks.md` § Phase 4 step 3 now reads "**A test is IN SCOPE if and only if
  it is one of:**" with three enumerated clauses, two branches carrying explicit prohibitions
  ("MUST NOT be closed by re-adding a cap that hides the behavior underneath"; "Do NOT attempt a
  production-code fix for an out-of-scope test"), and no escalation path ("This branch needs no
  escalation and no judgment call"). `plan.md` § Design → Non-Goals (`:83-90`), § Implementation
  Tasks phase 4 (`:204-206`), § Impact (`:152-156`), and decision `[7]` were all rewritten to match
  the user's verbatim rule. Clauses (ii) and (iii) are mechanical — clause (ii) names two test
  functions that exist (`e2e_join_test.rs:111`, `:137`), clause (iii) enumerates six call sites, one
  comment, and four task numbers. **Clause (i) is not mechanical**, and it selects from a
  near-universal population. See the new BLOCKER below.
- **Resolved: [INTENT_DRIFT] the recorded interview carried the forbidden out-of-scope reference.**
  `decision-log.md` § Interview lines 17-19 now read Q: "How should the plan handle the issue's final
  *Investigation not done* bullet?" / A: "Out of scope for this plan; it is not addressed in any
  artifact." A case-insensitive search of the whole plan directory for `sibling`, `companion
  project`, and adjacent repo-name forms returns exactly two classes of hit: `tasks.md:19`'s
  "sibling `*_tests.rs` file" (the unrelated CLAUDE.md test-layout rule) and this reviewer's own
  `review/round-1.md`. `plan.md` and both spec deltas are clean. The planner's self-reported
  near-miss did not survive into the artifacts.
- **Resolved: [HIDDEN_DEPENDENCY] Phase 1 could not run before Phase 3.** New task 1.0 adds
  `capped_result_sets(max_rows: u32)` and states "This change is **purely additive** — leave
  `connect_inner`'s `10000` default and `unbounded_result_sets` exactly as they are", declares
  "**Blocks tasks 1.1, 1.2 and 1.6**", and carries the design-intent doc comment. Task 3.1 is
  reduced to the flip plus the `unbounded_result_sets` deletion and states "`capped_result_sets`
  already exists from task 1.0". Tasks 1.1 and 1.2 each end with "Depends on task 1.0".
  `plan.md` § Parallelization lists "Task 1.0 → Group A" with the private-field reason, and
  § Implementation Tasks phase 1 repeats it. The ordering inversion is gone: nothing in Phase 1 now
  needs a symbol Phase 3 creates. Verified against the code — `ExaConn`'s `result_set_max_rows` is a
  private field and `unbounded_result_sets` (`exasol_ws.rs:137-144`) has the one-line body
  `self.result_set_max_rows = 0;`.
- **Resolved: [UNSTATED_ASSUMPTION] the Phase-2 gate rested on unverified byte arithmetic.** No
  artifact now asserts that `high_card_probe` chunks at 64 MiB. New task 1.6 says "**Do not compute
  this figure — run it**" and "No artifact in this plan may assert a value for this number before
  the run". `plan.md` § Context (`:38`) and § Verification (`:258-260`) both defer to task 1.6, and
  the round-1 claim "the only fixture large enough to exercise a multi-response fetch" is deleted.
  Decision `[4]` now states the ordering holds under either measurement outcome. Task 2.1 forces
  chunking at `num_bytes = 65_536` and asserts `rows == HIGH_CARD_ROWS` plus `responses >= 2`, which
  is sound: `seed.rs:2267` gives `HIGH_CARD_ROWS = 30_000` and `:2378` "unique 100-byte `token`
  values", so the result set is ~3 MB and exceeds a 64 KiB budget by ~46×, independent of
  server-side row packing. The spec delta's third-scenario GIVEN is now satisfiable ("a `numBytes`
  fetch budget smaller than the bytes its result set occupies"). Task 2.2's delegate claim checks
  out exactly: the production signature `fetch_result_columns(&mut self, result_set: &Value) ->
  Vec<Vec<Value>>` matches the code at `exasol_ws.rs:177-216`, and the twelve external call sites
  are precisely the twelve the task lists, file and line. Residual sequencing and design objections
  are advisory below; the round-1 defect itself is fixed.
- **Resolved: [UNSTATED_ASSUMPTION] two compile gates named the wrong cargo feature.** Verified
  every crate-root gate directly: `e2e_azure_test.rs:37` is `#![cfg(feature = "azure-e2e")]`,
  `cloud_e2e_test.rs:24` is `cloud-e2e`, `e2e_lakekeeper_test.rs:37` is `lakekeeper-e2e`, and the
  remaining nine (`e2e_scan_test`, `e2e_capability_test`, `e2e_count_distinct_test`,
  `e2e_join_test`, `e2e_positional_deletes_test`, `e2e_int96_timestamp_test`, `e2e_refresh_test`,
  `e2e_non_ascii_identifier_test`, `e2e_capture_pushdown`) are `exasol-e2e` — exactly the map
  `tasks.md` § Rules now carries. `crates/lakehouse-engine/Cargo.toml` declares no
  `required-features` and no `[[test]]` block, so the "empty binary, meaningless exit 0" warning is
  accurate. Task 4.10 now says `--features azure-e2e` and cites the gate line; task 3.4 runs all
  four invocations and the false single-gate claim is replaced by which gate catches which file.
  `plan.md` § Checklist gained the matching per-feature row.
- **Resolved: [REQUIREMENT_CONFLICT] a permanent spec asserted the deleted default with no delta.**
  `specs/_plans/fix-e2e-harness-undeclared-limit/e2e-harness/lakekeeper-e2e-harness/spec.md` exists,
  rewrites the Background bullet under `DELTA:CHANGED`, and carries the affected scenario under a
  second `DELTA:CHANGED` block. The rewritten bullet names `capped_result_sets(n)` and no longer
  names `unbounded_result_sets`, and it keeps the row-fetch-time point. The restated scenario is
  faithful: its GIVEN, WHEN, THEN and five trailing `AND` steps are byte-identical to the recorded
  scenario at `specs/e2e-harness/lakekeeper-e2e-harness/spec.md:119`, with two clauses added. Its
  test `lakekeeper_vended_broadcast_join_result_correct` exists at `e2e_lakekeeper_test.rs:882`,
  containing the `.unbounded_result_sets()` call at `:884` that task 3.2 deletes. `plan.md`
  § Features carries the CHANGED row, and task 3.5 lands the delta with task 3.2. The
  `cloud-e2e-harness` no-change claim is confirmed independently: `grep -niE
  'resultSetMaxRows|10000|unbounded_result_sets|row cap'` over that file returns nothing, and a
  repo-wide search across all recorded `spec.md` files returns exactly one hit —
  `lakekeeper-e2e-harness/spec.md:61`, the bullet this delta rewrites. No recorded spec references
  `fetch_result_columns`.

**Validator status confirmed as described, not escalated.** `speq plan validate
fix-e2e-harness-undeclared-limit` exits 0: "validation passed", 0 errors, one warning — the
Lakekeeper scenario's 7 `AND` steps against a recommendation of 3 or fewer. The recorded feature
already warns on the same scenario at 5 `AND` steps, and on three of its other four scenarios, so
the delta inherits a pre-existing pattern rather than introducing a defect. Warning only. Not a
finding.

## Intent Fidelity

Certified where it holds: the user's round-2 rule is transcribed verbatim into `decision-log.md`
§ Review Findings, and the two branches implement it faithfully — in-scope gets a real fix with
production code permitted and re-capping forbidden, out-of-scope gets `gh issue create` + explicit
`capped_result_sets(n)` with production fixes forbidden, in that order, every time. No escalation
path survives anywhere. Phase 4 remains open-ended by construction, and task 4.12 still re-runs both
suites after every per-binary fix. No fabricated measurement results appear in any artifact; task
1.2 and task 1.6 both defer to the live stack.

#### [SCOPE_REDUCTION] BLOCKER
- Location: tasks.md § Phase 4, loop step 3, clause (i); plan.md § Design → Non-Goals
- Issue: the membership test the user demanded be "firm, mechanical, no-judgment-call" turns on an
  undefined phrase in its broadest clause. Clause (i) reads "a test exercising one of the seven
  statement shapes measured in Phase 1, **for that shape's own assertion**". The seven shapes are
  "bare projection; projection + filter; single-group aggregate; `GROUP BY` aggregate;
  `COUNT(DISTINCT)`; `ORDER BY … LIMIT`; broadcast-eligible inner equi-join" (task 1.2) — which is
  substantially every statement any E2E test in this repository issues. The narrowing work is
  therefore done entirely by "for that shape's own assertion", and that phrase is defined nowhere:
  not in clause (i), not in `plan.md` § Design → Non-Goals (which repeats the same undefined phrase,
  "the seven Phase-1 shapes' own assertions"), and not in decision `[7]`. Take a red test in
  `e2e_capability_test` (69 `exa_conn()` sites) that issues a projection + filter to assert a
  predicate-pushdown capability. Read broadly, the statement is shape 2, so the test is IN SCOPE:
  fix it properly, production code is fair game, re-capping is forbidden. Read narrowly, the
  assertion's subject is the capability rather than the shape, so the test is OUT OF SCOPE: file an
  issue, re-cap, and touch no production code. Both readings are lawful under the text, they
  prescribe opposite actions, and they diverge on the two questions the user's rule exists to settle
  — whether production code changes and whether a GitHub issue gets filed. Across the 54-site and
  69-site binaries this is the classification decision an implementer makes dozens of times, and the
  narrow reading silently returns most of the remediation surface to the deferral branch the user's
  round-1 resolution closed.
- Fix: In tasks.md § Phase 4 step 3, replace clause (i) with an artifact-anchored lookup: a test is
  IN SCOPE under (i) if and only if it appears in the affected-assertion list task 1.4 records —
  i.e. task 1.3 measured its statement's shape as gaining a `limit`, and task 1.4 found its
  assertion prose does not describe a limit. State that a red test whose shape is one of the seven
  but which task 1.4 did not list is OUT OF SCOPE, and that task 1.4's list is amended in place
  (with the reason) if Phase 4 shows it missed a test, so the classification stays a lookup rather
  than a re-judgment. In plan.md § Design → Non-Goals, replace "the seven Phase-1 shapes' own
  assertions" with "the tests task 1.4's affected-assertion list names" and cross-reference tasks.md
  § Phase 4 step 3. Add a sentence to tasks.md task 1.4 stating that its output is Phase 4's
  in-scope roster, not only a size prediction.

## Feasibility

Certified: the four compile gates, the crate-root feature map, the absence of `required-features`,
`common/mod.rs:16`'s `#![allow(dead_code)]` (so Phase 1's third coexisting knob and Phase 3's
surviving `capped_result_sets` raise no clippy warning against the checklist's "0 warnings"), the
private `result_set_max_rows` field, `unbounded_result_sets`' one-line body, the twelve
`fetch_result_columns` call sites, `HIGH_CARD_ROWS = 30_000` at ~100 bytes, and all three test
functions named in Phase 4 clause (ii) and task 3.5. Task 2.1's `responses >= 2` invariant holds at
`num_bytes = 65_536` on total-bytes grounds alone, so it cannot be falsified by whatever task 1.6
measures. No new ExaConn-constructing binary was missed: an independent per-file census over
`crates/lakehouse-engine/tests/*.rs` finds `exa_conn()` or `ExaConn::connect*` in exactly the twelve
binaries the plan covers, and `tpch_loader` / `two_entry_points_test` construct neither.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: tasks.md task 2.1 (final sentences); tasks.md task 2.2
- Issue: task 2.1's stated red state is unreachable in the order the plan gives. The task says
  "Against the unfixed single-fetch reader this test MUST fail on assertion (a)" — but the test
  reads "through task 2.2's `numBytes`-parameterized entry point with `num_bytes = 65_536`" and
  asserts on a response count, and neither
  `fetch_result_columns_with_num_bytes(&mut self, result_set: &Value, num_bytes: u64) ->
  (Vec<Vec<Value>>, usize)` nor its returned count exists until task 2.2 runs. Against the actual
  unfixed reader (`exasol_ws.rs:177-216`, one `fetch`, no count returned) the test does not fail on
  assertion (a); it fails to compile. Task 2.2 then bundles the signature extraction and the loop
  into one `[expert]` task, so the plan never authorizes the intermediate state where the new
  signature exists over a still-single-fetch body — the only state in which the claimed assertion
  failure is observable. An implementer will most likely improvise that split, but the plan asserts
  a red state it does not sequence.
- Fix: In tasks.md task 2.2, split the work into two ordered steps and state them explicitly: (2.2a)
  extract `fetch_result_columns_with_num_bytes(…) -> (Vec<Vec<Value>>, usize)` around the existing
  single-`fetch` body, honouring the passed `num_bytes` and returning `1`, and add the
  `fetch_result_columns` delegate at `67_108_864`; (2.2b) replace the single fetch with the
  accumulate-until-`numRows` loop. Move 2.2a ahead of task 2.1 in § Phase 2 and note that 2.2a is
  purely mechanical and changes no behavior. In task 2.1, replace "Against the unfixed single-fetch
  reader this test MUST fail on assertion (a)" with "Against task 2.2a's extracted-but-still-single-
  fetch reader this test MUST fail on assertion (a), returning one response's rows instead of 30,000".

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: tasks.md task 1.6; tasks.md task 1.0; plan.md § Parallelization
- Issue: task 1.6 names no vehicle for the figure it forbids computing, and the gate it imposes on
  task 2.1 is decorative. The task requires reporting "the rows one response returned **and** how
  many responses the full result set took" at the present 64 MiB budget, but nothing in the harness
  today reports either: `fetch_result_columns` issues one `fetch` and discards the response
  metadata, and `query_columns` (`exasol_ws.rs:168-175`) just delegates to it. The response count is
  first obtainable from task 2.2's new return tuple — a task that task 1.6 blocks, through 2.1. A
  careful implementer can extract the rows-per-response figure from today's truncating reader (its
  single-`fetch` return *is* one response's rows) and derive the response count from it, but the
  plan states neither, while insisting "**Do not compute this figure — run it**". Separately,
  `plan.md` § Parallelization justifies the gate as "task 2.1's multi-response expectation cites
  1.6's measured rows-per-response figure as the basis for its `numBytes` choice", yet task 2.1
  fixes `num_bytes = 65_536` from its own arithmetic and its `responses >= 2` invariant is
  packing-independent. The gate therefore serialises the critical path behind an under-specified
  measurement without protecting anything. Task 1.0 compounds the confusion by claiming "Tasks 1.1,
  1.2 **and 1.6** must declare a small cap distinguishable from both `0` and `10000`" — task 1.6
  runs an *uncapped* scan and needs no such cap, so its dependency on 1.0 is spurious.
- Fix: In tasks.md task 1.6, name the vehicle: state that the present single-`fetch`
  `fetch_result_columns` returns exactly one response's rows, so an uncapped `query_columns` over
  `high_card_probe`'s `token` column measures rows-per-response directly, and that the response
  count is that figure divided into `HIGH_CARD_ROWS` — recorded as derived, not as a second
  measurement. Delete "**Blocks task 2.1**" from task 1.6 and "Depends on task 1.0" with it. In
  plan.md § Parallelization, delete the "Task 1.6 → Task 2.1" row and add a line stating that task
  2.1's `responses >= 2` invariant follows from the fixture's ~3 MB total against a 64 KiB budget
  and does not depend on task 1.6's figure. In tasks.md task 1.0, drop `1.6` from the "must declare
  a small cap" sentence and from "**Blocks tasks 1.1, 1.2 and 1.6**".

## Requirement Quality

Certified: both deltas validate with 0 errors. The `e2e-harness` delta's three scenarios each carry
a satisfiable GIVEN, a single WHEN, and testable THEN/AND clauses, and the third scenario's GIVEN no
longer depends on the unverified 64 MiB premise. The Lakekeeper delta's restated scenario preserves
every recorded clause verbatim and adds two, so `/speq:record` will merge a Background bullet and a
scenario that both describe the post-plan code. No delta contradicts another, and no recorded spec
outside `lakekeeper-e2e-harness/spec.md:61` records the cap behavior.

#### [COMPLETENESS_GAP] ADVISORY
- Location: tasks.md task 5.1; plan.md § Verification → Checklist
- Issue: task 5.1 adds a new E2E binary without requiring the crate-root feature gate every other
  E2E binary carries, and the checklist row it breaks is the cheapest one to run. The task says "Add
  `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs` and register it in the `make test-e2e`
  target's binary list" and stops there. `Makefile:80` runs `cargo test --features exasol-e2e
  --test …`, and all fourteen existing test binaries open with `#![cfg(feature = "…")]`, which is
  why the checklist's "Unit test | `cargo test` | 0 failures" row passes today — a bare `cargo test`
  compiles every E2E binary to nothing. A new file without that attribute runs under bare
  `cargo test`, has no stack, and fails. `tasks.md` § Rules now states the gating map for existing
  binaries but says nothing about a new one.
- Fix: In tasks.md task 5.1, require the new file to open with `#![cfg(feature = "exasol-e2e")]`
  before any other item, and state the reason: an ungated test binary runs under bare `cargo test`,
  where no Exasol stack exists, and would fail the plan's own `cargo test` checklist row. Add the
  same requirement as a sentence in tasks.md § Rules, alongside the existing feature-gate map.

## Task Breakdown

Certified — no objection. Traceability is intact in both directions after the revisions: the new
Lakekeeper delta has an implementing task (3.5), task 1.0 implements the `capped_result_sets` half
of the `e2e-harness` delta's first Background bullet, task 2.2 implements the second, and tasks 5.2 /
5.3 / 2.1 implement the three new scenarios. No task implements anything outside the two deltas. The
task 1.0 split did not orphan anything: task 3.1's residue (flip plus deletion) is still traced to
the first Background bullet, and task 1.5 picks up the doc-comment cross-reference that could not
exist at 1.0 time. Phase 4 still covers every ExaConn-constructing binary, and the independent
per-file census confirms the per-binary counts in tasks 4.1-4.11 exactly (54, 69, 19, 12, 9, 7, 2, 1,
8+1, 3+1, 5).

## Design Depth

Certified where it holds: the opt-out → opt-in inversion is unchanged and remains the substance of
the fix; task 1.0 gives the leaked fact one documented owner and task 1.5 wires the cross-reference;
the delegate keeps twelve call sites untouched rather than churning them for a test's benefit, and
the shared loop means the delegate's `67_108_864` constant is the only thing those twelve sites do
not re-exercise — Phase 4 and task 4.12 run all of them. No boundary violation; the change stays
inside test code.

#### [INFORMATION_LEAKAGE] ADVISORY
- Location: tasks.md task 2.2 (the returned tuple); tasks.md task 2.1 assertion (b)
- Issue: the reader's internal chunking is promoted to its public return type to satisfy one
  assertion that adds no coverage. Task 2.2 specifies `fetch_result_columns_with_num_bytes(…) ->
  (Vec<Vec<Value>>, usize)`, "whose second element is the number of `fetch` responses consumed", and
  task 2.1 asserts `responses >= 2` on it. How many round-trips the reader used is exactly the
  decision the helper exists to hide, and it is now visible to every future caller. The assertion is
  also redundant: at `num_bytes = 65_536` against a ~3 MB result set, a single-`fetch` reader
  returns a few hundred rows, so assertion (a) — `rows == HIGH_CARD_ROWS` — already fails, and it
  fails for the exact defect the task removes. Task 2.1 itself concedes the count is not
  load-bearing ("Do not assert an exact response count").
- Fix: In tasks.md task 2.2, change the entry point to
  `fetch_result_columns_with_num_bytes(&mut self, result_set: &Value, num_bytes: u64) ->
  Vec<Vec<Value>>` and delete the response-count element and the "whose second element" clause;
  keep `fetch_result_columns` as the delegate at `67_108_864`. In tasks.md task 2.1, delete
  assertion (b) and the `responses >= 2` invariant, and state that `rows == HIGH_CARD_ROWS` at
  `num_bytes = 65_536` is the whole red condition because a single-`fetch` reader returns one 64 KiB
  response's worth of rows against a ~3 MB result set. Keep the citation of task 1.6's figure in the
  test's doc comment as the basis for the 64 KiB choice.

## Prose Quality

Certified — no objection. The revisions read cleanly and front-load their conclusions: task 1.0
opens with what it adds and why, Phase 4 step 3 leads with the membership test before the branches,
and `tasks.md` § Rules' new feature-gate bullet states the consequence ("produces an EMPTY binary
and a meaningless exit 0") before the instruction. The `plan.md` § Summary rewrite lands the BLUF in
its first sentence. Normative keywords are used sparingly and in the right register throughout the
new text ("MUST NOT be closed by re-adding a cap", "Do NOT attempt a production-code fix"). One
sentence in `plan.md` § Summary still runs long, and the round-1 census figure in § Context is still
`186` against a verified `185` — both were round-1 ADVISORY findings and are out of scope for this
round.
