# Decision Log: fix-e2e-harness-undeclared-limit

## Interview

**Q:** Issue #312 warns that flipping the harness default (10000-row cap → Exasol's uncapped
default) is expected to unmask currently-passing E2E tests that secretly depended on the injected
limit, and that the total remediation size is unknown until the capped-vs-uncapped capture is run.
How should this plan bound that work?

**A:** Fix everything unmasked. Flip the default, run the full E2E suite, and fix every test that
turns red as part of this same plan, regardless of how many that turns out to be. The task list
cannot be fully sized until the flip actually happens; that is accepted. Do NOT artificially bound
remediation to only the seven named shapes — if the live capture or the full E2E run surfaces
failures outside those seven, they are still in scope and must be fixed here, not deferred to a
follow-up issue.

**Q:** How should the plan handle the issue's final *Investigation not done* bullet?

**A:** Out of scope for this plan; it is not addressed in any artifact.

**Q:** How should code discovery and navigation be performed, during planning and during
implementation?

**A:** Use Serena's MCP symbolic tools throughout, per this project's `CLAUDE.md` code-navigation
rules — including for the `exa_conn()` call-site census, rather than trusting the issue's
approximate counts. Carry the same directive into `tasks.md` so implementer agents follow it too.

## Design Decisions

### [1] Default `result_set_max_rows` to `0` rather than making the attribute optional

- **Decision:** `connect_inner` initializes `result_set_max_rows: 0` and `execute` / `try_execute`
  keep sending the `resultSetMaxRows` attribute unconditionally.
- **Alternatives:** Change the field to `Option<u32>` and omit the `attributes` object entirely when
  no cap is declared. Rejected.
- **Rationale:** `0` is Exasol's own documented "no limit" default, and it is already proven against
  this server by the six existing opt-out call sites. Omitting the attribute is unverified here and
  would add a second variable to a change whose entire value is a clean unmasked-failure signal. The
  design-philosophy objection — that a configuration parameter is a decision the module declined to
  make — is satisfied either way: the harness stops inventing a value and adopts the protocol's own
  vocabulary. Revisit only if the Phase 1 measurement shows `0` and omitted behave differently.
- **Promotes to ADR:** no

### [2] Replace the opt-out knob with an opt-in one

- **Decision:** Delete `ExaConn::unbounded_result_sets()` and add
  `ExaConn::capped_result_sets(max_rows: u32)`.
- **Alternatives:** (a) Keep `unbounded_result_sets` as a no-op for source compatibility — rejected,
  it is dead code that invites the reader to believe a cap exists. (b) Delete the knob entirely on
  YAGNI grounds — rejected, see rationale.
- **Rationale:** Three present-day callers need a declarable cap: the Phase 1 measurement and its
  `e2e_capture_pushdown` `CAPTURE_RESULT_SET_MAX_ROWS` knob, the Phase 5 regression test that pins the
  measured injection surface, and Phase 4's remediation rule, which re-caps an individual failing test
  against a filed issue. All three exist in this plan, so the knob is not speculative. Inverting
  opt-out to opt-in is the substance of the fix: the cap becomes visible where it is used and absent
  where it is not.
- **Promotes to ADR:** yes

### [3] The `capped_result_sets` doc comment owns the leaked fact

- **Decision:** The new method's doc comment states that a declared cap reaches the adapter as a
  pushdown `limit`, so a capped session exercises a different adapter plan. Per-test comments that
  re-derive this fact are deleted, not reworded — starting with
  `crates/lakehouse-engine/tests/e2e_join_test.rs:113-117`.
- **Alternatives:** Leave the explanation at each call site that needs it.
- **Rationale:** The current state is textbook back-door information leakage: the harness makes a
  decision, and test files independently rediscover and re-document its consequence. Giving the
  decision one documented home means a future reader learns it once, at the point of use.
- **Promotes to ADR:** no

### [4] Fix the truncating result reader before flipping the default

- **Decision:** `fetch_result_columns` must read a result set to completion (Phase 2) before the
  default flips (Phase 3). Task 1.6 measures the rows one `fetch` response actually returns, and the
  Phase 2 artifacts state their expectations from that measurement rather than from byte arithmetic
  done at planning time.
- **Alternatives:** Flip first and fix truncation in a follow-up issue.
- **Rationale:** This defect is not named in #312 and was found during planning.
  `fetch_result_columns` issues exactly one `fetch` at `startPosition: 0` with
  `numBytes: 67108864`, ignores how many rows that response returned, then closes the handle. Its
  correctness rests on an upstream row bound, not on the reader — which is a defect regardless of
  whether any present-day fixture crosses the bound.
  Whether the flip makes truncation newly *reachable* is NOT asserted here. The largest fixture,
  `high_card_probe`, is 30,000 rows of ~100-byte tokens — roughly 3 MB against a 64 MiB budget — so
  one response plausibly returns the whole result set today. CLAUDE.md's verification discipline
  forbids settling that from arithmetic, so task 1.6 measures it against the live stack and records
  the figure in `injection-surface.md`.
  The ordering holds under either outcome. If the measurement shows truncation is unreachable with
  present fixtures, Phase 2 is hardening that still precedes the flip — defense in depth: the flip
  removes the bound the reader relies on, and the next fixture or the next larger query is then the
  one that silently short-reads. If the measurement shows it is reachable, Phase 2 additionally
  prevents an invisible client-side truncation from corrupting the unmasked-failure signal this plan
  depends on. Phase 2's test does not wait on that answer: it forces chunking with a small `numBytes`
  through the parameterized entry point, so it fails against the unfixed reader either way.
- **Promotes to ADR:** no

### [5] The measured injection surface lands in `docs/debugging-pushdown.md`

- **Decision:** Phase 1's shape matrix goes into the operator-facing capture-tool documentation, in
  addition to the plan's `injection-surface.md` evidence artifact.
- **Alternatives:** Keep it in the plan directory only; or put it in the feature spec's Background.
- **Rationale:** `/speq:record` archives the plan directory, so a plan-only record disappears from
  tracked space. The spec Background states the invariant, not the measurement — a spec should not
  carry a table of observed vendor behavior that a version bump could change.
  `docs/debugging-pushdown.md` already documents what the capture tool shows an operator, which
  makes it the correct permanent home.
- **Promotes to ADR:** no

### [6] No Iceberg-spec compliance check for this plan

- **Decision:** The mandatory Iceberg table-spec compliance check in `CLAUDE.md` does not apply. No
  section of the Apache Iceberg spec is quoted, and no tracked exception is recorded.
- **Alternatives:** Run the check anyway to satisfy the rule literally.
- **Rationale:** The rule scopes itself to features "that touch scanning, pushdown, or schema/type
  handling". This plan changes a test-only WebSocket client
  (`crates/lakehouse-engine/tests/common/exasol_ws.rs`), its call sites, one manual diagnostic
  binary, and documentation. No adapter, scan, or type-mapping production code changes. What changes
  is how a test configures its own request, not how the engine reads a table. Recorded explicitly
  rather than skipped silently, so a reviewer can overturn it.
- **Promotes to ADR:** no

### [7] Phase 4 is a repeated per-binary procedure, not an enumerated fix list

- **Decision:** Phase 4 carries one task per E2E binary, each running the same loop: run the binary
  under the flipped default, then close every newly-red test by filing a GitHub issue and declaring
  `capped_result_sets(n)` at that test's own call site. It enumerates no individual assertion fix,
  classifies no test, and makes no production-code fix.
- **Alternatives:** Enumerate the expected failures in the plan; or bound remediation to the seven
  measured shapes and defer the rest; or split remediation into an in-scope branch that fixes
  production code and an out-of-scope branch that re-caps.
- **Rationale:** The user chose "fix everything unmasked" and accepted that the work cannot be sized
  in advance. Enumerating fabricated failures would be worse than leaving the shape open, because a
  wrong enumeration reads as authoritative. One uniform rule replaces the two-branch classification
  scheme the user rejected as overcomplicated (see § Review Findings): nothing is hidden, because
  every re-cap is paired with a tracking issue, and no implementer needs a judgment call about which
  branch applies. Two binaries (`e2e_azure_test`, `cloud_e2e_test`) cannot be executed locally, so
  the plan requires their non-execution to be stated plainly in the verification report rather than
  glossed as passing.
- **Promotes to ADR:** yes

### [8] The capture tool gets an env knob, and the capture script gets no change

- **Decision:** `e2e_capture_pushdown` reads an optional `CAPTURE_RESULT_SET_MAX_ROWS`; unset means
  uncapped. `scripts/capture-pushdown-payload.sh` is not modified.
- **Alternatives:** Add a second positional argument or a `--max-rows` flag to the script; or perform
  the Phase 1 measurement by temporarily editing the harness.
- **Rationale:** The binary's own module doc says it is "driven entirely by the `CAPTURE_SQL` env
  var" so future issues can reuse it without edits; a second env var follows that established seam
  and the script inherits it for free. A temporary harness edit would leave the measurement
  unreproducible after the plan is archived, which is exactly the gap #312 complains about.
- **Promotes to ADR:** no

### [9] Scope excludes #307 and the broadcast-join limit disqualifier

- **Decision:** No change to `join_requires_exasol_postprocessing` or any other adapter behavior.
- **Alternatives:** Fix #307 in the same plan, since the two defects meet at the same symptom.
- **Rationale:** #312 and #307 are independent defects and #312 states either order works. Removing
  the injection changes what the tests exercise; changing the disqualifier changes what the product
  does. Combining them would make it impossible to attribute a newly-red E2E test to one cause,
  which defeats Phase 4's diagnostic loop.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] Phase 4 authorized deferring any unmasked defect, contradicting "fix everything unmasked"

- **Finding:** `plan-reviewer` (round 1, SCOPE_REDUCTION, BLOCKER, Intent Fidelity) flagged
  `tasks.md` § Phase 4 loop step 3(c) — "record it, fix it if it is in this plan's scope, and raise an
  issue if it is not" — as decorative and subvertible. It let an implementer reclassify any red test as
  out of scope and defer it, contradicting the interview answer "fix everything unmasked … not
  deferred to a follow-up issue". `plan.md` § Design → Non-Goals reinforced the escape by listing all
  production-code change as a non-goal.
- **User's resolution, verbatim:** "For any E2E test that is NOT part of this plan's
  deliberately-scoped remediation work (i.e., not one of the 7 measured shapes from the live capture,
  not the broadcast-join workaround pair `e2e_broadcast_join_pushdown_shape`/`e2e_broadcast_join_result_correct`,
  not any other test this plan's Phase 1-3 design work directly targets) — if that test starts failing
  once the default flips to uncapped: (1) file a GitHub issue documenting the exposed defect/behavior,
  referencing #312, and (2) add an explicit `capped_result_sets(n)` call to that specific test's
  connection setup so it passes again for the purposes of landing this ticket. Do NOT attempt a
  production-code fix for these — that is exactly what the filed issue is for. Tests that ARE part of
  this plan's deliberate scope (the 7 measured shapes' own assertions, the broadcast-join pair, and
  anything Phase 1-3's design work directly targets) must still be fixed properly per the plan's actual
  design — they must NOT be closed by simply re-adding a cap to hide the underlying behavior, since
  fixing exactly those is the point of this plan."
- **Direction change:** `tasks.md` § Phase 4 step 3 now states an explicit membership test for "in
  scope" (three enumerated clauses) followed by the two branches, each with its permitted outcomes and
  its prohibitions. Neither branch escalates to the user. Step 4 requires reporting which branch closed
  each test and the issue number for every out-of-scope re-cap. `plan.md` § Design → Non-Goals,
  § Implementation Tasks phase 4, and § Impact were rewritten to match: a production-code fix IS in
  scope for the plan's own targets and is out of scope elsewhere, handled by issue-plus-re-cap.
  Decision `[7]` was restated from a "preference order" to the mechanical branch rule.
- **Promotes to ADR:** yes

### [plan-review] The recorded interview carried an out-of-scope reference the artifacts must exclude

- **Finding:** `plan-reviewer` (round 1, INTENT_DRIFT, BLOCKER, Intent Fidelity) found that
  `decision-log.md` § Interview quoted a Q/A pair carrying the very reference the interview instructed
  the plan to exclude from `plan.md`, `decision-log.md`, and the spec deltas. `plan.md` and the spec
  delta were already clean; the recorded transcript was the only leak.
- **Direction change:** That Q/A pair was replaced with wording that carries no such reference — Q:
  "How should the plan handle the issue's final *Investigation not done* bullet?" A: "Out of scope for
  this plan; it is not addressed in any artifact." Nothing else in § Interview changed.
- **Promotes to ADR:** no

### [plan-review] Phase 1 could not run before Phase 3 — it needed a knob Phase 3 created

- **Finding:** `plan-reviewer` (round 1, HIDDEN_DEPENDENCY, BLOCKER) found the plan's own ordering
  inverted. Phase 1 must declare a cap distinguishable from both `0` and `10000`, but
  `ExaConn::result_set_max_rows` is a private field
  (`crates/lakehouse-engine/tests/common/exasol_ws.rs:21`) whose only setter,
  `unbounded_result_sets`, can express `0` and nothing else — and `capped_result_sets` did not exist
  until task 3.1, which the plan gated behind Phase 1 completing.
- **Direction change:** Task 3.1 was split. New task 1.0 adds `capped_result_sets(max_rows: u32)` as a
  purely additive change — `connect_inner`'s `10000` default and `unbounded_result_sets` both stay,
  so all three knobs coexist through Phases 1 and 2 — and carries the design-intent doc comment that
  used to live in 3.1. Tasks 1.1, 1.2 and 1.6 declare their dependency on it. Task 3.1 is reduced to
  flipping the default to `0` and deleting `unbounded_result_sets`. Task 1.5 now adds the doc comment's
  cross-reference to the shape matrix, since that section does not exist at 1.0 time. Task 3.4 no
  longer re-points task 1.1. `plan.md` § Parallelization lists "Task 1.0 → Group A"; § Implementation
  Tasks phase 1 states that the knob lands first and why.
- **Promotes to ADR:** no

### [plan-review] The Phase-2-before-Phase-3 gate rested on unverified byte arithmetic

- **Finding:** `plan-reviewer` (round 1, UNSTATED_ASSUMPTION, BLOCKER) showed the truncation premise
  was almost certainly wrong. `HIGH_CARD_ROWS = 30_000` rows of ~100-byte tokens
  (`crates/lakehouse-engine/tests/common/seed.rs:2267`, `:2378`) is roughly 3 MB against the harness's
  `numBytes: 67108864` (64 MiB), so one `fetch` response likely returns the whole result set. Three
  artifacts depended on the opposite: task 2.1's "write the failing test first" could not fail first,
  the new spec scenario's GIVEN clause was unsatisfiable, and decision `[4]` asserted the flip made
  truncation newly reachable with no live measurement — which CLAUDE.md's verification discipline
  forbids.
- **Direction change:** New task 1.6 measures rows per `fetch` response against the live Docker stack
  for an uncapped `high_card_probe` scan at the present 64 MiB budget and records it in
  `injection-surface.md`, with an explicit instruction not to compute the figure. Task 2.1 no longer
  hopes the fixture chunks: it forces chunking through task 2.2's `numBytes`-parameterized entry point
  at `num_bytes = 65_536` and asserts `responses >= 2` plus `rows == HIGH_CARD_ROWS`, deliberately not
  an exact response count. Task 2.2 gained that entry point —
  `fetch_result_columns_with_num_bytes(…) -> (Vec<Vec<Value>>, usize)` carrying the loop, with the
  existing two-argument `fetch_result_columns` kept as a delegate so the twelve external call sites
  (verified with Serena) stay unchanged; parameterizing the existing signature directly would have
  churned all twelve for no test benefit. `plan.md` § Verification and § Context now point at task 1.6's
  measured basis instead of claiming the fixture is the only one large enough. Decision `[4]` was
  restated: the reader's dependence on an upstream bound is the defect, the ordering holds under either
  measurement outcome, and Phase 2 is named as hardening-that-precedes-the-flip if truncation proves
  unreachable with present fixtures. The spec delta's GIVEN clause was reworded to a `numBytes` budget
  smaller than the result set, which is satisfiable.
- **Promotes to ADR:** no

### [plan-review] Two compile gates named the wrong cargo feature and would pass vacuously

- **Finding:** `plan-reviewer` (round 1, UNSTATED_ASSUMPTION, BLOCKER) found task 4.10 compiling
  `e2e_azure_test` under `--features exasol-e2e` while the file is gated
  `#![cfg(feature = "azure-e2e")]` (`crates/lakehouse-engine/tests/e2e_azure_test.rs:37`) with no
  `required-features` wiring in `Cargo.toml` — an empty binary and an exit 0 that type-checks nothing.
  Task 3.4's claim that a stale `unbounded_result_sets` reference "breaks only under
  `--features exasol-e2e`" was false for the same reason: task 3.2 deletes a call at
  `e2e_lakekeeper_test.rs:884`, and that file is gated `#![cfg(feature = "lakekeeper-e2e")]`.
- **Direction change:** Verified all four crate-root gates and the absence of `required-features` in
  `crates/lakehouse-engine/Cargo.toml`. Task 4.10 now says `--features azure-e2e` and states why the
  wrong flag is vacuous. Task 4.11 already said `--features cloud-e2e` and was left unchanged. Task 3.4
  now runs four explicit invocations — `exasol-e2e`, `lakekeeper-e2e`, `azure-e2e`, `cloud-e2e` — and
  the false single-gate claim is deleted, replaced by which gate catches which file. `tasks.md` § Rules
  gained a per-binary feature-gate map and the rule that compiling under the wrong flag yields an empty
  binary and a meaningless exit 0.
- **Promotes to ADR:** no

### [plan-review] A permanent spec asserted the deleted default and the deleted opt-out, with no delta

- **Finding:** `plan-reviewer` (round 1, REQUIREMENT_CONFLICT, BLOCKER) found
  `specs/e2e-harness/lakekeeper-e2e-harness/spec.md:61` — a recorded Background bullet documenting the
  hardcoded 10000-row default and the `unbounded_result_sets()` opt-out as current behavior, and
  load-bearing for the recorded scenario "A two-table broadcast join over a vended-credential warehouse
  returns correct rows" (`:119`). This plan deletes the default, the method, and the exact call site
  the bullet describes (`e2e_lakekeeper_test.rs:884`, task 3.2), but filed no delta for that feature.
  `/speq:record` would have merged a plan leaving the permanent library asserting behavior that no
  longer exists and naming a symbol that no longer compiles.
- **Direction change:** Authored
  `specs/_plans/fix-e2e-harness-undeclared-limit/e2e-harness/lakekeeper-e2e-harness/spec.md` as a
  `DELTA:CHANGED` rewrite of that bullet: the harness declares no cap by default, a cap declared through
  `capped_result_sets` reaches the adapter as a pushdown `limit` and suppresses broadcast eligibility,
  and this scenario's connection needs no opt-out call because uncapped is the default. The bullet keeps
  its point that row-fetch-time verification still matters, since `EXPLAIN VIRTUAL` never carried the
  limit. Added the `e2e-harness/lakekeeper-e2e-harness` row (CHANGED) to `plan.md` § Features and task
  3.5 to `tasks.md` § Phase 3, landing the delta alongside task 3.2.
- **Checked `specs/e2e-harness/cloud-e2e-harness/spec.md` as instructed:** `:21` and `:85` both name
  `common/exasol_ws::ExaConn`, but only for its redacting connect mode. A content search of that file
  for `resultSetMaxRows`, `10000`, `unbounded_result_sets`, and "row cap" returns nothing. No cap
  behavior is recorded there, so **that feature needs no delta.** Recorded here rather than skipped
  silently.
- **Promotes to ADR:** no

### [plan-review] One uniform Phase-4 rule replaces the two-branch scope classification

**This entry states the rule that governs Phase 4.** It supersedes the round-1 and round-2
`SCOPE_REDUCTION` resolutions recorded above, including their carve-out making
`e2e_broadcast_join_pushdown_shape` / `e2e_broadcast_join_result_correct` a mandatory-real-fix case.
Those entries stay as history; where they conflict with this one, this one wins.

- **Finding:** `plan-reviewer` (round 2, SCOPE_REDUCTION, BLOCKER, Intent Fidelity) attacked the
  round-1 fix's membership test. Clause (i) of `tasks.md` § Phase 4 step 3 — "a test exercising one of
  the seven statement shapes measured in Phase 1, for that shape's own assertion" — covered
  substantially every statement any E2E test in this repository issues, so all the narrowing rested on
  the undefined phrase "for that shape's own assertion". Two lawful readings of it prescribed opposite
  actions on the two questions the rule existed to settle: whether production code changes, and
  whether an issue gets filed. Round 2 proposed anchoring the test to task 1.4's affected-assertion
  list. The user instead rejected the whole two-branch structure as overcomplicated.
- **User's resolution, verbatim:** "We are only: flipping a default behaviour, so setMaxResults is not
  set. If any e2e test fails, we file an issue and add an explicit max results to that test only. What
  is so complicated about that?"
- **Direction change:** `tasks.md` § Phase 4 now carries one flat loop applied uniformly to every test
  in every binary: run the binary under the flipped default; for each newly-failing test file a GitHub
  issue referencing #312, add `capped_result_sets(n)` to that one test's connection, and move on. The
  three-clause membership test, the in-scope/out-of-scope distinction, and the "real fix, no re-cap"
  branch are deleted. No production-code fix is made in Phase 4; the filed issue owns that work. Task
  1.4's list survives as the *predicted* set of affected shapes and gates nothing. `plan.md` § Design
  → Non-Goals, § Implementation Tasks phase 4, and § Impact were rewritten to the flat rule, and
  decision `[7]` was restated from the two-branch rule to it.
- **Verified against the code, not assumed:** the broadcast-join pair needs no new opt-in call.
  `e2e_broadcast_join_pushdown_shape` (`crates/lakehouse-engine/tests/e2e_join_test.rs:118`) and
  `e2e_broadcast_join_result_correct` (`:139`) already call `exa_conn().unbounded_result_sets()`,
  which sets `result_set_max_rows = 0` (`common/exasol_ws.rs:142`). Task 3.1 makes a plain
  `exa_conn()` send that same `0`, so the flip changes nothing about the request either test issues.
  Their two calls become redundant and are deleted as dead code by task 3.2 — already listed in
  `plan.md` § Dead Code Removal. Neither test is expected to turn red; if one does, it takes the same
  file-issue-and-cap route as any other test.
- **Promotes to ADR:** yes

## Implementation Corrections

### [task 1.2] The plan's premise — a declared cap reaches the adapter as a pushdown `limit` — was verified live and found false

- **Finding:** Task 1.2's live capped-versus-uncapped capture (`injection-surface.md` §
  "Capped-versus-uncapped pushdown shape matrix") tested all seven statement shapes named in
  `plan.md` and found the `pushdownRequest`, the full adapter exchange, and the generated scan
  SQL/scan-spec JSON byte-identical between the capped and uncapped capture of every shape. No
  shape gained a `limit`. Controls c1/c3/c6 confirm the cap is delivered and honored at the
  statement level and does not corrupt per-shard aggregate results, ruling out both "the cap
  never reached the server" and "the chosen value was unlucky" as explanations.
- **Direction change:** `plan.md` § Context and § Design state the premise "Exasol converts
  that cap into a `pushdownRequest` `limit`" as the reason this harness defect matters. That
  claim is superseded by this measurement — a declared cap changes only the delivered result
  set, never the adapter request — but `plan.md` itself is not rewritten; this entry is the
  append-only record of the correction. Task 5.3
  (`declared_cap_reaches_adapter_as_pushdown_limit`) and the e2e-harness spec scenario it backs
  ("A declared row cap reaches the adapter as a pushdown limit") cannot pass or be recorded as
  specified and need re-authoring against the measured behavior, not the premise.
- **What is unaffected:** the design's actual value holds regardless of which premise motivated
  it. Replacing an invented default with an explicit, visible cap declaration (decisions
  `[1]`/`[2]`) and making the result reader read to completion (decision `[4]`) are correct on
  their own terms — an explicit `capped_result_sets(n)` call is still clearer than a silent
  default, and a truncating reader is still a defect independent of whether any adapter-visible
  plan changes.
- **Evidence:** `specs/_plans/fix-e2e-harness-undeclared-limit/injection-surface.md` §
  "Capped-versus-uncapped pushdown shape matrix" and § "Consequences for downstream tasks".
- **Promotes to ADR:** no

### [task 1.2, second correction] The `[task 1.2]` correction above was itself wrong — a declared cap DOES reach the adapter as a pushdown `limit` on a real execution

**This entry supersedes the `[task 1.2]` entry immediately above.** That entry is preserved
unedited, per this project's paper-trail convention — the correction below states what future
readers should trust instead, it does not retroactively rewrite what was previously recorded.

- **Finding:** The `[task 1.2]` correction concluded, from 20 `EXPLAIN VIRTUAL` captures across
  all seven statement shapes, that "no shape converts a declared cap into a pushdown `limit`." A
  domain expert challenged that conclusion during this correction round: `EXPLAIN VIRTUAL` and a
  real query execution are two different exchanges with the adapter, and `resultSetMaxRows` is an
  attribute of whichever statement is actually sent to the server. An `EXPLAIN VIRTUAL` wrapper
  statement is never the statement a declared cap is targeting, so its echoed `pushdownRequest`
  cannot carry a limit that only the real statement's own request gained — regardless of which
  shape is captured, and regardless of the cap's value. The `[task 1.2]` measurement therefore
  established only that `EXPLAIN VIRTUAL` looks identical under a cap, which is a fact about the
  tool, not a fact about the adapter.
  A second measurement, directly capturing the adapter's raw incoming request during a REAL query
  execution (temporary instrumentation at the adapter's request-receipt point, reverted after use —
  bypassing `EXPLAIN VIRTUAL` entirely), for all seven statement shapes from the original
  measurement, found the opposite of the `[task 1.2]` conclusion: a capped connection's real
  request gains `"limit": {"numElements": n}` where an identical uncapped request has none. See
  `injection-surface.md`'s new "Real-execution-path pushdown-limit capture" section for the exact
  diff evidence, method, and per-shape results.
  The adapter's handling of that limit, once present, is exactly what a careful reading of the
  adapter's own pushdown code predicts and what `#312`/`#307` context already implied: applied
  safely for a raw scan (a per-shard limit plus an outer `LIMIT` wrapper), correctly withheld from
  beneath an aggregate (outer `LIMIT` only, so the aggregate value stays correct), and, for a
  broadcast-eligible inner equi-join, disqualifying broadcast pushdown — `ANY` limit in the
  pushdown request trips `join_requires_exasol_postprocessing`
  (`crates/lakehouse-engine/src/adapter/pushdown/joins/planning.rs`), falling back to the
  unaccelerated two-scan (`LHS_T0`/`LHS_T1`) plan.
- **Direction change:** Every artifact the `[task 1.2]` correction touched was corrected a second
  time to state the now-confirmed mechanism instead of the `EXPLAIN VIRTUAL`-based non-finding:
  - `crates/lakehouse-engine/tests/common/exasol_ws.rs`'s `capped_result_sets` doc comment now
    states the confirmed mechanism and the per-shape adapter behavior, with the `EXPLAIN VIRTUAL`
    blind spot named explicitly.
  - `crates/lakehouse-engine/tests/e2e_join_test.rs`'s comment above
    `e2e_broadcast_join_pushdown_shape`/`e2e_broadcast_join_result_correct` — deleted by task 3.3 as
    "stale" under the `[task 1.2]` premise, then restored and rewritten to state why those two
    tests must stay uncapped under the confirmed mechanism.
  - A new regression test, `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan`
    (`e2e_join_test.rs`), proves the disqualification directly via a SQL `LIMIT` — the fully
    `EXPLAIN VIRTUAL`-observable form of the same check a declared `resultSetMaxRows` cap triggers
    less visibly.
  - `crates/lakehouse-engine/tests/e2e_harness_row_cap_test.rs`'s
    `declared_cap_truncates_delivered_result_set_not_pushdown_request` — itself a rename introduced
    by the `[task 1.2]` correction — is renamed again to `declared_cap_truncates_returned_row_count`
    and narrowed to assert only the delivered row count; its `EXPLAIN VIRTUAL`-plan-equality
    assertion is deleted, since that assertion's only evidentiary value rested on the disproven
    premise that `EXPLAIN VIRTUAL` equality means adapter-request equality.
  - `docs/debugging-pushdown.md`'s shape matrix is rewritten to state the confirmed mechanism, the
    `EXPLAIN VIRTUAL` blind spot, and an explicit operator warning: a join-plan capture via
    `scripts/capture-pushdown-payload.sh` will show a broadcast plan regardless of a declared cap,
    and does not reflect what a real capped query does to that join.
  - Both spec deltas (`e2e-harness/e2e-harness`, `e2e-harness/lakekeeper-e2e-harness`) had their
    Background bullets corrected to state the confirmed mechanism; the `e2e-harness/e2e-harness`
    scenario was renamed to match the test rename above.
  - `specs/_plans/fix-e2e-harness-undeclared-limit/injection-surface.md` gained a new section
    documenting the real-execution-path capture, marking the earlier `EXPLAIN VIRTUAL`-based
    "Capped-versus-uncapped pushdown shape matrix" section superseded without deleting it — the
    same non-destructive convention this entry follows.
- **What is unaffected:** everything decision `[1]`/`[2]`/`[4]` established stays correct on its
  own terms — an explicit, visible `capped_result_sets(n)` call is still clearer than a silent
  default regardless of which mechanism motivated the inversion, and the fetch-completeness fix is
  still a real defect fix independent of this correction. Decision `[9]`'s scope boundary also
  stays intact: `join_requires_exasol_postprocessing` is pre-existing, unchanged production code —
  this plan does not touch it, it only corrects how this plan's own artifacts describe it. Phase
  4's "zero newly-failing tests" verdict is unaffected for a different reason than `[task 1.2]`
  assumed: it was always scoped to what the *default flip* changes for tests that declare no cap,
  and no in-scope test's own connection declares one either way, so nothing about what a *declared*
  cap does changes that verdict.
- **Evidence:** `specs/_plans/fix-e2e-harness-undeclared-limit/injection-surface.md` § "Real-execution-path
  pushdown-limit capture" (new section: capture method, provenance, and the exact request diff for
  all seven shapes).
- **Promotes to ADR:** no
