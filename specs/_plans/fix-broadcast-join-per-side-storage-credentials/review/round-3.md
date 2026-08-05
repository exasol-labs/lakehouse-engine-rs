# Plan Review Findings: fix-broadcast-join-per-side-storage-credentials (round 3)

Round-numbering note: `review/round-1.md` and `review/round-2.md` are from the original
planning session's own internal review cycle (both already resolved before implementation
started). This round reviews a SEPARATE, later revision — decision-log's own "Round 2:
Alias-Qualification Fix and Gate Repair" (tasks 1.5-1.10, decisions [14]-[16]) — triggered by
task 1.4's escalation during implementation. Per the delegation brief, this file is round-3.md;
round-1.md and round-2.md are untouched.

## Summary

- Axes checked: 6/6
- Total findings: 6 (Blockers: 0, Advisory: 6)
- Intent Fidelity blockers: 0

## Premortem

1. **Task 1.9's pass/fail call is a human eyeball match against decision-log prose, not an
   automatable assertion, and the wire-level claim it rests on is unverified against a live
   system.** Whoever runs 1.9 decides "credential-shaped" vs. "still alias-shaped" by reading
   free-text error output and comparing it to the manually captured 403 in decision-log — no
   exact substring or status code is named. Task 1.8's `unbounded_result_sets()` rests on
   "`0` is Exasol's own documented default meaning 'no limit'... verified against the Exasol
   websocket-api spec during planning, not assumed" — documentation, not the live-Exasol
   verification CLAUDE.md's "Verification discipline" demands for a capability/behavior claim.
   If `0` behaves differently than documented, 1.9 can fail for a third, unrelated reason that
   still superficially "looks like a failure" and gets waved through. → Feasibility
   `[UNSTATED_ASSUMPTION]`.
2. **The permanently-recorded `vs-adapter/pushdown-planning-alias-stripping` spec goes stale the
   moment this plan records.** Its own Background names `strip_table_alias`'s callers as "the
   single-table path and the join fan-out's inner leg" (two sites). This plan adds a THIRD call
   site (`render_broadcast_join`) and touches neither that spec nor plan.md's § Features list (not
   CHANGED, not "checked and deliberately NOT amended"). A future engineer auditing "who calls
   `strip_table_alias` and why" via the spec library gets an incomplete answer. → Requirement
   Quality `[REQUIREMENT_CONFLICT]`.
3. **Impact's confident claim outlives its own hedge.** § Impact states, unqualified, that a
   broadcast join "starts working... including when the query aliases the joined tables... how
   most real client SQL is written." Three paragraphs later, § Non-Goals concedes that "whether a
   real client driver's own default fetch-size attribute similarly suppresses broadcast
   eligibility in practice is a separate, unstudied question this plan does not resolve" — the
   exact mechanism (a row-limit-like attribute forcing the safe fallback) this very round
   discovered suppresses the broadcast path for the ENTIRE existing 18-test harness. A reader of
   Impact alone gets a rosier picture than the plan's own risk assessment supports. → Intent
   Fidelity.

## Prior-Round Blocker Recheck

Round-1 and round-2's blockers were resolved before implementation began; re-verified here against
the CURRENT tree (not merely re-read from decision-log) because this round's edits touch adjacent
text.

- **Resolved, still honored (round-1 #1 — access-set enumeration wrong in both directions).**
  plan.md § Iceberg table-spec compliance's evidence table still states the corrected set verbatim
  ("**No test currently guards this zero-head property**", line 52) with the round-2 correction
  layered on top (see next item). Unchanged by this round's edits.
- **Resolved, still honored (round-1 #2 — failed gate had no route).** Task 1.3's contingency
  table (four named artifacts) is present unchanged; decision `[12]` intact.
- **Resolved, still honored (round-1 #3 — three conflicting answers on escalation).** Verified the
  identical rule now appears at the § Implementation Tasks preamble (line 241), task 1.3, and
  § Parallelization — and this round's own addendum text ("An ALLOWED cross-table read at task 1.2
  HALTS every task in groups B through F") preserves it verbatim while extending the gate to
  include 1.9.
- **Resolved, still honored (round-1 #4 — `build_side_store` signature couldn't satisfy union
  redaction).** § Migration's row still reads `build_side_store(&ScanSide, budget, all_secrets: &[String])`
  with task 4.1 computing the union once; unchanged by this round.
- **Resolved, still honored (round-1 #5 — no falsifiable provenance test).** Task 5.6 is present,
  unchanged by this round.
- **Resolved, still honored (round-2 #1 — false zero-head regression-test attribution).** Verified
  against the live tree: `crates/lakehouse-engine/tests/scan_no_head_test.rs`'s `raw_spec` closes
  with `..Default::default()`, so all three of its tests exercise the schema-inference branch, and
  row 1 of plan.md's evidence table correctly carries no test citation. The false attribution is
  gone.
- **Resolved, still honored (round-2 #2 — five named tests with no producing task).** Tasks 5.7 and
  5.8 are present with the renamed/extended tests they describe; § Test Disposition and
  § Verification § Scenario Coverage both name them. Verified `crates/lakehouse-engine/tests/e2e_join_test.rs`'s
  and `scan_join_test.rs`'s cited line ranges are consistent with the current tree.

## Intent Fidelity

No objection to the substance of folding in the alias-qualification fix: it is exactly what the
user's clarifying-interview direction this round asked for (fold it in rather than track
separately), decision `[14]` records the rationale the user gave, and every element of the
credential fix from the original interview (always-route, error-on-unroutable, no N-scan
fallback, no credential comparison) is untouched by this round's edits.

#### [SCOPE_REDUCTION] ADVISORY

- Location: plan.md § Impact ¶1 (line 175); § Non-Goals (line 31)
- Issue: § Impact states unqualified that a broadcast join "starts working... including when the
  query aliases the joined tables... how most real client SQL is written." § Non-Goals, in the
  same document, discloses that "whether a real client driver's own default fetch-size attribute
  similarly suppresses broadcast eligibility in practice is a separate, unstudied question this
  plan does not resolve" — naming the exact class of mechanism (a row-limit-like session attribute
  forcing the safe fallback) this round's own investigation found suppresses the broadcast path
  for literally every one of the existing 18 `e2e_join_test.rs` tests before task 1.10. The two
  passages are not contradictory — Impact's claim about aliasing is accurate — but a reader who
  stops at Impact (the section written for exactly that audience) never learns that the plan's own
  risk assessment leaves open whether the fix reaches real client SQL at all in practice. This is
  not a re-litigation of the Non-Goals scoping decision itself (that call is reasonable and
  correctly disclosed) — it is that the disclosure does not reach the one section a reviewer skims
  for "does this work now."
- Fix: In plan.md § Impact, after the aliasing sentence, add one sentence cross-referencing
  § Non-Goals' fetch-size-attribute bullet: state plainly that whether a real client driver's own
  default row-limit-style attribute reproduces the SAME suppression this plan's own test harness
  had remains unstudied, so the practical reach of both fixes in production is not fully verified
  by this plan's test suite.

## Feasibility

Verified directly against the tree, not taken from decision-log's account: `strip_table_alias`
(`adapter/pushdown/support.rs:489`) is a pure recursive JSON-key filter with no side effects;
`render_expression_safe`'s `"column"` arm (`vs-expression/src/lib.rs:701-720`) qualifies as
`"ALIAS"."NAME"` exactly when `tableAlias` is present and non-empty, confirming the defect
mechanism; `build_join_sql` (`scan/join_scan.rs:115-199`) wraps each side as `SELECT {cols} FROM
{table}` with no outer alias, confirming why a qualified reference cannot resolve; `build_side_fan_out_sql`
(`sql_builders.rs:589-653`) already calls `strip_table_alias` on `side_filter` only, matching
decision `[15]`'s reuse claim exactly; `disjoint_schema_guard` (`joins/planning.rs:458-474`)
compares names only, confirming it is the correct safety precondition for bare rendering; and
`handle_pushdown` (`adapter/pushdown/mod.rs:145-187`) strips only after the join-detection gate
returns, confirming the single-table chokepoint never reaches a join request. Traced
`extract_join_projection` → `project_columns` (`support.rs:1151-1351`) and confirmed it reads only
`selectList`/`selectListDataTypes` from `pushdown_req` — no other field — so stripping the whole
`pushdown_req` before that call cannot silently affect anything else the function reads. Also
confirmed `join_requires_exasol_postprocessing` (`joins/planning.rs:476-507`) already routes any
ORDER BY or LIMIT on a join to the N-scan fallback before `render_broadcast_join` is ever called,
so the alias fix needs no ORDER BY/TopN handling. Task 1.9's tasks 1.6-1.8 dependency is a genuine
prerequisite chain: 1.6/1.7 make an aliased query resolve at the DataFusion level at all; 1.8 makes
the E2E test bypass the row-limit that forces the fallback; without either, the credential defect
is unreachable, exactly as decision-log's escalation found.

#### [UNSTATED_ASSUMPTION] ADVISORY

- Location: plan.md task 1.8 (line 285); task 1.9 (line 289)
- Issue: task 1.8's `unbounded_result_sets()` design rests on "`0` is Exasol's own documented
  default meaning 'no limit'... verified against the Exasol websocket-api spec during planning,
  not assumed" — this is documentation-only verification, not the live-Exasol verification
  CLAUDE.md's "Verification discipline" requires for a capability/behavior claim ("No assumptions
  about SQL capabilities, syntax, or pushdown reachability without checking them against a running
  Exasol instance"). Task 1.9's pass/fail criterion is also not reduced to an exact check: "Expect
  a FAILURE, now for the CREDENTIAL reason... matching the manual repro" is a judgment call against
  free-text error output, with no named substring or status code to grep for. The two risks
  compound: if `resultSetMaxRows: 0` does not mean "no limit" as documented, 1.9 could fail for an
  unrelated third reason (a connection-level error, a timeout, or a different Exasol-side rejection)
  that a reader might still judge "looks like a failure" and wave through as the expected credential
  denial, reopening groups B-F on a false signal.
- Fix: In task 1.9, add the literal expected substring (e.g. the exact `403 Forbidden` /
  `AccessDenied` text already captured in decision-log's "Task 1.4 follow-up investigation"
  section) as the concrete pass criterion, and state explicitly that any failure text NOT
  containing it — including a connection-level or timeout error — must NOT be treated as a passing
  gate and must itself be escalated. This makes the live run itself the verification task 1.8's
  documentation-only claim about `resultSetMaxRows: 0` currently lacks.

Otherwise no objection — axis checked. Task 1.4(a)'s promotion of helpers into
`crates/lakehouse-engine/tests/common/e2e_harness.rs` was independently verified present (all
twelve names grep-confirmed as `pub fn`/`pub const`); `crates/lakehouse-engine/tests/common/exasol_ws.rs`'s
`execute`/`try_execute` were independently confirmed to hardcode `"resultSetMaxRows": 10000`
exactly as task 1.8 describes, at exactly two call sites, so the described field-plus-builder
change is mechanically straightforward. `e2e_lakekeeper_test.rs`'s current
`lakekeeper_vended_broadcast_join_result_correct` was independently confirmed to still call
`exa_conn()` (no opt-out yet), consistent with tasks 1.8/1.9 being unimplemented (`[ ]` in
tasks.md).

## Requirement Quality

#### [REQUIREMENT_CONFLICT] ADVISORY

- Location: plan.md § Features (lines 153-171); `specs/vs-adapter/pushdown-planning-alias-stripping/spec.md`
- Issue: this plan adds a third call site to `strip_table_alias` (`render_broadcast_join`,
  task 1.6) without touching, or even naming, the already-recorded feature that owns this exact
  helper. `specs/vs-adapter/pushdown-planning-alias-stripping/spec.md`'s Background states
  `strip_table_alias` "is shared by the single-table path and the join fan-out's inner leg" —
  verified accurate for the CURRENT tree (two sites: `handle_pushdown`, `build_side_fan_out_sql`).
  Once task 1.6 lands, that sentence undercounts the caller set by one, in the permanent spec
  library this plan's own § Features table otherwise treats with real rigor (it lists five OTHER
  adjacent features "checked and deliberately NOT amended," each with a stated reason).
  `pushdown-planning-alias-stripping` appears in neither the CHANGED nor the checked-not-amended
  list — it is simply absent, the one omission in an otherwise exhaustive section.
- Fix: Add `vs-adapter/pushdown-planning-alias-stripping` to plan.md § Features. Either mark it
  CHANGED with a one-line delta amending its Background bullet to name the third caller, or add it
  to the checked-not-amended list with a stated reason (e.g., that this feature's own scenarios are
  scoped to the single-table path and never claim the caller list is exhaustive, so a third caller
  does not falsify anything the recorded scenarios assert). Add a corresponding task if CHANGED is
  chosen.

#### [AMBIGUOUS_REQUIREMENT] ADVISORY

- Location: plan.md task 1.7 (line 283)
- Issue: minor line-citation drift, the same class round-1 already flagged and round-2 corrected
  elsewhere in this document. Task 1.7 cites `render_broadcast_join_preserves_native_table_alias_unchanged`
  at `:1584-1607`; verified against the current tree the doc comment starts at `:1585` and the
  closing brace is at `:1608` — off by one at both ends. Low-impact (the test is locatable
  unambiguously by name), but this document has twice already corrected identical drift elsewhere
  and this round's own new citation reintroduces it.
- Fix: Correct task 1.7's citation to `:1585-1608`.

Otherwise no objection — axis checked. `speq feature validate` was not re-run by this review (no
write access), but no new scenario in the two touched deltas (`vs-adapter/pushdown-planning-join`,
`e2e-harness/lakekeeper-e2e-harness`) contradicts another delta in this plan or an existing
recorded spec found via `speq search query`/`speq feature get` beyond the one gap above. Verified
independently: `datafusion-scan/scan-execution-join/spec.md` has NOT been touched by this round's
alias-qualification work (correctly, since the alias defect is a planning-layer, not a scan-layer,
concern) and none of its scenario text conflicts with the new `vs-adapter/pushdown-planning-join`
bullets.

## Task Breakdown

#### [TASK_GRANULARITY] ADVISORY

- Location: tasks.md lines 9-16; plan.md § Parallelization (lines 342-344)
- Issue: plan.md's § Parallelization table distinguishes THREE separate groups this round —
  "Group G" (1.5→1.6→1.7, sequential), "Group A′" (1.8, explicitly stated to run "parallel with
  Group G" → 1.9), and an unnamed "(repair, non-gating)" group (1.10, independent of 1.9 and of
  groups B-F). tasks.md's own section header for the same tasks — "## Phase 1 (Round 2):
  Alias-qualification fix and gate closing (Group G, then gate re-run)" — presents all six tasks
  (1.5-1.10) as one flat, implicitly sequential list under a single label, with no marker that 1.8
  may start alongside 1.5 rather than after 1.7, and no marker that 1.10 is independent of 1.9.
  Running everything in the tasks.md list order is never incorrect, but it silently forfeits the
  parallelism plan.md itself claims is available, and an implementer following tasks.md alone
  (rather than cross-referencing § Parallelization) would not know the opportunity exists.
- Fix: Split tasks.md's "Phase 1 (Round 2)" section into three sub-groups matching plan.md's own
  labels — Group G (1.5-1.7), Group A′ (1.8-1.9, noting 1.8 may start in parallel with Group G),
  and a "Repair (non-gating)" line for 1.10 — so tasks.md's structure matches what plan.md already
  claims about concurrency.

Otherwise no objection — axis checked. Every new task (1.5-1.10) traces to a spec delta or a
decision-log entry, and none implements anything outside this round's stated scope. Task 1.9's
dependency on 1.6-1.8 is a genuine prerequisite chain (verified under Feasibility above), correctly
ordered in both tasks.md and § Parallelization's sequential-dependencies list.

## Design Depth

No objection — axis checked. The chosen design (strip-at-render, reusing the existing
`strip_table_alias` helper) is a genuine reuse of an established pattern, not a bespoke solution:
verified the helper is already shared by two call sites for the identical reason, and the new
third call site follows the same contract. The design leaks no new decision across module
boundaries — `strip_table_alias`'s recursive-JSON-filter shape is unchanged, and `render_broadcast_join`
constructs its own local stripped copies rather than mutating shared state, so `build_broadcast_join_sql`
(the sole downstream consumer, verified to read only the returned `RenderedJoinPushdown` struct and
never `pushdown_req` directly) and the N-scan fallback path (which never sees `render_broadcast_join`'s
local copies at all) are both structurally insulated from this change. `disjoint_schema_guard` is
the single correctly-identified owner of the safety invariant bare rendering depends on, checked
before either the condition or the filter is rendered. The rejected alternatives (threading the
alias through the wire; string-surgery recovery in the scan) are correctly rejected for the
coupling and fragility reasons decision `[15]` states, both verified against the tree rather than
assumed (`JoinSpec` genuinely has no per-side alias field today; `build_join_sql`'s derived
relations genuinely carry no outer alias to parse against).

## Prose Quality

#### [PROSE_BLOAT] ADVISORY

- Location: plan.md § Non-Goals (line 31)
- Issue: round-1 flagged this passage as a ~95-word semicolon chain; round-2 noted it had grown to
  ~110 words and seven exclusions in one sentence and deferred the fix a second time. This round
  appends an EIGHTH exclusion with its own embedded rationale, motivation, and cross-reference
  (the `join_requires_exasol_postprocessing` LIMIT-forces-fallback bullet), making the single
  sentence longer still. This is the same deferred finding made materially worse a second
  consecutive time, not merely left alone.
- Fix: Convert plan.md § Non-Goals to a bulleted list, one exclusion per bullet, as both prior
  rounds recommended. Give the newly-added LIMIT-forces-fallback exclusion its own bullet, since it
  carries the Intent Fidelity cross-reference this review's Impact finding above also asks for.

Otherwise no objection — axis checked. This round's new prose — decisions `[14]`-`[16]`, § Design
§ Alias-qualification fix, and the corrected § Test Disposition `e2e_join_test.rs` row — is
BLUF-compliant, states its direction change before its rationale, and is evidence-dense throughout
(every claim in decision `[15]`'s Rationale was independently verified against the tree in this
review's Feasibility section above).
