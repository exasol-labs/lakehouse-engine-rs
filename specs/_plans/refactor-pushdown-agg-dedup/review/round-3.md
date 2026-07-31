# Plan Review Findings: refactor-pushdown-agg-dedup (round 3)

## Summary
- Axes checked: 6/6
- Total findings: 3 (Blockers: 1, Advisory: 2)
- Intent Fidelity blockers: 0

## Round-2 Blocker Recheck

Both round-2 BLOCKERs are **resolved on evidence, not by rewording**. The revision replaced an inference chain with a dated live capture, and the capture corroborates independently.

- **Resolved: [UNSTATED_ASSUMPTION] Stop asserting the capture's conclusion before task 1.2 measures it.** All four locations round-2 named now carry measured claims sourced to the capture. plan.md § Context paragraph 4 (line 17) opens "task 1.2's live capture settled it" and cites the container, all four paths, `sqlCode 22002`, and `Schema error: No field named .`. decision-log § [7] Rationale (line 67) opens "Measured against the running Docker Exasol container by task 1.2 (captured 2026-07-31), not inferred" and carries the per-path `EXPLAIN VIRTUAL` verdict and the execution error, so its `Promotes to ADR: yes` flag is now backed by measurement rather than by the three-function chain round-2 objected to. Task 1.2 (lines 120-131) is marked `[x]` and rewritten as a verbatim capture record with the reproducible two-step method. § Impact (lines 98-111) collapses to the single measured branch and no longer names a "NOTHING changes" alternative. `grep -rn 'CAPTURE PENDING'` over the delta directory returns no marker in any spec file. **Independent corroboration that the capture is real rather than narrated:** the recorded error text names "Valid fields are `\"ID\", \"NAME\", \"SCORE\", \"EVENT_DATE\", \"EVENT_TS\"`" (plan.md:124, :234), and those are exactly the seed columns of the live fixture — `crates/lakehouse-engine/tests/common/seed.rs:67` (`E2E_TABLE = "events"`) and the fixture header at `e2e_capability_test.rs:15-18` (`score`, `name`, `event_date`, `event_ts` over `id = 1..20`). A fabricated error string would not reproduce that column list. `git status` shows only the four spec artifacts modified, which confirms task 1.2's claim that the scratch test was reverted and no production or test file carries it.

- **Resolved: [UNSTATED_ASSUMPTION] Task 1.7 asserts a second, unmeasured reachability claim for the grouped scalar-over-aggregate shape.** Task 1.2 extended from two ungrouped queries to four paths, and captures 3 and 4 are the two grouped ones round-2's `Fix:` line required. Capture 4 (`SELECT MOD(id, 4), SQRT(STDDEV(score + id)) … GROUP BY MOD(id, 4)`, plan.md:127) does not merely report "pushed"; it records the echoed selectList carrying `SQRT` over a nested `STDDEV` `function_aggregate` on an `ADD(SCORE, ID)` argument, the rendered merge wrapper `CAST(SQRT(CASE WHEN (…) IS NULL THEN NULL ELSE SQRT(GREATEST(0.0, …)) END) AS DOUBLE PRECISION)`, and the `grouped partial aggregate SQL error:` prefix. Those three together establish that `classify_scalar_over_aggregate` succeeded on the malformed plan and the grouped partial/merge path was taken, which is precisely the claim round-2 said nothing measured. Task 1.7 clause (3) (line 138) now cites capture 4 by name instead of inferring. § Impact states the verdict as "the same on every path measured" and enumerates all four. § Manual Testing gained the grouped and scalar-over-aggregate before/after rows (lines 236-239). `speq plan validate` passes on all three deltas; `speq feature validate` reports 0 errors library-wide.

**Error-literal correction verified applied at all three claimed sites, not merely claimed.** `grep -rn 'column "" not found'` over the plan directory returns five hits and every one is an explicit walk-back of the falsified prediction, never a standing assertion: plan.md:17, :109, :129 and decision-log:67, :129 all read "not as `column \"\" not found`" or "the text the inspection chain predicted". The three sites the planner-agent flagged are each confirmed fixed against `git diff`:

| Site | Before | After |
|------|--------|-------|
| `aggregate-extensions` delta Background bullet 4 (line 19) | "replaces `column \"\" not found` with `__MISSING_AGG_ARGUMENT__ not found`" | "replaces the captured empty field name in `Schema error: No field named .` with an explicit `__MISSING_AGG_ARGUMENT__` placeholder in the same schema error" |
| `scan-partial-agg-column-contract` GIVEN clause (line 40) | "rejects with an opaque `column \"\" not found`" | "rejects with an opaque `Schema error: No field named .` (captured live, plan `refactor-pushdown-agg-dedup` task 1.2)" |
| plan.md § Manual Testing after-state row (line 235) | "A `column \"\" not found` error means the decline did not take effect" | "Any `Schema error: No field named .` means the decline did not take effect" |

The split the correction had to preserve is preserved correctly: the rendered-`COUNT("")` claim is retained everywhere it describes rendering (delta bullet 2, plan.md:17, :107, decision-log:67), and only the predicted error text was walked back.

**Round-2 advisories, for the record.** The `[PROSE_BLOAT]` finding on § Impact's 52-word sentence is fixed: the sentence is now a 20-word lead plus four sub-bullets (plan.md:98-107). Two round-2 advisories carry to the PR unaddressed and are not re-raised: `[AMBIGUOUS_REQUIREMENT]` on the `aggregate-extensions` delta's fifth `*AND*` clause, which still ends "so their observable behavior is UNCHANGED" at function level where round-2 showed it is false for the select-list-plus-HAVING input (spec.md:34); and `[TRACEABILITY_GAP]` on `having_over_stat_aggregate_with_expression_argument_declines`, whose construction in task 1.7 paragraph 3 (plan.md:140) still omits the requirement that the SELECT list also carry the aggregate, so the test can still pass before the production edit.

## Premortem

Three failure stories against the revised artifacts, each routed into the taxonomy below.

1. **The plan is never implemented, and a human is asked a question the container already answered.** `/speq:implement-pr` runs, reads `open-questions.md` at step 2, finds it non-empty, and stops. A human opens the PR, sees `> **Status:** blocked` as plan.md's first content line and two open questions asserting the reachability claim is "not verified against a running Exasol instance", and either answers a void question or instructs the planner to walk § Context back to inspection-scoped wording, undoing the measured evidence this round installed. → Feasibility, `[HIDDEN_DEPENDENCY]`.
2. **The plan's only database-boundary byte-identity check is never run.** An implementer reaches plan.md:241 ("run all ten rows before the PR leaves draft") and issues Manual Testing row 1. Exasol answers `object LAKEHOUSE_VS not found`. After guessing the real schema name the implementer hits `object TS not found` and `object REGION not found`. Rows 1-4 are either silently skipped or re-invented against different columns, losing comparability with any before-run. → Feasibility, `[UNSTATED_ASSUMPTION]`.
3. **A Background bullet nobody wrote enters the permanent library.** `/speq:record` executes § Record Notes literally: "Add the seven new bullets inside `<!-- DELTA:NEW -->`". Six exist. `recorder-agent` reconciles the count by fabricating a seventh. → Requirement Quality, `[COMPLETENESS_GAP]`.

## Intent Fidelity

No objection on `[INTENT_DRIFT]` — axis checked: the revision replaced predicted evidence with measured evidence and changed no scope. Issue #179's four duplications still map to tasks 1.3, 1.4, 1.5, and 1.6, and the deliberate `STDDEV(<expression>)` decline the user's brief authorized is unchanged in substance.

No objection on `[SCOPE_CREEP]` — axis checked: the revision added no work. It added capture records to existing sections and corrected one falsified literal in three places.

No objection on `[SCOPE_REDUCTION]` — axis checked: nothing was dropped. § Impact narrowed from two branches to one only because the capture eliminated a branch, which is the brief's own condition ("a deliberate decline … if the live capture confirms it's a real bug").

## Feasibility

No objection on `[EFFORT_MISESTIMATION]` — axis checked: the revision removed work rather than adding it. Task 1.2 is done; § Parallelization's Group A note (line 160) reconciles the stale `1.1, 1.2` table row explicitly, and task 1.2 is `[x]`, so no runner re-executes it.

No objection on `[NFR_IGNORED]` — axis checked: the revision touches no security, concurrency, performance, or migration surface. § Impact's "No wire format, no adapter note, no DDL, no migration" still holds.

#### [HIDDEN_DEPENDENCY] BLOCKER
- Location: `plan.md` line 3 (`> **Status:** blocked — see open-questions.md`); `specs/_plans/refactor-pushdown-agg-dedup/open-questions.md` (both items)
- Issue: the revision made the facts true but left the block that the old facts triggered. `open-questions.md` is untouched by this round (`git diff --stat` lists only plan.md, decision-log.md, and the two deltas), and both of its items are still unchecked and still assert the opposite of what the artifacts now say. Item 1 reads "Both are code-inspection conclusions, **not verified against a running Exasol instance**" and asks whether § Context and § [7] should "be walked back to inspection-scoped wording pending task 1.2's actual capture". Item 2 asks whether task 1.2 should "add capture queries for the two grouped paths". Task 1.2 ran both grouped paths on 2026-07-31, so both questions are void and item 1's premise is now false — the same defect class as the two round-2 BLOCKERs (an artifact stating an unmeasured claim as settled), inverted in direction and left behind by the fix. This is not cosmetic: `/speq:implement` SKILL.md:53 and `/speq:implement-pr` SKILL.md:53 both define a hard gate — "read `specs/_plans/<plan-name>/open-questions.md`. If it exists and is non-empty → **stop**" — and `/speq:git-discipline` SKILL.md:40 records that the file is "present only while the plan is blocked". So the plan is unimplementable as written: the next workflow step halts, and the human it halts for is handed a false premise and invited to revert the measured evidence. Clearing the block is `/speq:plan-pr` SKILL.md:176 ("delete `specs/_plans/<plan-name>/open-questions.md` and the `> **Status:** blocked …` banner line from `plan.md`"), and it did not happen.
- Fix: Delete `specs/_plans/refactor-pushdown-agg-dedup/open-questions.md` and delete the `> **Status:** blocked — see open-questions.md` banner line from `plan.md` (line 3), per `/speq:plan-pr` step 176. If clearing the block belongs to the plan-pr orchestrator rather than to `planner-agent` in this run, escalate it to the orchestrator as a required pre-ready step and state in `decision-log.md` § Review Findings that both open questions closed on task 1.2's 2026-07-31 capture rather than on a human answer, naming the capture as the resolution.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: `plan.md` § Manual Testing rows 1-4 (lines 230-233)
- Issue: four manual-testing commands name a virtual schema and two columns that do not exist, so they cannot run. Independently confirmed: the real virtual schema is `MY_LAKEHOUSE` (`e2e_capability_test.rs:40`, `e2e_scan_test.rs:47`, `e2e_count_distinct_test.rs:50`, `e2e_positional_deletes_test.rs:52`, `e2e_int96_timestamp_test.rs:92`, `e2e_capture_pushdown.rs:25`), never `LAKEHOUSE_VS`; the seed table is `events` (`common/seed.rs:67`), so `.EVENTS` is right; the seed columns are `id`, `score`, `name`, `event_date`, `event_ts` (`e2e_capability_test.rs:15-18`, and the capture's own "Valid fields are" list at plan.md:124). There is no `ts` column and no `region` column — `grep -cin region crates/lakehouse-engine/tests/e2e_capability_test.rs` returns 0. Rows 1-4 use `LAKEHOUSE_VS.EVENTS`, `MIN(ts)`, `MAX(ts)`, and `GROUP BY region`. Confirmed **pre-existing** from the original authoring: `git diff` shows rows 1-4 as unchanged context. But the revision made the inconsistency sharper by fixing the schema name in rows 5-10 only (old rows read `LAKEHOUSE_VS.EVENTS`, new rows read `MY_LAKEHOUSE.EVENTS`), so one ten-row table now names one virtual schema two ways, against `/speq:writing-guardrails`' "use one term per concept". The harm is bounded, which is why this is ADVISORY rather than BLOCKER: byte-identity is gated primarily by the four new golden fixtures plus the twelve `dispatch_golden` fixtures, all of which run in CI. But plan.md:241 calls rows 1-2 "the only checks that distinguish a byte-identical refactor from a merely equivalent one at the database boundary" and instructs "run all ten rows before the PR leaves draft", and as written four of the ten error out before returning a value. Scope the fix to § Manual Testing only: task 1.1's `MIN(ts)`/`MAX(ts)`/`GROUP BY region` select list is harmless, because `dispatch_golden.rs` builds every request as a hand-written `serde_json::json!` literal (`:36`, `:53`, `:155`, `:386`) that no database resolves.
- Fix: In `plan.md` § Manual Testing rows 1-4, replace `LAKEHOUSE_VS.EVENTS` with `MY_LAKEHOUSE.EVENTS`, replace `MIN(ts), MAX(ts)` with `MIN(event_ts), MAX(event_ts)`, and replace both `region` occurrences in row 3 with the group key task 1.2's capture already used, `MOD(id, 4)` (row 3 becomes `SELECT MOD(id, 4), STDDEV(score), STDDEV_POP(score), VARIANCE(score), VAR_POP(score) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4);`). Leave task 1.1's fixture select list unchanged and add one sentence to task 1.1 stating that its column names are synthetic `json!` request fields, not live-schema columns.

## Requirement Quality

No objection on `[REQUIREMENT_CONFLICT]` — axis checked via `speq plan validate` (passed, 3 deltas, 0 errors) and `speq feature validate` (0 errors library-wide). The revision's one delta-scenario edit is the corrected error literal in `scan-partial-agg-column-contract`'s GIVEN clause, which now agrees with the `aggregate-extensions` delta's Background bullets 2 and 4 and with plan.md § Impact on both the measured error text and the retained `COUNT("")` rendering claim. No new normative statement was added.

No objection on `[AMBIGUOUS_REQUIREMENT]` — axis checked: the corrected GIVEN clause is testable as written, and the capture citation it gained is provenance rather than a requirement. Round-2's `[AMBIGUOUS_REQUIREMENT]` on the fifth `*AND*` clause carries unaddressed and is recorded in the recheck above.

#### [COMPLETENESS_GAP] ADVISORY
- Location: `plan.md` § Record Notes, first table row (line 191); `plan.md` § Features (line 94)
- Issue: the Background-bullet count is wrong for the third consecutive round, and § Record Notes is the artifact `recorder-agent` executes rather than reads. § Record Notes instructs "Add the **seven** new bullets inside `<!-- DELTA:NEW -->`" and § Features states the delta "adds one scenario and **seven** `## Background` bullets". The `DELTA:NEW` block holds **six** (`vs-adapter/pushdown-planning-aggregate-extensions/spec.md:16-21`). Rounds 1 and 2 both raised this as ADVISORY and both declined to re-raise it; it is re-raised once here because § Record Notes opens by stating that "`recorder-agent` applies this checklist; it MUST NOT infer these edits from the `DELTA:*` markers that wrap them", which removes the marker cross-check that would otherwise catch the discrepancy at record time. A recorder reconciling six bullets against an explicit instruction for seven can fabricate the seventh into the permanent library.
- Fix: In `plan.md` § Record Notes' first table row, change "Add the seven new bullets" to "Add the six new bullets"; in `plan.md` § Features, change "adds one scenario and seven `## Background` bullets" to "adds one scenario and six `## Background` bullets".

## Task Breakdown

No objection on `[TRACEABILITY_GAP]` — axis checked: the revision created no test and orphaned none. Every test named in § Verification § Scenario Coverage still has a creating task, and task 1.2's tests were a reverted scratch harness rather than a deliverable, consistent with the § Scenario Coverage table naming no task-1.2 test. Round-2's `[TRACEABILITY_GAP]` ADVISORY on the HAVING test's construction carries unaddressed and is recorded in the recheck above.

No objection on `[TASK_GRANULARITY]` — axis checked: the revision merged no task and split none. Task 1.2 shrank to a done record. Round-1's ADVISORY on task 1.3's size stands with the planner's recorded refusal in `decision-log.md` § Review Findings and carries to the PR.

## Design Depth

No objection on `[SHALLOW_DESIGN]` — axis checked against the Quick Diagnostic: the revision changed no interface, module, or boundary. `AggKind::partial_columns()` and `PartialAggColumn` are untouched by this round, so the table's questions were answered in round 1 and nothing re-opens them.

No objection on `[INFORMATION_LEAKAGE]` — axis checked: no design decision moved across a module boundary. The capture record added measurement prose only. Round-1's ADVISORY on the unowned `EMITS`-type dimension carries to the PR.

No objection on `[TACTICAL_SHORTCUT]` — axis checked: the revision took no shortcut. It closed both blockers by running the measurement the plan already required rather than by hedging the claim, which is the opposite of a shortcut.

No objection on `[BOUNDARY_VIOLATION]` — axis checked: no production visibility widens and no dependency edge is added. The capture ran through a reverted scratch test in `tests/e2e_capability_test.rs`, and `git status` confirms no test or production file retains it.

## Prose Quality

No objection on `[PROSE_UNCLEAR]` — axis checked: every revised passage names its actor, its measurement, and its date. § Context paragraph 4 and § Impact both separate what inspection establishes from what the capture measured, and both name which prediction was falsified.

#### Note on `[PROSE_BLOAT]`
Round-2's finding is resolved: § Impact paragraph 1 is now a 20-word lead sentence plus four sub-bullets (plan.md:98-107), inside the 25-word cap `/speq:writing-guardrails` sets for PR-facing content. No new prose finding is raised. § Context paragraph 4 runs long at nine sentences, but each carries one measurement and the paragraph is descriptive rather than normative, so it clears the bar.
