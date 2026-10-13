# Plan Review Findings: fix-trunc-round-exact-decimal-date (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 17 (Blockers: 5, Advisory: 12)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

The kernel design and the differential-oracle approach are sound. The test plan is compact: one fixture, five tests, about 150 statements, and native Exasol as the oracle. It is not yet provably wide. Five MECHANICAL blockers stand. The date `ROUND` rule in the spec contradicts its own examples and native Exasol. A Background carries facts no scenario uses. The E2E case matrix is not enumerated. The error E2E cannot catch the reframing defect it exists for. The broadcast-join run path has no E2E case.

## Premortem

Six months from now this plan failed. Three ways it could have happened:

1. A dashboard showed `ROUND(order_date, 'Q')` and `ROUND(order_date, 'W')` one period off from native Exasol near quarter and month ends. The spec said "the nearest period start, choosing the later one at the exact midpoint". The implementer coded that rule. The task 3.5 "field-arithmetic oracle" encoded the same rule, so the unit tests passed. The fixture held few truncated-week dates. Routed to: Requirement Quality, `[AMBIGUOUS_REQUIREMENT]`.
2. A `WHERE ROUND(amount, -1) > 0` over a 36-digit column failed in production with `scan failed: assigned data could not be read: ... numeric value out of range`, and support investigated object storage. The E2E passed because its runner only checked that the message contained `numeric value out of range`, which the storage-framed text also contains. Every error case also sat in the select list, not on the Parquet row-filter route. Routed to: Task Breakdown, `[TRACEABILITY_GAP]`.
3. A star-schema query with `WHERE TRUNC(f.ship_date, 'MM') = DATE '2024-05-01'` went through the broadcast join scan (`run_join_scan_with_session`). No E2E case had ever run an `exa_` call there. Routed to: Task Breakdown, `[TRACEABILITY_GAP]`.

## Intent Fidelity

No intent blocker: the plan covers both issues, uses the session-function approach the user chose, keeps `FN_TRUNC` advertised, and compares against native Exasol in the E2E. One advisory follows.

#### [INTENT_DRIFT] ADVISORY
- Location: decision-log.md § [11] Scope boundaries (Decision and Alternatives); plan.md § Impact, bullet "A `TRUNC` or `ROUND` over a string argument still fails at plan time, as before (#TBD)"
- Issue: The interview answer is conditional: "Until the date branches exist, a non-numeric first argument keeps failing at plan time." Entry [11] turns it into a requirement: "A string argument keeps failing at plan time, as the interview requires", and rejects the bare-VARCHAR decline "because the interview fixed the string behavior for this plan". This plan adds the date branches, so the condition expires inside it. The deferral is a planning choice, not an interview requirement.
- Fix: Reword decision-log.md [11] so the string-argument deferral is stated as this plan's scope choice (the two issues name no string form), and delete "as the interview requires" and "because the interview fixed the string behavior for this plan". Keep the `(#TBD)` open question.

## Feasibility

No feasibility blocker: the dependencies exist (`datafusion` 54.1, `arrow` 58, `chrono` in `crates/lakehouse-engine/Cargo.toml`), and the cited test lines in `crates/vs-expression/src/lib_tests.rs` (1188, 1195, 1882, 2613, 2614, 2907, 3044, 3087) match. `scripts/capture-pushdown-payload.sh`, `tests/micro_bench.rs`, `tests/common/timestamp_precision.rs`, and the pruning pattern files exist. The five `reframe_checked_division` call sites in `scan/emit_tests.rs` (809, 837, 867, 904, 945) match task 2.4. Six advisories follow.

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: plan.md § Implementation Tasks, task 2.8
- Issue: `crates/lakehouse-engine/tests/micro_bench.rs` is an external test crate. It can only call public items. `scan::checked_div` is a private module, and `build_session_context` is `pub(super)` (`scan/object_store.rs:30`). Task 2.8 runs `exa_trunc` in a session it builds itself, so it needs a public registration function that no task creates. Making one public only for a benchmark conflicts with `specs/testing.md` § Testing strategy: "No production function exists only so a test can inject a value."
- Fix: In task 2.8, place the benchmark as an `#[ignore]` test in `scan/exasol_fns/exasol_fns_tests.rs`, which has crate-private access, and change the Checklist row's command to match. Otherwise, name the public export in task 2.1 and justify it.

#### [NFR_IGNORED] ADVISORY
- Location: plan.md § Requirements; task 2.8; task 3.3
- Issue: The speed check covers only the decimal kernel. Today, `DATE_TRUNC('month'|'quarter'|'year', ...)` runs DataFusion's vectorized `date_trunc`, and DOUBLE `TRUNC` runs DataFusion's `trunc`. The plan replaces both with new kernels: per-row label arithmetic (task 3.3) and the power-table steps (task 2.3). No measurement checks that these paths stay as fast.
- Fix: Extend task 2.8 with `exa_date_trunc('month', ...)` against `date_trunc('month', ...)` over 8M `Timestamp(Microsecond)` and `Date32` values, and `exa_trunc` against `trunc` over 8M `Float64` values. Record the medians in the Checklist row.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md task 2.3; decision-log.md [5] and [10] (`D38_2`); trunc-round spec § Scenario "TRUNC and ROUND on DOUBLE reproduce native Exasol's floating-point steps"
- Issue: The spec requires "every value the query returns MUST equal the value native Exasol returns" for a decimal wider than 36 digits. Native Exasol parses the `VARCHAR` text into a DOUBLE. Arrow's cast computes `as_float(x) / 10_f64.powi(scale)` (arrow-cast 58.4.0, `src/cast/mod.rs:2320`), which rounds twice when the unscaled value exceeds 2^53 or the scale exceeds 22. Task 2.3 says only "converted to `Float64` first". The fixture limits `D38_2` to "values within 15 significant digits", so the E2E cannot see a one-ulp difference.
- Fix: In task 2.3, require a correctly rounded conversion, for example parsing the decimal's digit string with `str::parse::<f64>`. Add one `D38_2` value with 17 or more significant digits (unscaled value above 2^53) to decision-log [10].

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: decision-log.md [6]; plan.md task 2.2
- Issue: The overflow rule depends on telling a literal `n` (result scale `r`) from a per-row `n` (scale `s`). The kernel can only see `ColumnarValue::Scalar` versus an array. That mapping assumes DataFusion passes a literal as a scalar on every route: projection, Parquet row filter (`pushdown_filters` is on, `scan/scan_tests.rs:37`), and aggregate argument. It also assumes a cast column is never folded to a scalar. Neither entry states the assumption, and no test pins it outside the select list.
- Fix: State the assumption in decision-log [6]. Add a test to task 2.6 that runs `exa_round` over the 34-digit `DECIMAL(36,2)` value through `ctx.sql` as a `WHERE` predicate on a Parquet-backed table, once with a literal `n = 1` (succeeds) and once with a per-row `n = 1` (fails).

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Impact
- Issue: `classify_scalar_over_aggregate` (`adapter/pushdown/scalar_over_agg.rs:92-101`) uses the DataFusion dialect as a renderability check. `render_scalar_over_merge` then evaluates the scalar in the Exasol dialect. After this plan, `TRUNC(MAX(DT), 'D')` or `DATE_TRUNC('week', MAX(DT))` fails that check. The query moves from partial/merge decomposition to the qualified wrapper, and the result stays correct. plan.md § Impact does not mention this routing change.
- Fix: Add an Impact bullet stating that a declined format or unit around an aggregate moves the query from partial/merge decomposition to the qualified wrapper. Optionally add one decline case `SELECT DATE_TRUNC('week', MAX(DT))` to task 4.2.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: decision-log.md [8] Consequences and [10]; plan.md task 1.2
- Issue: Only pre-1582 labels get guidance for a readback mismatch. The fixture also holds `-0.0`, subnormals, `±1E290`, and FLOAT exact expansions. A reviewer probe on the `local` profile (Exasol 2026.2.0) showed `CAST(-0.0E0 AS DOUBLE)` printed as `0.0`. Nobody has checked that the scan's emitted `-0.0` reads back the same. A mismatch would fail `e2e_trunc_round_fixture_reads_back_like_native` with no stated next step.
- Fix: Add `-0.0`, one subnormal, and `1E290` round trips to task 1.2's probes. In decision-log [10], state the rule for a fixture value that does not read back like native: drop it and note it in decision-log [10], or report it as a separate defect.

## Requirement Quality

#### [AMBIGUOUS_REQUIREMENT] [REQUIREMENT_CONFLICT] BLOCKER
- Location: `sql-functions/vs-expression-translator-trunc-round/spec.md` § Scenario "TRUNC and ROUND on DATE and TIMESTAMP match native Exasol for every supported format", THEN step
- Issue: The THEN step says "`ROUND` SHALL return the nearest period start, choosing the later one at the exact midpoint". The scenario's own AND steps contradict it, and so does native Exasol:
  - `ROUND(DATE '2024-05-16', 'Q')` = `2024-07-01` per the spec, but `2024-04-01` is 45 days away and `2024-07-01` is 46.
  - `ROUND(DATE '2024-07-01', 'YYYY')` = `2025-01-01`, but `2024-01-01` is 182 days away and `2025-01-01` is 184.
  - The spec's "to the month the next month from the 16th" rounds the 16th of a 31-day month up, though the 1st is nearer.
  - A reviewer probe on the `local` profile (Exasol 2026.2.0) returned: `ROUND(DATE '2024-01-16','MM')` = `2024-02-01`, `ROUND(DATE '2020-12-31','WW')` = `2020-12-30`, and `ROUND(DATE '2024-06-30','W')` = `2024-06-29`. In both week cases "the later one" would pick the next period start.
  - The same probe returned `ROUND(DATE '2024-05-31','W')` = `2024-05-29`, though `2024-06-01` is nearer. It returned `ROUND(DATE '2024-06-30','IYYY')` = `2024-01-01` and `ROUND(DATE '2024-07-01','IYYY')` = `2024-12-30`.

  Task 3.2 says "with the midpoint rules of the spec", but the spec gives no consistent rule. The task 3.5 "field-arithmetic oracle" will encode the implementer's reading, so the unit layer cannot catch a wrong reading. The `IYYY`, `WW`, and `W` rounding rules are not stated at all.
- Fix: Replace the THEN step's `ROUND` clause with a rule table, one row per format group, that names the field and threshold where `ROUND` moves to the next period start. Examples: `CC` year of century >= 51; `YYYY` and `IYYY` month >= 7; `Q` second month of the quarter from day 16, or third month; `MM` day >= 16; `DD` hour >= 12; `HH` minute >= 30; `MI` second >= 30; `SS` fraction >= 0.5. State the rule for `WW`, `IW`, and `W`: round within the week's own 7-day span, and never toward the next year's or month's first day. Add the probe values above as AND examples. Add them to task 1.2's probe list so the pinned images confirm them. Add `2024-01-16` and `2024-05-31` to the `DT` fixture in decision-log [10].
- Escalation: MECHANICAL. Task 1.2 can measure each value on the local image. The user's intent (match native) is settled, so no requester judgment is needed.

#### [IMPLEMENTATION_LEAKAGE] BLOCKER
- Location: `sql-functions/vs-expression-translator-trunc-round/spec.md` § Background, bullets "DataFusion's own functions differ from Exasol" and "Measured on Exasol 2026.2.0 with column arguments"
- Issue: Three Background statements support no scenario in this spec:
  - "which changed 5,685 of 50,000 `TRUNC(S_ACCTBAL, 1)` values (11.4%)" is a staging statistic.
  - "`round` on `Decimal128(18,2)` declares `Decimal128(19,2)` but returns `Decimal128(18,2)`" is depended on only by the datafusion-scan delta's declared-type step, which is in another spec.
  - "Measured on Exasol 2026.2.0 with column arguments. The pinned Docker image `2025.1.16` is re-checked before the scan functions are built." describes plan-time process and goes stale once the plan is recorded.
- Fix: In the trunc-round spec Background, delete the "which changed 5,685 of 50,000 ... (11.4%)" clause. Delete the `round` declared-versus-returned type sentence, because the datafusion-scan delta states it. Delete the whole "Measured on Exasol 2026.2.0 ..." bullet. decision-log [12] already records the measurement version.
- Escalation: MECHANICAL. The Background rule is a skill-enforced rule, and the fix is a deletion.

#### [COMPLETENESS_GAP] BLOCKER
- Location: decision-log.md [10] (Decision and Rationale); plan.md task 4.2
- Issue: The user asked for "e2e testing a wide range of values ... and edge cases". Entry [10] lists the fixture values but not the arguments each statement applies to them, so nobody can check how wide the E2E is:
  - No literal `n` set is listed for any numeric domain. The spec's DECIMAL and DOUBLE scenarios require `n` at and above the scale, very negative `n`, non-integer `n` such as `1.7` and `-1.5`, and an absent `n`.
  - No statement says that each date statement covers every format group for both `TRUNC` and `ROUND`, plus no format, or every `DATE_TRUNC` unit.
  - "Each column covers one domain, padded with NULL" leaves the rows of `N_INT` and `N_DEC` unaligned with the value columns. `TRUNC(D18_2, N_INT)` may meet a NULL scale on most rows. Only the `P36_2_MAX` row's `N_INT = 1` is specified.
  - "8 select-list statements per side ... (one wide statement per domain)" does not match the 11 or more domains [10] lists (`D4_2`, `D18_2`, `D19_4`, `D36_*`, `D38_2`, `I32`/`I64`, `DBL`/`FLT`, `DT`, `DT_OLD`, `TS`, `TSTZ`).
- Fix: Add a case-matrix table to decision-log [10] with one row per domain and these columns: the functions; the literal `n` set or format/unit set; the per-row scale columns used; and the positions used (select list, `WHERE`, `GROUP BY`, aggregate argument, join leg). For every numeric domain the `n` set holds at least: absent, `-40`, `-(integer digits)`, `-1`, `0`, `1`, `s`, `s + 1`, `40`, `1.5`, `-1.5`, and `1.7`. For every date domain the set holds every format group for `TRUNC` and for `ROUND`, plus no format. For `DT` and `TS`, every `DATE_TRUNC` unit. State that `N_INT` and `N_DEC` are non-NULL on every row that holds a numeric value, cycling their value sets. Choose the pairing so no select-list row overflows; for example, `N_INT = 40` never meets `DBL = ±1E290`. Recompute the statement count and the runtime estimate from the table.
- Escalation: MECHANICAL. The planner can write the table from the plan's own scenarios.

#### [IMPLEMENTATION_LEAKAGE] ADVISORY
- Location: `datafusion-scan/scan-execution-expression-pushdown/spec.md` § Scenario "The scan session registers the Exasol TRUNC, ROUND, and DATE_TRUNC functions every pushed expression needs", AND step "each function SHALL be declared `Immutable` and SHALL evaluate one whole Arrow array per call, computing the power of ten for a literal scale once per call rather than once per row"
- Issue: The power-of-ten clause and "one whole Arrow array per call" describe how the code runs fast. Neither is behavior a user can see, and no Scenario Coverage test asserts either. The orchestrator brief also says "Benchmark requirements belong in bench/requirements.md, never specs/". This clause is a performance mechanism placed in a permanent scenario.
- Fix: Delete "and SHALL evaluate one whole Arrow array per call, computing the power of ten for a literal scale once per call rather than once per row" from the scenario. Keep "declared `Immutable`" only if a test in `exasol_fns_tests.rs` asserts it. The mechanism stays in task 2.2 and decision-log [5].

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: `sql-functions/vs-expression-translator-scalar-fns/spec.md` delta § Background (inside the `DELTA:CHANGED` block), sentence "Verified on live Exasol 2025.2.1 (the image pinned in `docker-compose.yml`)"
- Issue: The delta rewrites this Background block, so it re-asserts the sentence. `docker-compose.yml:77` pins `exasol/docker-db:2025.1.16`, and plan.md § Context says so too.
- Fix: In the delta's `DELTA:CHANGED` Background, change the parenthetical to "(verified when `docker-compose.yml` pinned that image)" or delete the parenthetical.

## Task Breakdown

#### [TRACEABILITY_GAP] BLOCKER
- Location: plan.md task 4.2 (runner) and § Scenario Coverage row "A TRUNC or ROUND failure reaches the user as the function's own error"; decision-log.md [10] (error cases)
- Issue: The scenario requires "the message MUST NOT carry the storage-read framing `scan failed: assigned data could not be read`". It is mapped to `e2e_trunc_round_errors_match_native`, whose runner only checks that "the VS message must contain" Exasol's phrase. The storage-framed message that task 2.4 exists to prevent also contains `numeric value out of range`. The E2E therefore passes with task 2.4 broken, which breaks `specs/testing.md` § Correctness oracles ("A regression test uses a discriminating fixture"). No planned error case sits in a `WHERE` predicate either. That is the Parquet row-filter route (`pushdown_filters` is on, `scan/scan_tests.rs:37`) where the text is flattened. The checked-division precedent asserts `!message.contains("assigned data could not be read")` (`tests/e2e_scan_test.rs:3294`).
- Fix: In task 4.2, have the runner assert that every VS failure message names `TRUNC` or `ROUND` and does not contain `assigned data could not be read`. In decision-log [10], add three `WHERE`-position error cases, each compared with native: `WHERE ROUND(P36_0_MAX, -1) > 0`, `WHERE TRUNC(P_DBL_HUGE, 10) > 0`, and `WHERE ROUND(P_DT_MAX, 'MM') > DATE '0001-01-01'`.
- Escalation: MECHANICAL. The precedent test shows the assertion.

#### [TRACEABILITY_GAP] BLOCKER
- Location: `sql-functions/vs-expression-translator-trunc-round/spec.md` § Background (definition of "pushdown position"); plan.md task 4.3 and § Scenario Coverage row "The scan session registers the Exasol TRUNC, ROUND, and DATE_TRUNC functions every pushed expression needs"; decision-log.md [10]
- Issue: The interview answer says "The E2E covers every pushdown position and the decline fallbacks". The datafusion-scan delta says registration reaches "a broadcast-join fact-leg filter" and all three run paths, including `run_join_scan_with_session`. The broadcast join renders its projection, leg filter, and join condition in the DataFusion dialect (`adapter/pushdown/joins/sql_builders.rs:61`, `:490`). A declined leg filter routes to the N-scan wrapper. The trunc-round spec defines "pushdown position" as four single-table places. The E2E covers only the raw-scan and partial-aggregate run paths, so the join run path has no proof against native. A self-join of `TRUNC_ROUND` cannot fill the gap: `disjoint_schema_guard` (`adapter/pushdown/joins/planning.rs:256`) sends any shared column name to the fallback.
- Fix:
  - Add "a broadcast-join projection, leg filter, or join condition" to the trunc-round spec's pushdown-position bullet.
  - In task 4.1, seed a 2- or 3-row dimension table whose column names do not occur in `TRUNC_ROUND` (for example `DIM_ID`, `DIM_MONTH`), with a native copy.
  - In task 4.2, add two join cases, each compared with native and checked with `EXPLAIN VIRTUAL` for the broadcast shape. The first has one accepted form in the select list and the fact-leg filter, for example `TRUNC(D18_2, 1)` and `WHERE TRUNC(DT, 'MM') = DIM_MONTH`. The second has one declined form, `TRUNC(DT, 'D')`, in the leg filter.
  - Add the join test to task 4.3 with its `/// Scenario:` lines.
- Escalation: MECHANICAL. The plan's own datafusion-scan delta names the position.

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md § Scenario Coverage; tasks 2.7 and 3.4; decision-log.md [10] Rationale ("This keeps each property at one layer")
- Issue: `specs/testing.md` § Testing strategy says "Each behavior is tested at one layer, not repeated at several." Ten of the 13 coverage rows list both E2E and unit tests for the same values. `calendar_matches_measured_exasol_labels` asserts the `DT_OLD` labels that the E2E already compares with native. `float_steps_match_measured_native_values` asserts the near-half `DBL` values the fixture holds. This works against the user's "maintaining the tests compact".
- Fix: Limit the unit tests to what the fixture cannot hold: the day-window sweeps, `i64` and `i128` agreement, every precision and scale, every `TimeUnit`, the Julian-only label failure, and the `c` boundary at `5.0E-14` and `5.1E-14`. Drop the measured-table assertions whose values the E2E fixture holds. Otherwise, state in decision-log [10] why the same values need both layers.

#### [TASK_GRANULARITY] ADVISORY
- Location: plan.md task 1.1 ("Drop from the E2E case table any form Exasol folds before pushdown")
- Issue: Task 1.1 belongs to group A, but the E2E case table is created by task 4.2 in group C, two groups later.
- Fix: Change task 1.1 to record the folded forms in decision-log [10], and make task 4.2 build the case table without them.

## Design Depth

No objection. Axis checked:
- Decision [1], the only ADR candidate, names rule-2 criteria 2 and 4 and states its `speq decision-log show` result. Its Decision holds no paths or signatures. Its `Architecture: no change` line gives a reason, which `specs/architecture.md:36` confirms ("datafusion-scan ... depends on: ... vs-expression"); architecture.md does not list session functions.
- No ADR conflict. Decision [9] keeps the record scoped to the session and created at registration, not threaded through `build_session_context`, as `checked-float-division-session-scoped-failure-recording` requires. Decision [1] keeps SQL renderings where SQL can reproduce Exasol, as `advertised-capability-null-contract-is-adapters-to-reproduce` decides. Name ownership follows `checked-float-division-name-owned-by-vs-expression`. The rename in [9] leaves that ADR's decision intact; only its function name goes out of date.
- The format vocabulary has one owner ([3]), and the canonical token is the only interface between the translator and the scan.
- The `scan/exasol_fns/` module hides type dispatch, kernels, calendar, and failure recording behind three registered names.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: `sql-functions/vs-expression-translator-trunc-round/spec.md` scenarios; `datafusion-scan/scan-execution-expression-pushdown/spec.md` scenarios
- Issue: `speq plan validate` warns on 11 scenarios with 5 to 10 AND steps. "TRUNC and ROUND on DATE and TIMESTAMP match native Exasol for every supported format" has 10. One AND step of "TRUNC and ROUND on DOUBLE reproduce native Exasol's floating-point steps" packs nine example values into one sentence, which takes several reads.
- Fix: Move each scenario's example values into a table under its THEN step: input, function, argument, expected. Split the DATE/TIMESTAMP scenario in two, one for the `TRUNC` and `ROUND` rule (with the rule table from the Requirement Quality blocker) and one for type, zone, and range handling.
