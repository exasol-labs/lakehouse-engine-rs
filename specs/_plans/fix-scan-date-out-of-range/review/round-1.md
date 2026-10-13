# Plan Review Findings: fix-scan-date-out-of-range (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 12 (Blockers: 2, Advisory: 10)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Six months from now this plan failed catastrophically. Three ways it could happen:

1. The E2E test from task 1.2 never passed as written. It asserts that no failure message contains `1970-01-01`, but the message that task 2.1 plans contains `1970-01-01`. The implementer then changed the error text or deleted the assertion without review. Routed to Feasibility `[UNSTATED_ASSUMPTION]` BLOCKER.
2. Issue #470 stayed open after the fix shipped, and a human opened a duplicate issue because Open Question 1 said none existed. Later, #476 typed the `GK_i` columns and removed `value_to_gk_string`. The author of that change read this plan's reason for the group-key guard ("declared `VARCHAR(2000000)`, uncoerced") as obsolete and removed the guard call. Routed to Feasibility `[UNSTATED_ASSUMPTION]` BLOCKER and `[HIDDEN_DEPENDENCY]` ADVISORY.
3. Users filed new bugs. `SELECT COUNT(DISTINCT D)` failed with an error naming output column `V`, and a `GROUP BY D` failure named `GK_0`. Neither query result contains a date, and neither name is a column the user typed. `docs/semantic-differences.md` listed only filter, `YEAR`, and `COUNT(*)` shapes. Routed to Requirement Quality `[COMPLETENESS_GAP]` and Intent Fidelity `[INTENT_DRIFT]` ADVISORY.

## Intent Fidelity

#### [INTENT_DRIFT] ADVISORY
- Location: decision-log.md § Design Decisions [2]; plan.md § Open Questions 2
- Issue: Issue #470 asks for "a clear error that names the column". On the aggregate path, the plan names the adapter's alias: `PARTIAL_max_0`, `GK_0`, and `V` for the `COUNT(DISTINCT)` row scan (see golden file `lone_count_distinct.sql`, `EMITS ("V" ...)`). Open Question 2 correctly asks the human about this, but decision [2] rejects the alternative because of a cost that the code does not show. The decision says "the grouped loop would need a second walk over the partial-column widths". In fact, `run_grouped_partial_aggregate` has `group_keys` in scope, so `GK_i` maps to `group_keys[i]` by index. `partial_row_from_batch` already walks the widths through `partial_columns()`, and `AggregatePlan.column` carries the source column. Decision [2] also cites ADR `declared-output-column-single-authority-emit-type` as "the one authority for an emitted column". That ADR governs the emitted type, not the name in an error message.
- Fix: In decision-log.md [2], replace the Alternatives reason with the real cost: a source label is available as `group_keys[i]` for keys, and as `AggregatePlan.column` through the existing `partial_columns()` walk for partials. Change the ADR citation so it supports the type authority only. In plan.md Open Question 2, add one sentence stating that naming the source (for example `GK_0 (group key "STORED_DATE")`) costs one label argument at the `coerce_partial_agg_columns` call sites, so the human can decide with accurate information.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Implementation Tasks 1.2 and 2.1
- Issue: Task 1.2 says to "assert that no failure message contains `1970-01-01`". Task 2.1's example message is "`... 100000000 days from 1970-01-01 lies outside Exasol's DATE range ...`". The E2E test fails against the planned implementation. The assertion also does not prove what it is meant to prove: a substitute date can appear only in a returned row, and a failed query returns no row. The failure itself proves the "MUST NOT return a substitute" step, and the in-range queries prove the values with exact assertions.
- Fix: In plan.md task 1.2, delete the clause "and assert that no failure message contains `1970-01-01`". Leave task 2.1's message unchanged.
- Escalation: MECHANICAL: the two task lines contradict each other, and reading them is enough to resolve it.

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Open Questions 1; § Implementation Tasks 4.4
- Issue: Open Question 1 says "No issue exists for this bug. A human opens one". Issue #470 ("fix(scan): DATE values outside Exasol's range are returned as 1970-01-01", OPEN, label `bug`) is this bug. Its text matches the user intent verbatim. AGENTS.md § Workflow requires citing an existing issue as `(#n)` and marking `(#TBD)` only when none exists. As written, a human opens a duplicate issue, and the implementing commit does not close #470.
- Fix: In plan.md, delete Open Question 1 and renumber the rest. Change task 4.4 to "The implementing commit carries `Closes #470`". Add `(#470)` to the § Summary sentence.
- Escalation: MECHANICAL: `gh issue list` settles the fact. No judgment call is needed.

#### [HIDDEN_DEPENDENCY] ADVISORY
- Location: plan.md § Context (group-key bullet); decision-log.md [1]
- Issue: Open issue #476 changes the same code path. It declares each `GK_i` with its real type, sends keys through `coerce_partial_agg_columns`, and removes `value_to_gk_string`. #476 also lists #470 as related. The plan does not mention #476. The guard keys on the Arrow type (decision [1]), so it still works after #476. But the plan's reason for guarding the keys ("declared `VARCHAR(2000000)`, uncoerced") becomes false after #476, so whichever change lands second can drop the guard by mistake.
- Fix: Add one Context bullet to plan.md: "#476 will type and coerce `GK_i` columns. The guard keys on the Arrow type, so it applies to group keys before and after #476. The task 1.2 `GROUP BY` assertion protects it." In decision-log.md [1], add a Consequences line with the same point.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 1.3 (second stop condition); § Context bullet on the format-neutral boundary
- Issue: The stop condition gives "footer-statistics pruning for the `STORED_DATE` filter" as an example of failure at plan time. The direct-storage reader never prunes on statistics. `ParquetFormatReader::resolve_scan` says "other predicates never prune (no file statistics)", so this check cannot fire on the fixture. The format that #470 reports is Iceberg, and both Iceberg and Delta prune on min/max bounds at plan time. Their bounds compare as integers (Iceberg `PrimitiveLiteral::Int(days)`, Delta `Scalar::Date(i32)`), so the risk is low. Still, the spec step "a query that tests the value in a filter ... SHALL NOT fail" is proven only on a format without plan-time pruning.
- Fix: In plan.md task 1.3, delete the footer-statistics example. Add one Context sentence: on Iceberg and Delta, the filter claim relies on min/max bound pruning comparing day counts as integers, and the direct-storage fixture does not exercise that path.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Implementation Tasks 2.1
- Issue: Task 2.1 says to replace both `.unwrap_or(epoch)` calls in `arrow_value_at` "with an error from the same message builder". That builder names the column, but `arrow_value_at(col, row)` gets no column name. The implementer has to choose between changing the signature, using an optional name, or writing a different message. After the guard, this branch cannot be reached in production.
- Fix: In plan.md task 2.1, state that the `arrow_value_at` error names the day count and the range but no column, and that the signature stays unchanged.

#### [UNSTATED_ASSUMPTION] ADVISORY
- Location: plan.md § Open Questions 4
- Issue: Open Question 4 says a TIMESTAMP value outside Exasol's range "may have the same defect". The repo already answers part of this. On the aggregate path, `timestamp_to_naive_datetime` (`convert.rs`) returns `UdfError::User` and does not substitute a value. `specs/datafusion-scan/scan-execution/spec.md` states that a value above year 9999 "fails at the Exasol emit boundary with a `TIMESTAMP` range error".
- Fix: Rewrite Open Question 4 as: "The aggregate path already fails on an out-of-range TIMESTAMP (`timestamp_to_naive_datetime`), and `datafusion-scan/scan-execution` records an Exasol-side failure above year 9999 on the raw path. Whether a TIMESTAMP between chrono's range and Exasol's range is substituted is not checked (#TBD)."

## Requirement Quality

#### [COMPLETENESS_GAP] ADVISORY
- Location: specs/_plans/fix-scan-date-out-of-range/datafusion-scan/scan-execution-value-conversion/spec.md § Scenario, step "a query whose emitted dates all lie inside the range, including 0001-01-01 and 9999-12-31, SHALL return them unchanged"; plan.md task 1.2
- Issue: The step covers all three shapes that the WHEN step names. Task 1.2 proves the boundary values for rows and for `MIN`/`MAX` only. The `GROUP BY` key shape takes a different route. The key is converted to text with chrono `Display`, then cast back by Exasol's outer `CAST("GK_0" AS DATE)`. No in-range `GROUP BY` assertion exists.
- Fix: In plan.md task 1.2, add a query: `SELECT STORED_DATE, COUNT(*) ... WHERE ID IN (1, 2, 3) GROUP BY STORED_DATE ORDER BY 1` returns three groups: 0001-01-01, 2024-01-01, and 9999-12-31.

#### [COMPLETENESS_GAP] ADVISORY
- Location: plan.md § Impact (third bullet); § Implementation Tasks 3.1
- Issue: Two gaps. First, Impact says a query that "passes it to a pushed function, returns the same result as before". That is false for a pushed function that returns a DATE (for example `ADD_DAYS(STORED_DATE, 1)`), because the result leaves the scan as Date32 and the guard fails it. Second, task 3.1 lists only queries that do not fail. Users also hit failures in queries whose result holds no date: `COUNT(DISTINCT STORED_DATE)` runs a row scan that emits the column as `V` (golden file `lone_count_distinct.sql`). Any other shape that passes the column to the Exasol-side wrapper behaves the same way.
- Fix: In plan.md § Impact, change "or passes it to a pushed function" to "or passes it to a pushed function whose result is not a DATE". In task 3.1, add `SELECT COUNT(DISTINCT STORED_DATE) FROM ...` to the live-observed list. Document it as a query that fails even though its result holds no date, and record the column name that the error shows.

#### [REQUIREMENT_CONFLICT] ADVISORY
- Location: specs/datafusion-scan/scan-execution-value-conversion/spec.md § Scenario "Output columns are coerced ...", step "a column already of the required Arrow type SHALL be passed through unchanged, on the zero-copy fast path"
- Issue: A Date32 column declared DATE is already of the required type, so the recorded step says it SHALL be passed through unchanged. The new scenario says the call MUST fail when that column holds an out-of-range value. Read as describing coercion only, the two steps work together. Read as describing what is emitted, they conflict on the same input.
- Fix: In decision-log.md [1], add one Rationale sentence: the recorded pass-through step describes coercion only, and the guard reads the column without copying it and fails the call afterwards. No `DELTA:CHANGED` on the recorded scenario is needed.

## Task Breakdown
[no objection, axis checked: one Parallelization group covers the single delta and the modules it changes (`convert.rs`, `emit.rs`, `partial_agg.rs`, the direct-storage E2E file, the docs page). The scenario maps to E2E test `out_of_range_date_fails_the_query_on_every_emit_path`, and unit test 2.2 supports coverage. The tasks run reproduce, then guard, then docs, then verification. The Dead Code row matches task 2.1. All three emit sites (`emit.rs:58`, `partial_agg.rs:86`, `partial_agg.rs:151`) pass through the two guarded functions. No other test enumerates `VS_DIRECT` tables or reads the `direct/` base path, so the new `DATE_RANGE` fixture breaks no existing test.]

## Design Depth
[no objection, axis checked: one guard owns the Exasol DATE range in `convert.rs`, next to the `nan_domain_error` precedent. Its two callers are the existing coercion functions that every emit site passes through, so the range decision lives in one module. Decision [1] rejects `coerce_column` for a reason the code confirms: the early `return Ok(batch)` and the uncoerced group keys skip it. All three entries are `Promotes to ADR: no`, so `[ADR_OVERPROMOTION]` does not apply. "Architecture: no change" holds: `specs/architecture.md` § Constraints lists no type limits, and no component or boundary changes. ADR digest checked for `declared-output-column-single-authority-emit-type`, `no-exasol-side-fallback-for-an-advertised-capability`, `positional-unique-emits-naming-expr-real-name-column`, and `refuse-iceberg-date-promotion-from-schema-history`: no conflict. `speq plan validate` passes.]

## Prose Quality

#### [PROSE_BLOAT] ADVISORY
- Location: specs/_plans/fix-scan-date-out-of-range/datafusion-scan/scan-execution-value-conversion/spec.md § Scenario "A DATE value outside Exasol's DATE range fails the query"
- Issue: The step "the scan MUST NOT return a substitute value, such as 1970-01-01" repeats the THEN step, because a query that fails returns no value. The GIVEN step also carries a reason ("which Iceberg, Delta, and Parquet can store because they keep a date as a 32-bit day count from 1970-01-01"). That is the AGENTS.md trade-off note, placed inside a precondition.
- Fix: Delete the "MUST NOT return a substitute value" AND step. Shorten the GIVEN to "a table whose `date` column holds a value outside Exasol's DATE range of 0001-01-01 to 9999-12-31, which the Iceberg and Delta `date` types allow".

#### [PROSE_UNCLEAR] ADVISORY
- Location: decision-log.md § Design Decisions [3]
- Issue: The entry has no **Alternatives:** line. The rejected option (-719164, taken from Exasol's `DAYS_BETWEEN`) appears only inside the Rationale. A `## Design Decisions` entry exists to record a rejected alternative.
- Fix: Add "**Alternatives:** -719164, from Exasol's `DAYS_BETWEEN(DATE '0001-01-01', DATE '1970-01-01')`. Rejected: Exasol counts pre-1582 dates in the Julian calendar, so the bound would be two days off from Arrow's count." Remove that sentence from the Rationale.
