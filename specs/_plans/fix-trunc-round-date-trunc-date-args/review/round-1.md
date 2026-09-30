# Plan Review Findings: fix-trunc-round-date-trunc-date-args (round 1)

## Summary
- Axes checked: 6/6
- Total findings: 13 (Blockers: 4, Advisory: 9)
- Intent Fidelity blockers: 0
- Human-escalation blockers: 0

## Premortem

Assume the plan shipped and failed six months later. Three failure stories:

1. A customer table stores `9999-12-31` as the "no end date" value. `ROUND(valid_to, 'MM')` fails with
   `scan failed: assigned data could not be read: ... datetime field overflow`. Support spends a day on
   S3 credentials before anyone reads the tail of the message. Routed to Feasibility (error framing).
2. An Iceberg `timestamptz_ns` column holds 18th-century values. `TRUNC(ts, 'CC')` returns
   `1601-01-01` natively and fails in the scan, because that value does not fit
   `Timestamp(Nanosecond)`. No spec names the deviation. Routed to Requirement Quality (ns underflow).
3. #431 merges and records that `TRUNC(x, n)` renders as `exa_trunc(x, n)` for any `n`. This plan
   records a decline for `TRUNC(x, n_col)`. `speq record` then holds two contradictory scenarios for
   one node. Routed to Requirement Quality (conflict with the math-function scenario).

## Intent Fidelity

No objection, axis checked. Evidence: task 4.2 covers #201's four failing shapes, `GROUP BY` included.
Task 4.3 covers the `D`/`DAY`/`DY` and `'week'` fallbacks in four positions at both
`NLS_FIRST_DAY_OF_WEEK` values. Task 3.1 computes `Date32` through chrono, never a nanosecond
timestamp. Task 3.2 returns the input type. Task 3.1 asserts the `0001-01-01` and `9999-12-31` edges.
Task 1.1 gates on #431 (interview A1), and § Non-Goals excludes #227. The separate `<date-trunc-fn>`
departs from interview A2's "via `exa_trunc`". The departure rests on a measurement:
`TRUNC(<date>, 'HH')` fails with `22769`, while `DATE_TRUNC('hour', <date>)` returns the date, and a
type-blind translator cannot serve both through one entry point. decision-log.md discloses it (the
interview note and decision [2]). It keeps A2's unit mapping and decline set and shares the TRUNC core,
as #201 asks ("through the same date-truncation code"). It changes the called name, not the problem.
The decline of a numeric non-literal second argument follows #201's own rule ("the format argument is
not a string literal"), and plan.md § Impact discloses it.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER: UDF errors reach the user under the storage-read framing
- Location: decision-log.md § [6] Rationale, plan.md § Impact (bullet "now with Exasol's message"), plan.md task 3.2, scan delta scenarios "An hour, minute, or second format on a DATE fails TRUNC and ROUND but not DATE_TRUNC" and "A result outside Exasol's date range fails the query"
- Issue: Decision [6] states "`Execution` keeps the message text through the Parquet row filter's `{e:?}` flattening, so no typed recovery on the session is needed." The text survives, but the surfaced message is not Exasol's. Every scan stream error goes through `classify_scan_error` (`scan/emit.rs:209`, called from `raw_scan.rs:43`, `partial_agg.rs:73`, `partial_agg.rs:128`, `partial_agg.rs:134`, `join_scan.rs:41`, and `emit.rs:55`). An error that is neither a checked division nor `ResourcesExhausted` goes to `redact_storage_error`, which prefixes `scan failed: assigned data could not be read:`. The user therefore sees `scan failed: assigned data could not be read: ... unsupported format in date trunc` or `... datetime field overflow`. The recorded `datafusion-scan/scan-execution-expression-pushdown` checked-division scenario names this exact framing a defect: "`scan failed: assigned data could not be read` misnames a user arithmetic error". Decision [6] cites ADR 084 as "the same reasoning", but ADR 084's session-scoped typed record exists to remove this framing. The plan cites the precedent without that part and never says so. The `contains` checks in tasks 3.4 and 4.4 pass either way, so no test catches the gap.
- Fix: In decision-log.md § [6], replace the "no typed recovery on the session is needed" rationale with the actual surfaced message on the filter route and the projection route. Then choose one option and record it in decision [6]. Option (a): add a scan-delta scenario and a task in plan.md § 3 that extend the session-scoped typed-failure record and `reframe_checked_division` to the date/time UDF errors, so the storage-read framing appears nowhere. Option (b): add an AND step to both scan-delta error scenarios that names the storage-read framing as an accepted limitation, with its reason. Rewrite the plan.md § Impact bullet to match the chosen option. In decision [6], also state whether the DATE plus hour/minute/second check raises at planning time or per row. `return_field_from_args` sees the literal format, and a planning-time raise also fails a zero-row or fully pruned scan, as native Exasol's compile-time `22769` does.
- Escalation: MECHANICAL. The recorded spec, ADR 084, and `scan/emit.rs` settle the facts. The choice between (a) and (b) is the planner's, and the recorded precedent informs it.

#### [UNSTATED_ASSUMPTION] ADVISORY: pre-reform "matches Exasol" rests on an unverified emit path
- Location: scan delta § "A date before the Gregorian reform follows the proleptic Gregorian calendar as a tracked exception", third step, and decision-log.md § [7]
- Issue: The step "TRUNC with a century, year, quarter, month, week-of-month, or day token SHALL still match Exasol outside October 1582, because both calendars give those results the same label" is an end-to-end claim. It holds only if the emit path gives Exasol a year-month-day label. The `Value` path does, because `scan/convert.rs` builds a chrono `NaiveDate`. The raw-scan path emits Arrow IPC through `ctx.emit_batch`, and nothing checks how the SLC converts `Date32` to DATE. If the SLC converts day counts, every pre-reform label shifts by up to two days (the plan measured `719164` against `719162`). The one mapped test, `pre_reform_dates_follow_proleptic_gregorian`, is a unit test.
- Fix: In the scan delta, scope the step to the `Date32` label the UDF returns. Alternatively, add a row to plan.md task 4.2 that pushes `TRUNC(<pre-1582 date>, 'MM')` through the raw-scan path and compares it with the native copy.

#### [EFFORT_MISESTIMATION] ADVISORY: task 3.2 changes #431's signature mechanism
- Location: plan.md task 3.2
- Issue: The task says "accept `Date32` and `Timestamp` of any unit and zone without coercion". This changes #431's signature, and task 1.1 only records its mechanism. #431 can rely on signature coercion, for example `Int64` to `Float64`, as DataFusion's built-in `trunc` does. In that case, dropping coercion moves every numeric input type into #431's numeric branch. Task 3.2 does not show that rework and carries no `[expert]` tag.
- Fix: In plan.md task 3.2, add: "keep #431's numeric coercion unchanged (for example, add temporal arms to a `Signature::one_of`), or extend the numeric branch to every type the old signature coerced, with #431's numeric tests still green". Tag task 3.2 `[expert]` if task 1.1 finds a coercible signature.

#### [HIDDEN_DEPENDENCY] ADVISORY: new E2E file needs the feature gate
- Location: plan.md task 4.2
- Issue: Every E2E file starts with `#![cfg(feature = "exasol-e2e")]` (for example `crates/lakehouse-engine/tests/e2e_capability_test.rs:5`), and `make test-e2e` passes `--features exasol-e2e`. Task 4.2 names the Makefile entry but not the gate. Without the gate, host `cargo test` runs the file with no database, and the Checklist's `cargo test` row fails.
- Fix: In plan.md task 4.2, add "start the file with `#![cfg(feature = "exasol-e2e")]`".

## Requirement Quality

#### [REQUIREMENT_CONFLICT] BLOCKER: the numeric non-literal decline contradicts the recorded math-function scenario
- Location: `sql-comprehension/vs-expression-translator-datetime-trunc/spec.md` § "A session-week, unknown, or non-literal format declines in the DataFusion dialect and renders verbatim in the Exasol dialect", last AND step, plan.md task 1.1, and decision-log.md § [4] Consequences
- Issue: The delta says "`TRUNC(x, n_col)` declines and Exasol evaluates it". Decision [4] says "This narrows #431's surface and is recorded in the translator delta." The recorded scenario `sql-comprehension/vs-expression-translator-scalar-fns` "Math scalar functions translate to DataFusion math calls" lists `ROUND` and `TRUNC` and renders them "applied to its rendered arguments in order". Its only error case is an arity mismatch. After both merge, two features give opposite outcomes for the same node. Task 1.1 adds a CHANGED delta to that scenario only "If it did not" change the names. The decline conflicts whatever names #431 lands, and #431's own recorded wording probably renders any second argument. The numeric-form rule also sits in a feature titled "Date/Time TRUNC and ROUND".
- Fix: Add `specs/_plans/fix-trunc-round-date-trunc-date-args/sql-comprehension/vs-expression-translator-scalar-fns/spec.md` with a `DELTA:CHANGED` block for "Math scalar functions translate to DataFusion math calls". The block removes `ROUND` and `TRUNC` from the generic rule and points to `sql-comprehension/vs-expression-translator-datetime-trunc` for both names, the non-literal decline included. Add the row to plan.md § Features. In plan.md task 1.1, make this CHANGED delta unconditional, keep only the name replacement conditional, and reconcile the block's wording with #431's recorded scenario after #431 merges.
- Escalation: MECHANICAL. Reading the recorded scenario beside the delta settles it.

#### [COMPLETENESS_GAP] BLOCKER: nanosecond TRUNC results before the unit's range are unspecified
- Location: scan delta § "TRUNC on a DATE or TIMESTAMP returns Exasol's value for every non-session-week format" and § "A result outside Exasol's date range fails the query", and plan.md task 3.1
- Issue: The TRUNC scenario requires "the Background table's TRUNC result" for "a `Timestamp` argument in seconds, milliseconds, microseconds, or nanoseconds". The type scenario requires the argument's exact Arrow type. `Timestamp(Nanosecond)` starts at `1677-09-21 00:12:43`. For every nanosecond value from `1677-09-21` through `1700-12-31`, `CC` truncates to `1601-01-01`. Near the range start, `YYYY`, `IYYY`, `Q`, `MM`, the week units, and even `DD` (a value at `1677-09-21 00:30`) truncate below it. These results cannot be represented, so the scenario cannot be implemented for these inputs. Native Exasol returns a value. The overflow scenario covers only `<round-fn>` and "a nanosecond timestamp after `2262-04-11`". Task 3.1 says "an overflow error past `9999-12-31` or the unit's `i64` range", which is broader than the spec. The input is reachable: task 3.4 itself tests a `Timestamp(Nanosecond, None)` column, and `datafusion-scan/type-mapping-timestamp-precision` maps Iceberg `timestamptz_ns`. CLAUDE.md requires each known type-handling deviation to be "an explicit, accurately-scoped exception", never a silent gap. #201 fixes the result type ("Return the input type"), so a raise is the only behavior that keeps the type.
- Fix: In the scan delta, widen "A result outside Exasol's date range fails the query" to `<trunc-fn>`, `<round-fn>`, and `<date-trunc-fn>`. Also cover a result before the unit's range, with a nanosecond `TRUNC(<1690 timestamp>, 'CC')` as the example. Name it an explicit exception against native Exasol, with the input-type rule as the reason. Add "a result the argument's time unit can represent" to the TRUNC scenario's GIVEN. Add a row to plan.md § Scenario Coverage, for example `trunc_below_nanosecond_range_raises`. Quote the Iceberg § Primitive Types row for `timestamp_ns` in the scan delta's compliance bullet.
- Escalation: MECHANICAL. The Arrow type's range and #201's input-type rule settle it.

#### [IMPLEMENTATION_LEAKAGE] BLOCKER: two Background facts that no scenario step uses
- Location: `sql-comprehension/vs-expression-translator-datetime-trunc/spec.md` § Background, last bullet, and `datafusion-scan/scan-execution-datetime-trunc/spec.md` § Background, first bullet
- Issue: The translator Background states that "`TRUNC(1.2345, 'MM')` fails with SQL state `22018`". No GIVEN/WHEN/THEN step in that spec depends on it. Only the scan delta's "A date/time format token on a numeric argument fails the query" covers the numeric-with-token case. The scan Background says the UDFs are registered "next to the checked-division UDF". No step depends on the registration site.
- Fix: In the translator delta, either delete the `TRUNC(1.2345, 'MM')` clause, or add an AND step to "Date/time TRUNC and ROUND render as the truncation and rounding UDFs in the DataFusion dialect". The step states that a vocabulary token renders whatever the first argument's type is, and that `<trunc-fn>` then fails a numeric argument as Exasol's `22018` does. In the scan delta, delete "next to the checked-division UDF", because task 3.3 already names the site.
- Escalation: MECHANICAL. Reading each spec's steps against its Background settles it.

#### [COMPLETENESS_GAP] ADVISORY: UDF behavior for inputs the translator can still send is unspecified
- Location: scan delta § Scenarios, plan.md tasks 1.2 and 3.2
- Issue: Task 3.2 raises on "a non-string format, an array format", but no scenario states it. Two input classes lack a stated outcome. (a) A numeric literal format on a temporal argument, for example `TRUNC(d, 2)` or `ROUND(ts, 0)`. The translator renders these, because task 2.2 passes every numeric literal. (b) `D`, `DAY`, `DY`, or a token outside the vocabulary. The translator never forwards these, but a UDF that parses them would silently compute a Monday-based week. Task 1.2 measures `TRUNC(EVENT_DATE, 2)`, but no scenario step uses the result.
- Fix: Add a scan-delta scenario: for a `Date32` or `Timestamp` argument, a non-string or array format, a session-week token, or a token outside the vocabulary fails the query and never returns a value. Cite task 1.2's `TRUNC(EVENT_DATE, 2)` measurement in that scenario, and map one unit test to it in plan.md § Scenario Coverage.

#### [COMPLETENESS_GAP] ADVISORY: ORDER BY and join positions are outside "every pushdown path"
- Location: `vs-adapter/pushdown-planning-capability-extensions/spec.md`, both new scenarios, and plan.md tasks 4.1 to 4.3
- Issue: Both titles say "every pushdown path", but the WHEN steps name four positions. Two more paths render these calls. A declined `ORDER BY ... LIMIT` sort key goes to `parse_declined_sort_key` (`adapter/pushdown/topn.rs`), which renders it in the Exasol dialect. A join leg partitions conjuncts by `datafusion_renderable`, and `plan_join` falls back to the N-scan join. `ORDER BY DATE_TRUNC('week', d) LIMIT n` currently pushes a Monday-based key. After this plan it takes the declined-sort path, and no test covers that path. plan.md § Architecture names "join → residual conjunct", but no task tests it.
- Fix: Either retitle both scenarios to name the four positions, or add `ORDER BY ... LIMIT` and a join-leg filter to both WHEN steps, with one E2E row each in plan.md tasks 4.2 and 4.3.

## Task Breakdown

#### [TRACEABILITY_GAP] ADVISORY: a second DATE_TRUNC test breaks, and one coverage row names one test for four
- Location: plan.md § Dead Code Removal, task 2.3, § Scenario Coverage (last row), task 4.1
- Issue: `renders_date_trunc_verbatim_in_exasol_dialect` (`crates/vs-expression/src/lib_tests.rs:2892`) also asserts the DataFusion dialect, `date_trunc('month', "TS")` at line 2907. Task 2.3 breaks it, but only `renders_date_trunc` is listed for retargeting. Separately, the coverage row maps the fallback scenario to one test, `datetime_session_week_forms_route_to_fallbacks` in `request_shape_tests.rs`. Task 4.1 instead places tests for `classify_where_filter`, `project_columns`, `classify_request_shape`, and `detect_aggregates` in "the existing sibling `_tests.rs` files".
- Fix: Add `renders_date_trunc_verbatim_in_exasol_dialect` to plan.md task 2.3 and the Dead Code Removal table. List each task 4.1 test with its file in the Scenario Coverage row.

## Design Depth

#### [INFORMATION_LEAKAGE] ADVISORY: the DataFusion adaptation is written in two modules
- Location: plan.md § Architecture, tasks 3.2 and 3.3, and the Quick Diagnostic row "Business logic depends inward only?"
- Issue: The DataFusion-side adaptation covers Arrow unit dispatch, zone retention, NULL propagation, format-scalar extraction, and the error type. Task 3.2 writes it into #431's module for `<trunc-fn>` and `<round-fn>`. Task 3.3 writes it again for `<date-trunc-fn>` in `datetime_trunc.rs`. The two copies can drift on zone handling or error text. A DataFusion UDF in `datetime_trunc.rs` also contradicts the Quick Diagnostic's "The core is pure (no I/O, no DataFusion types)" for that file.
- Fix: In plan.md tasks 3.1 to 3.3, keep `datetime_trunc.rs` pure. Add one shared adapter function that takes an Arrow array, the format scalar, and a TRUNC/ROUND/DATE_TRUNC mode, and returns an `ArrayRef`. Make all three UDFs call it, and put `<date-trunc-fn>` beside #431's two UDFs.

No `[ADR_OVERPROMOTION]`: all nine decision-log entries carry `Promotes to ADR: no`, so the promotion gate has nothing to check.

## Prose Quality

#### [PROSE_BLOAT] ADVISORY: descriptive sentences over the 25-word cap
- Location: plan.md § Summary, the scan delta feature description, and the translator delta feature description (first sentence of each)
- Issue: The plan.md Summary's first sentence has about 35 words, and the scan description's first sentence has about 40. Each joins two facts with "and" or "so".
- Fix: Split each of the three sentences into two, with one fact each.

#### [PROSE_UNCLEAR] ADVISORY: "that date" has two readings
- Location: scan delta § Background, measured values, the "ROUND of `DATE '2024-05-15'` equals its TRUNC" bullet
- Issue: "ROUND of `TIMESTAMP '2024-05-15 10:37:12.789'` gives that date at `00:00:00` for every unit from century to day." A reader takes "that date" as `2024-05-15`. For `IW` the value is `2024-05-13`, and for `CC` it is `2001-01-01`. The intended referent is the table's last column.
- Fix: Replace "gives that date at `00:00:00`" with "gives the table's TRUNC value for `DATE '2024-05-15'` at `00:00:00`".
