# Plan: fix-trunc-round-date-trunc-date-args

## Summary

Pushed Exasol `TRUNC`, `ROUND`, and `DATE_TRUNC` over DATE and TIMESTAMP arguments return native
Exasol's values, through date/time branches in #431's truncation and rounding UDFs and a new
date-trunc UDF that shares their calendar code. Forms that depend on the session's
`NLS_FIRST_DAY_OF_WEEK`, unknown formats, and non-literal formats decline and fall back to Exasol
(#201).

## Design

### Context

Issue #201. The adapter advertises `FN_TRUNC`, `FN_ROUND`, and `FN_DATE_TRUNC` for every argument
type, so Exasol delegates the date/time forms. `crates/vs-expression` renders them as DataFusion's
built-in `trunc`, `round`, and `date_trunc`. Reproduced through the local virtual schema
(`exasol/docker-db:2025.1.16`, seeded `events` table):

- `TRUNC(EVENT_DATE, 'MM')` in a select list, the same call in a `WHERE`, `ROUND(EVENT_DATE, 'MM')`,
  one-argument `TRUNC(EVENT_DATE)`, `TRUNC(EVENT_TS, 'HH')`, and `GROUP BY TRUNC(EVENT_DATE, 'YYYY')`
  all fail at DataFusion planning with `F-UDF-CL-RUST-9001` (SQL state `22002`).
- `DATE_TRUNC('week', EVENT_DATE)` returns Monday `2024-01-01` for `2024-01-01`. Native Exasol
  returns Sunday `2023-12-31` at `NLS_FIRST_DAY_OF_WEEK = 7`.

The forces:

- The translator is type-blind (ADR 084, `specs/_decision/084-fix-float-div-predicate-divzero.md`).
  A scalar UDF sees its argument's Arrow type at DataFusion planning time, so the type decision
  belongs in the UDF.
- DataFusion's `date_trunc` converts a `Date32` to a nanosecond timestamp (years 1677 to 2262).
  Exasol DATE covers years 1 to 9999.
- `D`, `DAY`, `DY`, and `DATE_TRUNC('week')` read `NLS_FIRST_DAY_OF_WEEK`. No pushdown request
  carries it.
- Exasol never re-applies a delegated capability (CLAUDE.md, Virtual Schema pushdown delegation).
  Every decline therefore needs a fallback that returns native rows. The fallbacks exist and are
  confirmed in code: a declined `WHERE` goes through `classify_where_filter`
  (`adapter/pushdown/support.rs:631`) to the qualified wrapper's outer `WHERE`
  (`adapter/pushdown/mod.rs:305`). A declined select item sets `project_columns`'s widening flag
  (`support.rs:664`) and routes to the qualified wrapper (`mod.rs:461`). A declined group key
  makes `detect_group_by_aggregates` return `None` (`grouped_agg.rs:96`), and
  `classify_request_shape` then returns `GroupByWrapper` (`request_shape.rs:73`). A declined
  aggregate argument makes `arg_column_or_expr` return `None` (`scalar_over_agg.rs:155`), which
  declines `parse_agg_item` and routes to one of the two wrappers. Join legs partition conjuncts by
  `datafusion_renderable` (`joins/rendering.rs`).
- `TRUNC(<date>, 'HH')` fails natively with `22769`, while `DATE_TRUNC('hour', <date>)` returns
  the date unchanged. Exasol still delegates `TRUNC(EVENT_DATE, 'HH')` (seen in `EXPLAIN VIRTUAL`).
  One UDF cannot serve both calls. The user confirmed a separate date-trunc UDF over the shared
  TRUNC core after seeing this measurement (decision [2]).
- Every scan error that is not a checked division or a memory exhaustion reaches the user as
  `scan failed: assigned data could not be read: …` (`classify_scan_error`, `scan/emit.rs`). A
  planning error reaches the user as `DataFusion SQL error: …`. ADR 084's session-scoped typed
  record removes both prefixes for the checked division.
- #431 (open) introduces the truncation and rounding UDFs this plan extends.

- **Goals**: native Exasol values for every non-session-week format on DATE and on TIMESTAMP in any
  Arrow unit, zoned or not, in every pushdown position. Declines for session-week, unknown, and
  non-literal formats. Error parity for an hour, minute, or second format on a DATE and for results
  past `9999-12-31`.
- **Non-Goals**: the numeric forms of `TRUNC`/`ROUND` (#431). Passing `NLS_FIRST_DAY_OF_WEEK` to
  the scan (#161, #216 track session NLS settings). Emulating Exasol's Julian calendar before
  1582-10-15 (tracked exception, `#TBD`). `DATE_TRUNC` units outside year, quarter, month, day,
  hour, minute, and second. The adapter's column-type pass and its GROUP BY gap (#227). The
  grouped path's existing `VARCHAR(2000000)` staging of every group key.

### Decision

#### Architecture

```
Exasol pushdown JSON: TRUNC(d,'MM') | ROUND(ts,'HH') | DATE_TRUNC('month', d)
        │
        ▼
crates/vs-expression
  ├─ datetime_format.rs: the one vocabulary (token → unit, session-week set,
  │                      DATE_TRUNC unit → token), ASCII case-insensitive
  ├─ TRUNC/ROUND arm (#431)  + second-argument decline (this plan)
  └─ DATE_TRUNC arm → <date-trunc-fn>(src, '<token>')  or decline
        │  DataFusion SQL text inside the ScanSpec
        ▼
crates/lakehouse-engine scan session (scan/object_store.rs registers all UDFs)
  ├─ <trunc-fn>, <round-fn> (#431): numeric branches (#431) | Date32/Timestamp branches (new)
  ├─ <date-trunc-fn> (new): the TRUNC core, except DATE + HH/MI/SS keeps the date
  ├─ scan/datetime_trunc.rs: pure calendar core over Date32 days and Timestamp(i64, unit)
  └─ run_scan_dispatch → reframe from the session's typed records (division | date/time)

Declined render ──► existing adapter fallbacks (no production change):
  WHERE → outer WHERE of the qualified wrapper; select item, group key,
  aggregate argument → qualified single-table wrapper; join → residual conjunct
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Type dispatch inside a session UDF | `<trunc-fn>`, `<round-fn>`, `<date-trunc-fn>` | The translator has no column types. DataFusion resolves the Arrow type at planning time on every render path, expressions and group keys included. |
| One owner of a cross-crate contract | `crates/vs-expression` exports the vocabulary and `<date-trunc-fn>` | Corollary of ADR 084: the translator's forwarded tokens and the UDF's parser cannot drift. |
| Decline in DataFusion, verbatim in Exasol | TRUNC/ROUND/DATE_TRUNC arms | The existing precedent (`vs-expression-translator-date-fns`, refused arity). Exasol evaluates the declined form with the session's own NLS settings. |
| Deep module | `scan/datetime_trunc.rs` | A two-function interface (truncate, round) hides chrono arithmetic, the ISO-year quirks, the thresholds, and range checks. |

Quick Diagnostic for the new modules (`datetime_format.rs`, `datetime_trunc.rs`) and the new UDF:

| Question | Answer |
|---|---|
| One-sentence responsibility? | `datetime_format.rs`: which Exasol format tokens exist and what unit each names. `datetime_trunc.rs`: how a unit truncates or rounds a Date32 or Timestamp value. `<date-trunc-fn>`: Exasol `DATE_TRUNC` over the same core. |
| Easier to call than to reimplement? | Yes. Callers pass a value, a time unit, and a parsed unit. The ISO-year rules alone are non-standard (measured). |
| Would an internal change force an edit outside? | No. A new token changes only the vocabulary. A threshold fix changes only the core. |
| Public doc comment states intent? | The `<date-trunc-fn>` constant documents its contract, as `CHECKED_FLOAT_DIV_FN` does. |
| One owner per decision? | Vocabulary: `crates/vs-expression`. Semantics: `crates/lakehouse-engine`. Routing on decline: the existing adapter code. |
| Clear boundaries? | The only cross-crate surface is the vocabulary type and three name constants. |
| Tactical shortcut with a follow-up? | The pre-reform calendar gap is a tracked exception (`#TBD`, open question). |
| Business logic depends inward only? | The core is pure (no I/O, no DataFusion types). The UDF wrappers adapt DataFusion to it. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| A separate `<date-trunc-fn>` shares the TRUNC core (user-confirmed) | `<trunc-fn>` with identity on DATE + HH; `<trunc-fn>` that raises; declining DATE_TRUNC's sub-day units | Measured: TRUNC raises and DATE_TRUNC keeps the date for the same DATE argument. Only a second entry point gives parity for both without losing `DATE_TRUNC('hour', <timestamp>)` pushdown. |
| Decline every non-literal second argument, numeric form included | Decline only non-literal string arguments | The translator cannot type a column node. Exasol constant-folds foldable arguments, so only row-dependent values decline. |
| Decline unknown string tokens | Forward them and let the UDF raise | A string on numeric `TRUNC` is a number to Exasol (`'2'` gives `1.23`). Exasol evaluates the declined call with its own semantics. |
| UDFs raise Exasol's errors through ADR 084's typed session record | Return a value; raise with the storage-read framing as an accepted limitation | A filter consumes the value inside DataFusion. A returned value changes row counts silently. The record keeps Exasol's text on every route. |
| Pre-reform dates stay proleptic Gregorian, as a tracked exception | Emulate Exasol's Julian calendar in the UDF | The divergence is engine-wide (every `Date32` is read proleptically). A Julian-only result such as `0100-02-29` is not representable as `Date32`. |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-expression-translator-datetime-trunc | NEW | `sql-comprehension/vs-expression-translator-datetime-trunc/spec.md` |
| vs-expression-translator-date-fns | CHANGED | `sql-comprehension/vs-expression-translator-date-fns/spec.md` |
| vs-expression-translator-scalar-fns | CHANGED | `sql-comprehension/vs-expression-translator-scalar-fns/spec.md` |
| scan-execution-datetime-trunc | NEW | `datafusion-scan/scan-execution-datetime-trunc/spec.md` |
| pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |

## Impact

- Queries that failed with `22002` return native values: `TRUNC`/`ROUND` on DATE or TIMESTAMP in a
  `WHERE`, select list, group key, or aggregate argument, and one-argument `TRUNC`.
- `DATE_TRUNC('week', …)` and `TRUNC`/`ROUND` with `D`, `DAY`, or `DY` return the session-correct
  value instead of a Monday-based one. This changes a silently wrong result, so it is a behavior
  change, not a breaking one.
- Declined forms run through the qualified wrapper. A declined `WHERE` predicate prunes no files and
  scans every row. This is slower but correct, and these forms failed or returned wrong values before.
- `DATE_TRUNC` with `decade`, `century`, `millennium`, `milliseconds`, or `microseconds` falls back
  to Exasol. DataFusion's `date_trunc` rejects these units, so they failed before.
- Numeric `TRUNC`/`ROUND` with a column or string second argument falls back to Exasol. The result
  is correct and loses pushdown.
- `TRUNC`/`ROUND` with an hour, minute, or second format on a DATE still fails. The message
  carries Exasol's text (`unsupported format in date trunc` or `... date round`) with no
  `scan failed` or `DataFusion SQL error` prefix. A result past `9999-12-31` fails the same way.
- `TRUNC`, `ROUND`, or `DATE_TRUNC` of a nanosecond timestamp whose result falls before
  `1677-09-21 00:12:43` fails with `datetime field overflow`. Native Exasol returns a value.
- No capability changes. No configuration changes.

## Dependencies

- #431 merged into `main` (task 1.1 gates all implementation).
- `chrono` (workspace dependency, `Cargo.toml:50`), already used by `crates/lakehouse-engine`.
- The implementing commit references the issue: `Closes #201`.

## Implementation Tasks

### 1. Gate and live re-verification

- [ ] 1.1 Confirm #431 is merged. Record its UDF constant names, module path, signature mechanism, and numeric dispatch. Replace `<trunc-fn>` and `<round-fn>` in the five spec deltas and this plan with the landed names, and name `<date-trunc-fn>` in the same convention. Reconcile the `sql-comprehension/vs-expression-translator-scalar-fns` CHANGED block with #431's recorded wording of "Math scalar functions translate to DataFusion math calls". Keep the block's pointer of `ROUND` and `TRUNC` to `sql-comprehension/vs-expression-translator-datetime-trunc`. If #431 is not merged or does not dispatch on the first argument's Arrow type, stop and return to planning.
- [ ] 1.2 Re-run every Exasol expression the five spec deltas cite through `exapump` against the Docker Exasol container (DSN with `validateservercertificate=0`). Add `ROUND` with `CC` and `IYYY` on a TIMESTAMP at the threshold and `EXPLAIN VIRTUAL` of `TRUNC(EVENT_DATE, 2)`. Correct any delta value that differs.

### 2. Translator (`crates/vs-expression`)

- [ ] 2.1 Add `src/datetime_format.rs` with sibling `datetime_format_tests.rs`: the one public vocabulary declaration (token to unit, the session-week set, the seven DATE_TRUNC units to token), ASCII case-insensitive, no trimming. Export the `<date-trunc-fn>` constant with its contract doc comment.
- [ ] 2.2 In the TRUNC/ROUND arm as #431 lands it, classify the second argument. An absent argument, a numeric literal, or a supported token renders. A session-week token, an unknown string, or any other node returns an error. This covers the `vs-expression-translator-scalar-fns` CHANGED delta too. Add unit tests to `src/lib_tests.rs`.
- [ ] 2.3 Replace the DATE_TRUNC arm's `date_trunc(...)` with `<date-trunc-fn>(<src>, '<token>')` for the seven units and an error otherwise. Retarget `renders_date_trunc`, add decline tests, and add `<date-trunc-fn>` to the banned tokens of `exasol_dialect_renders_declared_verbatim_surface`.

### 3. Scan UDFs (`crates/lakehouse-engine/src/scan`)

- [ ] 3.1 Add the pure calendar core to `datetime_trunc.rs` with sibling `datetime_trunc_tests.rs`: TRUNC and ROUND of a `Date32` day count and of a `Timestamp` `i64` in every `TimeUnit`, with the Background's ISO-year rules, the 3.5-day week threshold, and checked arithmetic. Return a typed error enum whose `Display` is Exasol's text. Raise `datetime field overflow` for a TRUNC, ROUND, or DATE_TRUNC result past `9999-12-31` or outside the unit's `i64` range. Unit tests assert every measured value, the range edges, `TRUNC` with `CC` of a nanosecond `1690` timestamp, pre-1970 values, and the pinned pre-reform divergences. [expert]
- [ ] 3.2 Extend `<trunc-fn>` and `<round-fn>`: accept `Date32` and `Timestamp` of any unit and zone without coercion, with a `Utf8`, `LargeUtf8`, or `Utf8View` scalar format. Return the first argument's type and dispatch to the core. Raise in `return_field_from_args` for a `Date32` with an hour, minute, or second token. DataFusion 54.1 passes the literal format there (`datafusion-expr` `expr_schema.rs:583-595`). Raise per batch on a non-string format, an array format, or a numeric first argument with a vocabulary token. Wrap every error as `DataFusionError::External` over the core's typed error. Propagate NULL and keep `Volatility::Immutable`.
- [ ] 3.3 Add `<date-trunc-fn>` to `datetime_trunc.rs` over the same core, keeping a DATE unchanged for an hour, minute, or second token. Register it next to `register_checked_float_div_udf` in `object_store.rs`.
- [ ] 3.4 Carry the date/time failures to the user as ADR 084 carries a checked division. Record the first failure on one session-scoped record that the three UDFs share, the planning-time raise included. Recognise the typed error by type in `classify_scan_error` (`scan/emit.rs`). Generalise `reframe_checked_division`, called from `run_scan_dispatch` (`scan/mod.rs:209`), to consult the division record and then the date/time record. Keep the replace-except-memory-exhaustion rule. Keep the checked-division tests in `emit_tests.rs` and `checked_div_tests.rs` green. [expert]
- [ ] 3.5 Add DataFusion session tests to `datetime_trunc_tests.rs`: a `MemTable` with `Date32`, `Timestamp(Microsecond, None)`, `Timestamp(Microsecond, "UTC")`, and `Timestamp(Nanosecond, None)` columns. Cover projection, a filter against a literal, a `GROUP BY` key, and an aggregate argument. Assert exact result types and NULL. Assert that each failure is recorded on the session, through a Parquet filter built with `row_filter_pushdown_parquet_format` and at planning time. Add reframe tests to `emit_tests.rs`. They assert the Exasol text and no `scan failed` or `DataFusion SQL error` prefix.
- [ ] 3.6 Add a contract test to `datetime_trunc_tests.rs`: every token the translator can forward, iterated from the `crates/vs-expression` vocabulary and DATE_TRUNC mapping, parses in the scan.

### 4. Adapter parity, E2E, and docs

- [ ] 4.1 Add adapter unit tests with no production change: `classify_where_filter` declines `TRUNC(d, 'D')` and `DATE_TRUNC('week', d)`, `project_columns` widens for both, and `classify_request_shape` returns `GroupByWrapper` for a group key and for an aggregate argument. Also assert that `detect_aggregates` returns `None` for a single-group aggregate argument. Place them in the existing sibling `_tests.rs` files.
- [ ] 4.2 Add `crates/lakehouse-engine/tests/e2e_datetime_trunc_test.rs` with its own virtual schema over the seeded `events` table (`EVENT_DATE`, `EVENT_TS`) and `fact_orders` table (`O_ORDERDATE`), with native `CREATE TABLE AS` copies as the oracle. Add it to the `test-e2e` target in `Makefile`. Cover pushed-path parity for `WHERE`, select list, `GROUP BY`, and aggregate argument, including #201's four shapes (`GROUP BY TRUNC(O_ORDERDATE, 'YYYY')` among them), and assert the UDF call in `EXPLAIN VIRTUAL`.
- [ ] 4.3 Add E2E fallback tests for `TRUNC`/`ROUND` with `D`, `DAY`, `DY` and `DATE_TRUNC('week')` in all four positions, at `NLS_FIRST_DAY_OF_WEEK` `7` and `1`. Assert the qualified wrapper in `EXPLAIN VIRTUAL`.
- [ ] 4.4 Add E2E error parity tests: `TRUNC`/`ROUND` of `EVENT_DATE` with `'HH'` fails in a projection and in a filter, and `DATE_TRUNC('hour', EVENT_DATE)` returns the date. Assert that each message contains Exasol's text and neither `scan failed: assigned data could not be read` nor `DataFusion SQL error`.
- [ ] 4.5 Add a `docs/capabilities.md` "Handled by Exasol" row for the session-week formats and the declined DATE_TRUNC units.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| G: gate | 1.1-1.2 | — | all five spec deltas; #431's merged PR and spec |
| A: translator | 2.1-2.3 | G | spec deltas `sql-comprehension/vs-expression-translator-datetime-trunc`, `sql-comprehension/vs-expression-translator-date-fns`, `sql-comprehension/vs-expression-translator-scalar-fns`; `crates/vs-expression/src/lib.rs`, `datetime_format.rs`, `lib_tests.rs`, `datetime_format_tests.rs` |
| B: scan UDFs | 3.1-3.6 | A (reads the vocabulary) | spec delta `datafusion-scan/scan-execution-datetime-trunc`; recorded `datafusion-scan/scan-execution-expression-pushdown` (checked division); `crates/lakehouse-engine/src/scan/datetime_trunc.rs`, `datetime_trunc_tests.rs`, #431's UDF module, `scan/object_store.rs`, `scan/emit.rs`, `emit_tests.rs`, `scan/checked_div.rs`, `checked_div_tests.rs`, `scan/mod.rs` |
| C: adapter parity, E2E, docs | 4.1-4.5 | A, B | spec delta `vs-adapter/pushdown-planning-capability-extensions`; `crates/lakehouse-engine/src/adapter/pushdown/*_tests.rs`, `crates/lakehouse-engine/tests/e2e_datetime_trunc_test.rs`, `Makefile`, `docs/capabilities.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Rendering | `crates/vs-expression/src/lib.rs`, DATE_TRUNC arm `date_trunc({unit}, {src})` | Replaced by `<date-trunc-fn>` |
| Test expectation | `crates/vs-expression/src/lib_tests.rs`, `renders_date_trunc` | Retargeted to the new rendering |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Date/time TRUNC and ROUND render as the truncation and rounding UDFs in the DataFusion dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_datetime_trunc_and_round_as_udf_calls` |
| (same) forwarded tokens parse in the scan | Unit | `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs` | `every_forwarded_vocabulary_token_parses_in_the_scan` |
| A session-week, unknown, or non-literal format declines in the DataFusion dialect and renders verbatim in the Exasol dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `trunc_round_decline_session_week_unknown_and_non_literal_formats` |
| Math scalar functions translate to DataFusion math calls | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_math_scalar_functions` (as #431 lands it), `trunc_round_decline_session_week_unknown_and_non_literal_formats` |
| DATE_TRUNC renders as the date-trunc UDF for the seven calendar units | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_date_trunc_as_date_trunc_udf_for_calendar_units`, `exasol_dialect_renders_declared_verbatim_surface` |
| DATE_TRUNC declines week, every other unit, and a non-literal unit in the DataFusion dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `date_trunc_declines_week_unmapped_and_non_literal_units` |
| TRUNC on a DATE or TIMESTAMP returns Exasol's value for every non-session-week format | Unit | `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs` | `trunc_matches_measured_exasol_values` |
| ROUND on a DATE or TIMESTAMP rounds at Exasol's thresholds | Unit | `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs` | `round_matches_measured_exasol_thresholds` |
| The result keeps the argument's Arrow type and DataFusion plans every pushdown position | Integration | `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs` | `datetime_udfs_plan_every_position_and_keep_the_arrow_type` |
| An hour, minute, or second format on a DATE fails TRUNC and ROUND but not DATE_TRUNC | Unit + E2E | `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs`, `crates/lakehouse-engine/tests/e2e_datetime_trunc_test.rs` | `date_with_sub_day_token_fails_trunc_and_round_but_not_date_trunc`, `e2e_date_with_hour_format_fails_like_native` |
| A date/time format token on a numeric argument fails the query | Unit | `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs` | `format_token_on_a_numeric_argument_fails` |
| A result outside Exasol's date range or the argument's time unit fails the query | Unit + Integration | `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs` | `round_past_exasol_range_raises_datetime_field_overflow`, `trunc_below_nanosecond_range_raises`, `overflow_in_a_filter_fails_the_query` |
| A date/time UDF failure reaches the user with Exasol's message on every route | Unit + Integration + E2E | `crates/lakehouse-engine/src/scan/emit_tests.rs`, `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs`, `crates/lakehouse-engine/tests/e2e_datetime_trunc_test.rs` | `reframe_names_a_datetime_failure_the_error_chain_lost`, `reframe_drops_the_planning_error_prefix_for_a_datetime_failure`, `datetime_failures_are_recorded_on_the_session`, `e2e_date_with_hour_format_fails_like_native` |
| A zoned timestamp truncates on the UTC wall clock the scan emits | Unit | `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs` | `zoned_timestamp_truncates_on_utc_wall_clock` |
| A date before the Gregorian reform follows the proleptic Gregorian calendar as a tracked exception | Unit | `crates/lakehouse-engine/src/scan/datetime_trunc_tests.rs` | `pre_reform_dates_follow_proleptic_gregorian` |
| A pushed date/time TRUNC, ROUND, or DATE_TRUNC returns native Exasol's result on every pushdown path | E2E | `crates/lakehouse-engine/tests/e2e_datetime_trunc_test.rs` | `e2e_pushed_datetime_trunc_round_match_native_on_every_path` |
| A session-week or declined date/time truncation falls back to Exasol on every pushdown path | Unit + E2E | `crates/lakehouse-engine/src/adapter/pushdown/request_shape_tests.rs`, `crates/lakehouse-engine/tests/e2e_datetime_trunc_test.rs` | `datetime_session_week_forms_route_to_fallbacks`, `e2e_declined_datetime_truncations_fall_back_under_both_week_starts` |

### Manual Testing

Run after `make cross-udf-build` and `make bucketfs-upload-so`, against the E2E virtual schema
`MY_LAKEHOUSE` (seeded `events`: `EVENT_DATE` 2024-01-01 to 2024-01-20). `$DSN` is
`exasol://sys:exasol@localhost:28563?validateservercertificate=0`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-expression-translator-datetime-trunc | `exapump sql "SELECT PUSHDOWN_SQL FROM (EXPLAIN VIRTUAL SELECT TRUNC(EVENT_DATE, 'MM') FROM MY_LAKEHOUSE.EVENTS)" -d "$DSN"` | The scan spec contains `<trunc-fn>(\"EVENT_DATE\", ''MM'')`, not `trunc(` |
| vs-expression-translator-scalar-fns | `exapump sql "SELECT PUSHDOWN_SQL FROM (EXPLAIN VIRTUAL SELECT TRUNC(SCORE, ID) FROM MY_LAKEHOUSE.EVENTS)" -d "$DSN"` | The wrapper SQL applies `TRUNC(` verbatim, and the scan spec contains no `<trunc-fn>` call |
| vs-expression-translator-date-fns | `exapump sql "SELECT COUNT(*) FROM MY_LAKEHOUSE.EVENTS WHERE DATE_TRUNC('week', EVENT_DATE) = DATE '2023-12-31'" -d "$DSN"` | `6` (January 1 to 6 at `NLS_FIRST_DAY_OF_WEEK = 7`) |
| scan-execution-datetime-trunc | `exapump sql "SELECT ID, ROUND(EVENT_DATE, 'MM'), TRUNC(EVENT_TS, 'HH') FROM MY_LAKEHOUSE.EVENTS ORDER BY ID" -d "$DSN"` | 20 rows, `2024-01-01` for IDs 1 to 15 and `2024-02-01` for IDs 16 to 20; no error |
| scan-execution-datetime-trunc | `exapump sql "SELECT TRUNC(EVENT_DATE, 'HH') FROM MY_LAKEHOUSE.EVENTS" -d "$DSN"` | Fails; the message contains `unsupported format in date trunc` and not `scan failed` |
| pushdown-planning-capability-extensions | `exapump sql "SELECT TRUNC(EVENT_DATE, 'YYYY'), COUNT(*) FROM MY_LAKEHOUSE.EVENTS GROUP BY TRUNC(EVENT_DATE, 'YYYY')" -d "$DSN"` | One row: `2024-01-01`, `20` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures, `e2e_datetime_trunc_test` included |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
