# Code Review Findings: add-clock-function-pushdown

## Summary
- Files reviewed: 34
- Total findings: 6

Evidence: `cargo test --workspace --lib -- clock time_zone single_group properties capabilities now_family` exits 0 (`target/speq-review-unit.log`). The E2E suites were not run by the reviewer.

## Findings

### crates/lakehouse-engine/src/adapter/pushdown/clock.rs

#### [BROKEN_INVARIANT] The fold reintroduces constant sort keys that `PushdownRequest::parse` removed
- Location: `StatementClock::fold`, line 79 (`Ok(pushdown.with_json(json))`)
- Issue: `PushdownRequest::parse` runs `drop_constant_sort_keys` (`support.rs:908`), and `PushdownRequest::json` documents its result as "minus its constant sort keys". `handle_pushdown` folds after `parse`, so an `orderBy` key that was `CURRENT_DATE`, `SYSTIMESTAMP`, or another clock call becomes a `literal_date` or `literal_timestamp` key that stays. The rows stay correct, because a date or timestamp literal is never read as a select-list position. But the request breaks the documented invariant, and the bounded top-N path (`topn.rs:188`, `parse_sort_key_element`) declines a literal key, so `ORDER BY CURRENT_DATE, id LIMIT 3` scans unbounded where `ORDER BY id LIMIT 3` is a bounded top-N.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/support.rs` make `drop_constant_sort_keys` `pub(super)`. In `StatementClock::fold`, call `super::support::drop_constant_sort_keys(&mut json)` right before `Ok(pushdown.with_json(json))`. In `clock_tests.rs`, add `a_folded_clock_sort_key_is_dropped`: build a request whose `orderBy` holds `CURRENT_DATE` then a column key (use `.field("orderBy", …)` with `isAscending` and `nullsLast` set), fold it with `tokyo_clock()`, and assert the folded `orderBy` holds only the column key.

### crates/lakehouse-engine/src/adapter/pushdown/clock_tests.rs

#### [PLANNED_CASE_MISSING] The TSTZ fold test omits the join condition and the aggregate argument
- Location: `tstz_values_fold_to_the_wall_clock_outside_tstz_select_items`, lines 108-144
- Issue: The plan's Verification lists this test as covering "filter, `having`, join condition, aggregate argument, and a plain select-list item". The test covers the filter, `having`, the plain select-list item, and the TSTZ select-list items. It has no join condition and no aggregate argument. A clock call inside an aggregate's arguments, such as `MAX(CASE WHEN ts < CURRENT_TIMESTAMP …)` in a TSTZ-typed item, is the one place where the select-list rule and the comparison rule meet, and nothing pins it.
- Fix: In `tstz_values_fold_to_the_wall_clock_outside_tstz_select_items`, add `.field("from", …)` with a `join` node whose `condition` compares `ts()` with `tstz_lit("2024-03-01 09:00:00.000")`, and a select-list item `agg_fn("MAX", compare("predicate_less", ts(), scalar_fn("CURRENT_TIMESTAMP", [])))` (or the crate's existing aggregate builder) declared `ExasolType::Boolean`. Assert that the join condition's right side becomes `timestamp_lit("2024-03-01 18:00:00.000")` and that the aggregate's argument becomes `timestamp_lit("2024-01-16 08:30:45.123")`.

### crates/lakehouse-e2e/tests/e2e_local/clock.rs

#### [WRONG_COMMENT] The comment states the wrong UTC instant of the Tokyo-session literal
- Location: line 377, in `a_differing_session_zone_gets_the_folding_zone_values`
- Issue: The comment reads `09:00 UTC is 02:00 in Berlin`. The session zone is `ASIA/TOKYO`, so `CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)` is 10:00 Tokyo, which is 01:00 UTC and 02:00 Berlin. 09:00 UTC is 10:00 Berlin, which would keep only two rows.
- Fix: Replace the comment with `// In this Tokyo session the literal is 01:00 UTC, 02:00 in Berlin, so every row's TS lies after it; Tokyo's 10:00 keeps two.`

#### [WEAK_ASSERTION] The empty-TZ case accepts any error that contains "TZ"
- Location: `an_unreadable_tz_fails_only_a_query_that_needs_it`, the `("CLOCK_TZ_EMPTY", "CLOCK_VS_TZ_EMPTY", "", "TZ")` case, and the final `error_text(&conn.try_execute(&sysdate), &sysdate)` line
- Issue: The spec requires the failure to name the `TZ` value. For the empty case the test only checks that the error contains `TZ`, which any unrelated error that mentions `TZ` also satisfies. The adapter's message for this case is either `TZ '' names no time zone` or `TZ is unset`, depending on how the SLC applies `%env TZ=;`. The last `SYSDATE` check after `SET SESSION_TIME_ZONE` asserts that the query fails, but not why.
- Fix: Add a fifth tuple element, the adapter message prefix `"cannot fold a clock function or TIMESTAMP WITH LOCAL TIME ZONE literal in the adapter's TZ zone"`, and assert `error.contains(prefix) && error.contains(named)` for both queries. Change the empty case's `named` to `"TZ '' names no time zone"` if the live run shows an empty value, or `"TZ is unset"` if it shows an unset one. Change the final line to `let error = error_text(&conn.try_execute(&sysdate), &sysdate); assert!(error.contains(named), "{sysdate} must still fail naming {named}: {error}");`.

### crates/lakehouse-e2e/tests/e2e_local/capability.rs

#### [FLAKY_TOLERANCE] A folded SYSTIMESTAMP must match the native value within one second
- Location: `e2e_clock_projection_matches_native_session_local_value`, the `for expr in ["CURRENT_TIMESTAMP", "SYSTIMESTAMP"]` loop (`deviation < 1.0`) and the comment `Measured within one statement, so no wall-clock time elapses between the two values`
- Issue: `SYSTIMESTAMP` now comes from the adapter VM's own clock, read when the adapter plans the statement. Exasol's statement clock is read earlier. The gap includes the adapter VM's start and the catalog load, which can exceed one second on a loaded CI runner. `docs/semantic-differences.md` documents that gap, and the plan sets a 60-second tolerance for every value check. The comment is also false for `SYSTIMESTAMP` now. `CURRENT_TIMESTAMP` routes to the wrapper and is still evaluated by Exasol, so the 1-second bound holds for it.
- Fix: In that loop, keep `deviation < 1.0` for `CURRENT_TIMESTAMP` only, and for `SYSTIMESTAMP` assert `deviation <= 60.0` together with `(deviation - offset_seconds.abs()).abs() > 60.0`, so a UTC-shift regression still fails. Replace the comment with `// CURRENT_TIMESTAMP is evaluated by Exasol in this statement; SYSTIMESTAMP comes from the adapter's clock, read when it plans the statement.`

### docs/semantic-differences.md

#### [DOC_WRONG_EXAMPLE] The example's Berlin and Tokyo dates are the same day
- Location: the paragraph `For example, with DBTIMEZONE EUROPE/BERLIN, … at 23:30 UTC returns the Berlin date, one day before the Tokyo date native Exasol returns.`
- Issue: At 23:30 UTC it is 00:30 (winter) or 01:30 (summer) of the next day in Berlin, and 08:30 of the next day in Tokyo. Both zones show the same date, so the example shows no difference.
- Fix: Replace `at 23:30 UTC` with `at 15:30 UTC`. At that time Berlin still shows the current date (16:30 or 17:30) and Tokyo shows the next date (00:30), so the sentence holds as written.
