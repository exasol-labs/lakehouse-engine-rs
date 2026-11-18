# Code Review Findings: fix-instant-column-tstz-comparisons

## Summary
- Files reviewed: 12
- Total findings: 7

`cargo test --workspace --lib clock` passes (8 tests, exit 0). The fold logic matches [D2] and [D3]: a TSTZ value set directly against an instant column by a comparison, `BETWEEN`, or `IN` folds to UTC, and a TSTZ-typed select-list item and an expression over the column keep the folding-zone rule.

## Findings

### crates/lakehouse-e2e/tests/common/pruning_cases.rs

#### [NON_DISCRIMINATING_TEST] The instant-column pruning case passes with the old fold on the default local image
- Location: `INSTANT_CLOCK_CASES` and `assert_instant_clock_cases_hold` (lines 507-531); caller `clock_and_tstz_filters_prune_files` in `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` line 159
- Issue: `PRUNING_ICEBERG` has no `SESSION_TIME_ZONE`, so its folding zone is `DBTIMEZONE`. The default local image is Nano (`Makefile` `EXASOL_IMAGE ?= exasol/nano:...`), whose `DBTIMEZONE` is UTC (`capability.rs` line 930, `clock.rs` `session_in_db_timezone`). With a UTC folding zone, the pre-fix fold (wall clock in the folding zone) and the new UTC fold produce the same `2024-01-01 00:10:00` bound, so the case cannot fail on the old code in `make e2e-up test-e2e`. The scenario's pruning step requires a non-UTC folding zone (`EUROPE/BERLIN`).
- Fix: In `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` `setup`, create `VS_ICEBERG` with `VsProps::new(VS_ICEBERG, PRUNING_NAMESPACE).with_property("SESSION_TIME_ZONE", "EUROPE/BERLIN")`. In `clock_and_tstz_filters_prune_files`, replace the `DBTIMEZONE` lookup and its `ALTER SESSION` with `conn.execute("ALTER SESSION SET TIME_ZONE = 'EUROPE/BERLIN'")` before `assert_clock_cases_hold`, so the `CLOCK_CASES` session still equals the folding zone, and update the `CLOCK_CASES` doc comment from "a session zone that is `DBTIMEZONE`" to "a session zone that is the folding zone `EUROPE/BERLIN`".

### crates/lakehouse-e2e/tests/e2e_local/clock.rs

#### [NON_DISCRIMINATING_TEST] The `CURRENT_TIMESTAMP` and `LOCALTIMESTAMP` instant comparisons cannot detect a wrong fold
- Location: `a_tstz_value_compared_with_an_instant_column_compares_the_instants`, the `TZ > ADD_HOURS(CURRENT_TIMESTAMP, -1)`, `TZ BETWEEN ... AND CURRENT_TIMESTAMP`, and `TZ < LOCALTIMESTAMP` predicates
- Issue: `CLOCK_ROWS` (`tests/common/seed.rs` line 4125) holds three rows in March 2024 and one far-future row, so shifting "now" by any zone offset (at most 14 hours) changes no returned row. These predicates pass whether the adapter folds `CURRENT_TIMESTAMP`, or `LOCALTIMESTAMP` on Exasol 8, to UTC or to the folding zone's wall clock. Only the unit test checks that fold, and AGENTS.md requires a pushdown claim to be verified against a live Exasol. The scenario step "on Exasol 8 ... `WHERE tz < LOCALTIMESTAMP` SHALL compare the instants" therefore has no discriminating E2E check.
- Fix: In `a_tstz_value_compared_with_an_instant_column_compares_the_instants`, after the `KIRITIMATI` session is set and with folding zone `BERLIN`, run `explain_virtual_pushdown_sql` on `SELECT ID FROM <INSTANT_VS table> WHERE TZ < CURRENT_TIMESTAMP` (and on `... WHERE TZ < LOCALTIMESTAMP` when `local_is_tstz`), take the single value of `folded_timestamps(&pushed)`, and assert it lies within `TOLERANCE_SECONDS` of `SELECT TO_CHAR(CONVERT_TZ(SYSTIMESTAMP, DBTIMEZONE, 'UTC'), 'YYYY-MM-DD HH24:MI:SS.FF3')`.

### crates/lakehouse-engine/src/adapter/pushdown/clock.rs

#### [DUPLICATE_CODE] `InstantColumns::of` re-walks `involvedTables` instead of reusing `involved_columns`
- Location: `InstantColumns::of` (lines 53-84)
- Issue: `support.rs` `involved_columns` already reads one involved table's `name`, each column's `name`, its `dataType` through `exasol_type_from_json`, and its `adapterNotes`. `InstantColumns::of` repeats that walk over the same JSON, so the request's column layout is decoded in two places. The plan's rules forbid code that duplicates existing code.
- Fix: Rewrite `InstantColumns::of` to return `Result<Self, UdfError>` and build its list from `(0..table_count).map(|index| involved_columns(request, |tables| tables.get(index)))`, where `table_count` is the length of `request["involvedTables"]`. For each `InvolvedColumns`, zip `types` with `notes.columns` and keep `(notes.table, name)` where the type is `ExasolType::Timestamp { .. }` and the note satisfies `declares_utc_instants`, using the original-case `name` from `notes.columns`. Propagate the error with `?` in `StatementClock::fold`.

#### [DUPLICATE_CODE] The TSTZ-literal recognition is written twice
- Location: `tstz_value` (lines 193-207) and the `literal_timestamputc | literal_timestamp_utc` arm of `folded` (lines 214-219)
- Issue: Both functions decide which node types are a TSTZ literal and parse its `value` with `parse_tstz`. A new literal spelling has to be added in both places, and a mismatch would make the instant fold and the wall-clock fold disagree on what a TSTZ literal is.
- Fix: Add `fn tstz_literal_value(node: &Json) -> Result<Option<Tstz>, UdfError>` that matches `type` against `"literal_timestamputc" | "literal_timestamp_utc"` and returns `node.get("value").and_then(Json::as_str).map(parse_tstz).transpose()`, otherwise `Ok(None)`. Call it from the literal arm of `folded` (`tstz_literal_value(node)?.map(tstz).transpose()`) and from the first arm of `tstz_value`.

### crates/lakehouse-engine/src/adapter/pushdown/clock_tests.rs

#### [HISTORY_COMMENT] A test doc comment narrates history
- Location: doc comment of `only_a_value_compared_directly_with_an_instant_column_folds_to_utc` (lines 500-501)
- Issue: "keep the old rule" refers to the behavior before this change. AGENTS.md forbids comments that narrate history.
- Fix: Replace "keep the old rule" with "keep the folding-zone rule".

#### [MISSING_CASE] `=` and `<>` against an instant column are untested
- Location: `tstz_values_compared_with_an_instant_column_fold_to_utc`, the `shapes` array (lines 453-474)
- Issue: The shapes cover `predicate_less` in both operand orders, `BETWEEN`, and `IN`. `docs/semantic-differences.md` states the UTC fold "holds for `=`, `<>`, `<`, `<=`, `>`, `>=`", and `compared_with_instant_column` lists `predicate_equal` and `predicate_notequal`, but no unit or E2E test sends either.
- Fix: Change `shapes` to `[Shape; 6]` and add `("TZ = v", |v| compare("predicate_equal", tz(), v), &["/filter/right"])` and `("v <> TZ", |v| compare("predicate_notequal", v, tz()), &["/filter/left"])`.

### docs/tuning.md

#### [CONTRADICTORY_DOC] "compares in UTC, in no zone" contradicts itself
- Location: `SESSION_TIME_ZONE` row, sentence "A `TIMESTAMP WITH LOCAL TIME ZONE` value compared with an instant column compares in UTC, in no zone."
- Issue: The sentence names a zone and then says no zone applies. The behavior is that the instants are compared, independent of `SESSION_TIME_ZONE`.
- Fix: Replace the sentence with "A `TIMESTAMP WITH LOCAL TIME ZONE` value compared with an [instant column](semantic-differences.md#instant-columns) compares the instants, so this property does not affect it."
