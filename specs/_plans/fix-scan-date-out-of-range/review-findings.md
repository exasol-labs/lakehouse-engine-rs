# Code Review Findings: fix-scan-date-out-of-range

## Summary
- Files reviewed: 6
- Total findings: 7 (standard: 7, expert: 0)

## Standard fixes

### crates/lakehouse-engine/src/scan/convert.rs

#### [REDUNDANT_COMMENT] Guard doc runs three lines and narrates SLC behavior
- Location: lines 34-36, doc comment of `check_date32_in_exasol_range`
- Issue: The doc is three lines. AGENTS.md § Code style limits a comment to one or two lines. The parenthetical "(or reject without naming its column)" describes what the SLC might do without this function. It does not state this function's contract.
- Fix: In crates/lakehouse-engine/src/scan/convert.rs, replace the three doc lines above `check_date32_in_exasol_range` with exactly these two lines:
  ```rust
  /// Fails on the first non-null Date32 cell outside Exasol's DATE range, naming `output_column`.
  /// Any other column type passes, so callers can check every emitted column unconditionally.
  ```

### crates/lakehouse-engine/src/scan/convert_tests.rs

#### [MISSING_BOUNDARY_TEST] No unit test pins the date that a boundary day count converts to
- Location: line 190, `date32_guard_accepts_the_range_bounds_and_rejects_one_day_beyond`
- Issue: The change rewrote the Date32 arithmetic in `arrow_value_at` to `from_num_days_from_ce_opt(days + UNIX_EPOCH_DAYS_FROM_CE)`. The test asserts only `conversion.is_ok()`. The only other Date32 conversion test (`arrow_columns_map_to_value_variants`, line 108) asserts only `matches!(Value::Date(_))`. With `UNIX_EPOCH_DAYS_FROM_CE` set to 719_162, every date would shift by one day and all unit tests would still pass. Only the E2E `MIN`/`MAX` assertion would catch it.
- Fix: In crates/lakehouse-engine/src/scan/convert_tests.rs, in `date32_guard_accepts_the_range_bounds_and_rejects_one_day_beyond`, change `cases` from `[(i32, bool); 6]` to `[(i32, Option<NaiveDate>); 6]`. Set the expected dates `NaiveDate::from_ymd_opt(1, 1, 1)` for -719_162, `NaiveDate::from_ymd_opt(1970, 1, 1)` for 0, `NaiveDate::from_ymd_opt(9999, 12, 31)` for 2_932_896, and `None` for -719_163, 2_932_897, and 100_000_000. Inside the loop, bind `let accepted = expected.is_some();` for the guard assertions and replace `assert_eq!(conversion.is_ok(), accepted, ...)` with `assert_eq!(conversion.ok(), expected.map(Value::Date), "conversion of {days} days");`.

#### [VAGUE_TEST_NAME] Test name omits three of the behaviors it asserts
- Location: lines 202-213
- Issue: The test name states only the range bounds. The test also asserts that the guard reports the first offender in a column, passes a null-only column, and passes a non-Date32 column. A reader of the name cannot tell those behaviors are covered, and the test holds more than one concept.
- Fix: In crates/lakehouse-engine/src/scan/convert_tests.rs, move the `first_out_of_range`, `nulls_only`, and `not_a_date` blocks (lines 202-213) out of `date32_guard_accepts_the_range_bounds_and_rejects_one_day_beyond` into a new `#[test] fn date32_guard_names_the_first_offender_and_passes_nulls_and_non_date_columns()` placed directly after it. Keep their assertions unchanged.

### crates/lakehouse-engine/src/scan/emit.rs

#### [UNTESTED_ERROR_PATH] The guard on the fully matching branch of `coerce_batch_to_exa_types` has no failing test
- Location: line 127
- Issue: `coerce_batch_to_exa_types` runs the guard on two branches. The E2E raw-row query `SELECT ID, STORED_DATE ... WHERE ID = 4` reaches only the coercion branch (line 150), because `ID` is Int64 and the adapter declares it `DECIMAL(20,0)`, whose target is Decimal128. No unit test in emit_tests.rs holds an out-of-range Date32 value (the only Date32 data there is 0, 1, 2, and 19_000). Deleting line 127 leaves every test green, although a query whose columns all match their declared types, such as `SELECT STORED_DATE FROM t`, would then emit an out-of-range date.
- Fix: In crates/lakehouse-engine/src/scan/emit_tests.rs, add `#[tokio::test] async fn emit_stream_fails_on_an_out_of_range_date_and_emits_nothing()`. Build a one-column `RecordBatch` with `Field::new("STORED_DATE", DataType::Date32, true)` and `Date32Array::from(vec![Some(19_723), Some(2_932_897)])`. Create the context with `CapturingCtx::declaring(&[("STORED_DATE", ExaType::Date)])`, so every column already matches its target type. Run `emit_stream(&mut ctx, Box::pin(VecStream::new(vec![batch])), &[], &mut PhaseTimers::start())` and `expect_err`. Assert the error text contains `STORED_DATE` and `2932897`, and assert `ctx.ipc_batches.is_empty()`.

#### [REDUNDANT_COMMENT] Doc of `check_emitted_dates_in_range` restates the name and the zip
- Location: lines 93-95
- Issue: The doc is three lines, over the AGENTS.md limit of two. "Checks every Date32 column against Exasol's DATE range" restates the function name. "which `declared[i]` aligns positionally with `columns[i]`" restates the `zip` in the body.
- Fix: In crates/lakehouse-engine/src/scan/emit.rs, replace the three doc lines above `check_emitted_dates_in_range` with exactly these two lines:
  ```rust
  /// Keyed on the Arrow type, not the declaration, because a `GROUP BY` key is declared `VARCHAR`
  /// yet leaves as Date32. Errors name `declared[i].name` for `columns[i]`.
  ```

### crates/lakehouse-engine/src/scan/partial_agg.rs

#### [REDUNDANT_COMMENT] Added sentence restates the guard call
- Location: lines 354-355, doc comment of `coerce_partial_agg_columns`
- Issue: The added sentence "Every returned column, keys included, is checked against Exasol's DATE range." restates the `check_emitted_dates_in_range(&columns, declared)?` line in the body. It grows the comment on this private function to four lines.
- Fix: In crates/lakehouse-engine/src/scan/partial_agg.rs, delete the sentence "Every returned column, keys included, is checked against Exasol's DATE range." from the doc of `coerce_partial_agg_columns`, so the doc ends at "identity across shards." and spans three lines.

### docs/semantic-differences.md

#### [OUTDATED_COMMENT] The page keys the failure on "returns" where the code keys it on "emits"
- Location: line 37 ("A query that returns one fails") and line 49 ("A query that reads such a value without returning it does not fail.")
- Issue: The guard fails any batch the scan emits to Exasol, whether or not the query returns the value. The spec delta scenario says "emits". `SELECT COUNT(DISTINCT STORED_DATE) FROM t` returns only a count. Per `pushdown-aggregates/pushdown-planning-count-distinct`, each shard still emits its distinct dates, declared as DATE, through the row-scan path and `emit_one_batch`, so that query fails. The page tells users it does not.
- Fix: First run `exapump sql "SELECT COUNT(DISTINCT STORED_DATE) FROM DIRECT_LAKEHOUSE.DATE_RANGE" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` against the local Docker Exasol after the direct-storage suite has created the fixture. If it does not fail with an error naming `100000000`, stop and report the observed result instead of editing the page. If it fails, edit docs/semantic-differences.md: on line 37 replace "A query that returns one fails" with "A query fails when the scan passes such a date to Exasol". Replace the sentence on line 49 with "A query fails even when it does not return the value, if the scan passes the value to Exasol: `SELECT COUNT(DISTINCT STORED_DATE) FROM t` fails, because each shard passes its distinct dates to Exasol to count. A query that evaluates the value inside the scan does not fail." Keep the rest of that paragraph, starting at "In the following results", unchanged.

## Expert fixes
[none]
