# Code Review Findings: fix-scan-second-fraction

## Summary
- Files reviewed: 7
- Total findings: 4 (standard: 4, expert: 0)
- The fraction-range guard in `render_seconds` (`crates/vs-expression/src/lib.rs` line 240) stays, and it is no finding. No producer reaches it: `classify_second` admits only precisions up to `MAX_FRACTIONAL_DIGITS`, and `timestamp_from_json` bounds a CAST's precision the same way. Rust still needs an arm for the `u8` values 10 to 255, which the public `render_expression` can read from any JSON node. The alternatives are worse. `unreachable!()` would panic inside the adapter UDF. Folding 10 to 255 into the nanosecond arm would make the `u8` subtraction `unit_digits - fraction` wrap in a release build and emit wrong SQL. A test of the guard would need a hand-built `scan_second` node, which ties the test to the node's internal JSON shape. The 90% changed-line coverage gate allows this one uncovered line.
- A scratch probe outside the repo confirmed the evidence below: `render_scan_expression` declines `SECOND(TS, 3)` with the `UNTYPED_SECOND` text, declines `CAST(TS AS TIMESTAMP(6) WITH LOCAL TIME ZONE)` on its own ("unsupported CAST target type"), and renders `TIMESTAMP(1)` and `TIMESTAMP(7)` columns as the SQL quoted in the fixes. `cargo test --workspace --lib` passes every changed unit test.

## Standard fixes

### crates/vs-expression/src/lib.rs

#### [CONTEXTLESS_ERROR] The SECOND decline names the wrong constraint for a two-argument call
- Location: lines 224-226 (`UNTYPED_SECOND`), used at lines 521 and 888
- Issue: `render_scan_expression(SECOND(ts, 3))` runs the typed scan pass. `classify_second` returns `None` for two arguments, so the call reaches the `"SECOND"` per-name arm and declines with "SECOND renders only from the typed scan pass, which knows its argument's precision". The typed pass did run, so the message is false. The constraint the call actually breaks, one argument only, appears nowhere. Before this change, the same call failed with "function_scalar SECOND requires 1 argument, got 2".
- Fix: In crates/vs-expression/src/lib.rs, change the string of the constant `UNTYPED_SECOND` to `"SECOND renders only as a one-argument call through the typed scan pass, which knows the argument's precision"`, and change its doc comment to `/// Only the typed scan pass knows the fractional digits a one-argument SECOND keeps.` Leave both call sites unchanged.

### crates/vs-expression/src/scan_typing_tests.rs

#### [MISSING_BOUNDARY_TEST] The microsecond-to-nanosecond boundary is not pinned
- Location: line 288, `second_renders_the_fraction_of_its_argument_precision`
- Issue: The spec scenario "SECOND keeps the fraction of its argument's declared precision" reads `date_part('microsecond', …)` for p from 1 to 6 and `date_part('nanosecond', …)` for p from 7 to 9. The case table covers p = 0, 3, 6, and 9. It still passes if the `1..=6` / `7..=9` arms in `render_seconds` move by one, for example to `1..=7`, and it never exercises the nanosecond path with a divisor. `specs/testing.md` § Testing strategy requires a unit test that pins a rule to use an input that fails under the wrong rule. A CAST cannot supply p = 1 or p = 7, because `datafusion_cast_target` renders only TIMESTAMP(0), (3), (6), and (9). Only a typed column can supply them.
- Fix: In crates/vs-expression/src/scan_typing_tests.rs, add `("TS1", ExasolType::timestamp(1))` and `("TS7", ExasolType::timestamp(7))` to `columns()`. Add two rows to the `cases` array of `second_renders_the_fraction_of_its_argument_precision`: `(col("TS1"), r#"CAST((CAST((date_part('microsecond', "TS1") / 100000) AS DECIMAL(3,0)) * CAST('0.1' AS DECIMAL(1,1))) AS DECIMAL(3,1))"#, ExasolType::decimal(3, 1))` and `(col("TS7"), r#"CAST((CAST((date_part('nanosecond', "TS7") / 100) AS DECIMAL(9,0)) * CAST('0.0000001' AS DECIMAL(7,7))) AS DECIMAL(9,7))"#, ExasolType::decimal(9, 7))`.

#### [UNTESTED_ERROR_PATH] The local-time-zone decline row passes without the SECOND rule
- Location: line 337, `second_over_any_other_argument_declines`
- Issue: The row `second(cast_to(local_time_zone, col("TS")))` declines whether or not `classify_second` rejects a `TIMESTAMP WITH LOCAL TIME ZONE` argument. The DataFusion CAST renderer already declines `CAST(TS AS TIMESTAMP(6) WITH LOCAL TIME ZONE)` with "unsupported CAST target type". If `classify_second` accepted that type, the row would still pass, so no test guards the decline. A local-time-zone column renders on its own, so a row over such a column fails if the rule breaks.
- Fix: In crates/vs-expression/src/scan_typing_tests.rs, add `("TSLTZ", ExasolType::TimestampWithLocalTimeZone { precision: 6 })` to `columns()`, and add `second(col("TSLTZ"))` to the array that `second_over_any_other_argument_declines` iterates. Keep the existing CAST row, because the spec scenario names that example.

### crates/vs-expression/src/lib_tests.rs

#### [REDUNDANT_COMMENT] The helper doc comment restates its one-line body
- Location: line 15, `microsecond_ts`
- Issue: `/// Types the column `ts` as `TIMESTAMP(6)`.` repeats the body `typed_columns(&[("ts", ExasolType::timestamp(6))])` and gives no reason. `AGENTS.md` § Code style allows a comment only for a non-obvious why and forbids restating the code.
- Fix: In crates/vs-expression/src/lib_tests.rs, delete the doc comment line above `fn microsecond_ts()`.

## Expert fixes
[none]
