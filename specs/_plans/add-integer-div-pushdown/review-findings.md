# Code Review Findings: add-integer-div-pushdown

## Summary
- Files reviewed: 18
- Total findings: 6 (standard: 6, expert: 0)
- `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets --all-features -- -D warnings` pass. `cargo test --workspace --lib -- div checked registers_both classify_scan_error reports_audited exasol_dialect_renders_declared a_computed_integer` passes: 68 tests, 0 failures.
- No E2E suite was run.

## Standard fixes

### crates/lakehouse-engine/src/scan/checked_div.rs

#### [UNTESTED_ERROR_PATH] `i128::MIN / -1` aborts the VM instead of failing the query
- Location: line 317 (`quotients.push(numerator / divisor);` in `divide_integers`), and the doc comment at line 302
- Issue: Rust's `/` on `i128` panics on `i128::MIN / -1` in every build profile. The doc comment says operands of at most 38 digits cannot reach that case. Nothing enforces that bound: `as_i128` clones a `Decimal128` array as it is, and Arrow does not check stored values against the declared precision. A Parquet file can store any 16-byte value in a `DECIMAL(p,0)` column. The panic happens on a DataFusion worker thread, and `scan/diagnostics.rs` states that such a panic aborts the VM, which also kills the sibling VMs. The checked division then crashes the VM where it should return an error. No test covers this case.
- Fix: In `crates/lakehouse-engine/src/scan/checked_div.rs`, add the variant `IntegerOverflow { numerator: i128, divisor: i128 }` to `CheckedDivError`. Its `Display` arm writes `f.write_str(&numeric_overflow(format_args!("{CHECKED_INT_DIV_FN}({numerator}, {divisor}) exceeds the 128-bit internal number representation")))`. In `divide_integers`, replace `quotients.push(numerator / divisor);` with `quotients.push(numerator.checked_div(divisor).ok_or(CheckedDivError::IntegerOverflow { numerator, divisor })?);`. Replace the second doc line of `divide_integers` with "`checked_div` truncates toward zero like `DIV`. A stored value outside its declared precision can still reach `i128::MIN / -1`, so that case fails instead of panicking." In `crates/lakehouse-engine/src/scan/checked_div_tests.rs`, add `checked_int_div_raises_numeric_overflow_on_i128_min_over_minus_one`. It calls `int_divide(decimal_of(38, 0, vec![Some(i128::MIN)]), decimal_of(38, 0, vec![Some(-1)]))`, expects an error, and asserts that `find_checked_div_error(&error)` matches `Some(CheckedDivError::IntegerOverflow { .. })` and that the message contains `numeric value out of range`.

#### [MAGIC_NUMBER] The integer NULL-row filler and the Null-operand precision are bare literals
- Location: line 311 (`quotients.push(0);` in `divide_integers`), line 293 (`return Ok(1);` in `integer_operand_precision`)
- Issue: The float path names its NULL-row filler `NULL_ROW_FILLER`. The integer path pushes a bare `0` for the same purpose. `integer_operand_precision` also returns a bare `1` for a Null operand.
- Fix: In `crates/lakehouse-engine/src/scan/checked_div.rs`, add `const NULL_ROW_INTEGER_FILLER: i128 = 0;` directly below `NULL_ROW_FILLER`, with the doc comment "/// The integer counterpart of [`NULL_ROW_FILLER`].", and push it in `divide_integers` in place of `0`. Add `const NULL_OPERAND_PRECISION: u8 = 1;` with the doc comment "/// A Null operand types as the smallest decimal, one digit.", and return it in `integer_operand_precision` in place of `1`. Remove the sentence "A Null operand has the one-digit precision of the smallest decimal." from the doc comment of `integer_operand_precision`, because the constant now states it.

#### [OUTDATED_COMMENT] The module doc says `/` matches `DIV` over `Int64`, then names an `Int64` failure
- Location: lines 5-7
- Issue: The module doc says DataFusion's `/` matches `DIV` over two `Int64` operands. The same sentence then lists `i64::MIN / -1` overflowing, which is an `Int64` case, and an unrecognised zero-divisor error, which also applies to `Int64`. The sentence contradicts itself.
- Fix: In `crates/lakehouse-engine/src/scan/checked_div.rs`, replace the module-doc text from "`vs_checked_int_div` exists because" through "not recognised by type." with: "`vs_checked_int_div` exists because DataFusion's `/` does not reproduce `DIV`: over `Decimal128(p, 0)` it returns a scaled decimal, `i64::MIN / -1` overflows `Int64`, and its zero-divisor error is not recognised by type."

### crates/lakehouse-engine/src/scan/checked_div_tests.rs

#### [UNTESTED_ERROR_PATH] The argument-count checks of `CheckedIntDivUdf` have no test
- Location: `CheckedIntDivUdf::return_type` (checked_div.rs line 248) and `CheckedIntDivUdf::invoke_with_args` (checked_div.rs line 265)
- Issue: Both methods reject a call that does not have exactly two arguments, with "takes exactly two arguments, got <n>". No test covers either path. The float function's matching path is covered by `checked_float_div_refuses_an_argument_count_other_than_two`. The helper behind that test, `invoke_with_argument_count`, works only with `CheckedFloatDivUdf`.
- Fix: In `crates/lakehouse-engine/src/scan/checked_div_tests.rs`, add a first parameter `udf: ScalarUDF` to `invoke_with_argument_count` and use it in place of the `CheckedFloatDivUdf` it builds. Pass `ScalarUDF::from(CheckedFloatDivUdf::new(Arc::default()))` at the existing call. Add `checked_int_div_refuses_an_argument_count_other_than_two`, which runs two checks with one and with three `int64` arrays. The first check passes `ScalarUDF::from(CheckedIntDivUdf::new(Arc::default()))` to `invoke_with_argument_count`. The second check calls `CheckedIntDivUdf::new(Arc::default()).return_type` with one and with three `DataType::Int64` entries. For every call, assert that the error message contains "takes exactly two arguments" and `got <count>`.

#### [REDUNDANT_COMMENT] A unit test carries a Scenario line that its E2E test already proves
- Location: line 641 (`/// Scenario: A pushed-down integer division by zero fails the query` on `checked_int_div_over_two_literals_surfaces_a_division_by_zero_message`). The same applies to `crates/vs-expression/src/lib_tests.rs` line 2248 (on `div_renders_checked_integer_division_only_in_the_datafusion_dialect`) and to `crates/vs-expression/src/scan_typing_tests.rs` lines 256-257 (on `div_runs_in_the_scan_only_over_integer_operands`).
- Issue: `specs/testing.md` § Coverage rule says a unit test carries a `/// Scenario:` line only when it is the scenario's one proof. `e2e_integer_div_matches_native_over_a_native_copy` proves both "Integer division over …" scenarios, and `e2e_integer_div_by_zero_fails_with_division_by_zero` proves "A pushed-down integer division by zero fails the query". The `FLOAT_DIV` unit tests carry no Scenario lines.
- Fix: Delete the `/// Scenario:` line at `crates/lakehouse-engine/src/scan/checked_div_tests.rs` line 641, the line at `crates/vs-expression/src/lib_tests.rs` line 2248, and the two lines at `crates/vs-expression/src/scan_typing_tests.rs` lines 256-257. Keep the Scenario lines on the two E2E tests in `crates/lakehouse-e2e/tests/e2e_local/scan.rs`.

### crates/lakehouse-e2e/tests/e2e_local/scan.rs

#### [MISSING_BOUNDARY_TEST] The E2E case table omits a NULL numerator over a zero divisor and a DIV group key
- Location: lines 3686-3707 (`cases` in `e2e_integer_div_matches_native_over_a_native_copy`)
- Issue: The scenario "Integer division over integer operands runs in the scan" requires two things that no E2E case checks. First, a NULL operand yields NULL even over a zero divisor. The table has a NULL divisor (`NULLIF(ID - 6, 0)`) but no NULL numerator over a zero divisor, the case where checking for zero before checking for NULL returns an error instead of NULL. Second, the `DIV` node must not be why the scan declines a key. The table covers the select list and a filter but no `GROUP BY` key.
- Fix: In `crates/lakehouse-e2e/tests/e2e_local/scan.rs`, in `e2e_integer_div_matches_native_over_a_native_copy`, change the type of `cases` from `[(&str, PathCheck); 5]` to `[(&str, PathCheck); 7]` and append two cases: `("SELECT ID, DIV(NULLIF(ID - 7, ID - 7), ID - ID) FROM {T}", PathCheck::Has(CHECKED_INT_DIV_FN))` and `("SELECT DIV(ID, 4), COUNT(*) FROM {T} GROUP BY DIV(ID, 4)", PathCheck::Has(CHECKED_INT_DIV_FN))`.

## Expert fixes
[none]
