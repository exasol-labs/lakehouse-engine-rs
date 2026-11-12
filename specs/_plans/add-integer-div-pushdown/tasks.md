# Tasks: add-integer-div-pushdown

## Phase 2: Implementation (Group A: Translator)
- [x] 1.1 Add `CHECKED_INT_DIV_FN`, rename `string_call` to `datafusion_call`, add `ResultRule::IntegerOverIntegers` and the `DIV` row
- [x] 1.2 `scan_typing.rs`: decline `IntegerOverIntegers` unless every argument is an integer; map the rule to `TypeClass::INTEGER`
- [x] 1.3 Translator tests (`lib_tests.rs`, `scan_typing_tests.rs`)

## Phase 2: Implementation (Group B: Scan function)
- [x] 2.1 `checked_div.rs`: `CheckedDivError`, `IntegerZeroDivisor`, `CheckedIntDivUdf`, `register_checked_div_udfs`
- [x] 2.2 Scan function tests (`checked_div_tests.rs`, `emit_tests.rs`, `object_store_tests.rs`)

## Phase 2: Implementation (Group C: Advertise and prove)
- [x] 3.1 Advertise `FN_DIV` and update `capabilities_tests.rs`
- [x] 3.2 Docs: `capabilities.md`, `capability-matrix.md`
- [x] 3.3 E2E tests and harness constant

## Phase 3: Verification
- [x] 4.1 Build `make cross-udf-build`
- [x] 4.2 Unit tests `cargo test --workspace --lib`
- [x] 4.3 Local E2E on own stack
- [x] 4.4 `make fmt-check lint`
- [x] 4.5 Manual testing steps

## Phase 4: Review Fixes
- [x] 4.6 `checked_div.rs`: add `CheckedDivError::IntegerOverflow`, use `checked_div` in `divide_integers`, fix its doc, and add test `checked_int_div_raises_numeric_overflow_on_i128_min_over_minus_one`
- [x] 4.7 `checked_div.rs`: add `NULL_ROW_INTEGER_FILLER` and `NULL_OPERAND_PRECISION` constants and use them
- [x] 4.8 `checked_div.rs`: rewrite the module doc sentence on `vs_checked_int_div`
- [x] 4.9 `checked_div_tests.rs`: parameterize `invoke_with_argument_count` by UDF and add `checked_int_div_refuses_an_argument_count_other_than_two`
- [x] 4.10 Delete the redundant `/// Scenario:` lines in `checked_div_tests.rs`, `lib_tests.rs`, `scan_typing_tests.rs`
- [x] 4.11 `scan.rs` E2E: add the NULL-numerator-over-zero-divisor and `GROUP BY DIV` cases to `e2e_integer_div_matches_native_over_a_native_copy`
