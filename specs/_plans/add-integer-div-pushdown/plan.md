# Plan: add-integer-div-pushdown

## Summary

The adapter advertises `FN_DIV`, and the scan computes Exasol's integer division `DIV` through a checked integer-division function when both operands are integers (#615). A `DIV` with a decimal or DOUBLE operand keeps running in Exasol, and `FLOAT_DIV` behavior does not change.

## Context

- Part 3 of #615. Parts 1 and 2 are on this branch.
- `FN_DIV` is unadvertised today, so Exasol computes every `DIV` over the scanned rows.
- Live checks on Exasol 2025.1: `DIV` truncates toward zero, raises `22012` on a zero divisor, yields NULL for a NULL operand, and declares `DECIMAL(<numerator precision>,0)` for integer operands. Exasol pushes `DIV` as `function_scalar` `DIV` in the select list, filter, group keys, and aggregate arguments.
- DataFusion 55's `/` does not reproduce `DIV`: `Decimal128(p,0) / Decimal128(p,0)` returns a scaled decimal (`-3.5`), `i64::MIN / -1` overflows, and its zero-divisor message carries the storage-read framing.
- `crates/lakehouse-engine/src/scan/checked_div.rs` already holds the checked `FLOAT_DIV` function, its error type, its by-type error lookup, and its registration. `exasol_numeric.rs` already types integer Arrow inputs (`exact_ps`) and widens them to `i128` (`as_i128`).
- The typed scan pass already classes every operand as integer, NULL, or other, and a declined expression already takes each path's existing fallback.
- `FLOAT_DIV` behavior is unchanged, so `sql-comprehension/vs-expression-translator-float-div` and `datafusion-scan/scan-execution-expression-pushdown` get no delta.
- Architecture: no change.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| VS Expression Translator, Scalar Operations | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |
| Pushdown Planning, Capability Extensions | CHANGED | `pushdown-capabilities/pushdown-planning-capability-extensions/spec.md` |
| Pushdown Planning, String Function and Type-Conversion Capability Extensions | CHANGED | `pushdown-capabilities/pushdown-planning-string-conversion-capability/spec.md` |

## Impact

- A query with `DIV` over integer operands computes the quotient in the scan. Before, the scan returned the operand columns and Exasol divided.
- A pushed `DIV` by zero fails the query with a `division by zero` message, as native Exasol does.
- `docs/capabilities.md` and `docs/capability-matrix.md` list `FN_DIV` as advertised.
- No other observable change.

## Implementation Tasks

1. Translator (`crates/vs-expression`)
   - [ ] 1.1 Add `pub const CHECKED_INT_DIV_FN: &str = "vs_checked_int_div"` beside `CHECKED_FLOAT_DIV_FN` in `lib.rs`. Its doc states the contract the registered function meets: two integer arguments (Arrow signed or unsigned integers, `Decimal128(p,0)`, or Null), truncation toward zero, a `Decimal128(<numerator precision>,0)` result, NULL skipped before the zero check, and an error on a zero divisor. In `scalar_functions.rs`, widen `string_call` to any same-arity DataFusion call: rename the field and builder to `datafusion_call` and update its doc and the generic-arm comment in `lib.rs`. Add `ResultRule::IntegerOverIntegers` and the row `F::verbatim("DIV").typed(T::Never, R::IntegerOverIntegers).datafusion_call(CHECKED_INT_DIV_FN)`, so the generic arm renders `DIV(<l>, <r>)` for Exasol and `vs_checked_int_div(<l>, <r>)` for DataFusion with no per-name arm.
   - [ ] 1.2 In `scan_typing.rs`, `scalar_function_class` declines an `IntegerOverIntegers` call unless every argument `is_integer()`, with a `decline` reason that names the node. `ResultRule::class` maps the rule to `TypeClass::INTEGER`.
   - [ ] 1.3 Tests. `lib_tests.rs`: delete `div_falls_through_as_unsupported`, add the `DIV` sweep fixture (`DIV("A", "B")`), add `CHECKED_INT_DIV_FN` to the Exasol-dialect banned tokens, and add `div_renders_checked_integer_division_only_in_the_datafusion_dialect`. `scan_typing_tests.rs`: add the case table `div_runs_in_the_scan_only_over_integer_operands` (integer column and literal, NULL on either side, a nested integer `ADD` render; a scaled decimal or DOUBLE on either side, and text, decline), and an `integer quotient` row in `a_computed_integer_converts_like_a_column`.
2. Scan function (`crates/lakehouse-engine/src/scan`)
   - [ ] 2.1 Turn `checked_div.rs` into the module for both checked divisions. Rename `CheckedFloatDivError` to `CheckedDivError` and add `IntegerZeroDivisor { numerator: i128 }`, whose message opens with `data exception - division by zero`. The `FLOAT_DIV` variants keep their messages. Rename `find_checked_float_div_error` to `find_checked_div_error`. Add `CheckedIntDivUdf` (`Signature::any(2, Volatility::Immutable)`, the same `RaisedFailure` recording). `return_type` takes the numerator's precision from `exact_ps` with scale 0, returns `Decimal128(1,0)` for a Null numerator, and fails planning for a non-zero scale or a non-integer type on either side. `invoke_with_args` widens both operands with `as_i128`, unions their null masks, skips NULL rows before the zero check, and divides with `i128` `/`, which truncates toward zero; 38-digit operands cannot reach `i128::MIN / -1`. Replace `register_checked_float_div_udf` with `register_checked_div_udfs`, which registers both functions, and update the module doc and the call sites in `scan/mod.rs` and `emit.rs`. Make `exact_ps` and `as_i128` in `exasol_numeric.rs` `pub(super)` for this reuse, unchanged.
   - [ ] 2.2 Tests. `checked_div_tests.rs`: rename the float helpers' type only, then add integer cases: the three sign cases with an odd numerator, a zero divisor raising `IntegerZeroDivisor` and recording it on the session, NULL over zero and NULL on either side, `Int64` `i64::MIN / -1` = `9223372036854775808`, `UInt64::MAX / 1`, `DECIMAL(36,0)` `999…9 / 7`, the result type per input (Int8 3, Int16 5, Int32 10, Int64 19, UInt8 3, UInt16 5, UInt32 10, UInt64 20, `Decimal128(p,0)` p, Null 1), planning failures for `Float64` and `Decimal128(10,2)`, and through `scan_session()` SQL: a 20-digit literal numerator divides, and `vs_checked_int_div(7, 0)` surfaces `division by zero` that `find_checked_div_error` finds. `emit_tests.rs`: add an `IntegerZeroDivisor` row to `classify_scan_error_names_a_checked_division_failure_without_the_storage_prefix`. `object_store_tests.rs`: the registration test also checks `CHECKED_INT_DIV_FN`.
3. Advertise, docs, E2E
   - [ ] 3.1 Add `"FN_DIV"` after `"FN_FLOAT_DIV"` in `adapter/capabilities.rs`. In `capabilities_tests.rs`, remove `FN_DIV` from the must-not-advertise list and add it to the arithmetic must-advertise list of `reports_audited_capability_set`.
   - [ ] 3.2 Docs: add `FN_DIV` to the Arithmetic row of `docs/capabilities.md`. In `docs/capability-matrix.md`, mark `DIV` advertised with the note "The scan computes integer operands. Exasol SQL computes decimal and DOUBLE operands."
   - [ ] 3.3 E2E. Add `CHECKED_INT_DIV_FN` to `crates/lakehouse-e2e/tests/common/e2e_harness.rs`. In `crates/lakehouse-e2e/tests/e2e_local/scan.rs`, beside the `FLOAT_DIV` tests, add `e2e_integer_div_matches_native_over_a_native_copy`: a case table over `typed_table()` checked with `create_native_copy` and `native_copy_mismatch` (rows and column types). Rows: `SELECT ID, DIV(ID - 7, 2), DIV(ID - 7, -2), DIV(ID - 7, NULLIF(ID - 6, 0)), DIV(C_QTY, ID) FROM {T}` and `SELECT ID FROM {T} WHERE DIV(ID - 7, 2) < 0` with `Has(CHECKED_INT_DIV_FN)`; `SELECT ID, DIV(C_DECIMAL_A, 2) FROM {T}`, `SELECT ID, DIV(C_DOUBLE, 2) FROM {T}`, and `SELECT ID FROM {T} WHERE DIV(C_DECIMAL_A, 3) > 5` with `Lacks(CHECKED_INT_DIV_FN)`. Add `e2e_integer_div_by_zero_fails_with_division_by_zero`: `DIV(ID, ID - ID)` in the select list, the same in a WHERE filter, and `DIV(ID - ID, ID - ID)` in the select list, each through `division_by_zero_failure_sql_code` with `UDF_ERROR_SQL_CODE`.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Translator | 1.1-1.3 | none | spec delta `sql-comprehension/vs-expression-translator-scalar-ops`; `crates/vs-expression/src/{lib.rs,scalar_functions.rs,scan_typing.rs}`, `lib_tests.rs`, `scan_typing_tests.rs` |
| B: Scan function | 2.1-2.2 | A (reads `CHECKED_INT_DIV_FN`) | spec delta `sql-comprehension/vs-expression-translator-scalar-ops` (scenario "A pushed-down integer division by zero fails the query"); `crates/lakehouse-engine/src/scan/{checked_div.rs,exasol_numeric.rs,emit.rs,mod.rs}`, `checked_div_tests.rs`, `emit_tests.rs`, `object_store_tests.rs` |
| C: Advertise and prove | 3.1-3.3 | A, B | spec deltas `pushdown-capabilities/pushdown-planning-capability-extensions`, `pushdown-capabilities/pushdown-planning-string-conversion-capability`, `sql-comprehension/vs-expression-translator-scalar-ops`; `crates/lakehouse-engine/src/adapter/capabilities.rs`, `capabilities_tests.rs`, `docs/capabilities.md`, `docs/capability-matrix.md`, `crates/lakehouse-e2e/tests/e2e_local/scan.rs`, `crates/lakehouse-e2e/tests/common/e2e_harness.rs` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test | `crates/vs-expression/src/lib_tests.rs` `div_falls_through_as_unsupported` | `DIV` now renders |
| Test list entry | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` `"FN_DIV"` in the must-not-advertise list | `FN_DIV` is advertised |
| Function | `crates/lakehouse-engine/src/scan/checked_div.rs` `register_checked_float_div_udf` | Replaced by `register_checked_div_udfs` |

## Out of scope

- An operand the typed pass classes as integer but DataFusion types as `Float64`, such as a `CASE`, `GREATEST`, or `LEAST` over integers wider than 36 digits, fails scan planning instead of falling back.
- The float-div spec's phrase "the single checked-division function the crate declares" predates this plan. The plan writes no float-div delta, because `FLOAT_DIV` behavior is unchanged.
- A cheap guard conjunct such as `WHERE b <> 0 AND DIV(a, b) > 0` can return the guarded rows where native Exasol raises, the same known gap the float-div spec records for `/` (#392).

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Arithmetic operators translate to binary SQL expressions | Unit | `crates/vs-expression/src/lib_tests.rs`, `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `arithmetic_operators_render_identically_in_both_dialects`, `reports_audited_capability_set` |
| Integer division over integer operands runs in the scan | E2E | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_integer_div_matches_native_over_a_native_copy` |
| Integer division over integer operands runs in the scan (`i64::MIN / -1`, result type per input) | Unit | `crates/lakehouse-engine/src/scan/checked_div_tests.rs` | the integer cases of task 2.2 |
| Integer division over a decimal or DOUBLE operand runs in Exasol | E2E | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_integer_div_matches_native_over_a_native_copy` |
| A pushed-down integer division by zero fails the query | E2E | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_integer_div_by_zero_fails_with_division_by_zero` |
| Conversion and unary-negation capabilities are advertised so CAST and unary-minus expressions push down | Unit, E2E | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs`, `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `reports_audited_capability_set`, `cast_to_a_non_emittable_target_matches_native` |

The capability-extensions delta changes only its Background, so it maps no scenario.

### Manual Testing

Run against an own stack per `.claude/CLAUDE.md` (`COMPOSE_PROJECT_NAME=lakehouse-engine-rs-2`, `LH_EXASOL_PORT=38563`), after `make cross-udf-build` and `make e2e-up`. `DSN="exasol://sys:$EXASOL_SYS_PASSWORD@localhost:$LH_EXASOL_PORT?validateservercertificate=0"`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Scalar Operations | `exapump sql "EXPLAIN VIRTUAL SELECT ID, DIV(ID - 7, 2) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE" -d "$DSN"` | Pushdown SQL contains `vs_checked_int_div(` |
| Scalar Operations | `exapump sql "SELECT ID, DIV(ID - 7, 2), DIV(ID - 7, -2) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE ORDER BY ID" -d "$DSN"` | ID 2: `-2`, `2`; ID 8: `0`, `0`; ID 10: `1`, `-1` |
| Scalar Operations | `exapump sql "SELECT ID, DIV(ID, ID - ID) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE" -d "$DSN"` | Error naming `division by zero`, without `assigned data could not be read` |
| Scalar Operations | `exapump sql "EXPLAIN VIRTUAL SELECT ID, DIV(C_DECIMAL_A, 2) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE" -d "$DSN"` | No `vs_checked_int_div`; the wrapper SQL contains `DIV(` |
| Capability Extensions | `exapump sql "EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE DIV(ID, 2) = 3" -d "$DSN"` | The scan filter contains `vs_checked_int_div(`, so Exasol delegated `FN_DIV` |
| String Conversion Capability | `exapump sql "EXPLAIN VIRTUAL SELECT CAST(ID AS VARCHAR(20)), -ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE" -d "$DSN"` | The scan projection contains `CAST(` and the negation of `ID` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace --lib` | 0 failures |
| E2E | `make e2e-up test-e2e` with the own-stack overrides from `.claude/CLAUDE.md` | 0 failures |
| Lint and format | `make fmt-check lint` | 0 errors or warnings, no changes |
