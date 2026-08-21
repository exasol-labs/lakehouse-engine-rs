# Verification Report: fix-float-div-truncation

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `FLOAT_DIV` renders `(CAST(<left> AS DOUBLE) / <right>)` in the DataFusion dialect only; Exasol dialect and ADD/SUB/MULT/NEG stay byte-identical. All repro, unit, characterization, and regression tests pass; review findings fixed; full suite green. |
| Code review | 6 findings — 6 fixed (5 standard, 1 expert) |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test --workspace`) | ✓ |
| Lint (`cargo clippy --all-targets`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (covered by automated E2E equivalents — see Notes) |
| E2E (`make test-e2e`) | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed | Ignored |
|------|-----|--------|--------|---------|
| Unit + doc (`cargo test --workspace`) | 1609 | 1607 | 0 | 2 |
| E2E (`make test-e2e`, live local Docker stack) | 303 | 303 | 0 | 0 |

New tests added by this plan, all passing:

**Unit (`crates/vs-expression/src/lib_tests.rs`):** `renders_arithmetic_div` (repinned), `arithmetic_operator_set_matches_advertised_capabilities` (repinned), `arithmetic_operators_render_identically_in_both_dialects` (retargeted to ADD/SUB/MULT/NEG identity only), `float_div_casts_to_double_only_in_the_datafusion_dialect` (new divergence test, split out per review), plus the operand-matrix/NULL-propagation suite: `float_div_casts_column_left_operand_against_column_right_operand`, `float_div_casts_literal_left_operand_against_{column,literal}_right_operand`, `float_div_casts_nested_expression_left_operand_against_{column,literal}_right_operand`, `float_div_casts_aggregate_left_operand_against_{column,literal}_right_operand`, `float_div_with_null_left_operand_casts_the_null_literal`, `float_div_with_null_right_operand_divides_by_null`, `float_div_null_over_zero_casts_the_null_literal_over_a_zero_literal`.

**E2E (`crates/lakehouse-engine/tests/e2e_scan_test.rs`, `e2e_capability_test.rs`):** `e2e_float_div_int_over_int_matches_native_oracle`, `e2e_float_div_filter_row_count_matches_native_oracle`, `e2e_float_div_decimal_over_int_matches_native_oracle`, `e2e_float_div_decimal_over_decimal_matches_native_oracle`, `e2e_float_div_pushes_double_cast_projection`, `e2e_float_div_by_zero_projected_fails_with_inf_out_of_range`, `e2e_zero_div_zero_projected_returns_silent_null`.

Confirmed unedited and still passing (Exasol-dialect / regression guard, task 4.1 and 5.3): `exasol_dialect_renders_declared_verbatim_surface`, `scalar_over_agg_tests.rs::render_substitutes_the_callers_merged_expressions_by_plan_slot`, `single_group_agg_tests.rs::merge_select_interleaves_items_in_selectlist_order_with_per_item_casts`, both `dispatch_golden/single_group_scalar_over_aggregate_{dedup,interleaved}.sql`, `e2e_scan_test.rs`'s two scalar-over-aggregate native-oracle tests, `e2e_join_test.rs`'s two `RETURN_PCT` tests — all byte-identical to `main`.

### Manual Tests

| Feature | Result |
|---------|--------|
| `EXPLAIN VIRTUAL` shows `(CAST("L_ORDERKEY" AS DOUBLE) / "L_LINENUMBER")` with `emit_exa_types: ["DOUBLE PRECISION"]` | ✓ — asserted live by `e2e_float_div_pushes_double_cast_projection` (task 5.1) |
| int/int, decimal/int, decimal/decimal division match native oracle | ✓ — asserted live by the four `e2e_float_div_*_matches_native_oracle` tests; decimal cases confirmed **bit-exact** (0 relative error) against the native oracle at `id=6`, well inside the `1e-15` bound |
| Filter row count matches native oracle | ✓ — `e2e_float_div_filter_row_count_matches_native_oracle`, additionally grounded against `FACT_LINEITEM`'s real `LINES_PER_ORDER` seed layout per review fix 8.5 |
| Exasol-dialect merge SQL unmoved (`ROUND((SUM(.../SUM(...)), 2)`, no CAST) | ✓ — task 4.1 confirmed byte-identical to `main` |
| `x/0` projected fails at `22002` ("value inf ... out of range") | ✓ — `e2e_float_div_by_zero_projected_fails_with_inf_out_of_range` |
| `0/0` projected succeeds with silent NULL (tracked by #246) | ✓ — `e2e_zero_div_zero_projected_returns_silent_null` |

The plan's manual-testing table (8 `exapump` command rows) is covered by the automated E2E tests above, which assert the identical live behavior programmatically; a second manual `exapump` pass was not run separately to avoid duplicating the same live queries against the Docker stack.

## Tool Evidence

### Linter

```
cargo clippy --all-targets
    Checking lakehouse-catalog v0.2.0
    Checking vs-expression v0.2.0
    Checking lakehouse-engine v0.41.0
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 19.61s
0 warnings
```

### Formatter

```
cargo fmt --check
(no output — clean)
```

## Scenario Coverage

| Feature | Scenario | Test Location | Test Name | Passes |
|---------|----------|----------------|-----------|--------|
| vs-expression-translator-scalar-ops | Arithmetic operators translate to binary SQL expressions (CHANGED) | `crates/vs-expression/src/lib_tests.rs` | `arithmetic_operator_set_matches_advertised_capabilities` | Pass |
| vs-expression-translator-scalar-ops | FLOAT_DIV renders true float division in the DataFusion dialect (NEW) | `crates/vs-expression/src/lib_tests.rs` | `renders_arithmetic_div` + operand-matrix/NULL suite | Pass |
| vs-expression-translator-scalar-ops | FLOAT_DIV renders true float division in the DataFusion dialect (NEW) | `crates/lakehouse-engine/tests/e2e_scan_test.rs`, `e2e_capability_test.rs` | `e2e_float_div_int_over_int_matches_native_oracle`, `e2e_float_div_filter_row_count_matches_native_oracle`, `e2e_float_div_decimal_over_int_matches_native_oracle`, `e2e_float_div_decimal_over_decimal_matches_native_oracle`, `e2e_float_div_pushes_double_cast_projection` | Pass |
| vs-expression-translator-scalar-ops | The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator (NEW) | `crates/vs-expression/src/lib_tests.rs` | `float_div_casts_to_double_only_in_the_datafusion_dialect`, `exasol_dialect_renders_declared_verbatim_surface` (unedited) | Pass |
| vs-expression-translator-scalar-ops | The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator (NEW) | `dispatch_golden_tests.rs` | `single_group_scalar_over_aggregate_dedup_matches_golden`, `..._interleaved_matches_golden` (both unedited) | Pass |
| vs-expression-translator-scalar-ops | A pushed-down division by zero fails the query rather than returning a wrong value (NEW) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_by_zero_projected_fails_with_inf_out_of_range` | Pass |
| vs-expression-translator-scalar-ops | Zero divided by zero reaches the tracked NaN-at-emit gap (NEW) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_zero_div_zero_projected_returns_silent_null` | Pass |
| vs-expression-translator-scalar-fns | Integer division DIV is deliberately not translated (CHANGED) | `crates/vs-expression/src/lib_tests.rs` | `div_falls_through_as_unsupported` (unedited) | Pass |
| vs-expression-translator-scalar-fns | FLOAT_DIV stays outside the verbatim rule in both dialects (NEW) | `crates/vs-expression/src/lib_tests.rs` | `exasol_dialect_renders_declared_verbatim_surface` (unedited) | Pass |

## Notes

- **Predicate-position divide-by-zero is a separate, tracked gap, not part of this fix's scope.** Task 1.2's live measurement found a divide-by-zero in a `WHERE` predicate (single-table and broadcast-join) silently returns a wrong row count rather than raising — distinct from `#246` (a projected-value NaN-to-NULL divergence). Filed as issue [#370](https://github.com/exasol-labs/lakehouse-engine-rs/issues/370); the spec delta's placeholder clause was replaced with a citation to it.
- **Code review (6 findings, all fixed):** deduplicated the `/` operator spelling and `DOUBLE` constant into single sources of truth, replacing a fallible JSON-round-trip helper with an infallible pure `cast_to_double`; split a divergence assertion out of an identity-named test rather than have it assert a contradiction; removed a byte-identical duplicate test; removed two unmandated inline comments; grounded a filter-row-count oracle test against the real fixture's seed layout instead of an unverified literal; and tightened the decimal-oracle tolerance from an effectively ~1e-5-relative bound (due to an `.max(1.0)` floor) to a true `1e-15` relative bound matching the plan's requirement — re-verified live, with the pushed-down value found bit-exact (0 relative error) against the native oracle.
- **No version bump in this PR**, per explicit instruction for this run.
- Decimal-numerator E2E assertions use a relative tolerance (`1e-15`, decision-log [7]'s measured ~1 ULP worst case) rather than string/bit equality, except the scale-0 int/int case which is asserted bit-exact as the plan permits.
