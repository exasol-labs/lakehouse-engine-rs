# Plan: fix-string-fn-type-coercion-udf

## Summary

String functions and string CASTs over non-string values fail or return wrong values on the GROUP BY and aggregate-argument pushdown paths (issue #227). This plan adds a DataFusion session function, `exa_to_varchar`, that converts every Arrow type to Exasol's text. The DataFusion dialect wraps every string-converted argument in it. The adapter keeps no string-conversion decision, and the two adapter rewrites it replaces are deleted. The plan also closes #223.

## Design

### Context

Exasol converts a non-string argument to text before it applies a string function (`UPPER`, `SUBSTR`, `LENGTH`, `CONCAT`, ...) or a `CAST(... AS VARCHAR/CHAR)`. DataFusion does not. Two adapter passes compensate today by rewriting the expression JSON with column types: `string_function_arg_type_guard` (#210) and `rewrite_decimal_stringifications` (#211), both inside `apply_type_rewrites` (`adapter/pushdown/support.rs`). They run on the WHERE filter and the plain select list only.

Two paths skip them. `detect_group_by_aggregates` (`grouped_agg.rs`) renders GROUP BY keys and non-aggregate grouped select items with bare `render_expression`. `arg_column_or_expr` (`scalar_over_agg.rs`) renders aggregate arguments the same way, for `parse_agg_item`, HAVING, and scalar-over-aggregate items. Issue #227 reproduced both failure kinds on TPC-H SF100: `GROUP BY UPPER(c_custkey)` fails with `F-UDF-CL-RUST-9001` (SQL state `22002`), and `GROUP BY CAST(c_acctbal AS VARCHAR(20))` returns `0.50` where Exasol returns `0.5`. The aggregate path also pushes `INSTR(C_NAME,'0',12,1)` as a `strpos` without the start position, so `MAX(INSTR(c_name, '0', 12))` returns `10` where Exasol returns `12`.

Extending the rewrites to those paths is fragile. Plans and group keys are matched by rendered text at eight sites, the rewritten node is valid only in DataFusion, and column types cover bare columns only. DataFusion also types some values differently from Exasol: `c_acctbal * 1.5` is `Float64` in DataFusion and DECIMAL in Exasol, so an adapter-side type check cannot see what reaches the scan.

- **Goals**
  - Every surface that renders DataFusion SQL converts a string-function or string-CAST argument to Exasol's text, for every Arrow type.
  - The only point that knows the value's type, the scan session, owns the conversion.
  - A VARCHAR argument keeps today's DataFusion plan.
  - The two adapter rewrites and their node are deleted.
- **Non-Goals**
  - `like_subject_type_guard` (#207) keeps its adapter rewrite.
  - Session NLS settings other than the defaults (#216), faithful 3- and 4-argument `INSTR`/`LOCATE` (#228 step 2), `SUBSTR` with `start <= 0` (#432), and the empty-string rule (#203).
  - DataFusion's `Float64` typing of fractional literals (decision-log [10]).
  - A shared session-UDF registry for #431 and #201 (decision-log [2]).

### Decision

Convert in the scan session by Arrow type for every type, and keep the adapter out of string conversion (decision-log [1]).

#### Architecture

```
pushdown request
   │
   ▼
adapter  (pushdown/*.rs)
   ├─ apply_type_rewrites: like_subject_type_guard only, no string-conversion decision
   ├─ classify_request_shape: unchanged
   └─ empty_result_sql: unchanged (decision-log [8])
   ▼
vs-expression renderer  (crates/vs-expression/src/lib.rs)
   ├─ DataFusion dialect: string-converted arg ─► exa_to_varchar(arg), booleans included
   │                      string CAST          ─► CAST(exa_to_varchar(x) AS VARCHAR)
   │                      INSTR/LOCATE > 2 args ─► render error ─► surface fallback
   └─ Exasol dialect: unchanged, never emits exa_to_varchar
   ▼
scan UDF session  (scan/object_store.rs::build_session_context)
   └─ exa_to_varchar  (scan/to_varchar.rs)
         simplify: string input ─► the argument itself (plan unchanged)
         invoke:   Arrow-type dispatch ─► Exasol default-session text
```

#### Key Interfaces

- `vs_expression::EXA_TO_VARCHAR_FN: &str` = `"exa_to_varchar"`, the only link between renderer and registration.
- `scan::to_varchar::register_exa_to_varchar_udf(ctx: &SessionContext)`, `pub(super)`, and `ExaToVarcharUdf: ScalarUDFImpl` with `return_field_from_args`, `simplify`, and `invoke_with_args`.
- `support::apply_type_rewrites(expr, col_types) -> Option<Json>`: the signature is unchanged, and the body runs `like_subject_type_guard` alone.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Session function named by a `vs-expression` constant | `scan/to_varchar.rs`, `vs-expression` | Same split as `CheckedFloatDivUdf` / `CHECKED_FLOAT_DIV_FN` (#370). The renderer stays free of DataFusion. |
| Type dispatch where the type is known | `ExaToVarcharUdf` | DataFusion knows the Arrow type of a computed argument at planning time. The adapter does not. |
| Identity simplification | `ExaToVarcharUdf::simplify` | A string argument needs no conversion, so the optimizer removes the call and the plan stays as today. |
| Render error as a decline trigger | `INSTR`/`LOCATE` beyond two arguments | Every surface already routes a DataFusion render error to native Exasol evaluation. |

#### Quick Diagnostic

The change adds one module (`scan/to_varchar.rs`), and one constant. It removes seven adapter functions and one renderer node.

| Question | Answer |
|----------|--------|
| One-sentence responsibility? | `to_varchar.rs` converts an Arrow array to Exasol's default-session text. The renderer decides which arguments Exasol converts. |
| Easier to call than reimplement? | Yes. The renderer emits one call. The type dispatch, the DOUBLE formatter, and the JSON-fallback reuse stay inside the function. |
| Internal change forces an outside edit? | No. A conversion rule changes inside `to_varchar.rs` alone. The string-converted table changes inside `vs-expression` alone. |
| Doc comments explain the reasoning? | Required by tasks 2.1 and 3.1: each states why the decision lives there. |
| One owner per decision? | Yes: argument positions in `vs-expression`, text per Arrow type in `to_varchar.rs`. The adapter owns neither. |
| Boundaries visible? | The constant is the only crossing between the crates. |
| Tactical shortcut with a follow-up? | The standalone registration (decision-log [2]) is revisited when #431 or #201 lands. The `Float64` value range (decision-log [10]) gets its own issue in task 1.3. |
| Business logic independent of delivery mechanisms? | The string-converted table is pure syntax over JSON. Only `to_varchar.rs` names DataFusion, and it lives in the scan layer. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Convert every Arrow type in `exa_to_varchar`, and keep the adapter out (decision-log [1]) | Issue #227's adapter decline check plus a planning error for DOUBLE, BOOLEAN, and TIMESTAMP | DataFusion yields `Float64` where Exasol yields DECIMAL, so the planning error fails queries that work today. The three types have deterministic Exasol text. |
| Boolean arguments wrap like any other (decision-log [4]) | The #200 CASE form in every arm | One owner of boolean text |
| DOUBLE text gated by a live parity corpus (decision-log [6]) | C's `%.15g` | Exasol's text differs from `%.15g` on captured values |
| No zero-row wrapper on the empty path (decision-log [8]) | A builder that keeps the one NULL row for a fully pruned `INSTR` aggregate | The reviewer asked for it to go, and the `INSTR` case is an edge of an edge |
| One named session-setting exception, #216 (decision-log [9]) | Decline DOUBLE and TIMESTAMP for session dependence | DECIMAL depends on the session too |
| Standalone registration (decision-log [2]) | Shared registry for #431/#201 | Both issues are open, so a registry would be shaped by one consumer |

### Iceberg and Delta compliance gate

The rule applies, and the check finds no deviation (decision-log [12]). The conversion dispatches on the Arrow types of Iceberg and Delta primitives, and neither spec defines a query text form for a value. The quoted rows and the one named Exasol target-type trade-off, the 37- and 38-digit decimal, live in `datafusion-scan/scan-execution-exa-to-varchar`.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator-string-conversion | NEW | `specs/_plans/fix-string-fn-type-coercion-udf/sql-comprehension/vs-expression-translator-string-conversion/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-fns | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/sql-comprehension/vs-expression-translator-scalar-fns/spec.md` |
| sql-comprehension/vs-expression-translator-concat | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/sql-comprehension/vs-expression-translator-concat/spec.md` |
| sql-comprehension/vs-expression-translator-cast | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/sql-comprehension/vs-expression-translator-cast/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |
| datafusion-scan/scan-execution-exa-to-varchar | NEW | `specs/_plans/fix-string-fn-type-coercion-udf/datafusion-scan/scan-execution-exa-to-varchar/spec.md` |
| datafusion-scan/type-mapping-module-structure | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/datafusion-scan/type-mapping-module-structure/spec.md` |
| vs-adapter/pushdown-planning-string-fn-type-coercion | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-string-fn-type-coercion/spec.md` |
| vs-adapter/pushdown-planning-string-fn-type-coercion-composition | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-string-fn-type-coercion-composition/spec.md` |
| vs-adapter/pushdown-planning-decimal-string-format | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-decimal-string-format/spec.md` |
| vs-adapter/pushdown-planning-join-filter-type-coercion | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-join-filter-type-coercion/spec.md` |
| vs-adapter/pushdown-planning-like-type-coercion | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-like-type-coercion/spec.md` |
| vs-adapter/pushdown-module-dedup-consolidation | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-module-dedup-consolidation/spec.md` |
| vs-adapter/pushdown-col-types-consolidation | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-col-types-consolidation/spec.md` |

## Impact

- **Fixed:** every #227 repro returns the native Exasol result. A string function or string CAST pushes down on GROUP BY keys, aggregate arguments, HAVING, ORDER BY, and scalar-over-aggregate items. `MAX(INSTR(c_name, '0', 12))` returns `12`, not `10`.
- **New pushdowns, #223 closed:** a string function or string CAST over a DOUBLE, BOOLEAN, or TIMESTAMP value, bare or computed, pushes down and returns Exasol's text (`1e20`, `TRUE`, `2024-05-15 10:00:00.000000`). Before, `UPPER(c_double)` ran in the native-evaluation wrapper, and `CAST(c_ts AS VARCHAR(30))` returned DataFusion's `T`-separated text.
- **No new failure:** `CAST(ROUND(c_acctbal / 3, 2) AS VARCHAR(40))` and `CAST(c_acctbal * 1.5 AS VARCHAR(40))` keep working.
- **Named exceptions:** a session with non-default `NLS_NUMERIC_CHARACTERS`, `NLS_DATE_FORMAT`, or `NLS_TIMESTAMP_FORMAT` gets default-setting text from a pushed query (#216). A value Exasol types DECIMAL and DataFusion computes as `Float64` matches only at up to 15 significant digits and a magnitude from `1e-4` below `1e15` (#TBD, task 1.3).
- **Lost pushdown, rare shapes:** `INSTR`/`LOCATE` beyond two arguments on the grouped and aggregate paths, and in a scalar-over-aggregate residual, run in the wrapper. The result is correct and slower.
- **Sibling project:** a DataFusion-dialect consumer of `crates/vs-expression` MUST register a function under `EXA_TO_VARCHAR_FN`, as it already does for `CHECKED_FLOAT_DIV_FN`. The `decimal_to_varchar_exasol` node disappears.
- **Cost:** a non-string string-converted argument gains one `exa_to_varchar` call per batch. A string argument simplifies away, so its plan equals today's.
- **Unchanged:** the `ScanSpec` wire format, the advertised capabilities, and every configuration key. The VS adapter and the scan UDF ship in one `.so`, so one redeploy moves both.

## Dependencies

None blocking. #431 and #201 are not prerequisites (decision-log [2]). Task 1.3 opens the issue that replaces `#TBD` in the spec deltas. The implementing commit uses `Closes #227` and `Closes #223`, and `Refs #228, #216` (decision-log [13]).

## Implementation Tasks

### 1. Reproduce #227 on the Docker Exasol container (knowledge: E2E harness)

- [ ] 1.1 In `crates/lakehouse-engine/tests/e2e_capability_test.rs`, add a #227 section over `typed_distinct_probe` (`ID` for `c_custkey`, `C_DECIMAL_A` for `c_acctbal`, `C_DOUBLE`) and `dim_customer` (`C_NAME`). Each test asserts an independent oracle and fails, not skips, without a database: `e2e_group_by_upper_integer_key_matches_native`, `e2e_aggregate_over_upper_integer_matches_native`, `e2e_grouped_having_and_order_by_over_upper_integer_push_down`, `e2e_scalar_over_aggregate_of_upper_integer_matches_native`, `e2e_group_by_decimal_cast_key_trims_like_native`, `e2e_max_over_decimal_cast_trims_like_native`, `e2e_group_by_upper_double_key_matches_native`, `e2e_instr_three_args_on_grouped_paths_matches_native`, `e2e_max_instr_with_start_position_matches_native` (`MAX(INSTR(C_NAME, 'c', 2))`, native `0`).
- [ ] 1.2 Before any production change, run `make cross-udf-build`, then `cargo test --features exasol-e2e --test e2e_capability_test <name> -- --test-threads=1` for each new test. Record every observed failure (error text and SQL state, or wrong value) in `tasks.md` as the reproduction evidence CLAUDE.md requires.
- [ ] 1.3 Open a GitHub issue for the `Float64` value range that `datafusion-scan/scan-execution-exa-to-varchar` names (decision-log [10]). Replace every `#TBD` in this plan's spec deltas and in `plan.md` with its number.

### 2. Renderer string conversion (knowledge: `sql-comprehension` deltas)

- [ ] 2.1 Add `pub const EXA_TO_VARCHAR_FN: &str = "exa_to_varchar"` to `crates/vs-expression/src/lib.rs`, with a contract doc comment in the style of `CHECKED_FLOAT_DIV_FN`.
- [ ] 2.2 Add a private helper that owns the string-converted argument table and the string CAST rule. Test every table row, `LPAD`/`RPAD` at two and three arguments, `CHR`/`UNICODECHR`, a non-string CAST, and other node types in `lib_tests.rs`.
- [ ] 2.3 In the DataFusion dialect, render every string-converted argument as `exa_to_varchar(<arg>)` in the string-function arm, the `CONCAT` arm, and the `INSTR`/`LOCATE` arms, boolean-producing arguments included. Keep the #200 CASE rewrite for the Exasol dialect only, and leave that dialect's output unchanged.
- [ ] 2.4 In `render_cast`, render a DataFusion-dialect string CAST as `CAST(exa_to_varchar(<source>) AS VARCHAR)` for every source. Restrict the boolean CASE branch to the Exasol dialect.
- [ ] 2.5 Make `INSTR`/`LOCATE` with more than two arguments a DataFusion-dialect render error, and keep the two-argument `strpos` rendering.
- [ ] 2.6 Update the `lib_tests.rs` expectations that pin DataFusion string renderings (`renders_string_scalar_functions`, `renders_concat_as_nullif_wrapped_concat_call`, `renders_cast_varchar`, and the other string-CAST tests). Add the Exasol-dialect sweep and the determinism tests. Gate: `cargo test -p vs-expression`.

### 3. The exa_to_varchar session function (knowledge: `datafusion-scan/scan-execution-exa-to-varchar`)

- [ ] 3.1 Create `crates/lakehouse-engine/src/scan/to_varchar.rs` with `ExaToVarcharUdf` (`ScalarUDFImpl`, one argument of any type, `Immutable`) and `register_exa_to_varchar_udf`. Declare `mod to_varchar;` in `scan/mod.rs`, end the new file with `#[cfg(test)] #[path = "to_varchar_tests.rs"] mod tests;`, and call the registration in `build_session_context` (`scan/object_store.rs`) beside `register_checked_float_div_udf`.
- [ ] 3.2 Implement `return_field_from_args`: the argument's own data type and nullability for a string argument, and `Utf8` otherwise. Implement `simplify` to return the argument for a `Utf8`, `LargeUtf8`, or `Utf8View` input. In `exa_to_varchar_string_argument_simplifies_away`, test that the optimized logical plan of `upper(exa_to_varchar("C_VARCHAR"))` carries no `exa_to_varchar` call, and that an `Int64` argument keeps it.
- [ ] 3.3 Implement the dispatch in this arm order: string types, `Null`, integers, in-domain `Decimal128`, `Float32`/`Float64`, `Boolean`, `Date32`, `Timestamp`, then the JSON fallback. Implement the string, `Null`, integer, `Decimal128`, `Boolean`, and `Date32` conversions. Test `-0.50`, `100.10`, `5.00`, `0.00`, `0.05`, `0.5`, scale 0, `i64::MIN`/`i64::MAX`, `TRUE`/`FALSE`, and NULL through a `SessionContext` in `exa_to_varchar_renders_integers_as_digits`, `exa_to_varchar_trims_decimal_trailing_zeros`, `exa_to_varchar_renders_boolean_as_uppercase`, `exa_to_varchar_renders_date32_as_iso`, and `exa_to_varchar_converts_null_type_to_null_text`.
- [ ] 3.4 Implement the `Timestamp` conversion for every unit: six fraction digits, a sub-microsecond fraction truncated, and the time zone ignored. Test a whole second, a millisecond value, a nanosecond value ending in `9999999`, and NULL in `exa_to_varchar_renders_timestamps_with_six_truncated_fraction_digits`.
- [ ] 3.5 Implement the `Float32`/`Float64` conversion to Exasol's DOUBLE text: a NaN yields NULL, and an infinite value raises an error naming `exa_to_varchar`. Test every captured value of the spec's Background and DOUBLE scenarios in `exa_to_varchar_renders_double_as_exasol_text`, and the non-finite values in `exa_to_varchar_maps_nan_to_null_and_rejects_infinity`. If a captured value cannot be reproduced, stop and report it rather than weakening the test. [expert]
- [ ] 3.6 Implement the JSON-fallback conversion by reusing `types::mapping::needs_json_fallback` and `needs_nested_json_rendering` with the scan's existing conversions (`render_nested_column_as_json`, and the Arrow cast to `Utf8` that `raw_scan::build_scan_sql` emits), with the cast options of DataFusion's SQL `CAST`. Test that `Decimal128(38, 4)`, a list, `Binary`, and `Time64` match the emitted text in `exa_to_varchar_matches_scan_emit_text_for_json_fallback_types`.
- [ ] 3.7 Move the three tests of `crates/lakehouse-engine/tests/boolean_to_string_casing_test.rs` into `to_varchar_tests.rs`, register `exa_to_varchar` in their `SessionContext`, keep their assertions unchanged, and delete the old file.
- [ ] 3.8 Add `build_session_context_registers_the_exa_to_varchar_function` to `scan/object_store_tests.rs`, reading the name from `vs_expression::EXA_TO_VARCHAR_FN`. Add `float64_arithmetic_converts_with_the_double_rule` to `to_varchar_tests.rs`: a `Decimal128` column times the literal `1.5`, and a rounded `Float64` quotient, convert to `1067.34` and `237.19`.

### 4. Adapter rewrite removal (knowledge: adapter type-rewrite deltas)

- [ ] 4.1 Rewire `apply_type_rewrites` to `like_subject_type_guard` alone. Delete `string_function_arg_type_guard`, `coerce_string_position_arg`, `StringPositionArgs`, `string_position_args`, `rewrite_decimal_stringifications`, `is_bare_decimal_column`, `wrap_decimal_to_varchar`, their tests, and the `decimal_to_varchar_exasol` arm of `project_columns`. Update the doc comments of `apply_type_rewrites`, `type_accepted_rewrite`, `rewrite_expr_tree`, `like_subject_type_guard`, `column_exa_type`, `wrap_cast_to_varchar`, `project_columns`, the `RowScan` arm comment in `pushdown/mod.rs`, and `ExaTypeClass`/`classify_exa_type` in `types/mapping.rs` so each names `guard_like_subject` as the one consumer.
- [ ] 4.2 Remove the `decimal_to_varchar_exasol` arm and `format_decimal_exasol_style` from `crates/vs-expression/src/lib.rs`, with `renders_decimal_to_varchar_exasol`, `decimal_to_varchar_exasol_wrong_arity_errors`, and `format_decimal_exasol_style_renders_exact_regex_sql`. Update the comments in `tests/e2e_capability_test.rs`, `tests/e2e_join_test.rs`, and `scan/emit_tests.rs` that name removed items. Depends on 4.1.
- [ ] 4.3 After 4.1, update the `lakehouse-engine` test expectations that pin DataFusion string renderings changed by group 2 (`pushdown_tests.rs`, `support_tests.rs`, `grouped_agg_tests.rs`, `single_group_agg_tests.rs`, `topn_tests.rs`, `joins/rendering_tests.rs`, `joins/sql_builders_tests.rs`), editing no other assertion.
- [ ] 4.4 Add the adapter tests of the string-fn, composition, LIKE, col-types, dedup, and join-filter deltas in `support_tests.rs`, `pushdown_tests.rs`, and `joins/sql_builders_tests.rs` (§ Scenario Coverage). Confirm that the `dispatch_golden` fixtures pass unedited.

### 5. Grouped and aggregate paths (knowledge: aggregate deltas)

- [ ] 5.1 Prove that every text-match site agrees under the wrapping renderer, and fix any site that re-renders differently: `detect_group_by_aggregates`, `build_grouped_order_by_clause`/`group_key_output_ordinal`, `parse_agg_item`/`arg_column_or_expr`, `fold_aggregate_plan`, `ordinary_plans`/`single_group_plan_types`, `render_having_over_merge`, `render_scalar_over_merge`/`classify_scalar_over_aggregate`, `parse_count_distinct`, and `empty_agg_sql`/`empty_scalar_over_aggregate_literal`. Add the surface-scenario tests in `grouped_agg_tests.rs`, `single_group_agg_tests.rs`, and `empty_result_tests.rs`, including `COUNT(DISTINCT UPPER(c_custkey))`. [expert]
- [ ] 5.2 Assert in `grouped_agg_tests.rs` and `single_group_agg_tests.rs` that the merge-wrapper SQL built for a string-function aggregate contains no `exa_to_varchar`.
- [ ] 5.3 Test that `INSTR` beyond two arguments routes a grouped request to `RequestShape::GroupByWrapper` and a single-group aggregate to `RequestShape::RowScan` in `request_shape_tests.rs`.

### 6. Live verification (knowledge: E2E harness)

- [ ] 6.1 Add to `tests/e2e_capability_test.rs`: `e2e_double_text_matches_native_parity_corpus` (every captured DOUBLE value of the spec, each against an in-session native oracle), `e2e_string_functions_over_double_boolean_timestamp_match_native` (`WHERE UPPER(C_DOUBLE) = '0.5'`, `CAST(C_BOOL AS VARCHAR(5))`, `MAX(CAST(C_TS AS VARCHAR(30)))`), `e2e_computed_float64_string_argument_matches_native` (`CAST(ROUND(C_DECIMAL_A / 3, 2) AS VARCHAR(40))`, `CAST(C_DECIMAL_A * 1.5 AS VARCHAR(40))`), `e2e_session_nls_settings_affect_only_the_tracked_types` (under `NLS_NUMERIC_CHARACTERS = ',.'`, DECIMAL text diverges as #216 records, and integer and BOOLEAN text match), and `e2e_max_instr_with_start_position_all_files_pruned_returns_one_null_row` (`WHERE C_CUSTKEY > 1000`, one NULL row for `MAX`, `0` for `COUNT`, and no `LAKEHOUSE_SCAN` in `EXPLAIN VIRTUAL`).
- [ ] 6.2 Run `make test-e2e`. Every group 1 test passes, and the existing #210, #211, #228, #374, #200, join, and complex-type E2E tests pass with no assertion edit. Record the `EXPLAIN VIRTUAL` evidence that each grouped repro carries `exa_to_varchar(` inside the scan spec.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| R: Reproduce #227 | 1.1-1.3 | — | issue #227 repros; `crates/lakehouse-engine/tests/e2e_capability_test.rs`, `crates/lakehouse-engine/tests/common/seed.rs` (`typed_distinct_probe`, `dim_customer`) |
| A: Renderer string conversion | 2.1-2.6 | R (reproduction precedes any fix) | spec deltas `sql-comprehension/vs-expression-translator-string-conversion`, `-scalar-fns`, `-concat`, `-cast`; `crates/vs-expression/src/lib.rs`, `crates/vs-expression/src/lib_tests.rs` |
| B: exa_to_varchar session function | 3.1-3.8 | A (reads `EXA_TO_VARCHAR_FN`) | spec delta `datafusion-scan/scan-execution-exa-to-varchar`; `crates/lakehouse-engine/src/scan/to_varchar.rs`, `to_varchar_tests.rs`, `scan/mod.rs`, `scan/object_store.rs`, `scan/object_store_tests.rs`, `tests/boolean_to_string_casing_test.rs` (moved), `types/mapping.rs` (read only) |
| C: Adapter rewrite removal | 4.1-4.4 | A (4.2 edits `lib.rs` after A, 4.3 pins A's renderings) | spec deltas `vs-adapter/pushdown-planning-string-fn-type-coercion`, `-string-fn-type-coercion-composition`, `-decimal-string-format`, `-like-type-coercion`, `-join-filter-type-coercion`, `pushdown-module-dedup-consolidation`, `pushdown-col-types-consolidation`, `datafusion-scan/type-mapping-module-structure`, `sql-comprehension/vs-expression-translator-scalar-ops`; `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, `support_tests.rs`, `mod.rs`, `pushdown_tests.rs`, `joins/rendering_tests.rs`, `joins/sql_builders_tests.rs`, `types/mapping.rs`; expectation strings only (task 4.3) in `grouped_agg_tests.rs`, `single_group_agg_tests.rs`, `topn_tests.rs` |
| D: Grouped and aggregate paths | 5.1-5.3 | C (updated expectations) | spec deltas `sql-comprehension/vs-expression-translator-string-conversion` (surface scenario); `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs`, `scalar_over_agg.rs`, `single_group_agg.rs`, `request_shape_tests.rs`, `empty_result_tests.rs` and their `_tests.rs` files |
| E: Live verification | 6.1-6.2 | B, C, D | every spec delta's E2E scenario; `crates/lakehouse-engine/tests/e2e_capability_test.rs` |

The groups run R, A, then B and C in parallel, then D, then E. Group A's gate is `cargo test -p vs-expression`. The `lakehouse-engine` suites that pin DataFusion string renderings go green again at task 4.3, and `tests/boolean_to_string_casing_test.rs` fails from group A until task 3.7 moves it. Group C and group A share `crates/vs-expression/src/lib.rs` only through task 4.2, which runs after A finishes. Group C and group D share `grouped_agg_tests.rs` and `single_group_agg_tests.rs` only through task 4.3's expectation-string updates, which finish before D starts. Group R and group E share the E2E file by design: CLAUDE.md requires the reproduction before the fix, and group E proves the same tests green after it.

Task 3.5 carries `[expert]`, so group B routes to the expert implementer: matching Exasol's DOUBLE formatter on the captured edge cases is non-obvious correctness. Task 5.1 carries `[expert]`, so group D routes to the expert implementer: it spans eight text-match sites in five files whose agreement is a cross-file behavioral dependency. Groups R, A, C, and E are untagged.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-engine/src/adapter/pushdown/support.rs::string_function_arg_type_guard` | Replaced by renderer wrapping and `exa_to_varchar` |
| Function | `support.rs::coerce_string_position_arg` | Per-type rewrite replaced by `exa_to_varchar` |
| Enum + function | `support.rs::StringPositionArgs`, `support.rs::string_position_args` | Table moved into `vs-expression`. The arity decline is a render error. |
| Function | `support.rs::rewrite_decimal_stringifications`, `support.rs::is_bare_decimal_column`, `support.rs::wrap_decimal_to_varchar` | Decimal trim moved into `exa_to_varchar` |
| Match arm | `support.rs::project_columns`, `"decimal_to_varchar_exasol"` | The node no longer exists |
| Match arm + function | `crates/vs-expression/src/lib.rs`, `"decimal_to_varchar_exasol"` arm and `format_decimal_exasol_style` | Replaced by `exa_to_varchar` |
| Test file | `crates/lakehouse-engine/tests/boolean_to_string_casing_test.rs` | Moved into `scan/to_varchar_tests.rs` (task 3.7) |
| Test | `support_tests.rs`: `string_fn_guard_*`, `string_position_args_*`, `rewrite_*`, `decimal_rewrite_*`, `stringify_*`, `selectlist_upper_decimal_arg_coerced_not_full_row`, `selectlist_instr_decimal_arg_coerces_first_position_only` | They test the removed passes |
| Test | `lib_tests.rs`: `renders_decimal_to_varchar_exasol`, `decimal_to_varchar_exasol_wrong_arity_errors`, `format_decimal_exasol_style_renders_exact_regex_sql` | They test the removed node |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| string-conversion: String-converted function arguments render through exa_to_varchar in the DataFusion dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `string_converted_function_args_render_through_exa_to_varchar` |
| string-conversion: A string CAST renders as a cast of exa_to_varchar in the DataFusion dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `string_cast_renders_as_cast_of_exa_to_varchar` |
| string-conversion: The Exasol dialect never renders exa_to_varchar | Unit | `crates/vs-expression/src/lib_tests.rs` | `exasol_dialect_never_renders_exa_to_varchar` |
| string-conversion: INSTR and LOCATE beyond two arguments are a DataFusion-dialect render error | Unit | `crates/vs-expression/src/lib_tests.rs` | `instr_and_locate_beyond_two_args_are_a_datafusion_render_error` |
| string-conversion: One node renders one text on every DataFusion surface | Unit + Integration | `grouped_agg_tests.rs`, `single_group_agg_tests.rs`, `empty_result_tests.rs`; `tests/e2e_capability_test.rs` | `string_fn_group_key_over_integer_decomposes_with_or_without_select_item`, `grouped_order_by_string_fn_key_resolves_to_group_key_ordinal`, `grouped_having_matches_string_fn_aggregate_by_rendered_text`, `grouped_scalar_over_string_fn_aggregate_keeps_conversion_in_scan`, `aggregate_over_string_fn_of_integer_decomposes`, `count_distinct_over_string_fn_carries_wrapped_arg`, `single_group_scalar_over_string_fn_aggregate_decomposes`, `empty_path_resolves_same_shape_for_string_fn_aggregates`; `e2e_group_by_upper_integer_key_matches_native`, `e2e_aggregate_over_upper_integer_matches_native`, `e2e_grouped_having_and_order_by_over_upper_integer_push_down`, `e2e_scalar_over_aggregate_of_upper_integer_matches_native` |
| scalar-fns: String scalar functions translate to DataFusion string calls | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_string_scalar_functions` |
| concat: CONCAT translates to a NULL-skipping DataFusion concat call | Unit + Integration | `crates/vs-expression/src/lib_tests.rs`; `scan/to_varchar_tests.rs` | `renders_concat_as_nullif_wrapped_concat_call`; `concat_predicate_matches_exasol_uppercase_not_datafusion_lowercase`, `concat_group_by_key_uses_exasol_uppercase_labels` |
| cast: CAST renders the mapped target type per dialect | Unit + Integration | `crates/vs-expression/src/lib_tests.rs`; `scan/to_varchar_tests.rs` | `renders_cast_varchar`; `explicit_cast_bool_to_varchar_renders_exasol_casing` |
| exa-to-varchar: The scan session registers exa_to_varchar for every scan spec | Integration | `crates/lakehouse-engine/src/scan/object_store_tests.rs` | `build_session_context_registers_the_exa_to_varchar_function` |
| exa-to-varchar: A string argument passes through and the call simplifies away | Integration | `crates/lakehouse-engine/src/scan/to_varchar_tests.rs` | `exa_to_varchar_string_argument_simplifies_away` |
| exa-to-varchar: An integer argument converts to plain digits | Integration | `scan/to_varchar_tests.rs` | `exa_to_varchar_renders_integers_as_digits` |
| exa-to-varchar: A DECIMAL argument converts with trailing scale zeros removed | Integration | `scan/to_varchar_tests.rs`; `tests/e2e_capability_test.rs`, `tests/e2e_join_test.rs` | `exa_to_varchar_trims_decimal_trailing_zeros`; `e2e_decimal_cast_trims_trailing_zeros`, `e2e_decimal_concat_trims_trailing_zeros`, `e2e_decimal_length_reflects_trimmed_string`, `e2e_decimal_length_where_count_matches_trimmed_semantics`, `e2e_join_decimal_stringification_matches_native_at_both_surfaces` |
| exa-to-varchar: A DOUBLE argument converts to Exasol's DOUBLE text | Integration | `scan/to_varchar_tests.rs` | `exa_to_varchar_renders_double_as_exasol_text`, `exa_to_varchar_maps_nan_to_null_and_rejects_infinity` |
| exa-to-varchar: DOUBLE text matches native Exasol on a live parity corpus | Integration | `tests/e2e_capability_test.rs` | `e2e_double_text_matches_native_parity_corpus` |
| exa-to-varchar: A BOOLEAN argument converts to TRUE or FALSE | Integration | `scan/to_varchar_tests.rs` | `exa_to_varchar_renders_boolean_as_uppercase` |
| exa-to-varchar: A DATE argument converts to ISO date text | Integration | `scan/to_varchar_tests.rs` | `exa_to_varchar_renders_date32_as_iso` |
| exa-to-varchar: A TIMESTAMP argument converts to Exasol's default timestamp text | Integration | `scan/to_varchar_tests.rs` | `exa_to_varchar_renders_timestamps_with_six_truncated_fraction_digits` |
| exa-to-varchar: A NULL-typed argument converts to NULL text | Integration | `scan/to_varchar_tests.rs` | `exa_to_varchar_converts_null_type_to_null_text` |
| exa-to-varchar: A JSON-fallback type converts to the text the scan emits for it | Integration | `scan/to_varchar_tests.rs` | `exa_to_varchar_matches_scan_emit_text_for_json_fallback_types` |
| exa-to-varchar: Session-dependent text follows the default session settings | Integration | `tests/e2e_capability_test.rs` | `e2e_session_nls_settings_affect_only_the_tracked_types` |
| exa-to-varchar: A value DataFusion computes as Float64 converts with the DOUBLE rule | Integration | `scan/to_varchar_tests.rs`; `tests/e2e_capability_test.rs` | `float64_arithmetic_converts_with_the_double_rule`; `e2e_computed_float64_string_argument_matches_native` |
| type-mapping-module-structure: One classifier names the Exasol type-string families the pushdown guards branch on | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `classify_exa_type_matches_pushdown_guard_predicates` |
| string-fn: A string-position VARCHAR or CHAR column argument pushes down unchanged | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `selectlist_upper_varchar_renders_exa_to_varchar`; `e2e_upper_varchar_pushdown` |
| string-fn: A string-position DECIMAL column argument renders through Exasol's trimmed decimal-to-string form | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `selectlist_upper_decimal_pushes_down_unrewritten`; `e2e_upper_id_trims_to_plain_integer_string`, `e2e_ltrim_decimal_trims_trailing_zeros` |
| string-fn: A string-position DATE column argument pushes down as ISO date text | Integration | `tests/e2e_capability_test.rs` | `e2e_lower_date_formats_as_iso` |
| string-fn: A DOUBLE, BOOLEAN, or TIMESTAMP column argument pushes down with Exasol's text | Unit + Integration | `pushdown_tests.rs`, `support_tests.rs`; `tests/e2e_capability_test.rs` | `where_filter_upper_double_pushes_into_scan_spec`, `selectlist_string_cast_over_boolean_pushes_down`; `e2e_string_functions_over_double_boolean_timestamp_match_native`, `e2e_group_by_upper_double_key_matches_native` |
| string-fn: An INSTR or LOCATE call beyond two arguments reaches native Exasol evaluation on every surface | Unit + Integration | `pushdown_tests.rs`, `request_shape_tests.rs`; `tests/e2e_capability_test.rs` | `where_filter_instr_beyond_two_args_self_applies`, `instr_beyond_two_args_routes_grouped_and_single_group_to_wrapper`; `e2e_instr_arity_decline_where_matches_native_oracle`, `e2e_instr_three_args_on_grouped_paths_matches_native`, `e2e_max_instr_with_start_position_matches_native` |
| string-fn: A computed string-converted argument converts by its DataFusion type | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `computed_string_argument_pushes_down_unchanged`; `e2e_computed_float64_string_argument_matches_native` |
| composition: The LIKE subject guard and the renderer's conversion compose without double conversion | Unit | `support_tests.rs` | `type_rewrite_pipeline_composes_like_guard_with_renderer_conversion` |
| decimal: A DECIMAL stringification in a GROUP BY key or an aggregate argument renders the trimmed form | Unit + Integration | `grouped_agg_tests.rs`; `tests/e2e_capability_test.rs` | `grouped_decimal_cast_key_matches_select_item_by_rendered_text`; `e2e_group_by_decimal_cast_key_trims_like_native`, `e2e_max_over_decimal_cast_trims_like_native` |
| join-filter: A join filter with no type-rewrite trigger emits byte-identical SQL | Unit | `joins/sql_builders_tests.rs` | `join_filter_without_string_conversion_trigger_emits_byte_identical_sql` |
| like: A select-list LIKE over a DATE column projects the CAST-to-VARCHAR form | Unit | `support_tests.rs` | `selectlist_like_over_date_projects_cast_expr` |
| dedup: The type-aware tree walk uses the shared post-order primitive | Unit | `support_tests.rs` | `like_guard_reaches_nested_node_and_declines_whole_tree` |
| dedup: One ordered pipeline function owns the type-rewrite pass order | Unit | `support_tests.rs` | `type_rewrite_pipeline_runs_like_guard` |
| col-types: One helper resolves a bare column node's Exasol type for every type-rewrite guard | Unit | `support_tests.rs` | `column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list` |
| col-types: The LIKE subject guard reads its type family from the shared classifier | Unit | `support_tests.rs` | `like_guard_classifies_every_type_family` |

Unit rows cover pure JSON-to-SQL rendering and adapter planning, which perform no I/O. Adapter test files live under `crates/lakehouse-engine/src/adapter/pushdown/`.

### Manual Testing

Run after `make test-e2e` has set up the Docker stack. `DSN` = `exasol://sys:exasol@localhost:28563?validateservercertificate=0`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-expression-translator-string-conversion | `exapump sql "EXPLAIN VIRTUAL SELECT UPPER(ID), COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE GROUP BY UPPER(ID)" -d "$DSN"` | Pushdown SQL whose scan spec carries `upper(exa_to_varchar(\"ID\"))`, and whose outer wrapper carries no `exa_to_varchar` |
| scan-execution-exa-to-varchar | `exapump sql "SELECT CAST(C_DOUBLE * 2e19 AS VARCHAR(40)), CAST(C_BOOL AS VARCHAR(5)) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE ID = 1" -d "$DSN"` | `1e19` and the row's `TRUE`/`FALSE`, equal to `SELECT CAST(CAST(1e19 AS DOUBLE) AS VARCHAR(40))` run natively |
| pushdown-planning-string-fn-type-coercion | `exapump sql "SELECT UPPER(C_DOUBLE), COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE GROUP BY UPPER(C_DOUBLE) ORDER BY 1" -d "$DSN"` | Rows `0.5`/3, `1.5`/2, `2.5`/2, `3.5`/1, `4.5`/1, `5.5`/1, NULL/2, and no `22002` error; `EXPLAIN VIRTUAL` shows the GROUP BY inside the scan spec |
| pushdown-planning-decimal-string-format | `exapump sql "SELECT CAST(C_DECIMAL_A AS VARCHAR(20)) K, COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DECIMAL_A IN (10.50, 30.00) GROUP BY CAST(C_DECIMAL_A AS VARCHAR(20)) ORDER BY 1" -d "$DSN"` | Two rows: `10.5`, `3` and `30`, `2` |
| string-conversion surface scenario | `exapump sql "SELECT MAX(UPPER(ID)), COUNT(UPPER(ID)) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE" -d "$DSN"` | One row: `9`, `12` |
| #216 boundary | `exapump sql "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = ',.'; SELECT CAST(C_DECIMAL_A AS VARCHAR(20)) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE ID = 1" -d "$DSN"` | `10.5`, where native Exasol returns `10,5` (the tracked exception) |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures; fails, not skips, without the Docker Exasol container |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors or warnings |
| Format | `cargo fmt --check` | No changes |
| Plan | `speq plan validate fix-string-fn-type-coercion-udf` | Pass |
