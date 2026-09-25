# Plan: fix-string-fn-type-coercion-udf

## Summary

String functions and string CASTs over non-string columns fail or return wrong values on the GROUP BY and aggregate-argument pushdown paths (issue #227). This plan moves the conversion into a DataFusion session function `exa_to_varchar`, renders it in the DataFusion dialect only, keeps one read-only adapter decline check for DOUBLE, BOOLEAN, and TIMESTAMP columns, and deletes the adapter rewrites it replaces.

## Design

### Context

Exasol converts a non-string argument to text before it applies a string function (`UPPER`, `SUBSTR`, `LENGTH`, `CONCAT`, ...) or a `CAST(... AS VARCHAR/CHAR)`. DataFusion does not. Two adapter passes compensate today by rewriting the expression JSON with column types: `string_function_arg_type_guard` (#210) and `rewrite_decimal_stringifications` (#211), both inside `apply_type_rewrites` (`adapter/pushdown/support.rs`). They run on the WHERE filter and the plain select list only.

Two paths skip them. `detect_group_by_aggregates` (`grouped_agg.rs`) renders GROUP BY keys and non-aggregate grouped select items with bare `render_expression`. `arg_column_or_expr` (`scalar_over_agg.rs`) renders aggregate arguments the same way, for `parse_agg_item`, HAVING, and scalar-over-aggregate items. Issue #227 reproduced both failure kinds on TPC-H SF100: `GROUP BY UPPER(c_custkey)` fails with `F-UDF-CL-RUST-9001` (SQL state `22002`), and `GROUP BY CAST(c_acctbal AS VARCHAR(20))` returns `0.50` where Exasol returns `0.5`.

Extending the rewrites to those paths is fragile. Plans and group keys are matched by rendered text at eight sites, the rewritten node is valid only in DataFusion, and column types cover bare columns only.

- **Goals**
  - Every surface that renders DataFusion SQL converts a string-function or string-CAST argument the way Exasol does, or routes the request to native Exasol evaluation.
  - One module owns which arguments are string-converted.
  - No adapter-made node can reach Exasol-dialect SQL, and the adapter rewrites no tree for string conversion.
  - The two adapter rewrites and their node are deleted.
- **Non-Goals**
  - `like_subject_type_guard` (#207) keeps its adapter rewrite.
  - A computed DOUBLE, BOOLEAN, or TIMESTAMP argument (#223), a non-default `NLS_DATE_FORMAT` (#216), faithful 3- and 4-argument `INSTR`/`LOCATE` (#228 step 2), `SUBSTR` with `start <= 0` (#432), and the empty-string rule (#203).
  - A shared session-UDF registry for #431 and #201 (decision-log [2]).

### Decision

Adopt issue #227's recommended approach as specified (decision-log [1]).

#### Architecture

```
pushdown request
   │
   ▼
adapter  (pushdown/support.rs, pushdown/request_shape.rs)
   ├─ apply_type_rewrites(filter | select item | join filter)
   │     like_subject_type_guard ──► string_conversion_declined ──decline──► existing surface fallback
   └─ classify_request_shape(groupBy, selectList, having, orderBy)
         string_conversion_declined ──decline──► GroupByWrapper | RowScan (wrapper)
   │   both read: vs_expression::string_converted_args + column_exa_type + classify_exa_type
   │   neither rewrites the tree
   ▼
vs-expression renderer  (crates/vs-expression/src/lib.rs)
   ├─ DataFusion dialect: string-converted arg ─► exa_to_varchar(arg)
   │                      boolean-producing arg ─► #200 CASE form, no wrapper
   │                      string CAST          ─► CAST(exa_to_varchar(x) AS VARCHAR)
   │                      INSTR/LOCATE > 2 args, unconvertible literal ─► render error
   └─ Exasol dialect: unchanged, never emits exa_to_varchar
   ▼
scan UDF session  (scan/object_store.rs::build_session_context)
   └─ exa_to_varchar  (scan/to_varchar.rs): Arrow-type dispatch ─► text
```

#### Key Interfaces

- `vs_expression::EXA_TO_VARCHAR_FN: &str` = `"exa_to_varchar"`, the only link between renderer and registration.
- `vs_expression::string_converted_args(node: &Json) -> Vec<&Json>`: the string-converted argument table, read by the renderer and by the adapter.
- `support::string_conversion_declined(expr: &Json, col_types: &[(String, String)]) -> bool`, `pub(super)`: the one decline decision.
- `support::apply_type_rewrites(expr, col_types) -> Option<Json>`: the signature is unchanged, and the body becomes `like_subject_type_guard`, then the decline check.
- `scan::to_varchar::register_exa_to_varchar_udf(ctx: &SessionContext)`, `pub(super)`, and `ExaToVarcharUdf: ScalarUDFImpl`.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Session function named by a `vs-expression` constant | `scan/to_varchar.rs`, `vs-expression` | Same split as `CheckedFloatDivUdf` / `CHECKED_FLOAT_DIV_FN` (#370); the renderer stays free of DataFusion |
| One table, two readers | `vs_expression::string_converted_args` | Renderer and adapter check agree by construction, not by a copied list |
| Read-only predicate over the curated post-order walker | `string_conversion_declined` via `rewrite_expr_tree` | Same reach and decline propagation as the LIKE guard, no rewritten tree to drift from the rendered text |
| Render error as a decline trigger | `INSTR`/`LOCATE` beyond two arguments, unconvertible literals | Every surface already routes a DataFusion render error to native Exasol evaluation |

#### Quick Diagnostic

The change adds one module (`scan/to_varchar.rs`), one public `vs-expression` function, one constant, and one adapter predicate.

| Question | Answer |
|----------|--------|
| One-sentence responsibility? | `to_varchar.rs` converts an Arrow array to Exasol text. `string_converted_args` names the arguments Exasol converts. `string_conversion_declined` answers whether a tree converts an unconvertible column. |
| Easier to call than reimplement? | Yes. Callers pass a node or an array; the type dispatch, decimal trim, and JSON-fallback reuse stay inside. |
| Internal change forces an outside edit? | No. The table can grow in `vs-expression` alone, and both readers follow. The conversion rules change inside `to_varchar.rs` alone. |
| Doc comments explain the reasoning? | Required by task 2.1, 2.2, 3.1, and 4.2: each states why the decision lives there. |
| One owner per decision? | Yes: argument positions in `vs-expression`, type-family pass/decline in `string_conversion_declined`, Arrow conversion in `to_varchar.rs`. |
| Boundaries visible? | The constant and `string_converted_args` are the only crossings between the crates. |
| Tactical shortcut with a follow-up? | The standalone registration (decision-log [2]) is revisited when #431 or #201 lands. |
| Business logic independent of delivery mechanisms? | `string_converted_args` is pure syntax over JSON. The predicate reads injected `col_types`. Only `to_varchar.rs` names DataFusion, and it lives in the scan layer. |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Convert in DataFusion by Arrow type; adapter only declines (decision-log [1]) | Run `apply_type_rewrites` on the two unguarded paths | Deterministic rendering keeps every text match consistent, and a DataFusion-only wrapper cannot reach Exasol SQL |
| Standalone registration (decision-log [2]) | Shared registry for #431/#201 | Both issues are open; a registry would be shaped by one consumer |
| Delete the rewrites now (decision-log [3]) | Keep both paths for a release | Both would wrap the same argument |
| One predicate, two consumers (decision-log [4]) | One whole-request check in `build_dispatch_sql`; per-site checks | Keeps each surface's fallback and the empty path's agreement |
| JSON-fallback types convert to their emitted text (decision-log [5]) | The issue's "anything else errors" row | A `Decimal128(38, s)` column MUST convert to the text it returns |
| Literals and long `INSTR`/`LOCATE` are render errors (decision-log [6]) | Adapter literal checks | The renderer owns literal typing |

### Iceberg and Delta compliance gate

The rule applies, and the check finds no deviation (decision-log [7]). The conversion dispatches on the Arrow types of Iceberg and Delta primitives, and neither spec defines a query text form for a value. The one named Exasol target-type trade-off is the 37- and 38-digit decimal, which both specs allow and Exasol's DECIMAL does not: it converts as JSON-fallback text with every scale digit.

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
| vs-adapter/pushdown-planning-grouped-agg-multikey | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-grouped-agg-multikey/spec.md` |
| vs-adapter/pushdown-planning-expression-aggregate | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-expression-aggregate/spec.md` |
| vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate/spec.md` |
| vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-single-group-agg-scalar-over-aggregate/spec.md` |
| vs-adapter/pushdown-planning-join-filter-type-coercion | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-join-filter-type-coercion/spec.md` |
| vs-adapter/pushdown-planning-like-type-coercion | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-like-type-coercion/spec.md` |
| vs-adapter/pushdown-module-dedup-consolidation | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-module-dedup-consolidation/spec.md` |
| vs-adapter/pushdown-col-types-consolidation | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-col-types-consolidation/spec.md` |
| vs-adapter/pushdown-planning-empty-result | CHANGED | `specs/_plans/fix-string-fn-type-coercion-udf/vs-adapter/pushdown-planning-empty-result/spec.md` |

## Impact

- **Fixed:** every #227 repro returns the native Exasol result. A string function or string CAST over an integer, DECIMAL, or DATE argument pushes down on GROUP BY keys, aggregate arguments, HAVING, and scalar-over-aggregate items. A computed DECIMAL argument (`UPPER(c_decimal_a * 2)`) now converts with Exasol's trim. `INSTR`/`LOCATE` beyond two arguments return the native result on the grouped and aggregate paths too. A single-group aggregate that routes to the row-scan wrapper returns its one row when every file is pruned.
- **Behavior change, CAST over DOUBLE, BOOLEAN, or TIMESTAMP columns:** these now run through the native-evaluation wrapper instead of pushing DataFusion's text (`true`, `T`-separated timestamps). The result is correct and slower.
- **Behavior change, computed DOUBLE, BOOLEAN, or TIMESTAMP arguments of `CONCAT` or a string CAST:** these now fail at planning time with an error naming `exa_to_varchar` (tracked by #223), where they previously returned DataFusion's text.
- **Lost pushdown, rare shapes:** a string-converted fractional, DOUBLE, or TIMESTAMP literal, and a scalar-over-aggregate residual using `INSTR`/`LOCATE` with three arguments, route to the wrapper.
- **Sibling project:** a DataFusion-dialect consumer of `crates/vs-expression` MUST register a function under `EXA_TO_VARCHAR_FN`, as it already does for `CHECKED_FLOAT_DIV_FN`.
- **Cost:** every string-converted argument gains one `exa_to_varchar` call per batch. A string input passes through without a copy.
- **Unchanged:** the `ScanSpec` wire format, the advertised capabilities, and every configuration key. The VS adapter and the scan UDF ship in one `.so`, so one redeploy moves both.

## Dependencies

None. #431 and #201 are not prerequisites (decision-log [2]).

## Implementation Tasks

### 1. Reproduce #227 on the Docker Exasol container (knowledge: E2E harness)

- [ ] 1.1 In `crates/lakehouse-engine/tests/e2e_capability_test.rs`, add a #227 section over `typed_distinct_probe` (`ID` for `c_custkey`, `C_DECIMAL_A` for `c_acctbal`, `C_DOUBLE`, `C_VARCHAR`), each test asserting an independent oracle and failing, not skipping, without a database: `e2e_group_by_upper_integer_key_matches_native`, `e2e_aggregate_over_upper_integer_matches_native`, `e2e_grouped_having_and_order_by_over_upper_integer_push_down`, `e2e_scalar_over_aggregate_of_upper_integer_matches_native`, `e2e_group_by_decimal_cast_key_trims_like_native`, `e2e_max_over_decimal_cast_trims_like_native`, `e2e_group_by_upper_double_key_declines_to_native_oracle`, `e2e_instr_three_args_on_grouped_paths_matches_native`.
- [ ] 1.2 Before any production change, run `make cross-udf-build`, then `cargo test --features exasol-e2e --test e2e_capability_test <name> -- --test-threads=1` for each new test. Record every observed failure (error text and SQL state, or wrong value) in `tasks.md` as the reproduction evidence CLAUDE.md requires.

### 2. Renderer string conversion (knowledge: `sql-comprehension` deltas)

- [ ] 2.1 Add `pub const EXA_TO_VARCHAR_FN: &str = "exa_to_varchar"` to `crates/vs-expression/src/lib.rs`, with a contract doc comment in the style of `CHECKED_FLOAT_DIV_FN`.
- [ ] 2.2 Add `pub fn string_converted_args(node: &Json) -> Vec<&Json>`, owning the string-converted argument table and the string CAST rule, with tests in `lib_tests.rs` for every table row, `LPAD`/`RPAD` at two and three arguments, `CHR`/`UNICODECHR`, a non-string CAST, and other node types.
- [ ] 2.3 In the DataFusion dialect, render every argument `string_converted_args` returns as `exa_to_varchar(<arg>)` in the string-function arm, the `CONCAT` arm, and the `INSTR`/`LOCATE` arms. Apply the #200 CASE form, unwrapped, to a boolean-producing argument in every one of these arms, and leave the Exasol dialect untouched. Add `lib_tests.rs` cases for `UPPER(<predicate_less>)` and `UPPER(TRUE)`.
- [ ] 2.4 In `render_cast`, render a DataFusion-dialect string CAST as `CAST(exa_to_varchar(<source>) AS VARCHAR)`, keeping the #200 boolean branch and the Exasol dialect unchanged.
- [ ] 2.5 Make `INSTR`/`LOCATE` with more than two arguments a DataFusion-dialect render error.
- [ ] 2.6 Make a string-converted `literal_double`, `literal_exactnumeric` with a decimal point or exponent, and `literal_timestamp` / `literal_timestamp_utc` / `literal_timestamputc` a DataFusion-dialect render error.
- [ ] 2.7 Update the `lib_tests.rs` expectations that pin DataFusion string renderings (`renders_string_scalar_functions`, `renders_concat_as_nullif_wrapped_concat_call`, `renders_cast_varchar`, and the other string-CAST tests), and add the Exasol-dialect sweep and determinism tests. Gate: `cargo test -p vs-expression`.

### 3. The exa_to_varchar session function (knowledge: `datafusion-scan/scan-execution-exa-to-varchar`)

- [ ] 3.1 Create `crates/lakehouse-engine/src/scan/to_varchar.rs` with `ExaToVarcharUdf` (`ScalarUDFImpl`, one argument of any type, `Immutable`) and `register_exa_to_varchar_udf`. Declare `mod to_varchar;` in `scan/mod.rs`, end the new file with `#[cfg(test)] #[path = "to_varchar_tests.rs"] mod tests;`, and call the registration in `build_session_context` (`scan/object_store.rs`) beside `register_checked_float_div_udf`.
- [ ] 3.2 Implement `return_type`: the input's own string type for a string input, `Utf8` for integer, in-domain `Decimal128`, `Date32`, `Null`, and JSON-fallback inputs, and a planning error naming `exa_to_varchar` and the type for `Float16`/`Float32`/`Float64`, `Boolean`, and `Timestamp`.
- [ ] 3.3 Implement the string, integer, in-domain `Decimal128`, `Date32`, and `Null` conversions. Test in `to_varchar_tests.rs` through a `SessionContext`: `-0.50`, `100.10`, `5.00`, `0.00`, `0.05`, `0.5`, scale 0, `i64::MIN`/`i64::MAX`, and NULL.
- [ ] 3.4 Implement the JSON-fallback conversion by reusing `types::mapping::needs_json_fallback` and `needs_nested_json_rendering` with the scan's existing conversions (`render_nested_column_as_json`, and the Arrow cast to `Utf8` that `raw_scan::build_scan_sql` emits). Test that `Decimal128(38, 4)`, a list, `Binary`, and `Time64` match the emitted text.
- [ ] 3.5 Test the planning error for `Float64`, `Boolean`, and `Timestamp` before any row is read, and add `build_session_context_registers_the_exa_to_varchar_function` to `scan/object_store_tests.rs`, reading the name from `vs_expression::EXA_TO_VARCHAR_FN`.

### 4. Adapter decline check and rewrite removal (knowledge: adapter string-conversion deltas)

- [ ] 4.1 Update the `lakehouse-engine` test expectations that pin DataFusion string renderings changed by group 2 (`pushdown_tests.rs`, `support_tests.rs`, `grouped_agg_tests.rs`, `single_group_agg_tests.rs`, `topn_tests.rs`, `joins/rendering_tests.rs`, `joins/sql_builders_tests.rs`), editing no other assertion.
- [ ] 4.2 Add `string_conversion_declined` to `pushdown/support.rs`: walk the tree with `rewrite_expr_tree`, inspect each `vs_expression::string_converted_args` argument that is a bare `column`, resolve it with `column_exa_type`, and match `classify_exa_type` exhaustively (`Character`/`Date`/`Decimal` pass, `Other`/miss decline). Test every family, the lookup miss, a nameless column, a case-mismatched name, non-converted arguments, `CHR`, computed arguments, and nested reach in `support_tests.rs`.
- [ ] 4.3 Rewire `apply_type_rewrites` to `like_subject_type_guard`, then the decline check, and rename `type_rewrite_pipeline_runs_like_guard` to `type_rewrite_pipeline_runs_like_guard_then_string_conversion_check` asserting both passes. Delete `string_function_arg_type_guard`, `coerce_string_position_arg`, `StringPositionArgs`, `string_position_args`, `rewrite_decimal_stringifications`, `is_bare_decimal_column`, `wrap_decimal_to_varchar`, their tests, and the `decimal_to_varchar_exasol` arm of `project_columns`. Update the doc comments of `apply_type_rewrites`, `type_accepted_rewrite`, `rewrite_expr_tree`, `like_subject_type_guard`, `wrap_cast_to_varchar`, `project_columns`, the `RowScan` arm comment in `pushdown/mod.rs`, and `ExaTypeClass`/`classify_exa_type` in `types/mapping.rs`.
- [ ] 4.4 Remove the `decimal_to_varchar_exasol` arm and `format_decimal_exasol_style` from `crates/vs-expression/src/lib.rs`, with `renders_decimal_to_varchar_exasol`, `decimal_to_varchar_exasol_wrong_arity_errors`, and `format_decimal_exasol_style_renders_exact_regex_sql`. Update the comments in `tests/e2e_capability_test.rs`, `tests/e2e_join_test.rs`, and `scan/emit_tests.rs` that name removed items. Depends on 4.3.
- [ ] 4.5 Call the decline check in `classify_request_shape` (`pushdown/request_shape.rs`) on `groupBy`, `selectList`, `having`, and `orderBy` before tier 1: `GroupByWrapper` for a GROUP BY request, `RowScan` otherwise. Test each surface in `request_shape_tests.rs`, and the empty-path agreement in `empty_result_tests.rs`. In `empty_result_sql`, route a widened `RowScan` whose select list carries an aggregate (`contains_aggregate_node`, made `pub(super)`) to a new `joins/sql_builders.rs` builder. The builder renders `outer_wrapper_clauses` over a zero-row `CAST(NULL AS <ty>) AS "<col>"` derived table from `referenced_column_projection`. Pass `request` to `empty_result_sql` for `JoinLegs::for_single_scan`, and update its callers. Test `COUNT(UPPER(c_double))` and `MAX(INSTR(c_varchar, 'b', 3))` in `empty_result_tests.rs`, and confirm the six `empty_*` golden fixtures pass unedited.
- [ ] 4.6 Test the pipeline composition through `apply_type_rewrites` in `support_tests.rs`, the select-list and join surfaces in `joins/rendering_tests.rs` and `joins/sql_builders_tests.rs`, and confirm the `dispatch_golden` fixtures pass unedited.

### 5. Grouped and aggregate paths (knowledge: aggregate deltas)

- [ ] 5.1 Prove that every text-match site agrees under the wrapping renderer, and fix any site that re-renders differently: `detect_group_by_aggregates`, `build_grouped_order_by_clause`/`group_key_output_ordinal`, `parse_agg_item`/`arg_column_or_expr`, `fold_aggregate_plan`, `ordinary_plans`/`single_group_plan_types`, `render_having_over_merge`, `render_scalar_over_merge`/`classify_scalar_over_aggregate`, `parse_count_distinct`, and `empty_agg_sql`/`empty_scalar_over_aggregate_literal`. Add one test per aggregate scenario in `grouped_agg_tests.rs`, `scalar_over_agg_tests.rs`, `single_group_agg_tests.rs`, and `empty_result_tests.rs`. [expert]
- [ ] 5.2 Assert in `grouped_agg_tests.rs` and `single_group_agg_tests.rs` that the merge-wrapper SQL built for a string-function aggregate contains no `exa_to_varchar`.

### 6. Live verification (knowledge: E2E harness)

- [ ] 6.1 Add `e2e_numeric_literal_string_argument_matches_native` (`C_VARCHAR || 1.5` against an in-session native oracle), `e2e_where_upper_double_declines_to_native_oracle` (`WHERE UPPER(C_DOUBLE) = '0.5'` against the native oracle), `e2e_computed_double_string_argument_fails_with_exa_to_varchar_error` (`UPPER(C_DOUBLE * 2)`, #223), and `e2e_declined_single_group_aggregate_all_files_pruned_returns_one_row` (`COUNT(UPPER(C_DOUBLE))` returns `0` and `MAX(INSTR(C_VARCHAR, 'b', 3))` returns NULL under `WHERE ID > 1000`, with `EXPLAIN VIRTUAL` showing `FROM DUAL` and no `LAKEHOUSE_SCAN`) to `tests/e2e_capability_test.rs`. Record from `EXPLAIN VIRTUAL` whether Exasol pushed the literal unfolded (decision-log [6]).
- [ ] 6.2 Run `make test-e2e`. Every group 1 test passes, and the existing #210, #211, #228, #374, #200, join, and complex-type E2E tests pass with no assertion edit. Record the `EXPLAIN VIRTUAL` evidence that each grouped repro carries `exa_to_varchar(` inside the scan spec.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| R: Reproduce #227 | 1.1-1.2 | — | issue #227 repros; `crates/lakehouse-engine/tests/e2e_capability_test.rs`, `crates/lakehouse-engine/tests/common/seed.rs` (`typed_distinct_probe`) |
| A: Renderer string conversion | 2.1-2.7 | R (reproduction precedes any fix) | spec deltas `sql-comprehension/vs-expression-translator-string-conversion`, `-scalar-fns`, `-concat`, `-cast`; `crates/vs-expression/src/lib.rs`, `crates/vs-expression/src/lib_tests.rs` |
| B: exa_to_varchar session function | 3.1-3.5 | A (reads `EXA_TO_VARCHAR_FN`) | spec delta `datafusion-scan/scan-execution-exa-to-varchar`; `crates/lakehouse-engine/src/scan/to_varchar.rs`, `to_varchar_tests.rs`, `scan/mod.rs`, `scan/object_store.rs`, `scan/object_store_tests.rs`, `types/mapping.rs` (read only) |
| C: Adapter decline check and removal | 4.1-4.6 | A (reads `string_converted_args`; 4.4 edits `lib.rs` after A) | spec deltas `vs-adapter/pushdown-planning-string-fn-type-coercion`, `-string-fn-type-coercion-composition`, `-decimal-string-format`, `-like-type-coercion`, `-join-filter-type-coercion`, `pushdown-module-dedup-consolidation`, `pushdown-col-types-consolidation`, `-empty-result`, `datafusion-scan/type-mapping-module-structure`, `sql-comprehension/vs-expression-translator-scalar-ops`; `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, `support_tests.rs`, `request_shape.rs`, `request_shape_tests.rs`, `mod.rs`, `pushdown_tests.rs`, `joins/rendering_tests.rs`, `joins/sql_builders.rs`, `joins/sql_builders_tests.rs`, `types/mapping.rs`; the widened-`RowScan` arm of `empty_result.rs` and its `empty_result_tests.rs` cases (task 4.5); expectation strings only (task 4.1) in `grouped_agg_tests.rs`, `single_group_agg_tests.rs`, `topn_tests.rs` |
| D: Grouped and aggregate paths | 5.1-5.2 | C (classifier check, updated expectations) | spec deltas `vs-adapter/pushdown-planning-grouped-agg-multikey`, `-expression-aggregate`, `-grouped-agg-scalar-over-aggregate`, `-single-group-agg-scalar-over-aggregate`; `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs`, `scalar_over_agg.rs`, `single_group_agg.rs`, `empty_result.rs` and their `_tests.rs` files |
| E: Live verification | 6.1-6.2 | B, C, D | every spec delta's E2E scenario; `crates/lakehouse-engine/tests/e2e_capability_test.rs` |

The groups run R, A, then B and C in parallel, then D, then E. Group A's gate is `cargo test -p vs-expression`. The `lakehouse-engine` suites that pin DataFusion string renderings go green again at task 4.1. Group C and group A share `crates/vs-expression/src/lib.rs` only through task 4.4, which runs after A finishes. Group C and group D share `grouped_agg_tests.rs` and `single_group_agg_tests.rs` only through task 4.1's expectation-string updates, which finish before D starts. They share `empty_result.rs` and `empty_result_tests.rs` only through task 4.5's widened-`RowScan` arm, which also finishes before D starts. Group D's task 5.1 touches the `SingleGroupAgg` arm only. Group R and group E share the E2E file by design: CLAUDE.md requires the reproduction before the fix, and group E proves the same tests green after it.

Task 5.1 carries `[expert]`, so group D routes to the expert implementer. It spans eight text-match sites in five files whose agreement is a cross-file behavioral dependency. Groups R, A, B, C, and E are untagged.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/lakehouse-engine/src/adapter/pushdown/support.rs::string_function_arg_type_guard` | Replaced by renderer wrapping plus `string_conversion_declined` |
| Function | `support.rs::coerce_string_position_arg` | Per-type rewrite replaced by `exa_to_varchar` |
| Enum + function | `support.rs::StringPositionArgs`, `support.rs::string_position_args` | Table moved to `vs_expression::string_converted_args`; the arity decline is a render error |
| Function | `support.rs::rewrite_decimal_stringifications`, `support.rs::is_bare_decimal_column`, `support.rs::wrap_decimal_to_varchar` | Decimal trim moved into `exa_to_varchar` |
| Match arm | `support.rs::project_columns`, `"decimal_to_varchar_exasol"` | The node no longer exists |
| Match arm + function | `crates/vs-expression/src/lib.rs`, `"decimal_to_varchar_exasol"` arm and `format_decimal_exasol_style` | Replaced by `exa_to_varchar` |
| Test | `support_tests.rs`: `string_fn_guard_*`, `string_position_args_*`, `rewrite_*`, `decimal_rewrite_*`, `stringify_*`, `selectlist_upper_decimal_arg_coerced_not_full_row`, `selectlist_instr_decimal_arg_coerces_first_position_only` | Test removed passes; replaced by `string_conversion_check_*` tests |
| Test | `lib_tests.rs`: `renders_decimal_to_varchar_exasol`, `decimal_to_varchar_exasol_wrong_arity_errors`, `format_decimal_exasol_style_renders_exact_regex_sql` | Test the removed node |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| string-conversion: String-converted function arguments render through exa_to_varchar in the DataFusion dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `string_converted_function_args_render_through_exa_to_varchar`, `boolean_producing_string_converted_arg_renders_case_form_in_every_arm` |
| string-conversion: A string CAST renders as a cast of exa_to_varchar in the DataFusion dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `string_cast_renders_as_cast_of_exa_to_varchar` |
| string-conversion: The Exasol dialect never renders exa_to_varchar | Unit | `crates/vs-expression/src/lib_tests.rs` | `exasol_dialect_never_renders_exa_to_varchar` |
| string-conversion: INSTR and LOCATE beyond two arguments are a DataFusion-dialect render error | Unit | `crates/vs-expression/src/lib_tests.rs` | `instr_and_locate_beyond_two_args_are_a_datafusion_render_error` |
| string-conversion: A string-converted literal DataFusion cannot convert faithfully is a DataFusion-dialect render error | Unit + Integration | `crates/vs-expression/src/lib_tests.rs`; `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `unconvertible_string_converted_literal_is_a_datafusion_render_error`; `e2e_numeric_literal_string_argument_matches_native` |
| string-conversion: The string-converted argument table is one query shared with the adapter | Unit | `crates/vs-expression/src/lib_tests.rs` | `string_converted_args_returns_exactly_the_converted_arguments` |
| scalar-fns: String scalar functions translate to DataFusion string calls | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_string_scalar_functions` |
| concat: CONCAT translates to a NULL-skipping DataFusion concat call | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_concat_as_nullif_wrapped_concat_call` |
| cast: CAST renders the mapped target type per dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_cast_varchar` |
| exa-to-varchar: The scan session registers exa_to_varchar for every scan spec | Integration | `crates/lakehouse-engine/src/scan/object_store_tests.rs` | `build_session_context_registers_the_exa_to_varchar_function` |
| exa-to-varchar: String and integer arguments convert to Exasol text | Integration | `crates/lakehouse-engine/src/scan/to_varchar_tests.rs` | `exa_to_varchar_passes_strings_and_renders_integers_as_digits` |
| exa-to-varchar: A DECIMAL argument converts with trailing scale zeros removed | Integration | `crates/lakehouse-engine/src/scan/to_varchar_tests.rs` | `exa_to_varchar_trims_decimal_trailing_zeros` |
| exa-to-varchar: A DATE argument converts to ISO date text | Integration | `crates/lakehouse-engine/src/scan/to_varchar_tests.rs` | `exa_to_varchar_renders_date32_as_iso` |
| exa-to-varchar: A NULL-typed argument converts to NULL text | Integration | `crates/lakehouse-engine/src/scan/to_varchar_tests.rs` | `exa_to_varchar_converts_null_type_to_null_text` |
| exa-to-varchar: A JSON-fallback type converts to the text the scan emits for it | Integration | `crates/lakehouse-engine/src/scan/to_varchar_tests.rs` | `exa_to_varchar_matches_scan_emit_text_for_json_fallback_types` |
| exa-to-varchar: A DOUBLE, BOOLEAN, or TIMESTAMP argument fails at planning time | Integration | `crates/lakehouse-engine/src/scan/to_varchar_tests.rs`; `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `exa_to_varchar_rejects_double_boolean_timestamp_at_planning`; `e2e_computed_double_string_argument_fails_with_exa_to_varchar_error` |
| type-mapping-module-structure: One classifier names the Exasol type-string families the pushdown guards branch on | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `classify_exa_type_matches_pushdown_guard_predicates` |
| string-fn: A string-position VARCHAR or CHAR column argument pushes down unchanged | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `string_conversion_check_accepts_varchar_and_char_arguments`; `e2e_upper_varchar_pushdown` |
| string-fn: A string-position DECIMAL column argument renders through Exasol's trimmed decimal-to-string form | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `string_conversion_check_accepts_decimal_argument`; `e2e_upper_id_trims_to_plain_integer_string`, `e2e_ltrim_decimal_trims_trailing_zeros` |
| string-fn: A string-position DATE column argument pushes down as ISO date text | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `string_conversion_check_accepts_date_argument`; `e2e_lower_date_formats_as_iso` |
| string-fn: A non-coercible resolvable column type in a WHERE-clause string function declines the whole filter | Unit + Integration | `pushdown_tests.rs`; `tests/e2e_capability_test.rs` | `where_filter_string_fn_over_double_declines`; `e2e_where_upper_double_declines_to_native_oracle` |
| string-fn: A non-coercible resolvable column type in a select-list string function falls back to the full base row | Unit + Integration | `support_tests.rs`, `joins/rendering_tests.rs`; `tests/e2e_capability_test.rs` | `selectlist_string_fn_over_double_falls_back_to_full_row`, `selectlist_string_cast_over_boolean_falls_back_to_full_row`, `join_projection_string_conversion_decline_widens_to_union`; `e2e_upper_double_declines_to_native_oracle` |
| string-fn: A string-position argument whose column name does not resolve declines fail-safe | Unit | `support_tests.rs` | `string_conversion_check_declines_unresolved_and_nameless_column`, `string_conversion_check_resolves_case_mismatched_column_name` |
| string-fn: The decline check inspects only the arguments vs-expression converts to text | Unit | `support_tests.rs` | `string_conversion_check_ignores_non_converted_arguments` |
| string-fn: An INSTR or LOCATE call beyond two arguments reaches native Exasol evaluation on every surface | Unit + Integration | `request_shape_tests.rs`; `tests/e2e_capability_test.rs` | `instr_beyond_two_args_routes_every_surface_to_its_fallback`; `e2e_instr_arity_decline_where_matches_native_oracle`, `e2e_instr_three_args_on_grouped_paths_matches_native` |
| string-fn: A computed string-converted argument converts by its DataFusion type | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `string_conversion_check_passes_computed_argument`; `e2e_computed_double_string_argument_fails_with_exa_to_varchar_error` |
| string-fn: The decline check reaches GROUP BY keys, aggregate arguments, HAVING, and ORDER BY | Unit + Integration | `request_shape_tests.rs`, `empty_result_tests.rs`; `tests/e2e_capability_test.rs` | `classify_request_shape_routes_string_conversion_decline_on_every_surface`, `empty_result_agrees_with_string_conversion_decline_shape`; `e2e_group_by_upper_double_key_declines_to_native_oracle` |
| composition: The LIKE subject guard and the string-conversion check compose without double conversion | Unit | `support_tests.rs` | `type_rewrite_pipeline_composes_like_guard_and_string_conversion_check` |
| decimal: Explicit CAST of a DECIMAL column to VARCHAR renders the trimmed form | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `selectlist_decimal_cast_routed_not_full_row_fallback`; `e2e_decimal_cast_trims_trailing_zeros` |
| decimal: Implicit CONCAT over a DECIMAL column renders the trimmed form, including nested concatenation | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `selectlist_nested_concat_decimal_arg_renders_through_exa_to_varchar`; `e2e_decimal_concat_trims_trailing_zeros` |
| decimal: Implicit LENGTH over a DECIMAL column renders the trimmed form | Unit + Integration | `support_tests.rs`; `tests/e2e_capability_test.rs` | `selectlist_length_decimal_arg_renders_through_exa_to_varchar`; `e2e_decimal_length_reflects_trimmed_string` |
| decimal: WHERE-clause stringification of a DECIMAL column renders the trimmed form | Unit + Integration | `pushdown_tests.rs`, `joins/sql_builders_tests.rs`; `tests/e2e_capability_test.rs`, `tests/e2e_join_test.rs` | `where_filter_decimal_stringification_renders_through_exa_to_varchar`, `join_decimal_stringification_renders_trimmed_at_both_join_sites`; `e2e_decimal_length_where_count_matches_trimmed_semantics`, `e2e_join_decimal_stringification_matches_native_at_both_surfaces` |
| decimal: A DECIMAL column in a non-stringifying filter context is left unchanged | Unit | `pushdown_tests.rs` | `filter_decimal_comparison_not_rewritten` |
| decimal: CAST, CONCAT, or LENGTH over a non-DECIMAL column converts by its type | Unit | `support_tests.rs` | `stringify_non_decimal_column_converts_by_type` |
| decimal: A DECIMAL stringification in a GROUP BY key or an aggregate argument renders the trimmed form | Unit + Integration | `grouped_agg_tests.rs`; `tests/e2e_capability_test.rs` | `grouped_decimal_cast_key_matches_select_item_by_rendered_text`; `e2e_group_by_decimal_cast_key_trims_like_native`, `e2e_max_over_decimal_cast_trims_like_native` |
| multikey: A string-function group key over a non-string column pushes down | Unit + Integration | `grouped_agg_tests.rs`; `tests/e2e_capability_test.rs` | `string_fn_group_key_over_integer_decomposes_with_or_without_select_item`, `grouped_order_by_string_fn_key_resolves_to_group_key_ordinal`; `e2e_group_by_upper_integer_key_matches_native`, `e2e_grouped_having_and_order_by_over_upper_integer_push_down` |
| expression-aggregate: An aggregate over a string function of a non-string column is pushed down | Unit + Integration | `single_group_agg_tests.rs`, `grouped_agg_tests.rs`; `tests/e2e_capability_test.rs` | `aggregate_over_string_fn_of_integer_decomposes`, `grouped_having_matches_string_fn_aggregate_by_rendered_text`; `e2e_aggregate_over_upper_integer_matches_native` |
| expression-aggregate: An aggregate over a string function resolves one shape on the empty-result path | Unit | `empty_result_tests.rs` | `empty_single_group_aggregate_over_string_fn_returns_one_null_row` |
| grouped scalar-over-aggregate: A scalar over an aggregate of a string function keeps the conversion inside the scan | Unit + Integration | `grouped_agg_tests.rs`; `tests/e2e_capability_test.rs` | `grouped_scalar_over_string_fn_aggregate_keeps_conversion_in_scan`; `e2e_scalar_over_aggregate_of_upper_integer_matches_native` |
| single-group scalar-over-aggregate: A single-group scalar over an aggregate of a string function decomposes on both paths | Unit | `single_group_agg_tests.rs`, `empty_result_tests.rs` | `single_group_scalar_over_string_fn_aggregate_decomposes`, `empty_scalar_over_string_fn_aggregate_does_not_panic` |
| join-filter: A join filter with no type-rewrite trigger emits byte-identical SQL | Unit | `joins/sql_builders_tests.rs` | `join_filter_without_string_conversion_trigger_emits_byte_identical_sql` |
| like: A select-list LIKE over a DATE column projects the CAST-to-VARCHAR form | Unit | `support_tests.rs` | `selectlist_like_over_date_projects_cast_expr` |
| dedup: The type-aware tree walks share one post-order primitive | Unit | `support_tests.rs` | `string_conversion_check_reaches_nested_node_and_declines_whole_tree` |
| dedup: One ordered pipeline function owns the type-rewrite pass order | Unit | `support_tests.rs` | `type_rewrite_pipeline_runs_like_guard_then_string_conversion_check` |
| col-types: One helper resolves a bare column node's Exasol type for every type-rewrite guard | Unit | `support_tests.rs` | `column_exa_type_resolves_unicode_folded_list_and_misses_ascii_folded_list` |
| col-types: The type-aware consumers read their type family from the shared classifier | Unit | `support_tests.rs` | `string_conversion_check_classifies_every_type_family` |
| empty-result: A single-group aggregate that routes to the row-scan wrapper returns one row when all files are pruned | Unit + Integration | `empty_result_tests.rs`; `tests/e2e_capability_test.rs` | `empty_row_scan_aggregate_renders_wrapper_over_zero_row_source`; `e2e_declined_single_group_aggregate_all_files_pruned_returns_one_row` |

Unit rows cover pure JSON-to-SQL rendering and adapter planning, which perform no I/O. Adapter test files live under `crates/lakehouse-engine/src/adapter/pushdown/`.

### Manual Testing

Run after `make test-e2e` has set up the Docker stack. `DSN` = `exasol://sys:exasol@localhost:28563?validateservercertificate=0`.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-expression-translator-string-conversion | `exapump sql "EXPLAIN VIRTUAL SELECT UPPER(ID), COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE GROUP BY UPPER(ID)" -d "$DSN"` | Pushdown SQL whose scan spec carries `upper(exa_to_varchar(\"ID\"))`, and whose outer wrapper carries no `exa_to_varchar` |
| scan-execution-exa-to-varchar | `exapump sql "SELECT CAST(C_DECIMAL_A AS VARCHAR(20)) K, COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DECIMAL_A IN (10.50, 30.00) GROUP BY CAST(C_DECIMAL_A AS VARCHAR(20)) ORDER BY 1" -d "$DSN"` | Two rows: `10.5`, `3` and `30`, `2` |
| pushdown-planning-string-fn-type-coercion | `exapump sql "SELECT UPPER(C_DOUBLE), COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE GROUP BY UPPER(C_DOUBLE)" -d "$DSN"` | One row per distinct `C_DOUBLE` value, keyed in Exasol's DOUBLE text (for example `0.5`), and no `22002` error; `EXPLAIN VIRTUAL` shows the GROUP BY in the outer wrapper, not in the scan spec |
| pushdown-planning-expression-aggregate | `exapump sql "SELECT MAX(UPPER(ID)), COUNT(UPPER(ID)) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE" -d "$DSN"` | One row: `9`, `12` |
| pushdown-planning-decimal-string-format | `exapump sql "SELECT MAX(CAST(C_DECIMAL_A AS VARCHAR(20))) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DECIMAL_A IN (10.50, 30.00)" -d "$DSN"` | One row: `30` |
| #223 boundary | `exapump sql "SELECT UPPER(C_DOUBLE * 2) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE" -d "$DSN"` | An error naming `exa_to_varchar` and `Float64` |
| pushdown-planning-empty-result | `exapump sql "SELECT COUNT(UPPER(C_DOUBLE)) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE ID > 1000" -d "$DSN"` | One row: `0`; `EXPLAIN VIRTUAL` shows `FROM DUAL` and no `LAKEHOUSE_SCAN` |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures; fails, not skips, without the Docker Exasol container |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors or warnings |
| Format | `cargo fmt --check` | No changes |
| Plan | `speq plan validate fix-string-fn-type-coercion-udf` | Pass |
