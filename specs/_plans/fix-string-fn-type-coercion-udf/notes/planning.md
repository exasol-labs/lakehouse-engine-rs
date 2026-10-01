# Planning hand-off map (local scratch, not for commit)

## Files and symbols checked (worktree lakehouse-engine-rs-444)

- `crates/lakehouse-engine/src/adapter/pushdown/request_shape.rs`: `classify_request_shape`, `RequestShape`
- `crates/lakehouse-engine/src/adapter/pushdown/empty_result.rs`: `empty_result_sql`, `empty_select_list_typed_sql`, `empty_agg_sql`
- `crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs`: `arg_column_or_expr`, `parse_agg_item`
- `crates/lakehouse-engine/src/adapter/pushdown/single_group_agg.rs`: `detect_aggregates`
- `crates/vs-expression/src/lib.rs`: `is_boolean_producing`, `render_bool_to_string_case`, `render_cast` boolean branch, `CONCAT` arm
- `crates/lakehouse-engine/tests/boolean_to_string_casing_test.rs`: three tests, bare `SessionContext::new()`
- `crates/lakehouse-engine/src/scan/checked_div.rs`: `register_checked_float_div_udf` visibility (`pub(super)`)
- `crates/lakehouse-engine/src/scan/mod.rs`: module visibility list
- `crates/lakehouse-engine/src/scan/convert.rs`: NaN handling, `Timestamp` arm ignores tz
- `crates/lakehouse-engine/src/scan/join_scan.rs`: NaN-as-NULL comment (#246)
- `crates/lakehouse-engine/tests/common/seed.rs`: `typed_probe` values, `dim_customer` (`C_NAME` = `customer-01..05`)
- `crates/lakehouse-engine/tests/e2e_capability_test.rs`: `e2e_instr_arity_decline_where_matches_native_oracle`
- `~/.cargo/registry/.../datafusion-expr-54.1.0/src/udf.rs`: `ScalarUDFImpl::simplify` signature and schema rule
- `~/.cargo/registry/.../datafusion-expr-54.1.0/src/simplify.rs`: `SimplifyContext` API
- Existence of cited test and helper names (grep over `crates/`)

## Specs checked

- Recorded Backgrounds diffed against plan copies: `pushdown-col-types-consolidation`, `pushdown-module-dedup-consolidation`, `pushdown-planning-like-type-coercion`, `pushdown-planning-join-filter-type-coercion`, `vs-expression-translator-concat`, `vs-expression-translator-scalar-ops`
- Recorded scenario lists: `pushdown-planning-string-fn-type-coercion`, `pushdown-planning-decimal-string-format`, `pushdown-planning-string-fn-type-coercion-composition`, `pushdown-planning-empty-result`, `type-mapping-module-structure`, `pushdown-module-dedup-consolidation`, `pushdown-col-types-consolidation`
- Recorded specs naming removed functions (grep over `specs/`, excluding `_plans`)
- `sql-comprehension/vs-expression-translator-float-div` (DOUBLE result of `/`, non-finite handling)

## Searches run

- `gh issue view 216`, `gh issue view 223`, `gh issue view 211`, `gh issue view 227` (titles, states, bodies)
- `gh issue list --search` for literal-typing issues ("decimal literal", "fractional literal", "Float64 DECIMAL", "numeric literal")
- grep `parse_float_as_decimal` over `crates/` and `specs/`

## Live Exasol checks (Docker container `lakehouse-engine-rs-exasol-1`, exasol/docker-db:2025.1.16, exaplus inside the container)

- Scratch SQL files: scratchpad `q1.sql` .. `q5.sql`
- BOOLEAN, DOUBLE (magnitudes, rounding carries, powers of ten, extremes, -0), TIMESTAMP(0/3/9), DATE, DECIMAL arithmetic with fractional literals, `NLS_NUMERIC_CHARACTERS` and `NLS_TIMESTAMP_FORMAT` effects
