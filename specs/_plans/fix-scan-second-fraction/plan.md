# Plan: fix-scan-second-fraction

## Summary

`SECOND(ts)` and `EXTRACT(SECOND FROM ts)` return the seconds with the fraction of the argument's declared precision when the scan computes them, typed `DECIMAL(2+p, p)` as in native Exasol (#603). The typed scan translator (#587) owns the rule, so the select list, WHERE filters, GROUP BY keys, aggregate arguments, and join conditions all get the fix.

## Context

- The DataFusion dialect renders both forms as `date_part('SECOND', ts)`, and DataFusion 55.2.0 returns whole seconds as `Int32`. The virtual schema `TS_PRECISION_VS` on the local 2025.1.16 container returns `0.000000` for `SECOND(TS)` where the values are `.000001` to `.123457`, counts 0 rows for `WHERE SECOND(TS) > 0.1`, and forms one group for `GROUP BY SECOND(TS)`.
- Exasol types the result `DECIMAL(2+p, p)` for a `TIMESTAMP(p)` argument (2025.1.16, p from 0 to 9; 8.29.13, p = 3). `SECOND` of a DATE is 0, typed `DECIMAL(2,0)` on 2025.1.16 and `DECIMAL(5,3)` on 8.29.13. `EXTRACT(SECOND FROM <date>)` fails natively with `Feature not supported` (0A000) on both versions, while the virtual schema returns 0 today.
- Exasol cuts digits beyond a precision, it does not round: `CAST(ts AS TIMESTAMP(3))` of `59.999999` is `59.999`. On 8.x a microsecond column is declared bare `TIMESTAMP`, so the scan holds more digits than the declared p.
- In DataFusion 55.2.0, `date_part('microsecond', ts)` returns `Int32` (seconds × 10^6 plus microseconds), `date_part('nanosecond', ts)` returns `Int64`, and both return 0 for a `Date32`. A decimal quotient has the scale s1 + 4 and loses digits, while a decimal product keeps s1 + s2 (decision-log [2]).
- `render_scan_expression` types the tree with `type_scan_tree` and takes the fragment type from `exact_expression_type`. Today that type exists only for a `CAST` or a column, so an unprojected `GROUP BY SECOND(ts)` routes to the wrapper (`group_key_exasol_types`), and `MAX(SECOND(ts)) + 1` declines the split (`nested_aggregate_type`).
- The scan's emit path casts every column to its declared `EMITS` type (`scan/emit.rs`), so a select item takes Exasol's `selectListDataTypes` type whatever the fragment type is.
- The change is an Exasol parity fix. It touches no Iceberg table spec or Delta Lake protocol rule.
- Accepted ADR `split-aggregate-columns-take-exact-types-or-decline` names the aggregate result type as the one rule the adapter copies. Decision-log [3] supersedes it. No architecture delta: `specs/architecture.md` § Constraints already routes every scan type rule through the one scan translator.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| VS Expression Translator, Date and Time Functions | CHANGED | `sql-functions/vs-expression-translator-date-fns/spec.md` |
| Pushdown Planning: Split-Aggregate Column Types | CHANGED | `pushdown-aggregates/pushdown-planning-split-aggregate-types/spec.md` |

## Impact

- `SECOND(ts)` and `EXTRACT(SECOND FROM ts)` return the fraction in the select list, WHERE filters, GROUP BY keys, aggregate arguments, and join conditions. Queries that returned wrong values or rows before now match native Exasol.
- An unprojected `GROUP BY SECOND(ts)` and an aggregate nested over `SECOND(ts)` now run as a split aggregate instead of in the wrapper.
- Behavior change: `EXTRACT(SECOND FROM <date>)` now fails with Exasol's `Feature not supported` error, as it does over a native table, instead of returning 0.
- `SECOND` over a computed argument or a `TIMESTAMP WITH LOCAL TIME ZONE` argument now runs in the Exasol wrapper.
- No property, CONNECTION field, install step, or advertised capability changes, so `docs/` stays unchanged. `docs/semantic-differences.md` lists no `SECOND` difference, and the `SECOND` and `EXTRACT` rows of `docs/capability-matrix.md` stay advertised.

## Open Questions

- (#TBD) `EXTRACT(HOUR FROM <date>)` and `EXTRACT(MINUTE FROM <date>)` also fail natively with `Feature not supported` but return 0 through the scan. This plan leaves them as they are; they need their own issue.

## Implementation Tasks

1. Typed SECOND in `crates/vs-expression` (#603)
   - [ ] 1.1 In `scan_typing.rs`, add one function that classifies the argument of `SECOND(x)` (one argument) or `EXTRACT(SECOND FROM x)` from `exact_expression_type(x, columns)`: `TIMESTAMP(p)` gives the fraction precision p, `DATE` under the `SECOND` name gives the date case, and anything else (computed argument, `VARCHAR`, `TIMESTAMP(p) WITH LOCAL TIME ZONE`, unknown column, `EXTRACT` over a DATE) declines. `type_scan_tree` and `exact_expression_type` both call it, so the rule is written once.
   - [ ] 1.2 In `type_scan_tree`, after typing the argument, rewrite a classified node into a synthetic DataFusion-only node (next to `INTEGER_TEXT_NODE`) that carries p or the date case and the typed argument. Its class is `TypeClass::of(DECIMAL(2+p, p))` (an integer for p = 0) and `TypeClass::Number` for the date case. A decline returns the scan-typing decline error. `SECOND(ts, n)` is not rewritten and declines in the renderer's `SECOND` arm (1.3). [expert]
   - [ ] 1.3 In `lib.rs`, render the synthetic node in the DataFusion dialect only: p = 0 or DATE as `CAST(date_part('second', <arg>) AS DECIMAL(2,0))`; p from 1 to 6 from `date_part('microsecond', <arg>)` and p from 7 to 9 from `date_part('nanosecond', <arg>)`, divided as an integer by 10^(6-p) or 10^(9-p) when that is above 1, cast to `DECIMAL(2+p, 0)`, multiplied by `CAST('<10^-p>' AS DECIMAL(p,p))`, and cast to `DECIMAL(2+p, p)` (decision-log [2]). Move `SECOND` out of the DataFusion `YEAR | MONTH | DAY | HOUR | MINUTE | SECOND` arm into its own DataFusion arm that declines every call with a message saying `SECOND` renders only from the typed scan pass, and decline the `SECOND` field in the DataFusion branch of `function_scalar_extract` with the same message. An untyped `SECOND` then never renders whole seconds, and the backstop arm's comment ("reachable only if a declared name loses its per-name arm") stays true. Leave every Exasol-dialect branch unchanged.
   - [ ] 1.4 In `exact_expression_type`, return `DECIMAL(2+p, p)` for a `SECOND` or `EXTRACT(SECOND)` node over a `TIMESTAMP(p)` argument through 1.1's function, and `None` for the date case.
   - [ ] 1.5 Unit tests in `scan_typing_tests.rs`, as case tables: the rendered SQL and the fragment type for p = 0 (`CAST(TS AS TIMESTAMP(0))`), 3 (`CAST(TS AS TIMESTAMP)`), 6 (column `TS`), 9 (`CAST(TS AS TIMESTAMP(9))`), and the DATE column `D`, each through `SECOND` and `EXTRACT(SECOND)`; declines for `SECOND(DATE_TRUNC('minute', TS))`, `SECOND(S)`, `SECOND(CAST(TS AS TIMESTAMP(6) WITH LOCAL TIME ZONE))`, and `EXTRACT(SECOND FROM D)`. Add rows to the existing CONCAT case table (`SECOND(CAST(TS AS TIMESTAMP(0)))` pushes, `SECOND(D)` declines; the existing "EXTRACT SECOND" row over `TS` keeps declining) and to `exact_expression_type_is_the_type_the_request_states` (`SECOND(TS)` is `DECIMAL(8,6)`, `EXTRACT(SECOND FROM CAST(TS AS TIMESTAMP(9)))` is `DECIMAL(11,9)`, `SECOND(D)` and `SECOND(DATE_TRUNC(...))` are `None`).
   - [ ] 1.6 In `lib_tests.rs`, drop `SECOND` from the DataFusion assertions of `renders_year_month_day_extract` and `renders_date_field_shortcuts_verbatim_in_exasol_dialect`, keep it in the Exasol assertions, add `EXTRACT(SECOND FROM ts)` to `renders_extract_as_exasol_extract_from_in_exasol_dialect` with the unchanged `EXTRACT(SECOND FROM "TS")`, assert in `renders_year_month_day_extract` that `render_expression` declines `SECOND(ts)`, and assert in `renders_extract` that it declines `EXTRACT(SECOND FROM ts)`. Keep the assertions of `second_with_precision_declines_for_datafusion_renders_for_exasol` unchanged and tag it `/// Scenario: A refused argument count declines for DataFusion and renders for Exasol`.
   - [ ] 1.7 In `lib_tests.rs` `exasol_dialect_renders_declared_verbatim_surface`, render the `SECOND` fixture's DataFusion assertion through `render_scan_expression` with a column lookup that types `TS` as `TIMESTAMP(6)`, and keep `render_expression` for every other declared name. A one-line comment states that `SECOND` renders only from the typed scan pass, because only that pass knows the argument's precision.
2. E2E parity against a native copy
   - [ ] 2.1 In `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs`, add one test over `qualified_probe()` with a native copy of `ID, TS, TSTZ, TS_NS`, and one case table of `(query, PathCheck, Expect)` rows run through `native_copy_mismatch`. Each of `SECOND(TS)`, `EXTRACT(SECOND FROM TS)`, `SECOND(TS_NS)`, and `EXTRACT(SECOND FROM TS_NS)` runs in the select list, in a WHERE filter (`> 0.1` over `TS`, `= 0.000000002` over `TS_NS`), and as a GROUP BY key, projected over `TS` and unprojected over `TS_NS` (`PathCheck::Split`). Further rows: `SECOND(TSTZ)` in the select list; `MAX(SECOND(TS)) + 1, SUM(SECOND(TS_NS))` (`PathCheck::Split`); `SECOND(CAST(TS AS TIMESTAMP))` and `SECOND(CAST(TS AS DATE))` in the select list; `EXTRACT(SECOND FROM CAST(TS AS DATE))` (`Expect::Error`); `SECOND(TS, 2)` (`Expect::Rows`, computed by Exasol). The select-list and filter rows assert the scan marker with `PathCheck::Has`: `date_part('microsecond'` for a microsecond column, and for a nanosecond column `date_part('nanosecond'` when `engine_honors_declared_precision` holds, else `date_part('microsecond'`.
   - [ ] 2.2 In the same test, after 2.1's rows, replace the native copy with `ID, C_TIMESTAMPTZ, C_TIMESTAMP_NS` of `LAKEHOUSE_VS`'s `E2E_ALL_TYPES_TABLE` (values at second 45.123, at the epoch, and NULL) through `create_native_copy`, and run a second case table: `SECOND(C_TIMESTAMPTZ)` and `EXTRACT(SECOND FROM C_TIMESTAMP_NS)` in the select list, `WHERE SECOND(C_TIMESTAMPTZ) > 45`, and `WHERE EXTRACT(SECOND FROM C_TIMESTAMP_NS) = 45.123`, with 2.1's scan markers. These rows compare the whole-second part against native Exasol, because every `ts_precision_probe` value sits at second 0.
   - [ ] 2.3 Build the `.so` with `make cross-udf-build`, run the test against the local Docker Exasol, and run the existing `e2e_split_aggregate_columns_match_native_oracle` and `timestamp_precision` tests to check that no other path changed.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Typed SECOND fraction | 1.1-1.7, 2.1-2.3 | none | spec deltas `sql-functions/vs-expression-translator-date-fns`, `pushdown-aggregates/pushdown-planning-split-aggregate-types`; `crates/vs-expression/src/scan_typing.rs`, `lib.rs`, `scalar_functions.rs`, `scan_typing_tests.rs`, `lib_tests.rs`; `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs`, `crates/lakehouse-e2e/tests/common/e2e_harness.rs` (`native_copy_mismatch`, `create_native_copy`, `PathCheck`, `Expect`), `crates/lakehouse-e2e/tests/common/seed.rs` (`E2E_ALL_TYPES_TABLE`, `TIMESTAMP_VALUES_TEXT`) |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Match arm entry | `SECOND` in the `YEAR \| MONTH \| DAY \| HOUR \| MINUTE \| SECOND` arm of `crates/vs-expression/src/lib.rs` | The synthetic node renders `SECOND` for the DataFusion dialect, and `SECOND`'s own DataFusion arm declines every untyped call |
| Test assertion | `SECOND` rows of the DataFusion assertions in `renders_year_month_day_extract` and `renders_date_field_shortcuts_verbatim_in_exasol_dialect` (`lib_tests.rs`) | They assert the whole-second rendering this plan removes |
| Test assertion | The untyped `render_expression` assertion for the `SECOND` fixture in `exasol_dialect_renders_declared_verbatim_surface` (`lib_tests.rs`) | Task 1.7 asserts `SECOND` through `render_scan_expression` instead |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A refused argument count declines for DataFusion and renders for Exasol | Unit | `crates/vs-expression/src/lib_tests.rs` | `second_with_precision_declines_for_datafusion_renders_for_exasol` |
| EXTRACT translates to the DataFusion date_part call | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_extract` |
| EXTRACT renders Exasol's EXTRACT FROM form in the Exasol dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_extract_as_exasol_extract_from_in_exasol_dialect` |
| Field-shortcut date functions translate to date_part of the matching field | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_year_month_day_extract` |
| Field-shortcut date functions render verbatim in the Exasol dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_date_field_shortcuts_verbatim_in_exasol_dialect` |
| SECOND keeps the fraction of its argument's declared precision | Unit | `crates/vs-expression/src/scan_typing_tests.rs` | `second_renders_the_fraction_of_its_argument_precision` |
| SECOND over a DATE computes 0 without a stated type | Unit | `crates/vs-expression/src/scan_typing_tests.rs` | `second_of_a_date_is_zero_without_an_exact_type` |
| SECOND over any other argument, or EXTRACT(SECOND) over a DATE, runs in Exasol | Unit | `crates/vs-expression/src/scan_typing_tests.rs` | `second_over_any_other_argument_declines` |
| SECOND and EXTRACT(SECOND) return native results through the virtual schema | Integration (E2E) | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `second_and_extract_second_match_a_native_copy` |
| A nested aggregate over an expression declines the split | Integration (E2E) | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_split_aggregate_columns_match_native_oracle` (unchanged) |
| An unprojected group key without an exact type declines the split | Integration (E2E) | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_split_aggregate_columns_match_native_oracle` (unchanged) |
| An unprojected SECOND group key takes the type of its argument's precision | Integration (E2E) | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `second_and_extract_second_match_a_native_copy` |

The DATE and decline scenarios also have E2E rows in `second_and_extract_second_match_a_native_copy` (`SECOND(CAST(TS AS DATE))`, `EXTRACT(SECOND FROM CAST(TS AS DATE))`).

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| VS Expression Translator, Date and Time Functions | `exapump sql -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0" "SELECT ID, SECOND(TS), SECOND(TS_NS) FROM TS_PRECISION_VS.TS_PRECISION_PROBE ORDER BY ID"` after `make e2e-up` and one run of the `timestamp_precision` E2E tests | `0.000001`, `0.000002`, `0.123456`, `0.123457` and `0.000000001`, `0.000000002`, `0.000000001`, `0.000000002`, not zeros |
| VS Expression Translator, Date and Time Functions | Same connection: `SELECT PUSHDOWN_SQL FROM (EXPLAIN VIRTUAL SELECT SECOND(TS) FROM TS_PRECISION_VS.TS_PRECISION_PROBE)` | The scan spec shows `date_part('microsecond', "TS")` scaled to `DECIMAL(8,6)`, and `EMITS` declares `DECIMAL(8,6)` |
| Pushdown Planning: Split-Aggregate Column Types | Same connection: `SELECT PUSHDOWN_SQL FROM (EXPLAIN VIRTUAL SELECT COUNT(*) FROM TS_PRECISION_VS.TS_PRECISION_PROBE GROUP BY SECOND(TS_NS))` | A partial aggregate whose `EMITS` declares the group key `DECIMAL(11,9)`, with no raw `"TS_NS"` projection |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace --lib` | 0 failures |
| E2E | `make e2e-up` then `cargo test --workspace --features lakehouse-e2e/exasol-e2e --test e2e_local -- timestamp_precision split_aggregate` | 0 failures |
| Coverage | `cargo llvm-cov --workspace --lcov --output-path target/llvm-cov/lcov.info` | At least 90% of the changed lines covered |
| Lint and format | `make fmt-check lint` | 0 errors, 0 warnings, no changes |
