# Plan: change-pushdown-keep-more-in-scan

## Summary

The scan keeps two kinds of work that Exasol does today: the rendered conjuncts of a WHERE with one declined conjunct, and an unprojected `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `WEEK` group key or its `EXTRACT` form (#615, item 5). A unit test pins the DataFusion limitation behind the timestamp-literal floor (#461), so the DataFusion upgrade that lifts it fails the test.

## Context

- Issue #615 is open. Items 1 to 4 landed in #621, #625, and #626. This plan covers item 5 except its first bullet, which asks Exasol to convert constant text before the scan (#615).
- Text compared with a DATE keeps running in Exasol. A typed literal (`d = DATE '2024-01-02'`) or a constant `TO_DATE(…, '<format>')`, which Exasol folds into a literal, keeps the scan filter and prunes files, and `docs/capabilities.md` § Text and session settings documents that rewrite.
- The typed pass (`crates/vs-expression/src/scan_typing.rs`) derives the exact type the Exasol version gives an expression, and one inference serves the scan and every declared type (`ScanTyping::exact_type`). Neither item-5 behavior of this plan and no #461 regression guard is on `main`.
- `ScanTranslator::split_where` (`crates/lakehouse-engine/src/adapter/pushdown/support.rs`) renders the WHERE as one unit. One declined conjunct sends the whole WHERE to the qualified single-table wrapper (`build_dispatch_sql`, `mod.rs`), and the broadcast join gives up its plan (`render_broadcast_join_emitting`, `joins/sql_builders.rs`). The N-scan legs already screen one conjunct at a time (`screen_leg_filter`, `joins/rendering.rs`).
- `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK` carry the result rule `Integer` (`crates/vs-expression/src/scalar_functions.rs`), an integer kind without a precision. `ScanTyping::exact_type` therefore returns `None`, and an unprojected key or a nested aggregate argument over them sends the split aggregate to the wrapper. `EXTRACT` of these fields reads the same rule (`ScanTyping::node_type`).
- DataFusion 55.2.0 (`Cargo.lock`) is the newest crates.io release and lacks apache/datafusion#22906. The fix is in tag 56.0.0-rc1, not in 55.3.0-rc1. The timestamp-literal floor (`floor_timestamp_literals`, `scan_typing.rs`) therefore stays, and `specs/testing.md` § Correctness oracles requires a unit test that asserts the library limitation still holds.
- No Iceberg table spec or Delta protocol section governs this change. File pruning reads the raw request filter as before, and no read path, schema, or type mapping changes.
- Architecture: no change. The WHERE split keeps the one scan translator that `specs/architecture.md` § Constraints requires.
- Open question: item 5 of #615 asks for the DATE text-literal case, which this plan does not deliver. The implementing commit uses `Closes #615` only after a human records on #615 that the typed-literal rewrite resolves that bullet, and `Refs #615` otherwise.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| Pushdown Declined-Filter Self-Application | CHANGED | `pushdown/pushdown-declined-filter-self-apply/spec.md` |
| Pushdown Planning | CHANGED | `pushdown/pushdown-planning/spec.md` |
| Pushdown Planning: Split-Aggregate Column Types | CHANGED | `pushdown-aggregates/pushdown-planning-split-aggregate-types/spec.md` |
| Pushdown Planning, Aggregate Extensions (HAVING & Statistical Aggregates) | CHANGED | `pushdown-aggregates/pushdown-planning-aggregate-extensions/spec.md` |

## Impact

- A single-table WHERE with a declined conjunct still runs through the qualified wrapper, and the scan now filters on the other conjuncts, so fewer rows reach Exasol.
- `GROUP BY YEAR(d)`, the other five date-part keys, and their `EXTRACT` forms keep the split aggregate when they are not selected.
- A digits-only text literal compared with a date-part call and longer than its precision now runs in Exasol, as for any integer operand.
- No breaking change. No property, CONNECTION field, advertised capability, or install step changes. `docs/capabilities.md` changes. `docs/capability-matrix.md` and `docs/semantic-differences.md` stay.

## Implementation Tasks

A test that implements a scenario carries one `/// Scenario: <title>` line for each scenario the Scenario Coverage table maps to it. Read `specs/testing.md` before adding a test: each behavior is proved at one layer, so the unit tests below cover only logic that no Exasol SQL observes.

### 1. Fixed types for date-part functions (`split-aggregate-types` delta)

- [ ] 1.1 On Exasol 8.29.13 and on 2025.1.16, measure the result type of `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK`, and of `EXTRACT` of `YEAR`, `MONTH`, `DAY`, `HOUR`, and `MINUTE`, over a DATE, a `TIMESTAMP(3)`, and, where the version has one, a `TIMESTAMP(9)` value: `CREATE TABLE … AS SELECT` each call, then read the column types from `EXA_ALL_COLUMNS`. Where a type differs from the spec delta, correct the delta before coding. A form whose type differs between the versions takes each version's type, and an `EXTRACT` field whose type differs from its function's keeps the integer kind.
- [ ] 1.2 `scalar_functions.rs` and `ScalarFunction::result_type` (`scan_typing.rs`): replace the `Integer` rule of `YEAR` with a fixed exact `DECIMAL(4,0)`, and that of `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK` with `DECIMAL(2,0)`. `EXTRACT` reads the same rule and needs no change unless 1.1 finds a difference.
- [ ] 1.3 E2E: add rows to `SPLIT_AGGREGATE_CASES` in `crates/lakehouse-e2e/tests/e2e_local/capability.rs`, each with `PathCheck::Split`: `SELECT COUNT(*) FROM {T} GROUP BY YEAR(C_DATE)`, `… GROUP BY MONTH(C_TS)`, `… GROUP BY WEEK(C_DATE)`, `… GROUP BY HOUR(C_TS)`, `… GROUP BY EXTRACT(MINUTE FROM C_TS)`, and `SELECT MAX(DAY(C_DATE)) + 1 FROM {T}`. Add the new scenario's line to `e2e_split_aggregate_columns_match_native_oracle`.
- [ ] 1.4 `docs/capabilities.md` § Aggregation: a date-part key that is not selected, such as `GROUP BY YEAR(d)`, splits, and another computed key, such as `GROUP BY qty * 2`, still needs a CAST.

### 2. WHERE split at AND (`declined-filter-self-apply`, `pushdown-planning`, and `aggregate-extensions` deltas)

- [ ] 2.1 `ScanTranslator::split_where` (`support.rs`): render the whole filter first, as today, so a filter that renders keeps its byte-identical SQL. When it declines, render each top-level conjunct, and return a new `ScanWhere` variant that holds the conjunction of the rendered SQL (`conjoin_scan_filters`) and the conjunction of the declined raw conjuncts as an owned tree. When no conjunct renders, return today's `ScanWhere::Declined` with the raw tree, so a fully declined filter keeps its SQL. Move `flatten_conjuncts` and `conjoin` from `joins/rendering.rs` into `support.rs`, and add one per-conjunct screen on `ScanTranslator` that `split_where` and `screen_leg_filter` both call, so the per-conjunct rule exists once and `support.rs` imports nothing from `joins::rendering`. Update the doc comments of `split_where` and `ScanWhere`.
- [ ] 2.2 `build_dispatch_sql` (`mod.rs`): route the new variant to `qualified_single_table_fallback_pushdown`, which already gives the rendered half to the fan-out spec and the declined half to the wrapper's `WHERE`. For that variant only, compute the wrapper's emission with `single_table_emission` over the request whose `filter` is the declined half, so a column that only a rendered conjunct names is not emitted. The `fallback()` closure for a filter that renders keeps today's emission and SQL. Remove the exclusivity statement ("so `fan_out.filter` must then be `None`") from the doc comment of `build_qualified_single_table_fallback_sql` (`joins/sql_builders.rs`).
- [ ] 2.3 `render_broadcast_join_emitting` (`joins/sql_builders.rs`): return `Ok(None)` for the new variant, so the join takes the N-scan fallback.
- [ ] 2.4 Update the tests that encode the whole-WHERE decline (`specs/testing.md` § Regression guards). (a) In `crates/lakehouse-e2e/tests/common/pruning_cases.rs`, add a `Placement::Split` variant that holds when the plan has both `LHS_T0` and `"filter":"`, and set it on the case `S LIKE 'xaa' ESCAPE 'a' AND SECOND(TS, 3) >= 0`, keeping its scanned-file list `["a1", "a2", "b1"]`. (b) Delete `nested_like_decline_routes_to_wrapper_where` (`support_tests.rs`), because the E2E test of 2.6 proves the split. (c) Rewrite `pruning_input_unchanged_when_df_render_declines` (`pushdown_tests.rs`) to assert that the new variant's declined half holds the original, un-rewritten `SECOND` and `LENGTH` conjuncts.
- [ ] 2.5 Golden `group_by_fallback_rendering_filter.sql` in `dispatch_golden_tests.rs`, generated before 2.1, for a grouped request that takes the wrapper fallback with a filter that renders. No golden changes after 2.1 to 2.3.
- [ ] 2.6 E2E in `capability.rs`, next to the `e2e_declined_filter_*` tests: `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan`, a case table over `WHERE C_DECIMAL_A LIKE '1%' AND ID > 1` as a row scan (`SELECT ID … ORDER BY ID`, ids 5 and 7), `SELECT C_VARCHAR …`, `COUNT(*)`, `GROUP BY C_BOOL`, `COUNT(DISTINCT ID)`, `ORDER BY ID LIMIT 1`, and `SELECT RANDOM() …`, plus `WHERE (C_DECIMAL_A LIKE '1%' OR ID = 2) AND ID > 1`. Each case matches native Exasol over a native copy, and its `EXPLAIN VIRTUAL` shows the `LHS_T0` wrapper whose `WHERE` holds the `LIKE` and a scan-spec `"filter":"` that names `ID` and holds no `LIKE`. For `SELECT C_VARCHAR …` the fan-out's `EMITS` names no `"ID"`, and for `SELECT RANDOM() …`, whose `selectList` is empty, the row count matches the native copy. Add the `/// Scenario:` line of "A declined single-table WHERE filter is applied in the adapter's own outer WHERE" to `e2e_declined_filter_second_arity_returns_filtered_rows`, add the line of "A declined filter does not widen the wrapper's projection" to `e2e_declined_filter_select_star_returns_full_row_shape` and to this test, and remove the line of "Filter predicate is pushed into the scan spec" from the unit test `filter_in_common_arg` (`support_tests.rs`), because the E2E test becomes that scenario's proof.
- [ ] 2.7 `docs/capabilities.md`: in § Filtering, replace "the whole filter runs in Exasol SQL and the scan returns the referenced columns unfiltered" with: a condition DataFusion cannot render runs in Exasol SQL on its own, and the scan still filters on the other conditions. In § Text and session settings, replace "On a single table it takes the whole WHERE with it, so the scan filters no rows" the same way.

### 3. Regression guard for the timestamp-literal floor (#461)

- [ ] 3.1 `datafusion_truncates_an_overprecise_timestamp_literal` in `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`: a `MemTable` with one `Timestamp(Millisecond, None)` row at `2025-06-01 00:00:00.123`, queried through `SessionContext::sql` with `WHERE ts = TIMESTAMP '2025-06-01 00:00:00.123456'`, returns the row (apache/datafusion#22142). Its comment states that a failure means the DataFusion release carries apache/datafusion#22906, and that `floor_timestamp_literals` (`crates/vs-expression/src/scan_typing.rs`) and this test go then.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: date-part types | 1.1-1.4 | none | spec delta `pushdown-aggregates/pushdown-planning-split-aggregate-types`; `crates/vs-expression/src/scalar_functions.rs`, `scan_typing.rs` (`ScalarFunction::result_type`); `crates/lakehouse-e2e/tests/e2e_local/capability.rs` (`SPLIT_AGGREGATE_CASES`); `docs/capabilities.md` § Aggregation |
| B: WHERE split | 2.1-2.7 | A (shares `capability.rs`, `docs/capabilities.md`) | spec deltas `pushdown/pushdown-declined-filter-self-apply`, `pushdown/pushdown-planning`, `pushdown-aggregates/pushdown-planning-aggregate-extensions`; `crates/lakehouse-engine/src/adapter/pushdown/support.rs` (`split_where`, `ScanWhere`), `mod.rs` (`build_dispatch_sql`), `joins/rendering.rs`, `joins/sql_builders.rs`, `dispatch_golden_tests.rs`, `testdata/dispatch_golden/`, `support_tests.rs`, `pushdown_tests.rs`; `crates/lakehouse-e2e/tests/common/pruning_cases.rs`; `capability.rs` (`e2e_declined_filter_*`); `docs/capabilities.md` § Filtering, § Text and session settings |
| D: #461 guard | 3.1 | none | `crates/lakehouse-engine/src/scan/raw_scan_tests.rs` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Scenario line | `filter_in_common_arg` in `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | The E2E test of 2.6 proves the scenario |
| Test | `nested_like_decline_routes_to_wrapper_where` in `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | It asserts the whole-WHERE decline, and the E2E test of 2.6 proves the split |
| Function body | the per-conjunct loop of `screen_leg_filter` (`joins/rendering.rs`) | Replaced by the shared per-conjunct screen of 2.1 |
| Doc comment | `split_where`, `ScanWhere`, `build_qualified_single_table_fallback_sql` | The scan filter and the declined filter are no longer exclusive |
| Doc text | `docs/capabilities.md` "the whole filter runs in Exasol SQL" and "On a single table it takes the whole WHERE with it" | Each describes the removed behavior |

The timestamp-literal floor stays until a DataFusion release carries apache/datafusion#22906.

## Out of scope

- Converting text compared with a DATE or another session-formatted type before the scan. The typed-literal rewrite in `docs/capabilities.md` § Text and session settings is the answer.
- Several recorded scenarios of `pushdown/pushdown-declined-filter-self-apply` have no test that carries their `/// Scenario:` line. This plan adds lines only for the scenarios it changes.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A declined single-table WHERE filter is applied in the adapter's own outer WHERE | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_filter_second_arity_returns_filtered_rows` |
| A declined filter does not widen the wrapper's projection | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_filter_select_star_returns_full_row_shape`, and the `SELECT RANDOM()` case of `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A single-table WHERE filter splits at AND between the scan and the wrapper | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| Filter predicate is pushed into the scan spec | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A declined WHERE filter routes the single-table request to the qualified wrapper | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs`; for the pruning step, `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan`; for the pruning step, the `Placement::Split` case of `pruning_cases.rs` through `iceberg_pruning_keeps_every_file_with_a_matching_row` |
| A declined WHERE filter under an aggregate request is applied ahead of the aggregate | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A date-part function takes its fixed native type | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_split_aggregate_columns_match_native_oracle` |

### Manual Testing

Run `make e2e-up test-e2e` first, which installs the `.so` and creates `MY_LAKEHOUSE.TYPED_DISTINCT_PROBE`. Each command runs `exapump sql` with `-d "exasol://sys:$EXASOL_SYS_PASSWORD@$EXASOL_HOST:$LH_EXASOL_PORT?validateservercertificate=0"`, the SQL on stdin.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Pushdown Declined-Filter Self-Application | `EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DECIMAL_A LIKE '1%' AND ID > 1;` | The `LHS_T0` wrapper's `WHERE` holds the `LIKE`, and the scan spec's `"filter":"` holds `"ID" > 1` |
| Pushdown Planning | `SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DECIMAL_A LIKE '1%' AND ID > 1 ORDER BY ID;` | Ids 5 and 7 |
| Pushdown Planning, Aggregate Extensions (HAVING & Statistical Aggregates) | `SELECT COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DECIMAL_A LIKE '1%' AND ID > 1;` | 2 |
| Pushdown Planning: Split-Aggregate Column Types | `EXPLAIN VIRTUAL SELECT COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE GROUP BY YEAR(C_DATE);` | The scan spec holds `"group_keys"`, the `EMITS` clause declares `"GK_0" DECIMAL(4,0)`, and there is no `LHS_T0` wrapper |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace` | 0 failures, including `datafusion_truncates_an_overprecise_timestamp_literal` on DataFusion 55.2.0 |
| E2E on Exasol 2025 | `make e2e-up test-e2e` | 0 failures |
| E2E on Exasol 8 | `make e2e-down; EXASOL_IMAGE=exasol/docker-db:8.29.13 make e2e-up test-e2e` | 0 failures |
| Lint and format | `make fmt-check lint` | Exit 0, no changes |
