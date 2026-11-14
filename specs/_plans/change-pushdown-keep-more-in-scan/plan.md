# Plan: change-pushdown-keep-more-in-scan

## Summary

The scan keeps three kinds of work that Exasol does today: a DATE compared with a text literal, the rendered conjuncts of a WHERE with one declined conjunct, and an unprojected `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `WEEK` group key (#615, item 5). A unit test pins the DataFusion limitation behind the timestamp-literal floor (#461), so the DataFusion upgrade that lifts it fails the test.

## Context

- Issue #615 item 5 lists four follow-ups of #596 (#615). Items 1 to 3 landed in #621 and #625. Item 4 is PR #626 (`specs/_plans/add-date-add-functions/`).
- A text literal compared with a DATE declines today, because the scan cannot parse text by the session's `NLS_DATE_FORMAT`. So `d = '2024-01-02'` runs in Exasol, and the scan filters no rows.
- `ScanTranslator::split_where` (`crates/lakehouse-engine/src/adapter/pushdown/support.rs`) renders the WHERE as one unit. One declined conjunct sends the whole WHERE to the qualified single-table wrapper, and the scan filters no rows. The N-scan legs already screen one conjunct at a time (`screen_leg_filter`).
- An unprojected group key that is not a column, a CAST, or a constant has no exact type, so the split aggregate declines. `exact_expression_type` (`crates/vs-expression/src/scan_typing.rs`) is the one source of request-certain expression types, and `group_key_exasol_types`, `nested_aggregate_type`, and `render_exasol_fragment` read it.
- DataFusion 55.2.0 (`Cargo.lock`) does not contain apache/datafusion#22906: its merge commit is not an ancestor of tag 55.2.0 and has no backport. The timestamp-literal floor (#461) therefore stays. `specs/testing.md` § Correctness oracles requires a unit test that asserts the library limitation still holds.
- Group C builds on the DATE class that PR #626 adds to the typed pass (`TypeClass::Date`). Today DATE and TIMESTAMP both fold into `TypeClass::Other`.
- No Iceberg table spec or Delta protocol section governs this change. File pruning reads the same request filter as before and takes no bound from a converted literal, and no read path, schema, or type mapping changes.
- Architecture: `architecture.md` in this plan changes § Components and § Data Flow. vs-expression owns the converted-literal marker (decision-log.md [1]), and Exasol evaluates the scan's `common` argument when the spec holds one.
- One-argument `SECOND` is typed by PR #624 (#603).
- Open question: text literals compared with a TIMESTAMP, DOUBLE, or scaled DECIMAL still decline, and `EXTRACT(YEAR|MONTH|DAY|HOUR|MINUTE FROM x)` gets no fixed type (#TBD). Item 5 of #615 names both, so the follow-up issue keeps them tracked after `Closes #615`. The user confirms the DATE-only scope.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| Pushdown Planning, Scan Text Conversion | CHANGED | `pushdown-types/pushdown-planning-scan-text-conversion/spec.md` |
| Pushdown Declined-Filter Self-Application | CHANGED | `pushdown/pushdown-declined-filter-self-apply/spec.md` |
| Pushdown Planning | CHANGED | `pushdown/pushdown-planning/spec.md` |
| Pushdown Planning: Split-Aggregate Column Types | CHANGED | `pushdown-aggregates/pushdown-planning-split-aggregate-types/spec.md` |
| Pushdown Planning, Aggregate Extensions (HAVING & Statistical Aggregates) | CHANGED | `pushdown-aggregates/pushdown-planning-aggregate-extensions/spec.md` |

## Impact

- A comparison, `BETWEEN`, `IN`, or simple `CASE` of a DATE with a text literal keeps its scan filter. Exasol converts the literal by the session's `NLS_DATE_FORMAT` before the scan. The adapter's file pruning still takes no bound from such a literal.
- An unparseable DATE literal fails the query whenever a shard scans, also when no row would reach the comparison. `docs/semantic-differences.md` documents it.
- A single-table WHERE with a declined conjunct still runs through the qualified wrapper, and the scan now filters on the other conjuncts, so fewer rows reach Exasol.
- `GROUP BY YEAR(d)` and the other five date-part keys keep the split aggregate when they are not selected.
- A text literal compared with a TIMESTAMP, DOUBLE, or scaled DECIMAL still runs in Exasol.
- No breaking change. No property, CONNECTION field, advertised capability, or install step changes. `docs/capabilities.md` and `docs/semantic-differences.md` change.

## Dependencies

- PR #624 merges before group A starts. It adds the typing-rule ADR that decision [3] relies on, the `SECOND` rule, and the Background text that the split-aggregate-types delta builds on.
- PR #626 merges before group C starts, because group C reads the DATE class it adds. The implementing commit then carries `Closes #615`, because items 1 to 4 are done. The open question in § Context tracks the rest of item 5.

## Implementation Tasks

Every test that implements a scenario carries one `/// Scenario: <title>` line for each scenario the Scenario Coverage table maps to it. Read `specs/testing.md` before adding a test.

### 1. Fixed types for date-part functions (`split-aggregate-types` delta)

- [ ] 1.1 On Exasol 8.29.13 and on 2025.1.16, confirm the result type of `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK` over a DATE, a `TIMESTAMP(3)`, and a `TIMESTAMP(9)` value: `CREATE TABLE … AS SELECT` each call, then read the column types from `EXA_ALL_COLUMNS`. If a type differs from the spec delta on either version, correct the delta before coding, and drop a function whose type differs between the versions.
- [ ] 1.2 `crates/vs-expression/src/scalar_functions.rs`: record a fixed result type in the `YEAR` entry (`DECIMAL(4,0)`) and in the `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK` entries (`DECIMAL(2,0)`). `exact_expression_type` (`scan_typing.rs`) returns it for a one-argument `function_scalar` node of these names. Update its doc comment, which says that only a CAST, `JSON_VALUE`, or column has a type.
- [ ] 1.3 Unit test in `crates/vs-expression/src/scan_typing_tests.rs`: `date_part_functions_take_fixed_exact_types` (the six types over a DATE and a TIMESTAMP column; `EXTRACT(YEAR FROM d)` returns `None`).
- [ ] 1.4 Unit tests: `unprojected_date_part_group_key_keeps_the_split` in `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs` (`GROUP BY YEAR(d)` declares `GK_0` as `DECIMAL(4,0)` and splits), and `nested_max_over_a_date_part_keeps_the_split` in `scalar_over_agg_tests.rs` (`SELECT MAX(MONTH(d)) + 1`).
- [ ] 1.5 E2E: add rows to `SPLIT_AGGREGATE_CASES` in `crates/lakehouse-e2e/tests/e2e_local/capability.rs`: `SELECT COUNT(*) FROM {T} GROUP BY YEAR(C_DATE)`, `… GROUP BY MONTH(C_TS)`, `… GROUP BY WEEK(C_DATE)`, `… GROUP BY HOUR(C_TS)`, and `SELECT MAX(DAY(C_DATE)) + 1 FROM {T}`, each with `PathCheck::Split`.
- [ ] 1.6 Regenerate each dispatch golden under `testdata/dispatch_golden/` that changes, and check that each change only drops a redundant CAST of a date-part item.
- [ ] 1.7 `docs/capabilities.md` § Aggregation: an unprojected `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `WEEK` key keeps the split, and another computed key, such as `GROUP BY qty * 2`, still needs a CAST.

### 2. WHERE split at AND (`declined-filter-self-apply` and `pushdown-planning` deltas)

- [ ] 2.1 `ScanTranslator::split_where` (`support.rs`): render the whole filter as today first, so a filter that renders keeps its byte-identical SQL. When it declines, flatten its top-level conjuncts with `flatten_conjuncts`, render each through the same render (`scan_rewrite`, then `render_scan_filter`), and return the conjunction of the rendered SQL and the conjunction of the declined raw conjuncts as owned `Json`. Both halves can be present. Reuse `flatten_conjuncts`, `conjoin`, and `conjoin_scan_filters` from `joins/rendering.rs` by widening their visibility, and leave `screen_leg_filter` unchanged. Update the doc comment, which states that the halves are exclusive.
- [ ] 2.2 `mod.rs` (the wrapper route near line 314): route to `qualified_single_table_fallback_pushdown` when the declined half is present, and pass both halves, so the fan-out's scan spec carries the rendered half. In `joins/sql_builders.rs`, remove the exclusivity statements from the doc comments of `build_qualified_single_table_fallback_sql` and `qualified_single_table_fallback_pushdown`. Only when a declined half is present, compute the fan-out projection (`referenced_columns`) over the request with its `filter` replaced by the declined half, so a column that only the scan filter reads is not projected. The `fallback()` closure in `mod.rs`, which passes a rendered `filter` without a declined half, keeps today's projection and SQL.
- [ ] 2.3 `render_broadcast_join` (`joins/sql_builders.rs`): fall through to the N-scan fallback whenever the declined half is present.
- [ ] 2.4 Unit tests: `split_where_screens_each_top_level_conjunct` in `support_tests.rs` (nested `AND` flattens, an `OR` stays whole, an all-rendering filter returns the whole-filter SQL, an all-declining filter returns no scan filter); `a_partly_declined_filter_keeps_its_rendered_conjuncts_in_the_scan` in `pushdown_tests.rs` over the row scan, grouped aggregate, `COUNT(DISTINCT)`, and ordered top-N shapes (the wrapper `WHERE` holds only the `LIKE`, the scan spec `filter` holds only `"ID" > 1`); `broadcast_declines_on_a_partly_declined_filter` in `joins/sql_builders_tests.rs`; `grouped_wrapper_fallback_with_a_rendering_filter_keeps_its_sql` in `pushdown_tests.rs` (a grouped request that cannot split and whose filter renders emits byte-identical SQL, including its fan-out projection). Add the two `/// Scenario:` lines of the Scenario Coverage table to `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper`.
- [ ] 2.5 E2E in `capability.rs`, next to the `e2e_declined_filter_*` tests: `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan`. `SELECT ID FROM {T} WHERE C_DECIMAL_A LIKE '1%' AND ID > 1 ORDER BY ID` returns ids 5 and 7, the same WHERE under `COUNT(*)` returns 2, and `EXPLAIN VIRTUAL` shows the `LHS_T0` wrapper with the `LIKE` in its `WHERE` and a scan-spec `"filter":"` that names `ID` and holds no `LIKE`. Add the scenario line of "A declined single-table WHERE filter is applied in the adapter's own outer WHERE" to `e2e_declined_filter_second_arity_returns_filtered_rows`.
- [ ] 2.6 Regression guard (#461), `datafusion_truncates_an_overprecise_timestamp_literal` in `support_tests.rs`: a `MemTable` with one `Timestamp(Millisecond, None)` row at `2025-06-01 00:00:00.123`, queried through `SessionContext::sql` with `WHERE ts = TIMESTAMP '2025-06-01 00:00:00.123456'`, returns the row (apache/datafusion#22142). Its comment states that a failure means the DataFusion release carries apache/datafusion#22906, and that the floor (`rewrite_overprecise_timestamp_comparisons`, `timestamp_literal_floor`) and this test go then.
- [ ] 2.7 `docs/capabilities.md` § Filtering and § Text and session settings: a declined condition runs in Exasol SQL on its own, and the scan still filters on the other conditions. Remove "On a single table it takes the whole WHERE with it, so the scan filters no rows".

### 3. DATE text literals (`scan-text-conversion` delta, architecture delta)

- [ ] 3.1 On the local Exasol, in sessions with `NLS_DATE_FORMAT = 'DD.MM.YYYY'` and `'YYYY-DD-MM'`, confirm that `CAST(<text> AS DATE)` returns the value that native `d = <text>`, `BETWEEN`, `IN`, and simple `CASE` compare with, and that `TO_CHAR(<date>, 'YYYY-MM-DD')` writes `0001-01-01` and `9999-12-31` in that form. Record when native Exasol fails for an unparseable literal: rows reach the comparison, another conjunct removes every row, and an empty table. A form whose CAST differs keeps declining, and the spec delta lists it.
- [ ] 3.2 Typed pass, `compare_without_session_parse` (`scan_typing.rs`): when every non-text operand has the DATE class and every text operand is a `literal_string`, mark each literal in the typed tree for conversion instead of declining, so that the renderer writes a converted-literal marker for it. Every other combination keeps today's result. [expert]
- [ ] 3.3 New `crates/vs-expression` module, with its sibling `_tests.rs`, that owns the converted-literal marker. The DataFusion-dialect renderer writes the marker where a DATE literal goes. The marker delimits itself: a start character, the original literal hex-encoded, and an end character, with start and end characters that `serde_json` does not escape. One public function turns a serialized common spec into the Exasol SQL of the `common` argument, with no count and no state from the render, so a fragment rendered and discarded or a marker in another leg's spec does not affect it. Without a marker character it returns the plain string literal that `sql_string_literal` returns. Otherwise it converts every well-formed marker: it returns a concatenation of string-literal parts and, per marker, an Exasol expression that converts the decoded literal, quoted as an Exasol string literal, with `CAST(… AS DATE)` and writes `DATE '<YYYY-MM-DD>'`, or `NULL` when the conversion returns NULL. It returns an error naming the cause when a marker character stands outside a well-formed marker. The adapter converts no literal in a request whose JSON holds a marker character. [expert]
- [ ] 3.4 `build_fan_out_inner` (`support.rs`) builds the `common` argument with the function of 3.3 instead of `sql_string_literal`, and needs no new parameter. Confirm that every path that serializes a common spec goes through it (row scan, top-N, both aggregate paths, `COUNT(DISTINCT)`, the wrapper fan-out, the broadcast join, and each N-scan leg's side fan-out), and that no other site writes a rendered scan fragment into SQL.
- [ ] 3.5 Unit tests in `scan_typing_tests.rs`: `date_compared_with_a_text_literal_converts_in_exasol` (`=`, `<>`, `<`, `BETWEEN`, `IN`, simple `CASE`, the literal on either side, a DATE column, a CAST to DATE, `ADD_DAYS` over a DATE), `text_literal_against_a_timestamp_or_number_still_declines`, and `date_compared_with_a_text_column_still_declines`.
- [ ] 3.6 Unit tests in the new module's `_tests.rs`: a spec without a marker character stays one string literal; one marker, several markers, and the same marker twice each concatenate; a literal holding `'` or `' || …` stays inside one Exasol string literal; the NULL branch; a stray or malformed marker returns an error.
- [ ] 3.7 Unit test `date_text_literal_keeps_the_scan_filter` in `pushdown_tests.rs`: the dispatch SQL has no wrapper, its `common` argument holds `CAST('01.02.2024' AS DATE)`, and the Iceberg pruning predicate takes no bound from the literal. `marker_character_in_the_request_disables_the_conversion` in the same file: a request holding a marker character in another literal takes today's decline path. `date_text_literal_next_to_a_conjunct_the_render_declines` (`WHERE C_DATE = '01.01.2024' AND SECOND(C_TS, 3) > 1`): the scan filter keeps the DATE conjunct, and the request does not fail. `date_text_literal_in_a_broadcast_attempt_that_falls_through` in `joins/sql_builders_tests.rs`: a join whose broadcast render declines on a widened projection takes the N-scan plan, and its SQL builds. `date_text_literal_in_each_n_scan_leg` in the same file: each leg's `common` argument converts its own literal. Add the golden `date_text_literal_single_table.sql` to `dispatch_golden_tests.rs`.
- [ ] 3.8 E2E in `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs`: `text_literal_compared_with_a_date_keeps_the_scan_filter`, with its own case tables under `DMY` and `YDM`: `C_DATE = '…'`, `<>`, `<=`, `BETWEEN`, `IN`, a select-list simple `CASE`, and a self-join of `{T}` with `a.C_DATE = '…'` with `Has(BROADCAST_JOIN)`, a grouped `COUNT(*)` under the filter with `PathCheck::Split`, every other case with `Has(SCAN_FILTER)`, and error cases with an unparseable literal. Move the `ERROR_CASES` row `C_DATE = '2024-01-01'` under `DMY` into this test's error cases with `Has(SCAN_FILTER)`. Add a `C_TS > '<text>'` case under `TS_DMY` to `CASES` with `Lacks(SCAN_FILTER)`.
- [ ] 3.9 Docs: in `docs/capabilities.md` § Text and session settings, a text literal compared with a DATE stays in the scan because Exasol converts it first, and the `d = '2024-01-02'` row leaves the "Runs in Exasol SQL" column. The TIMESTAMP row stays. In `docs/semantic-differences.md`, add a section on how an unparseable DATE literal fails, from the measurements of 3.1.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: date-part types | 1.1-1.7 | PR #624 merged | spec delta `pushdown-aggregates/pushdown-planning-split-aggregate-types`; `crates/vs-expression/src/scalar_functions.rs`, `scan_typing.rs`, `scan_typing_tests.rs`; `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs`, `grouped_agg_tests.rs`, `scalar_over_agg_tests.rs`, `testdata/dispatch_golden/`; `crates/lakehouse-e2e/tests/e2e_local/capability.rs` (`SPLIT_AGGREGATE_CASES`); `docs/capabilities.md` § Aggregation |
| B: WHERE split | 2.1-2.7 | A (shares `capability.rs`, `docs/capabilities.md`) | spec deltas `pushdown/pushdown-declined-filter-self-apply`, `pushdown/pushdown-planning`, `pushdown-aggregates/pushdown-planning-aggregate-extensions`; `crates/lakehouse-engine/src/adapter/pushdown/support.rs` (`split_where`), `mod.rs`, `joins/rendering.rs`, `joins/sql_builders.rs`, `support_tests.rs`, `pushdown_tests.rs`, `joins/sql_builders_tests.rs`; `capability.rs` (`e2e_declined_filter_*`); `docs/capabilities.md` § Filtering |
| C: DATE text literals | 3.1-3.9 | PR #626 merged; A (shares `scan_typing.rs`); B (shares `support.rs`, `docs/capabilities.md`) | spec delta `pushdown-types/pushdown-planning-scan-text-conversion`; `architecture.md` delta; `crates/vs-expression/src/scan_typing.rs` (`compare_without_session_parse`), `lib.rs` (DataFusion dialect), the new encoding module; `crates/lakehouse-engine/src/adapter/pushdown/support.rs` (`build_fan_out_inner`), `crates/lakehouse-engine/src/exasol.rs` (`sql_string_literal`), `pushdown_tests.rs`, `joins/sql_builders_tests.rs`, `dispatch_golden_tests.rs`; `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs`; `docs/capabilities.md` § Text and session settings, `docs/semantic-differences.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test case | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs`, `ERROR_CASES` row `C_DATE = '2024-01-01'` with `Lacks(SCAN_FILTER)` | Moves to the new test with `Has(SCAN_FILTER)` |
| Doc comment | `split_where`, `build_qualified_single_table_fallback_sql`, `qualified_single_table_fallback_pushdown` | The scan filter and the declined filter are no longer exclusive |
| Doc text | `docs/capabilities.md` "On a single table it takes the whole WHERE with it" and the `d = '2024-01-02'` row | Both describe the removed behavior |

The timestamp-literal floor stays until a DataFusion release carries apache/datafusion#22906.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Text the session format converts runs in Exasol | Integration | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `text_conversions_match_native_under_session_settings` |
| Exasol converts a text literal compared with a DATE before the scan | Integration | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `text_literal_compared_with_a_date_keeps_the_scan_filter` |
| Exasol converts a text literal compared with a DATE before the scan | Unit | `crates/vs-expression/src/scan_typing_tests.rs`, the new module's `_tests.rs`, `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`, `joins/sql_builders_tests.rs` | `date_compared_with_a_text_literal_converts_in_exasol`, the 3.6 tests, the 3.7 tests |
| A declined single-table WHERE filter is applied in the adapter's own outer WHERE | Integration | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_filter_second_arity_returns_filtered_rows` |
| A declined single-table WHERE filter is applied in the adapter's own outer WHERE | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper` |
| A single-table WHERE filter splits at AND between the scan and the wrapper | Integration | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A single-table WHERE filter splits at AND between the scan and the wrapper | Unit | `support_tests.rs`, `pushdown_tests.rs`, `joins/sql_builders_tests.rs` | `split_where_screens_each_top_level_conjunct`, `a_partly_declined_filter_keeps_its_rendered_conjuncts_in_the_scan`, `broadcast_declines_on_a_partly_declined_filter` |
| Filter predicate is pushed into the scan spec | Integration | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| Filter predicate is pushed into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `filter_in_common_arg` |
| A declined WHERE filter routes the single-table request to the qualified wrapper | Integration | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A declined WHERE filter routes the single-table request to the qualified wrapper | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper`, `a_partly_declined_filter_keeps_its_rendered_conjuncts_in_the_scan`, `grouped_wrapper_fallback_with_a_rendering_filter_keeps_its_sql` |
| A declined WHERE filter under an aggregate request is applied ahead of the aggregate | Unit | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `a_partly_declined_filter_keeps_its_rendered_conjuncts_in_the_scan` |
| A declined WHERE filter under an aggregate request is applied ahead of the aggregate | Integration | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A date-part function takes its fixed native type | Integration | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_split_aggregate_columns_match_native_oracle` |
| A date-part function takes its fixed native type | Unit | `scan_typing_tests.rs`, `grouped_agg_tests.rs`, `scalar_over_agg_tests.rs` | `date_part_functions_take_fixed_exact_types`, `unprojected_date_part_group_key_keeps_the_split`, `nested_max_over_a_date_part_keeps_the_split` |

### Manual Testing

Run `make e2e-up test-e2e` first, which installs the `.so` and creates `MY_LAKEHOUSE.TYPED_DISTINCT_PROBE`. Each command runs `exapump sql` with `-d "exasol://sys:$EXASOL_SYS_PASSWORD@$EXASOL_HOST:$LH_EXASOL_PORT?validateservercertificate=0"`, the SQL on stdin.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Pushdown Planning, Scan Text Conversion | `ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY'; EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DATE = '01.01.2024';` | The pushdown SQL holds `"filter":"` and `CAST('01.01.2024' AS DATE)`, and no `LHS_T0` wrapper |
| Pushdown Planning, Scan Text Conversion | The same session, `SELECT COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DATE = '01.01.2024';` | The count native Exasol returns for the same filter over a native copy |
| Pushdown Declined-Filter Self-Application | `EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DECIMAL_A LIKE '1%' AND ID > 1;` | The `LHS_T0` wrapper's `WHERE` holds the `LIKE`, and the scan spec's `"filter":"` holds `"ID" > 1` |
| Pushdown Planning | `SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DECIMAL_A LIKE '1%' AND ID > 1 ORDER BY ID;` | Ids 5 and 7 |
| Pushdown Planning: Split-Aggregate Column Types | `EXPLAIN VIRTUAL SELECT COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE GROUP BY YEAR(C_DATE);` | The scan spec holds `"group_keys"`, the `EMITS` clause declares `"GK_0" DECIMAL(4,0)`, and there is no `LHS_T0` wrapper |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace` | 0 failures, including `datafusion_truncates_an_overprecise_timestamp_literal` on DataFusion 55.2.0 |
| E2E | `make e2e-up test-e2e` | 0 failures |
| Lint and format | `make fmt-check lint` | Exit 0, no changes |
