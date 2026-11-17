# Plan: change-pushdown-keep-more-in-scan

## Summary

The scan keeps three kinds of work that Exasol does today: a DATE compared with a text literal, the rendered conjuncts of a WHERE with one declined conjunct, and an unprojected `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, or `WEEK` group key or its `EXTRACT` form (#615, item 5). A unit test pins the DataFusion limitation behind the timestamp-literal floor (#461), so the DataFusion upgrade that lifts it fails the test.

## Context

- Issue #615 is open. Items 1 to 4 landed in #621, #625, and #626. This plan covers item 5, so the implementing commit carries `Closes #615` (#615).
- The typed pass (`crates/vs-expression/src/scan_typing.rs`) derives DATE and the exact type the Exasol version gives an expression, and one inference serves the scan and every declared type (`ScanTyping::exact_type`). None of the three item-5 behaviors and no #461 regression guard is on `main`.
- `compare_without_session_parse` (`scan_typing.rs`) declines every comparison of text with a type other than an integer. So `d = '2024-01-02'` runs in Exasol, and the scan filters no rows.
- `ScanTranslator::split_where` (`crates/lakehouse-engine/src/adapter/pushdown/support.rs`) renders the WHERE as one unit. One declined conjunct sends the whole WHERE to the qualified single-table wrapper (`build_dispatch_sql`, `mod.rs`), and the broadcast join gives up its plan (`render_broadcast_join_emitting`, `joins/sql_builders.rs`). The N-scan legs already screen one conjunct at a time (`screen_leg_filter`, `joins/rendering.rs`).
- `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK` carry the result rule `Integer` (`crates/vs-expression/src/scalar_functions.rs`), an integer kind without a precision. `ScanTyping::exact_type` therefore returns `None`, and an unprojected key or a nested aggregate argument over them sends the split aggregate to the wrapper. `EXTRACT` of these fields reads the same rule (`ScanTyping::node_type`).
- DataFusion 55.2.0 (`Cargo.lock`) is the newest crates.io release and lacks apache/datafusion#22906. The fix is in tag 56.0.0-rc1, not in 55.3.0-rc1. The timestamp-literal floor (`floor_timestamp_literals`, `scan_typing.rs`) therefore stays, and `specs/testing.md` § Correctness oracles requires a unit test that asserts the library limitation still holds.
- No Iceberg table spec or Delta protocol section governs this change. File pruning reads the raw request filter, and `file-planning/pruning-predicate-translation` derives no bound for a DATE column from a text literal, so a converted literal prunes nothing. No read path, schema, or type mapping changes.
- Architecture: `architecture.md` in this plan changes § Components, § Data Flow, and § Constraints. vs-expression owns the converted-literal marker (decision-log.md [1]), Exasol evaluates the scan's `common` argument when the spec holds one, and the pushdown context records whether the request holds a marker character.
- Open question: text literals compared with a TIMESTAMP, DOUBLE, or scaled DECIMAL still decline (#TBD). Item 5 of #615 names them, so a follow-up issue keeps them tracked after `Closes #615`. The user confirmed the DATE-only scope.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| Pushdown Planning, Scan Text Conversion | CHANGED | `pushdown-types/pushdown-planning-scan-text-conversion/spec.md` |
| Pushdown Declined-Filter Self-Application | CHANGED | `pushdown/pushdown-declined-filter-self-apply/spec.md` |
| Pushdown Planning | CHANGED | `pushdown/pushdown-planning/spec.md` |
| Pushdown Planning: Split-Aggregate Column Types | CHANGED | `pushdown-aggregates/pushdown-planning-split-aggregate-types/spec.md` |
| Pushdown Planning, Aggregate Extensions (HAVING & Statistical Aggregates) | CHANGED | `pushdown-aggregates/pushdown-planning-aggregate-extensions/spec.md` |

## Impact

- A comparison, `BETWEEN`, `IN`, or simple `CASE` of a DATE with a text literal keeps its scan filter, and a join keeps its broadcast plan. Exasol converts the literal by the session's `NLS_DATE_FORMAT` before the scan. File pruning takes no bound from such a literal.
- An unparseable DATE literal fails the query whenever a shard scans, also when no row would reach the comparison. `docs/semantic-differences.md` documents it.
- A single-table WHERE with a declined conjunct still runs through the qualified wrapper, and the scan now filters on the other conjuncts, so fewer rows reach Exasol.
- `GROUP BY YEAR(d)`, the other five date-part keys, and their `EXTRACT` forms keep the split aggregate when they are not selected.
- A digits-only text literal compared with a date-part call and longer than its precision now runs in Exasol, as for any integer operand.
- A text literal compared with a TIMESTAMP, DOUBLE, or scaled DECIMAL still runs in Exasol.
- No breaking change. No property, CONNECTION field, advertised capability, or install step changes, so `docs/capability-matrix.md` stays. `docs/capabilities.md` and `docs/semantic-differences.md` change.

## Implementation Tasks

A test that implements a scenario carries one `/// Scenario: <title>` line for each scenario the Scenario Coverage table maps to it. Read `specs/testing.md` before adding a test: each behavior is proved at one layer, so the unit tests below cover only logic that no Exasol SQL observes.

### 1. Fixed types for date-part functions (`split-aggregate-types` delta), and the #461 guard

- [ ] 1.1 On Exasol 8.29.13 and on 2025.1.16, measure the result type of `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK`, and of `EXTRACT` of `YEAR`, `MONTH`, `DAY`, `HOUR`, and `MINUTE`, over a DATE, a `TIMESTAMP(3)`, and, where the version has one, a `TIMESTAMP(9)` value: `CREATE TABLE … AS SELECT` each call, then read the column types from `EXA_ALL_COLUMNS`. Where a type differs from the spec delta, correct the delta before coding. A form whose type differs between the versions takes each version's type, and an `EXTRACT` field whose type differs from its function's keeps the integer kind.
- [ ] 1.2 `scalar_functions.rs` and `ScalarFunction::result_type` (`scan_typing.rs`): replace the `Integer` rule of `YEAR` with a fixed exact `DECIMAL(4,0)`, and that of `MONTH`, `DAY`, `HOUR`, `MINUTE`, and `WEEK` with `DECIMAL(2,0)`. `EXTRACT` reads the same rule and needs no change unless 1.1 finds a difference.
- [ ] 1.3 E2E: add rows to `SPLIT_AGGREGATE_CASES` in `crates/lakehouse-e2e/tests/e2e_local/capability.rs`, each with `PathCheck::Split`: `SELECT COUNT(*) FROM {T} GROUP BY YEAR(C_DATE)`, `… GROUP BY MONTH(C_TS)`, `… GROUP BY WEEK(C_DATE)`, `… GROUP BY HOUR(C_TS)`, `… GROUP BY EXTRACT(MINUTE FROM C_TS)`, and `SELECT MAX(DAY(C_DATE)) + 1 FROM {T}`. Add the new scenario's line to `e2e_split_aggregate_columns_match_native_oracle`.
- [ ] 1.4 `docs/capabilities.md` § Aggregation: a date-part key that is not selected, such as `GROUP BY YEAR(d)`, splits, and another computed key, such as `GROUP BY qty * 2`, still needs a CAST.
- [ ] 1.5 Regression guard (#461), `datafusion_truncates_an_overprecise_timestamp_literal` in `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`: a `MemTable` with one `Timestamp(Millisecond, None)` row at `2025-06-01 00:00:00.123`, queried through `SessionContext::sql` with `WHERE ts = TIMESTAMP '2025-06-01 00:00:00.123456'`, returns the row (apache/datafusion#22142). Its comment states that a failure means the DataFusion release carries apache/datafusion#22906, and that `floor_timestamp_literals` and this test go then.

### 2. WHERE split at AND (`declined-filter-self-apply`, `pushdown-planning`, and `aggregate-extensions` deltas)

- [ ] 2.1 `ScanTranslator::split_where` (`support.rs`): render the whole filter first, as today, so a filter that renders keeps its byte-identical SQL. When it declines, render each top-level conjunct, and return a new `ScanWhere` variant that holds the conjunction of the rendered SQL (`conjoin_scan_filters`) and the conjunction of the declined raw conjuncts as an owned tree. When no conjunct renders, return today's `ScanWhere::Declined` with the raw tree, so a fully declined filter keeps its SQL. Move `flatten_conjuncts` and `conjoin` from `joins/rendering.rs` into `support.rs`, and add one per-conjunct screen on `ScanTranslator` that `split_where` and `screen_leg_filter` both call, so the per-conjunct rule exists once and `support.rs` imports nothing from `joins::rendering`. Update the doc comments of `split_where` and `ScanWhere`.
- [ ] 2.2 `build_dispatch_sql` (`mod.rs`): route the new variant to `qualified_single_table_fallback_pushdown`, which already gives the rendered half to the fan-out spec and the declined half to the wrapper's `WHERE`. For that variant only, compute the wrapper's emission with `single_table_emission` over the request whose `filter` is the declined half, so a column that only a rendered conjunct names is not emitted. The `fallback()` closure for a filter that renders keeps today's emission and SQL. Remove the exclusivity statement ("so `fan_out.filter` must then be `None`") from the doc comment of `build_qualified_single_table_fallback_sql` (`joins/sql_builders.rs`).
- [ ] 2.3 `render_broadcast_join_emitting` (`joins/sql_builders.rs`): return `Ok(None)` for the new variant, so the join takes the N-scan fallback.
- [ ] 2.4 Golden `group_by_fallback_rendering_filter.sql` in `dispatch_golden_tests.rs`, generated before 2.1, for a grouped request that takes the wrapper fallback with a filter that renders. No golden changes after 2.1 to 2.3.
- [ ] 2.5 E2E in `capability.rs`, next to the `e2e_declined_filter_*` tests: `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan`, a case table over `WHERE C_DECIMAL_A LIKE '1%' AND ID > 1` as a row scan (`SELECT ID … ORDER BY ID`, ids 5 and 7), `SELECT C_VARCHAR …`, `COUNT(*)`, `GROUP BY C_BOOL`, `COUNT(DISTINCT ID)`, `ORDER BY ID LIMIT 1`, and `SELECT RANDOM() …`, plus `WHERE (C_DECIMAL_A LIKE '1%' OR ID = 2) AND ID > 1`. Each case matches native Exasol over a native copy, and its `EXPLAIN VIRTUAL` shows the `LHS_T0` wrapper whose `WHERE` holds the `LIKE` and a scan-spec `"filter":"` that names `ID` and holds no `LIKE`. For `SELECT C_VARCHAR …` the fan-out's `EMITS` names no `"ID"`, and for `SELECT RANDOM() …`, whose `selectList` is empty, the row count matches the native copy. Add the `/// Scenario:` line of "A declined single-table WHERE filter is applied in the adapter's own outer WHERE" to `e2e_declined_filter_second_arity_returns_filtered_rows`, add the line of "A declined filter does not widen the wrapper's projection" to `e2e_declined_filter_select_star_returns_full_row_shape` and to this test, and remove the line of "Filter predicate is pushed into the scan spec" from the unit test `filter_in_common_arg` (`support_tests.rs`), because the E2E test becomes that scenario's proof.
- [ ] 2.6 `docs/capabilities.md`: in § Filtering, replace "the whole filter runs in Exasol SQL and the scan returns the referenced columns unfiltered" with: a condition DataFusion cannot render runs in Exasol SQL on its own, and the scan still filters on the other conditions. In § Text and session settings, replace "On a single table it takes the whole WHERE with it, so the scan filters no rows" the same way.

### 3. DATE text literals (`scan-text-conversion` delta, architecture delta)

- [ ] 3.1 On the local Exasol, in sessions with `NLS_DATE_FORMAT = 'DD.MM.YYYY'` and `'YYYY-DD-MM'`, confirm that `CAST(<text> AS DATE)` returns the value that native `d = <text>`, `BETWEEN`, `IN`, and simple `CASE` compare with, that `TO_CHAR(<date>, 'YYYY-MM-DD')` writes `0001-01-01`, `1500-03-01`, and `9999-12-31` in that form, and that a converted literal before 1582-10-15 compares as the same typed DATE literal does. Record when native Exasol fails for an unparseable literal with `=` and with `IN`: rows reach the comparison, another conjunct removes every row, and an empty table. A form whose CAST differs keeps declining, and the spec delta lists it.
- [ ] 3.2 Typed pass, `compare_without_session_parse` (`scan_typing.rs`): when every operand that is not text is a DATE and every text operand is a `literal_string`, replace each such literal in the scan tree with a new synthesized node beside `INTEGER_TEXT_NODE`, instead of declining. Every other combination keeps today's result.
- [ ] 3.3 New module `crates/vs-expression/src/converted_literal.rs`, with its sibling `converted_literal_tests.rs`, owns the converted-literal marker: a start character, the original literal hex-encoded, and an end character, with start and end characters that `serde_json` does not escape. The DataFusion-dialect arm for synthesized nodes in `lib.rs` writes the marker for the 3.2 node.
- [ ] 3.4 In the same module, one public function turns a serialized common spec into the Exasol SQL of the `common` argument, with no state from the render, so a fragment rendered and discarded or a marker in another leg's spec does not affect it. Without a marker character it returns one Exasol string literal. Otherwise it returns a concatenation of string-literal parts and, per marker, an Exasol expression that converts the decoded literal, quoted as an Exasol string literal, with `CAST(… AS DATE)` and writes `DATE '<YYYY-MM-DD>'`, or the four characters `NULL` when the conversion returns NULL. A marker character outside a well-formed marker returns an error that names the cause and quotes no part of the spec, which carries credentials. [expert]
- [ ] 3.5 `build_fan_out_inner` (`support.rs`) builds the `common` argument with the function of 3.4 instead of `sql_string_literal`. The pushdown context (`context.rs`) records once per request whether the request JSON holds a marker character. With one, no translator converts a literal and `build_fan_out_inner` writes today's plain string literal, so request text is never parsed as a marker. The flag reaches `build_fan_out_inner` through the `Shards` the context builds, so its signature does not change. Confirm that every path that serializes a common spec goes through `build_fan_out_inner` (row scan, top-N, both aggregate paths, `COUNT(DISTINCT)`, the wrapper fan-out, the broadcast join, and each N-scan leg's fan-out), and that no other site writes a rendered scan fragment into SQL. [expert]
- [ ] 3.6 Unit tests in `converted_literal_tests.rs`: a spec without a marker character stays one string literal; one marker, several markers, and the same marker twice each concatenate; a literal holding `'`, `' || …`, `"`, `\`, or a control character stays inside one Exasol string literal; the NULL branch yields the text `NULL`; a stray or malformed marker returns an error whose text holds no substring of the spec. In `pushdown_tests.rs`, `marker_character_in_the_request_disables_the_conversion`: a request with a marker character in another literal emits today's SQL.
- [ ] 3.7 E2E in `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs`: `text_literal_compared_with_a_date_keeps_the_scan_filter`, with its own case tables under `DMY` and `YDM` through `assert_cases_match_native`. `Has(SCAN_FILTER)`: `C_DATE = '…'`, `<>`, `<=`, `BETWEEN`, `IN`, `ADD_DAYS(C_DATE, 1) = '…'`, and `C_DATE = '…' AND C_DECIMAL_A LIKE '1%'`. `Has(SCAN_ITEM)`: a select-list `CASE C_DATE WHEN '…' THEN 1 ELSE 0 END`. `Split`: a grouped `COUNT(*)` under `C_DATE = '…'`, `GROUP BY CASE C_DATE WHEN '…' THEN 1 ELSE 0 END`, and `SUM(CASE WHEN C_DATE = '…' THEN 1 ELSE 0 END)`. `Has(BROADCAST_JOIN)`: a self-join of `{T}` with `a.C_DATE = '…'`. `NScan(2)`: the join of the existing `NScan(2)` case with `a.C_DATE = '…'` in its WHERE. Error cases: the unparseable literals that 3.1 measures, with `=` and `IN`. Move the `ERROR_CASES` row `C_DATE = '2024-01-01'` under `DMY` into this test's error cases with `Has(SCAN_FILTER)`. Add a `C_TS > '01.01.2024 00:00:00.000000'` case under `TS_DMY` to `CASES` with `Lacks(SCAN_FILTER)`, and the line of "Text the session format converts runs in Exasol" to `text_conversions_match_native_under_session_settings`.
- [ ] 3.8 Docs: in `docs/capabilities.md` § Text and session settings, a text literal compared with a DATE stays in the scan because Exasol converts it first, and the `d = '2024-01-02'` row leaves the "Runs in Exasol SQL" column. The TIMESTAMP row stays. In `docs/semantic-differences.md`, add a section on how an unparseable DATE literal fails, from the measurements of 3.1.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: date-part types, #461 guard | 1.1-1.5 | none | spec delta `pushdown-aggregates/pushdown-planning-split-aggregate-types`; `crates/vs-expression/src/scalar_functions.rs`, `scan_typing.rs` (`ScalarFunction::result_type`, `floor_timestamp_literals`); `crates/lakehouse-engine/src/scan/raw_scan_tests.rs`; `crates/lakehouse-e2e/tests/e2e_local/capability.rs` (`SPLIT_AGGREGATE_CASES`); `docs/capabilities.md` § Aggregation |
| B: WHERE split | 2.1-2.6 | A (shares `capability.rs`, `docs/capabilities.md`) | spec deltas `pushdown/pushdown-declined-filter-self-apply`, `pushdown/pushdown-planning`, `pushdown-aggregates/pushdown-planning-aggregate-extensions`; `crates/lakehouse-engine/src/adapter/pushdown/support.rs` (`split_where`, `ScanWhere`), `mod.rs` (`build_dispatch_sql`), `joins/rendering.rs`, `joins/sql_builders.rs`, `dispatch_golden_tests.rs`, `testdata/dispatch_golden/`, `support_tests.rs`; `capability.rs` (`e2e_declined_filter_*`); `docs/capabilities.md` § Filtering, § Text and session settings |
| C: DATE text literals | 3.1-3.8 | A (shares `scan_typing.rs`), B (shares `support.rs`, `docs/capabilities.md` § Text and session settings) | spec delta `pushdown-types/pushdown-planning-scan-text-conversion`; `architecture.md` delta; `crates/vs-expression/src/scan_typing.rs` (`compare_without_session_parse`), `lib.rs`, `converted_literal.rs`; `crates/lakehouse-engine/src/adapter/pushdown/support.rs` (`build_fan_out_inner`), `context.rs`, `pushdown_tests.rs`; `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs`; `docs/capabilities.md` § Text and session settings, `docs/semantic-differences.md` |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test case | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs`, `ERROR_CASES` row `C_DATE = '2024-01-01'` with `Lacks(SCAN_FILTER)` | Moves to the new test with `Has(SCAN_FILTER)` |
| Scenario line | `filter_in_common_arg` in `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | The E2E test of 2.5 proves the scenario |
| Function body | the per-conjunct loop of `screen_leg_filter` (`joins/rendering.rs`) | Replaced by the shared per-conjunct screen of 2.1 |
| Doc comment | `split_where`, `ScanWhere`, `build_qualified_single_table_fallback_sql` | The scan filter and the declined filter are no longer exclusive |
| Doc text | `docs/capabilities.md` "the whole filter runs in Exasol SQL", "On a single table it takes the whole WHERE with it", and the `d = '2024-01-02'` row | Each describes the removed behavior |

The timestamp-literal floor stays until a DataFusion release carries apache/datafusion#22906.

## Out of scope

- Several recorded scenarios of `pushdown/pushdown-declined-filter-self-apply` have no test that carries their `/// Scenario:` line. This plan adds lines only for the scenarios it changes.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Text the session format converts runs in Exasol | E2E | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `text_conversions_match_native_under_session_settings` |
| Exasol converts a text literal compared with a DATE before the scan | E2E | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `text_literal_compared_with_a_date_keeps_the_scan_filter` |
| A declined single-table WHERE filter is applied in the adapter's own outer WHERE | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_filter_second_arity_returns_filtered_rows` |
| A declined filter does not widen the wrapper's projection | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_filter_select_star_returns_full_row_shape`, and the `SELECT RANDOM()` case of `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A single-table WHERE filter splits at AND between the scan and the wrapper | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| Filter predicate is pushed into the scan spec | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A declined WHERE filter routes the single-table request to the qualified wrapper | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A declined WHERE filter under an aggregate request is applied ahead of the aggregate | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` |
| A date-part function takes its fixed native type | E2E | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_split_aggregate_columns_match_native_oracle` |

### Manual Testing

Run `make e2e-up test-e2e` first, which installs the `.so` and creates `MY_LAKEHOUSE.TYPED_DISTINCT_PROBE`. Each command runs `exapump sql` with `-d "exasol://sys:$EXASOL_SYS_PASSWORD@$EXASOL_HOST:$LH_EXASOL_PORT?validateservercertificate=0"`, the SQL on stdin.

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Pushdown Planning, Scan Text Conversion | `ALTER SESSION SET NLS_DATE_FORMAT = 'DD.MM.YYYY'; EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DATE = '01.01.2024';` | The pushdown SQL holds `"filter":"` and `CAST('01.01.2024' AS DATE)`, and no `LHS_T0` wrapper |
| Pushdown Planning, Scan Text Conversion | The same session, `SELECT COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DATE = '01.01.2024';` | The count native Exasol returns for the same filter over a native copy |
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
