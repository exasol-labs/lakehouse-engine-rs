# Feature: Partial-Aggregate Column Contract

`AggKind::partial_columns()` in `crates/lakehouse-engine/src/scan/spec.rs` is the one owner of how many partial columns each `AggKind` contributes, in what order, under what `PARTIAL_*` name, and what an empty shard puts in each. No generated SQL, EMITS clause, or emitted partial row changes.

## Background

* Five sites encode the contract: `partial_select_items`, `emit_null_partial_row`, and `partial_row_from_batch` in `scan/partial_agg.rs`, and `partial_emits_items` and `merge_select_items` in `adapter/pushdown/grouped_agg.rs`. A count disagreement is silent. `partial_row_from_batch` advances its own index and `emit_null_partial_row` builds a `Vec<Value>` by length alone, so a wrong count shifts every later aggregate's value into the wrong `EMITS` slot.
* Count, CountCol, Sum, Min, and Max contribute 1 column. Avg contributes 2 (sum, then count). The STDDEV/VARIANCE family contributes 3 (cnt, mean, m2).
* `AggKind` lives in `scan/spec.rs`, which both the scan and the adapter already import. `scan/spec.rs` imports only `serde` and must not depend on `exasol_udf_sdk`, so the descriptor expresses an empty shard's value as a boolean counter property and the emit site maps it to `Value::Int64(0)` or `Value::Null`.
* `empty_agg_literal` and `validate_agg_col_types` match on `AggKind` for other questions and are not part of the contract.
* `dispatch_golden_tests.rs` asserts full-string equality against committed fixtures under `testdata/dispatch_golden/`. Its ten fixtures cover no AVG or statistical aggregate, so baselines for those arities must exist before any contract site changes.

## Scenarios

### Scenario: One descriptor owns every aggregate's partial column set

* *GIVEN* the five sites that independently encode which partial columns an `AggKind` contributes — `partial_select_items`, `emit_null_partial_row`, and `partial_row_from_batch` in `scan/partial_agg.rs`, and `partial_emits_items` and `merge_select_items` in `adapter/pushdown/grouped_agg.rs`
* *WHEN* any of the five needs to know how many partial columns an aggregate produces, in what order, or under what `PARTIAL_*` name
* *THEN* exactly ONE item SHALL own that answer: `AggKind::partial_columns(&self) -> &'static [PartialAggColumn]` declared in `crates/lakehouse-engine/src/scan/spec.rs` alongside `AggKind`, returning the ordered partial columns for that kind
* *AND* `PartialAggColumn` SHALL enumerate the TEN distinct partial columns the contract admits — `CountStar`, `CountArg`, `Sum`, `Min`, `Max`, `AvgSum`, `AvgCnt`, `StatCnt`, `StatMean`, `StatM2` — and `AggKind::partial_columns` SHALL map `Count` to `[CountStar]`, `CountCol` to `[CountArg]`, `Sum`/`Min`/`Max` each to their single matching column, `Avg` to `[AvgSum, AvgCnt]`, and each of `VarPop`, `VarSamp`, `Variance`, `StddevPop`, `StddevSamp`, `Stddev` to `[StatCnt, StatMean, StatM2]`
* *AND* `CountStar` and `CountArg` SHALL be two variants rather than one, because they share a column name and an `EMITS` type but render DIFFERENT DataFusion SQL — `COUNT(*)` versus `COUNT(<arg>)` — so collapsing them would force the scan's renderer to re-consult the `AggKind` the descriptor exists to abstract
* *AND* `AggKind::partial_columns` SHALL match EXHAUSTIVELY over `AggKind` with NO wildcard arm, and each renderer SHALL match exhaustively over `PartialAggColumn` with no wildcard arm, so a new `AggKind` variant is a compile error at the descriptor and a new `PartialAggColumn` variant is a compile error at every renderer — the contract is extended by adding a case, never by editing a dispatch that silently defaults
* *AND* NO `partial_column_count()` method SHALL be added, even though issue #179 suggests one: a function whose whole body forwards to `partial_columns().len()` is the shallow pass-through this repository's recorded rule rejects, and `partial_row_from_batch` reads `.len()` directly
* *AND* the descriptor MUST NOT depend on `exasol_udf_sdk`, so it SHALL express the empty-shard identity as `PartialAggColumn::is_counter(&self) -> bool` — `true` for the four counter columns `CountStar`, `CountArg`, `AvgCnt`, and `StatCnt`, and `false` for the six value columns `Sum`, `Min`, `Max`, `AvgSum`, `StatMean`, and `StatM2` — and the emit site SHALL map `true` to `Value::Int64(0)` and `false` to `Value::Null`, keeping `scan/spec.rs` a serde-only module

### Scenario: The partial column name has one owner across the scan and the adapter

* *GIVEN* the `PARTIAL_<role>_<ordinal>` column name, hand-formatted at three sites that must agree byte-for-byte — the scan's `AS "PARTIAL_…"` aliases, the adapter's `"PARTIAL_…" <type>` `EMITS` items, and the adapter's `SUM("PARTIAL_…")` merge expressions
* *WHEN* any of the three renders a partial column name
* *THEN* the name SHALL be produced by ONE function taking the `PartialAggColumn` and the aggregate's ordinal and returning the UNQUOTED name, and each of the three sites SHALL apply its own quoting as it does today — the scan and the merge wrapping it in double quotes, the `EMITS` item wrapping it in double quotes ahead of its type
* *AND* the per-column name fragment SHALL be `count` for both `CountStar` and `CountArg`, `sum`, `min`, `max`, `avg_sum`, `avg_cnt`, `stat_cnt`, `stat_mean`, and `stat_m2` respectively, reproducing every name the pre-refactor code emits
* *AND* no site SHALL retain a `format!` literal containing the text `PARTIAL_`, so a renamed column can never be renamed at one site only

### Scenario: The statistical-aggregate partial argument routes through the shared argument renderer

* *GIVEN* the stat-family branch of `partial_select_items` (`partial_agg.rs:395-403`), which bypasses `agg_arg_sql` and instead reads `plan.column.as_deref().unwrap_or("")` — falling back to an EMPTY identifier that DataFusion rejects with an opaque `Schema error: No field named .` (captured live, plan `refactor-pushdown-agg-dedup` task 1.2) rather than to `agg_arg_sql`'s self-describing `__MISSING_AGG_ARGUMENT__` sentinel
* *WHEN* the scan renders the count, mean, and m2 sufficient statistics for a statistical aggregate
* *THEN* all three SHALL take their argument from `agg_arg_sql(plan)`, the same helper every other kind uses, and the branch SHALL retain NO `plan.column` read and NO local `quote_ident` call of its own
* *AND* the rendered SQL for a bare-column statistical aggregate MUST be byte-identical to the pre-refactor output, because `agg_arg_sql` returns `quote_ident(column)` for a plan with no `arg_expr` — which is exactly what the branch computes today
* *AND* routing through `agg_arg_sql` SHALL NOT be treated as making expression-argument statistical aggregates work: the plan shape it improves the error message for is declined upstream instead, per `pushdown-aggregates/pushdown-planning-aggregate-extensions`, so this clause changes only which identifier a plan that can no longer reach the scan would have produced

### Scenario: Generated SQL, EMITS clauses, and partial rows are byte-identical across the refactor

* *GIVEN* pre-refactor golden baselines captured through the production seams for every arity the contract admits — the single-column kinds, `AVG`'s two-column pair, and the statistical family's three-column triple — in both the single-group and the grouped shape
* *WHEN* the same requests and specs are rendered against the refactored code
* *THEN* the scan's partial-aggregate DataFusion SQL, the grouped partial-aggregate DataFusion SQL, the `EMITS` clause, and the outer merge SELECT MUST each be byte-identical to the captured baseline, asserted by full-string equality and NEVER by `.contains(...)`
* *AND* the emitted partial row for a non-empty shard and the fallback row for an empty single-group shard MUST carry the same values, in the same order, with the same lengths, as the pre-refactor code for every `AggKind`
* *AND* the baselines for the AVG and statistical arities SHALL be captured and committed BEFORE any contract site is edited, because `dispatch_golden_tests.rs`'s ten existing fixtures cover neither arity and would pass a change that shifted a multi-column aggregate's values
* *AND* every existing unit and integration test covering the partial-aggregate paths MUST pass with NO change to any test assertion or expected value — in particular `partial_agg.rs`'s `partial_agg_sql_stat_emits_cnt_mean_m2`, `stat_aggregate_null_fallback_row_has_three_values`, `stat_aggregate_index_follows_plan_order`, `partial_agg_sql_avg_emits_sum_count_pair`, `partial_agg_sql_mixed_column_order_and_indices`, and `partial_sql_uses_rendered_expression_argument`, and `grouped_agg.rs`'s `stat_aggregate_emits_three_partial_columns`
* *AND* those tests SHALL keep their LITERAL expected column names and arities and MUST NOT be rewritten to read `partial_columns()` or the shared name function, because a test that derives its expectation from the code under test asserts the descriptor against itself and would pass any consistent renaming

### Scenario: One in-crate test pins the scan-to-adapter column alignment per aggregate kind

* *GIVEN* that the scan's SELECT list and the adapter's `EMITS` clause are produced in two different modules and validated against each other only at query time inside Exasol, where a mismatch surfaces as a wrong value or an `EMITS` arity error rather than as a test failure
* *WHEN* the test suite runs
* *THEN* ONE test SHALL assert, for EVERY `AggKind` variant, that the scan's partial SELECT list and the adapter's `EMITS` item list contain the same number of columns in the same `PARTIAL_*` order
* *AND* that test SHALL reach both sides through the visibility that already exists — `crate::scan::build_partial_agg_sql` is re-exported under `#[cfg(test)]` in `scan/mod.rs` and `partial_emits_items` is `pub(super)` in `adapter::pushdown::grouped_agg` — so it SHALL be declared inside the crate, and NO production item's visibility SHALL widen and NO `#[cfg(test)]`-only export SHALL be added to enable it
* *AND* the test SHALL enumerate the `AggKind` variants it covers as an explicit literal list, and a variant added later that the list omits SHALL be caught by the compile error `partial_columns` raises rather than by this test, so the test asserts alignment and never doubles as an exhaustiveness check it cannot enforce
