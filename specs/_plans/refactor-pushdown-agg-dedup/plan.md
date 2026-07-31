# Plan: refactor-pushdown-agg-dedup

## Summary

Give the aggregate-pushdown partial-column contract one owner — `AggKind::partial_columns()` in `scan/spec.rs` — and collapse three hand-repeated SQL constructions in `grouped_agg.rs` and `single_group_agg.rs` onto one implementation each ([#179](https://github.com/exasol-labs/lakehouse-engine-rs/issues/179)). Every generated SQL string, `EMITS` clause, and emitted partial row stays byte-identical, with one deliberate exception: `STDDEV(<expression>)` stops erroring and declines the pushdown instead.

## Context

The partial/merge decomposition splits one user aggregate into per-shard partial columns that the outer wrapper re-aggregates. How many columns each `AggKind` contributes is the load-bearing number: the scan's DataFusion `SELECT` list produces them, the fan-out's `EMITS` clause declares them, the outer merge consumes them, and the emit path walks them positionally. Five functions in two modules encode that number independently, and nothing enforces agreement.

A disagreement is silent. `partial_row_from_batch` advances a column index it maintains itself; `emit_null_partial_row` builds a `Vec<Value>` whose only contract is its length. A site that advances by 2 where the `SELECT` list produced 3 shifts every later aggregate's value one slot left for the rest of the row — the query returns numbers, and they are wrong. That is the defect class this plan removes, and it is the reason the column-contract work anchors the task order.

The other three duplications are cosmetic by comparison but not risk-free. The König–Huygens statistical fragments are repeated sixteen times across four arms, and every test guarding them is a `.contains(...)` probe: a dropped parenthesis or a swapped denominator would leave all six green while changing every returned variance. The repository already owns the right instrument — `dispatch_golden.rs` asserts full-string equality against committed fixtures — but none of its ten fixtures covers an `AVG` or a statistical aggregate, so the two arities most at risk are exactly the ones currently unwatched. Capturing those baselines first is a prerequisite, not a nicety.

One finding is not cleanup, and task 1.2's live capture settled it. `STDDEV(<expression>)` produces an `AggregatePlan` with neither a `column` nor an `arg_expr`, passes `validate_agg_col_types` on a default type, gets three `EMITS` columns, and then renders `COUNT("")` in the scan, which DataFusion rejects. Exasol pushes that shape. Task 1.2 confirmed it against the Docker Exasol container on all four reachable paths: `EXPLAIN VIRTUAL` returned status `ok` and rendered the wrapper SQL for each, and each then failed at execution with `sqlCode 22002` and `Schema error: No field named .`. The whole query errors today, on both grouped paths as well as the ungrouped one, where a decline returns the correct answer natively. One inspection detail did not survive the capture: the rejection does not surface as `column "" not found`. DataFusion reports the empty identifier as an empty field name, so the measured error text names no column at all. The rendered-SQL claim stands; the predicted error string does not. The issue flags this as "fold in if cheap"; this plan folds it in as an explicit decline rather than a better error message.

- **Goals** — one owner for the per-`AggKind` partial column set, its ordering, and its `PARTIAL_*` names; one owner for the statistical merge fragments, the declared-type CAST rule, and the function-name-to-`AggKind` mapping; byte-identical generated SQL, `EMITS` clauses, and partial rows for every shape that works today; a byte-exact golden gate over the `AVG` and statistical arities that has none.
- **Non-Goals** — changing any arity, column name, merge formula, or `EMITS` type; extending the statistical family to expression arguments; touching `empty_agg_literal` or `validate_agg_col_types`' `needs_numeric` match (per-`AggKind`, but neither is per partial column); routing the unconditional `CAST(NULL AS <ty>)` arms of `empty_grouped_sql` through the shared cast helper; unifying the grouped emit path, which already consults no `AggKind`.

## Design

### One descriptor, two renderers, no shared arity arithmetic

```
        crates/lakehouse-engine/src/scan/spec.rs        [serde-only, no SDK dependency]
        ┌──────────────────────────────────────────────────────────────┐
        │ AggKind::partial_columns() -> &'static [PartialAggColumn]    │
        │   Count      -> [CountStar]                                  │
        │   CountCol   -> [CountArg]                                   │
        │   Sum/Min/Max-> [Sum] / [Min] / [Max]                        │
        │   Avg        -> [AvgSum, AvgCnt]                             │
        │   Var*/Stddev*-> [StatCnt, StatSum, StatSumSq]               │
        │                                                              │
        │ PartialAggColumn::is_counter()  -> 0-vs-NULL on empty shard  │
        │ partial_column_name(col, i)     -> "PARTIAL_<role>_<i>"      │
        └───────────────┬──────────────────────────────┬───────────────┘
                        │ (exhaustive match)           │ (exhaustive match)
         scan/partial_agg.rs                  adapter/pushdown/grouped_agg.rs
         ├─ partial_select_items   DataFusion ├─ partial_emits_items    Exasol
         │    expression per column           │    type per column
         ├─ emit_null_partial_row             └─ merge_select_items     names only
         │    is_counter -> Int64(0)/Null
         └─ partial_row_from_batch
              advances by .len()
```

The descriptor owns which columns exist, in what order, under what name, and what an empty shard puts in each. Each side owns only its own rendering: the scan chooses the DataFusion aggregate expression, the adapter chooses the Exasol `EMITS` type. Neither re-derives the count.

Dependency direction holds: `grouped_agg.rs:5` and `partial_agg.rs:17` already import `scan::spec`, and `scan::spec` imports neither. The descriptor adds no edge and no cycle. It stays serde-only — the empty-shard identity is a boolean, not a `Value`, so the wire-format module never learns about `exasol_udf_sdk`.

Against the Quick Diagnostic: the descriptor's responsibility fits one sentence; calling it is strictly cheaper than the five hand-written matches it replaces; a change to the column set is a compile error at both renderers, which is the intended coupling rather than leakage; and the contract is extended by adding a case, never by editing a wildcard that silently defaults.

### Why the statistical fragments compose byte-identically

The four arms are exactly `numer / pop_denom`, `numer / samp_denom`, `stddev_of(numer / pop_denom)`, and `stddev_of(numer / samp_denom)`, where `numer` carries its own outer parentheses and `stddev_of(v) = CASE WHEN (v) IS NULL THEN NULL ELSE SQRT(GREATEST(0.0, v)) END`. Substituting reproduces the current text character for character, including the double parenthesis after `CASE WHEN` — one from the wrapper, one from the numerator. Getting that nesting wrong is the failure mode the golden fixtures exist to catch.

### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Single owner per decision | `AggKind::partial_columns` | Replaces five sites agreeing only by convention with one they all read |
| Extend by adding a case | Exhaustive matches, no wildcard | A new `AggKind` or `PartialAggColumn` is a compile error, not a silent default |
| Shared primitive in `support` | the declared-type CAST helper | Its six consumers live in two sibling submodules, so neither can own it |
| Data table over repeated arms | `parse_agg_item`'s two name tables | Ten arms differing only in one field become two `[(&str, AggKind)]` lists |
| Characterize before refactoring | pre-refactor golden fixtures | The existing `.contains(...)` probes cannot distinguish a broken extraction |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `AggKind::partial_columns() -> &'static [PartialAggColumn]` | A bare `partial_column_count() -> usize` as the issue suggests | A count alone drives only one of the five sites; the other four need per-column identity for the name, the `EMITS` type, the DataFusion expression, and the 0-vs-NULL fallback |
| No `partial_column_count()` wrapper | Add it for readability | A body forwarding to `partial_columns().len()` is the shallow pass-through this repository's recorded rule rejects |
| Descriptor lives in `scan/spec.rs` | A new module, or `adapter/pushdown/support.rs` | Both consumers already depend on `scan::spec`; anywhere else adds an edge, and `support` is not reachable from the scan |
| `CountStar` and `CountArg` as two variants | One `Count` variant | They share a name and a type but render different SQL, so one variant forces the scan to re-consult the `AggKind` the descriptor abstracts |
| CAST helper takes `Option<&str>` | Keep the `&str` signature and add an `Option` wrapper | Five of six sites read from a `.get(i)` and already treat absence as no-cast; the `Option` is the general case and the `&str` form is the special one |
| Exclude `empty_grouped_sql`'s `GroupKey`/`Aggregate` arms | Fold all eight cast sites into the helper | Those arms cast unconditionally on purpose: a bare `NULL` in `SELECT … FROM DUAL WHERE 1=0` carries no `VARCHAR` type and would fail Exasol's positional `selectListDataTypes` check |
| `STDDEV(<expr>)` declines the pushdown | Route the scan's stat branch through `agg_arg_sql` only; or leave it and file a follow-up | Both alternatives keep a query that errors. Declining returns the correct rows through the qualified wrapper, and the issue explicitly sanctions "make the limit explicit" |
| Two separate name tables in `parse_agg_item` | One merged table | Each table's members share an argument-resolution rule the other's do not — expression-capable versus bare-column-only |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-partial-agg-column-contract | NEW | `datafusion-scan/scan-partial-agg-column-contract/spec.md` |
| vs-adapter/pushdown-agg-sql-consolidation | NEW | `vs-adapter/pushdown-agg-sql-consolidation/spec.md` |
| vs-adapter/pushdown-planning-aggregate-extensions | CHANGED | `vs-adapter/pushdown-planning-aggregate-extensions/spec.md` |

The two new features follow the precedent of `vs-adapter/pushdown-col-types-consolidation`: a dedup refactor gets its own feature recording who owns each decision, while the behavior it preserves stays owned by the features that already record it — `datafusion-scan/scan-execution-partial-agg`, `datafusion-scan/scan-execution-grouped-agg`, `vs-adapter/pushdown-planning-single-group-agg`, and `vs-adapter/pushdown-planning-grouped-agg`. None of those four needs a delta: no arity, name, formula, or type changes, and their scenarios stay true word for word.

`vs-adapter/pushdown-planning-grouped-agg-scalar-over-aggregate` is a fifth recorded feature that task 1.7 reaches, and it is assessed here rather than deltaed. Its four recorded scenarios stay true word for word: each names a decomposable shape (`ROUND(100.0 * SUM(CASE …) / COUNT(*), 2)`, an inner aggregate shared across select items, and scalar-over-aggregate items interleaved with keys and plain aggregates), and none involves a statistical aggregate over an expression argument. What task 1.7 does touch is that feature's § Background bullet 4, whose undecomposable-shape list ("`DISTINCT`, a SUM/stat over a non-numeric type, an untranslatable argument, or a non-aggregate/non-group-key node") gains a fifth reason. That reason is recorded in the feature that owns the statistical family's argument boundary: the `aggregate-extensions` delta's new scenario names `classify_scalar_over_aggregate` (`grouped_agg.rs:399`) as one of the five `parse_agg_item` callers that decline, so the new decline is recorded and cross-referenced rather than left silent. A Background-only delta on the scalar-over-aggregate feature was written and then withdrawn: `speq plan validate` rejects a delta spec with no scenario (`ERROR: No scenarios defined`), and inventing a scenario change for a feature whose scenarios do not change would be worse than recording the reason in the owning feature.

The `aggregate-extensions` delta adds one scenario and seven `## Background` bullets. Its Feature description and its first two Background bullets are quoted verbatim and unedited, to satisfy the spec-structure validator; see § Record Notes.

## Impact

Exactly one user-visible change, measured rather than inferred, and the same on every path measured. A query selecting a statistical aggregate over an expression (`STDDEV(score + id)`, `VARIANCE(score * 2)`) fails today and returns the correct value after the change.

Task 1.2 captured the before state against the Docker Exasol container on all four reachable paths. Exasol pushes the shape in every one: `EXPLAIN VIRTUAL` returns status `ok` and renders the wrapper SQL. Execution then fails with `sqlCode 22002` and `Schema error: No field named .`. The two ungrouped paths prefix that error with `partial aggregate SQL error:`, the two grouped paths with `grouped partial aggregate SQL error:`.

Code inspection explains the failure, one finding per line:

- the `AggregatePlan` carries neither a `column` nor an `arg_expr`;
- `validate_agg_col_types` admits it on the `DOUBLE PRECISION` default;
- three `EMITS` columns are sized from that default;
- the scan renders `COUNT("")`, which DataFusion rejects.

One inspection prediction did not survive the capture. The rejection surfaces as an empty field name (`No field named .`), not as `column "" not found`. The rendered SQL is as inspected; the error text is not.

After the change the adapter declines the aggregate pushdown for that item. Exasol computes the statistic natively over the returned rows. The query answers correctly and more slowly instead of failing.

Nothing else changes for a user or an operator. Every aggregate shape that works today produces a byte-identical scan spec, `EMITS` clause, and wrapper SQL, so plans captured by `EXPLAIN VIRTUAL` are unchanged and no result value moves. No wire format, no adapter note, no DDL, no migration.

## Implementation Tasks

- [ ] 1.1 Capture the missing golden baselines BEFORE editing any production code. Add two `dispatch_golden` fixtures rendered through the existing production seam: `testdata/dispatch_golden/single_group_all_agg_kinds.sql` from a single-group request whose select list is `COUNT(*)`, `COUNT(id)`, `SUM(score)`, `MIN(ts)`, `MAX(ts)`, `AVG(score)`, `STDDEV(score)`, `STDDEV_POP(score)`, `VARIANCE(score)`, `VAR_POP(score)`; and `testdata/dispatch_golden/grouped_all_agg_kinds.sql` from the same select list with `GROUP BY region`. One request per fixture covers every arity (1, 2, and 3 columns) and every statistical kind at once, and the mixed arities exercise the plan-ordinal-versus-column-ordinal distinction that is the drift risk. Assert each with a full-string `assert_eq!` against the committed file in `dispatch_golden.rs`, never `.contains(...)`, matching that module's existing style and its recorded rule that a diff is a regression rather than an expected update. Commit the fixtures as captured; do NOT hand-write their expected content.

  Those two fixtures cover only two of the four artifacts the byte-identity scenario names. A `dispatch_golden` fixture holds the outer merge SELECT, the `EMITS` clause, and the serialized scan spec, because the scan's own DataFusion SELECT list is built inside the UDF at runtime by `build_partial_agg_sql`, not by `build_dispatch_sql` — verified against `testdata/dispatch_golden/grouped_aggregate.sql`. So capture a THIRD and FOURTH pre-refactor fixture through the scan seam, in the same commit and equally before any production edit: `crates/lakehouse-engine/src/scan/testdata/partial_agg_golden/partial_agg_all_agg_kinds.sql` rendered by `crate::scan::build_partial_agg_sql` over an `AggregatePlan` list covering all ten `AggKind` variants (the same select list as the two dispatch fixtures, in the same order), and `crates/lakehouse-engine/src/scan/testdata/partial_agg_golden/grouped_partial_agg_all_agg_kinds.sql` rendered by `build_grouped_partial_agg_sql` over that same plan list with one group key and no filter. Assert both in `scan/partial_agg.rs`'s existing `#[cfg(test)] mod tests` with a full-string `assert_eq!` against an `include_str!` of the committed file, as `partial_agg_sql_all_agg_kinds_matches_golden` and `grouped_partial_agg_sql_all_agg_kinds_matches_golden`. These two are the ONLY baseline for the scan-side halves of the contract, and they are what task 1.3 rewrites: every existing scan-side test is a `.contains(...)` probe (`partial_agg.rs:533`, `:785`, `:847`) and not one of them asserts item order or total item count, and the § Manual Testing `EXPLAIN VIRTUAL` diffs show adapter output rather than UDF-rendered SQL. Commit these as captured too; do NOT hand-write them.
- [x] 1.2 DONE (captured 2026-07-31). The expression-argument statistical-aggregate failure is reproduced against the local Docker Exasol container, per CLAUDE.md § Verification discipline. All FOUR reachable paths are confirmed BOTH pushed by Exasol AND broken today. This task changed no code: the capture ran through a temporary scratch test in `crates/lakehouse-engine/tests/e2e_capability_test.rs`, added, run, and reverted, so no production or test file carries it.

  Method, reproducible as written: send each query through the deployed `MY_LAKEHOUSE` virtual schema twice — first as `EXPLAIN VIRTUAL <sql>`, which establishes whether Exasol pushes the shape at all, then executed, which establishes the outcome. Captured verbatim:

  1. `SELECT STDDEV(score + id) FROM MY_LAKEHOUSE.EVENTS` — ungrouped, path (1), `detect_aggregates`. `EXPLAIN VIRTUAL`: status `ok`, Exasol PUSHES the shape (pushdownRequest echoed, generated wrapper SQL rendered). Executed: status `error`, `sqlCode 22002`, `VM error: F-UDF-CL-RUST-9001: UDF error: UDF run returned error code 1: partial aggregate SQL error: Schema error: No field named . Valid fields are "ID", "NAME", "SCORE", "EVENT_DATE", "EVENT_TS".`
  2. `SELECT VARIANCE(score * 2) FROM MY_LAKEHOUSE.EVENTS` — ungrouped, path (1). Identical outcome: `EXPLAIN VIRTUAL` status `ok` and pushed, executed fails with the same `sqlCode 22002` `partial aggregate SQL error: Schema error: No field named .`
  3. `SELECT MOD(id, 4), STDDEV(score + id) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4)` — grouped, path (2), `detect_group_by_aggregates`. `EXPLAIN VIRTUAL`: status `ok`, pushed, grouped partial-aggregate wrapper SQL rendered. Executed: `sqlCode 22002`, `grouped partial aggregate SQL error: Schema error: No field named .`
  4. `SELECT MOD(id, 4), SQRT(STDDEV(score + id)) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4)` — grouped scalar-over-aggregate, path (3), `classify_scalar_over_aggregate`. `EXPLAIN VIRTUAL`: status `ok`, pushed; the echoed pushdownRequest's selectList shows the `SQRT` wrapping a nested `STDDEV` `function_aggregate` over the `ADD(SCORE, ID)` argument, and the generated wrapper SQL is `SELECT CAST(…) AS DECIMAL…, CAST(SQRT(CASE WHEN (…) IS NULL THEN NULL ELSE SQRT(GREATEST(0.0, …)) END) AS DOUBLE PRECISION) FROM (SELECT "LHVS".LAKEHOUSE_SCAN(…) …)`. Executed: `sqlCode 22002`, `grouped partial aggregate SQL error: Schema error: No field named .`

  What the capture settles. The shape is reachable rather than theoretical: Exasol pushes it on every path, so task 1.7 is a bug fix and not a structural guard, on the two grouped paths as much as on the ungrouped one. One inspection prediction was corrected rather than confirmed: the DataFusion rejection surfaces as an empty field name (`No field named .`), not as the `column "" not found` text inspection guessed; the rendered `COUNT("")` claim itself stands. Query 1 is the shape task 1.7's `e2e_stddev_over_expression_falls_back_and_returns_correct_value` asserts after the fix, so `/speq:implement` reproduces this capture by re-running the four queries above in the same two-step form.

  The outcome is now recorded in plan.md § Context paragraph 4, plan.md § Impact, plan.md § Manual Testing, decision-log § [7]'s Rationale, and the `aggregate-extensions` delta's § Background bullets 2 and 3. No `TASK 1.2 CAPTURE PENDING` marker and no unmeasured branch remains in any of them.
- [ ] 1.3 Add `PartialAggColumn` and `AggKind::partial_columns()` to `crates/lakehouse-engine/src/scan/spec.rs` and rewire all five contract sites onto them. Write the failing tests first, all four of them, none of which can compile before the descriptor exists: `partial_columns_arity_per_agg_kind` in `scan/spec.rs`, asserting the literal arity of every `AggKind` variant (1 for `Count`/`CountCol`/`Sum`/`Min`/`Max`, 2 for `Avg`, 3 for each of the four statistical kinds) and the literal `PartialAggColumn` order each returns; `is_counter_marks_the_four_count_columns` in `scan/spec.rs`, asserting `is_counter()` is `true` for exactly `CountStar`, `CountArg`, `AvgCnt`, and `StatCnt` and `false` for the other six; `partial_column_name_renders_role_and_ordinal` in `scan/spec.rs`, asserting the unquoted name each variant produces at a given ordinal against literal expected strings (`PARTIAL_count_0`, `PARTIAL_avg_sum_1`, `PARTIAL_stat_sumsq_2`, and so on for all ten); and the cross-module alignment test required by `datafusion-scan/scan-partial-agg-column-contract` § "One in-crate test pins the scan-to-adapter column alignment per aggregate kind". The three descriptor tests MUST carry literal expected values and MUST NOT read `partial_columns()` to build their own expectation, per that feature's own clause. Then add `PartialAggColumn` with its ten variants, `is_counter()`, and the shared `partial_column_name(col, ordinal) -> String` producing the unquoted `PARTIAL_<role>_<ordinal>`; keep `scan/spec.rs` free of any `exasol_udf_sdk` import. Rewire `partial_select_items` (match on `PartialAggColumn`, taking every argument from `agg_arg_sql` including the statistical branch, which drops its `plan.column.as_deref().unwrap_or("")` and its local `quote_ident`), `emit_null_partial_row` (map `is_counter()` to `Value::Int64(0)` / `Value::Null`), `partial_row_from_batch` (advance by `partial_columns().len()`), `partial_emits_items` (match on `PartialAggColumn` for the type, preserving `DECIMAL(20,0)` for the four counters, `DOUBLE PRECISION` for `AvgSum`/`StatSum`/`StatSumSq`, `sum_emit_type(col_type_for(…))` for `Sum`, and `col_type_for(…)` for `Min`/`Max`), and `merge_select_items` (names via `partial_column_name`, formulas unchanged in this task). Leave no `format!` literal containing `PARTIAL_` at any of the five sites. All FOUR fixtures from 1.1 and every existing partial-aggregate test MUST stay byte-identical, and the two halves cover different artifacts: the scan-seam fixtures `partial_agg_all_agg_kinds.sql` and `grouped_partial_agg_all_agg_kinds.sql` are the only baseline over `partial_select_items`' output, which this task rewrites, while the `dispatch_golden` fixtures `single_group_all_agg_kinds.sql` and `grouped_all_agg_kinds.sql` cover the `EMITS` clause and the outer merge SELECT this task also rewires. [expert]
- [ ] 1.4 Collapse `parse_agg_item` in `crates/lakehouse-engine/src/adapter/pushdown/single_group_agg.rs` onto two `[(&str, AggKind)]` tables — the expression-capable `SUM`/`MIN`/`MAX`/`AVG` set resolved through `arg_column_or_expr`, and the statistical `STDDEV`/`STDDEV_SAMP`/`STDDEV_POP`/`VARIANCE`/`VAR_SAMP`/`VAR_POP` set resolved through `column_from_first_arg`. Keep `COUNT` as its own branch (its kind depends on argument presence, not on the name), keep the `distinct: true` decline ahead of any lookup, and keep the unrecognized-name `None`. Leave `parse_agg_item_recognises_stat_functions`' own table (`single_group_agg.rs:839-846`) with its literal pairs — do NOT rewrite it to iterate the production constant.
- [ ] 1.5 Extract the statistical merge fragments in `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs`. Build `numer`, `pop_denom`, and `samp_denom` once per aggregate ordinal, add a `stddev_of(var)` helper rendering `CASE WHEN ({var}) IS NULL THEN NULL ELSE SQRT(GREATEST(0.0, {var})) END`, and reduce the four statistical arms of `merge_select_items` to `numer / pop_denom`, `numer / samp_denom`, `stddev_of(numer / pop_denom)`, and `stddev_of(numer / samp_denom)`. `numer` MUST carry its own outer parentheses and `stddev_of` MUST add exactly one pair around its `IS NULL` subject and none around its `GREATEST` argument — that is the pre-refactor nesting, and 1.1's two `dispatch_golden` fixtures are the only thing that will catch a deviation, because the merge SELECT is adapter output and the two scan-seam fixtures do not carry it. Keep the doc comment's König–Huygens derivation and the `GREATEST(0.0, NULL) = 0.0` rationale. Keep all six existing `.contains(...)` merge tests unchanged: each names why one guard exists, which a golden diff alone does not report. [expert]
- [ ] 1.6 Give the declared-type CAST rule one owner. Write the failing test first: `cast_to_declared_type_skips_the_varchar_default_and_absent_type` in `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, asserting all three arms of the rule against literal expected strings — `Some("DECIMAL(18,2)")` wraps the expression in `CAST(<expr> AS DECIMAL(18,2))`, `Some("VARCHAR(2000000)")` returns the expression unwrapped, and `None` returns it unwrapped — which cannot compile before the helper exists in `support.rs`. Then move the canonical implementation to `crates/lakehouse-engine/src/adapter/pushdown/support.rs` as `pub(super) fn cast_to_declared_type(expr: &str, declared: Option<&str>) -> String`, and delegate all six sites: `constant_projection_sql` (passing `declared.as_deref()`), the `gk_select` closure (`group_key_types.get(i).map(String::as_str)`), `cast_merge_items` (`aggregate_types.get(i).map(String::as_str)`), the `ScalarOverAggregate` outer-select cast (`Some(declared_type)`), `file_resolution.rs`'s `empty_agg_sql` (`aggregate_types.get(i).map(String::as_str)`), and the `ScalarOverAggregate` arm of `empty_grouped_sql` (`Some(declared_type)`). Delete the old private `cast_to_declared_type` from `grouped_agg.rs`. Do NOT touch the `GroupKey` and `Aggregate` arms of `empty_grouped_sql`: they cast unconditionally by design. Delete the convention notes the helper now enforces — `cast_to_declared_type`'s doc sentence naming its three mirror sites, and `constant_projection_sql`'s "mirrors the group-key and aggregate cast discipline" and "matching `group_key_exasol_types`" clauses. All ten pre-existing `dispatch_golden` fixtures plus 1.1's two `dispatch_golden` fixtures MUST stay byte-identical.
- [ ] 1.7 Make the expression-argument statistical-aggregate limit explicit. In `parse_agg_item`, decline (return `None`) when a statistical aggregate's first argument is not a bare `column` node, so no caller can produce an `AggregatePlan` with neither `column` nor `arg_expr`.

  `parse_agg_item` has FIVE production callers, not two, and the decline reaches every one of them. Verified: (1) `detect_aggregates` (`single_group_agg.rs:78`) declines the whole single-group select list, so the request falls through to the Tier 3 row scan and Exasol computes the statistic natively. (2) `detect_group_by_aggregates` (`grouped_agg.rs:215`) declines the whole grouped detection, so the request falls through to Tier 1b, the qualified single-table wrapper. (3) `classify_scalar_over_aggregate` (`grouped_agg.rs:399`) returns `None` for a select item that WRAPS such an aggregate (`SELECT SQRT(STDDEV(a + b)) … GROUP BY region`), which declines the whole grouped detection at `grouped_agg.rs:260` and routes to the same wrapper. This is a real behavior change and today's second failure path, and it is measured rather than inferred: task 1.2's capture 4 (`SELECT MOD(id, 4), SQRT(STDDEV(score + id)) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4)`) shows Exasol pushing exactly this shape — `EXPLAIN VIRTUAL` returns status `ok`, the echoed selectList carries the `SQRT` over a nested `STDDEV` `function_aggregate` on an `ADD(SCORE, ID)` argument, and the merge wrapper renders — and shows the execution failing with `sqlCode 22002`, `grouped partial aggregate SQL error: Schema error: No field named .`. The mechanism inside that failure is the one all the paths share: such an item classifies successfully on the malformed plan, gets three `EMITS` columns from the `DOUBLE PRECISION` default, and fails in the scan. Capture 3 (`SELECT MOD(id, 4), STDDEV(score + id) … GROUP BY MOD(id, 4)`) measures path (2) the same way, with the same error. (4) `render_scalar_over_merge` (`grouped_agg.rs:427`) returns `None`, reached for a scalar-over-aggregate inside a HAVING via `render_having_operand` (`grouped_agg.rs:1068`); after (3) the select-list route can no longer carry such an aggregate at all. (5) `render_having_over_merge` (`grouped_agg.rs:988`) returns `None`, which routes a HAVING (`request_shape.rs:133`, capability `AGGREGATE_HAVING` at `capabilities.rs:188`) or a merge ORDER BY (`build_grouped_order_by_clause`, `grouped_agg.rs:553`, as `GroupedOrderBy::Unresolvable`) to the qualified wrapper. For (4) and (5) the OBSERVABLE outcome is unchanged: both already declined these items at their `plans.iter().position(…)` lookup, which no malformed plan satisfies unless the select list carries the same shape — and that select list is itself declined by (2) or (3). The decline moves earlier and its reason becomes explicit; the shape's answer does not move.

  Write the failing tests first, then the production edit: `stat_aggregate_over_expression_argument_declines` in `single_group_agg.rs`, a `STDDEV` item over a `function_scalar` argument and over an arithmetic argument, each asserting `parse_agg_item` returns `None` and that `detect_aggregates` therefore declines the whole select list; `grouped_stat_aggregate_over_expression_argument_declines` in `grouped_agg.rs`, the mirror grouped-path test over `detect_group_by_aggregates`; `having_over_stat_aggregate_with_expression_argument_declines` in `grouped_agg.rs`, a grouped request whose HAVING compares a statistical aggregate over an arithmetic argument, asserting `render_having_over_merge` returns `None` so the shape routes to the qualified wrapper; and `scalar_over_stat_aggregate_with_expression_argument_declines` in `grouped_agg.rs`, a grouped select item wrapping such an aggregate, asserting both that `classify_scalar_over_aggregate` returns `None` and that `detect_group_by_aggregates` therefore declines. The last two live in `grouped_agg.rs`'s own test module, which already reaches both private functions, so NO production visibility widens. Assert separately that a bare-column `STDDEV` still parses to `AggKind::StddevSamp` with `column: Some("SCORE")` and `arg_expr: None`, unchanged.

  Add `e2e_stddev_over_expression_falls_back_and_returns_correct_value` to `crates/lakehouse-engine/tests/e2e_capability_test.rs` as a required deliverable of this task, not an optional extra: run `SELECT STDDEV(score + id) FROM <vs_table>` against the Docker Exasol container and assert the returned value equals the standard deviation computed over the same rows natively. It is the only automated CI guard on this task's behavior change, and per CLAUDE.md it MUST fail rather than skip when the container is absent. Re-running task 1.2's two queries by hand supplements that test and does NOT substitute for it. [expert]
- [ ] 1.8 Run the verification checklist below: `cargo test`, `cargo clippy --all-targets`, `cargo fmt`, `make cross-musl-udf-build`, and `make test-e2e`.

Three tasks carry `[expert]`. Task 1.3 is a cross-module refactor whose failure mode is silently misaligned values rather than a compile error, and it must hold five call sites byte-identical while changing what all five read. Task 1.5's correctness lives entirely in string nesting that no existing test can distinguish. Task 1.7 changes a routing decision at a shared entry point that five callers depend on, spanning two detection paths, a scalar-over-aggregate classifier, a HAVING rewriter, and a merge ORDER BY resolver, where over-declining silently removes working pushdown. Tasks 1.1, 1.2, 1.4, 1.6, and 1.8 are mechanical breadth, a live capture, a table substitution, a compiler-enumerated delegation, and a command run.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2 |
| Group B | 1.3, 1.4 |
| Group C | 1.5 |
| Group D | 1.6 |
| Group E | 1.7 |
| Group F | 1.8 |

Sequential dependencies:

- Group A → Group B (no production edit may land before the byte-identity baseline exists; 1.2 had to capture the failure before 1.7 fixes it, and that capture is DONE, so Group A is 1.1 alone)
- Group B → Group C (1.5 renders partial column names through the owner 1.3 introduces)
- Group C → Group D (1.5 and 1.6 both edit `grouped_agg.rs`; serializing them keeps a golden diff attributable to one task)
- Group B → Group E (1.7 edits the `parse_agg_item` body 1.4 restructures)
- Groups C, D, E → Group F (verification runs last)

1.1 and 1.2 are independent: one writes test fixtures, the other only reads from a database. 1.3 and 1.4 touch disjoint files — `scan/spec.rs` plus `scan/partial_agg.rs` plus `grouped_agg.rs` for the first, `single_group_agg.rs` alone for the second. Groups C, D, and E are single tasks by necessity: each is the smallest step that leaves `cargo test` compiling, and C and D would otherwise contend for the same file.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Match arms | `partial_select_items`' seven `AggKind` arms, `scan/partial_agg.rs:366-404` | Replaced by one match over `PartialAggColumn` |
| Match arms | `emit_null_partial_row`'s four `AggKind` arms, `scan/partial_agg.rs:265-277` | Replaced by `is_counter()` |
| Match arms | `partial_row_from_batch`'s three arms and its `col += 1/2/3` arithmetic, `scan/partial_agg.rs:420-436` | Replaced by `partial_columns().len()` |
| Statements | `let col = plan.column.as_deref().unwrap_or("")` and `let qcol = quote_ident(col)`, `scan/partial_agg.rs:396-397` | Replaced by `agg_arg_sql(plan)` |
| Format literals | Every `format!` containing `PARTIAL_` in `partial_select_items`, `partial_emits_items`, and `merge_select_items` | Replaced by the shared name function |
| Inline fragments | The six numerator, six population-denominator, and four sample-denominator inlinings across `merge_select_items`' four statistical arms, `grouped_agg.rs:914-959` | Replaced by three per-ordinal bindings and `stddev_of` |
| Function | `cast_to_declared_type`, `grouped_agg.rs:1113-1124` | Moved to `support.rs` with an `Option<&str>` declared type |
| Inline blocks | The `VARCHAR(2000000)` guard in `constant_projection_sql` (`:123-126`), the `gk_select` closure (`:627-632`), `cast_merge_items` (`:1106-1109`), `empty_agg_sql` (`file_resolution.rs:737-740`), and `empty_grouped_sql`'s `ScalarOverAggregate` arm (`file_resolution.rs:778-782`) | Replaced by the shared helper |
| Doc-comment sentences | `cast_to_declared_type`'s three-site mirror list; `constant_projection_sql`'s "mirrors the group-key and aggregate cast discipline" and "matching `group_key_exasol_types`" | The helper enforces what the notes asserted by convention |
| Match arms | `parse_agg_item`'s `SUM`/`MIN`/`MAX`/`AVG` arms (`single_group_agg.rs:246-277`) and its six statistical arms (`:280-299`) | Replaced by two `[(&str, AggKind)]` tables |

No test is deleted. Every existing assertion in scope stays as written, which is what makes the byte-identity claim falsifiable rather than self-certified.

## Record Notes

`/speq:spec-merge`'s marker table defines actions for scenarios only, so every edit a delta makes outside a `### Scenario:` block is listed here. `recorder-agent` applies this checklist; it MUST NOT infer these edits from the `DELTA:*` markers that wrap them.

| Delta file | Anchor | Edit |
|------------|--------|------|
| `vs-adapter/pushdown-planning-aggregate-extensions/spec.md` | `## Background`, append after the recorded "Credentials MUST NOT appear…" bullet | Add the seven new bullets inside `<!-- DELTA:NEW -->`; every recorded bullet after that point is unchanged and is NOT reproduced in the delta |
| `vs-adapter/pushdown-planning-aggregate-extensions/spec.md` | Feature description and `## Background` bullets 1-2 | Quoted verbatim to satisfy the spec-structure validator; NOT an edit |

The two NEW feature files are full specs and carry no `DELTA:*` markers, per the delta template's new-feature rule. No other recorded file's Feature description, Background, or prose changes.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| One descriptor owns every aggregate's partial column set | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `partial_columns_arity_per_agg_kind`, `is_counter_marks_the_four_count_columns` |
| One descriptor owns every aggregate's partial column set | Unit | `crates/lakehouse-engine/src/scan/partial_agg.rs` | `partial_agg_sql_stat_emits_cnt_sum_sumsq`, `stat_aggregate_null_fallback_row_has_three_values`, `partial_agg_sql_avg_emits_sum_count_pair` |
| The partial column name has one owner across the scan and the adapter | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `partial_column_name_renders_role_and_ordinal` |
| The partial column name has one owner across the scan and the adapter | Unit | `crates/lakehouse-engine/src/scan/partial_agg.rs` | `stat_aggregate_index_follows_plan_order`, `partial_agg_sql_mixed_column_order_and_indices` |
| The statistical-aggregate partial argument routes through the shared argument renderer | Unit | `crates/lakehouse-engine/src/scan/partial_agg.rs` | `partial_agg_sql_stat_emits_cnt_sum_sumsq`, `partial_sql_uses_rendered_expression_argument` |
| Generated SQL, EMITS clauses, and partial rows are byte-identical across the refactor | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | `single_group_all_agg_kinds_matches_golden`, `grouped_all_agg_kinds_matches_golden`, plus the ten pre-existing fixture tests (the `EMITS` clause and the outer merge SELECT) |
| Generated SQL, EMITS clauses, and partial rows are byte-identical across the refactor | Unit | `crates/lakehouse-engine/src/scan/partial_agg.rs` | `partial_agg_sql_all_agg_kinds_matches_golden`, `grouped_partial_agg_sql_all_agg_kinds_matches_golden` (the scan's own partial-aggregate and grouped partial-aggregate DataFusion SQL, which no `dispatch_golden` fixture can reach) |
| Generated SQL, EMITS clauses, and partial rows are byte-identical across the refactor | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_stddev_variance_pushdown` |
| Generated SQL, EMITS clauses, and partial rows are byte-identical across the refactor | Integration | `crates/lakehouse-engine/tests/scan_plan_shape.rs` | the aggregate-plan shape assertions at `scan_plan_shape.rs:429` |
| One in-crate test pins the scan-to-adapter column alignment per aggregate kind | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `scan_select_list_and_emits_agree_per_agg_kind` |
| The sufficient-statistics fragments have one owner per denominator | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | `single_group_all_agg_kinds_matches_golden`, `grouped_all_agg_kinds_matches_golden` |
| The sufficient-statistics fragments have one owner per denominator | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `var_pop_merge_formula_divides_by_n`, `var_samp_merge_formula_divides_by_n_minus_1`, `stddev_pop_merge_formula_uses_sqrt`, `stddev_samp_merge_formula_uses_sqrt_and_n_minus_1`, `stddev_pop_merge_null_passthrough_for_n_zero`, `stddev_samp_merge_null_passthrough_for_n_zero_and_n_one` |
| The sufficient-statistics fragments have one owner per denominator | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_stddev_variance_pushdown` |
| The declared-type CAST rule has one owner | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `cast_to_declared_type_skips_the_varchar_default_and_absent_type` |
| The declared-type CAST rule has one owner | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | all twelve fixture tests, including the five empty-result shapes |
| Aggregate function names map to AggKind through two tables | Unit | `crates/lakehouse-engine/src/adapter/pushdown/single_group_agg.rs` | `parse_agg_item_recognises_stat_functions`, `bare_column_aggregates_unchanged_regression` |
| Statistical aggregate over an expression argument declines the partial/merge pushdown | Unit | `crates/lakehouse-engine/src/adapter/pushdown/single_group_agg.rs` | `stat_aggregate_over_expression_argument_declines`, `stat_aggregate_over_bare_column_still_parses` |
| Statistical aggregate over an expression argument declines the partial/merge pushdown | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs` | `grouped_stat_aggregate_over_expression_argument_declines`, `having_over_stat_aggregate_with_expression_argument_declines`, `scalar_over_stat_aggregate_with_expression_argument_declines` |
| Statistical aggregate over an expression argument declines the partial/merge pushdown | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_stddev_over_expression_falls_back_and_returns_correct_value` |

Unit tests are the right instrument for the SQL builders, the descriptor, and the detection functions: every one of them is pure computation over strings and JSON with no I/O and no ambient state. The golden fixture tests are unit tests by mechanism but are the plan's primary byte-identity gate, and they assert full-string equality rather than substring presence. Every scenario whose behavior reaches the database also carries an integration test against the Docker Exasol container.

One scenario clause carries no automated test, deliberately. `datafusion-scan/scan-partial-agg-column-contract` requires that no production item's visibility widen to enable the cross-module alignment test. `cargo clippy --workspace --all-targets -- -D warnings` catches an unused widening, but a widening that the test then uses is invisible to it. The reviewer's diff is the check: the test reaches `crate::scan::build_partial_agg_sql` through the `#[cfg(test)]` re-export that already exists in `scan/mod.rs:43` and `partial_emits_items` through its existing `pub(super)`, so any `pub` added to a production item is a visible, unexplained line in the diff.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/scan-partial-agg-column-contract | `EXPLAIN VIRTUAL SELECT COUNT(*), AVG(score), STDDEV(score) FROM LAKEHOUSE_VS.EVENTS;` before and after the change, diffed | Byte-identical generated SQL. Any diff in the `EMITS` clause or the merge SELECT means an arity or a name moved and the refactor MUST NOT ship |
| datafusion-scan/scan-partial-agg-column-contract | `SELECT COUNT(*), COUNT(id), SUM(score), MIN(ts), MAX(ts), AVG(score) FROM LAKEHOUSE_VS.EVENTS;` | Same values as before the change. A shifted value in any column but the first is the misalignment this plan exists to prevent |
| vs-adapter/pushdown-agg-sql-consolidation | `SELECT region, STDDEV(score), STDDEV_POP(score), VARIANCE(score), VAR_POP(score) FROM LAKEHOUSE_VS.EVENTS GROUP BY region;` | Values match the pre-change run to full precision. A changed value means the fragment extraction altered the formula |
| vs-adapter/pushdown-agg-sql-consolidation | `SELECT STDDEV(score) FROM LAKEHOUSE_VS.EVENTS WHERE 1=0;` and a single-row group | `NULL`, not `0.0` — the `CASE WHEN … IS NULL` guard survived the extraction |
| vs-adapter/pushdown-planning-aggregate-extensions (task 1.2 capture, DONE) | `SELECT STDDEV(score + id) FROM MY_LAKEHOUSE.EVENTS;` before the change | MEASURED 2026-07-31: `EXPLAIN VIRTUAL` status `ok`, Exasol pushes the shape; execution fails with `sqlCode 22002`, `partial aggregate SQL error: Schema error: No field named . Valid fields are "ID", "NAME", "SCORE", "EVENT_DATE", "EVENT_TS".` `SELECT VARIANCE(score * 2) FROM MY_LAKEHOUSE.EVENTS;` gives the identical outcome |
| vs-adapter/pushdown-planning-aggregate-extensions | `SELECT STDDEV(score + id) FROM MY_LAKEHOUSE.EVENTS;` after the change | The correct standard deviation of `score + id`, computed by Exasol over returned rows. Any `Schema error: No field named .` means the decline did not take effect |
| vs-adapter/pushdown-planning-aggregate-extensions (task 1.2 capture, DONE) | `SELECT MOD(id, 4), STDDEV(score + id) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4);` before the change | MEASURED 2026-07-31: `EXPLAIN VIRTUAL` status `ok`, pushed, grouped partial-aggregate wrapper SQL rendered; execution fails with `sqlCode 22002`, `grouped partial aggregate SQL error: Schema error: No field named .` This is path (2), `detect_group_by_aggregates` |
| vs-adapter/pushdown-planning-aggregate-extensions | `SELECT MOD(id, 4), STDDEV(score + id) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4);` after the change | The correct standard deviation of `score + id` per group, computed by Exasol over the qualified wrapper's rows. Any `grouped partial aggregate SQL error` means the grouped decline did not take effect |
| vs-adapter/pushdown-planning-aggregate-extensions (task 1.2 capture, DONE) | `SELECT MOD(id, 4), SQRT(STDDEV(score + id)) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4);` before the change | MEASURED 2026-07-31: `EXPLAIN VIRTUAL` status `ok`, pushed as a scalar-over-aggregate (echoed selectList carries `SQRT` over a nested `STDDEV` on `ADD(SCORE, ID)`, merge wrapper rendered); execution fails with `sqlCode 22002`, `grouped partial aggregate SQL error: Schema error: No field named .` This is path (3), `classify_scalar_over_aggregate` |
| vs-adapter/pushdown-planning-aggregate-extensions | `SELECT MOD(id, 4), SQRT(STDDEV(score + id)) FROM MY_LAKEHOUSE.EVENTS GROUP BY MOD(id, 4);` after the change | `SQRT` of the correct standard deviation per group, computed by Exasol over the qualified wrapper's rows. A pushed scalar-over-aggregate plan in `EXPLAIN VIRTUAL` means `classify_scalar_over_aggregate` still admits the malformed plan |

The two before-and-after diffs are the only checks that distinguish a byte-identical refactor from a merely equivalent one at the database boundary, because the golden fixtures pin the adapter's output while these pin what Exasol actually receives and returns. The three `aggregate-extensions` before-state rows are already captured (task 1.2); run all ten rows before the PR leaves draft, so each after-state pairs with a measured before-state.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Test (unit) | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| Build (UDF `.so`) | `make cross-musl-udf-build` | Exit 0 |
| Test (E2E, Docker Exasol) | `make test-e2e` | 0 failures |
