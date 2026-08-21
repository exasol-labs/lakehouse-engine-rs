# Code Review Findings: fix-single-group-scalar-over-aggregate

## Summary
- Files reviewed: 24 (18 modified, 6 new — 2 Rust, 6 golden fixtures)
- Total findings: 13 (standard: 9, expert: 4)

Scope notes: the project's no-test-code-in-production-files rule is satisfied — `scalar_over_agg.rs`
declares `#[cfg(test)] #[path = "scalar_over_agg_tests.rs"] mod tests;` as its last item and the
sibling file matches `[0-9a-zA-Z_-]+[_-]tests\.rs`. The `aggregate_exasol_types` deletion the plan's
Dead Code Removal table required was carried out (no definition remains). The dead-code table's
`grouped_agg.rs` relocation was carried out verbatim with no pass-throughs left behind.

## Standard fixes

### crates/lakehouse-engine/tests/e2e_join_test.rs

#### [OUTDATED_COMMENT] Doc comment describes a limitation this plan removed
- Location: lines 883-899, doc comment on `ensure_ground_truth_lineitem_table`
- Issue: the comment states that "the single-table grouped-aggregate pushdown
  (`detect_group_by_aggregates`) declines any select list containing a non-`function_aggregate`
  item — such as the `ROUND(100.0*SUM(CASE..)/COUNT(*),2)` scalar-over-aggregate used here — and
  falls back to a raw full-row scan with the wrong column count, hard-failing with *Expected number
  of columns is 5 but pushdown query has 6*". `detect_group_by_aggregates` now classifies exactly
  that item (`grouped_agg.rs:272` builds `GroupedSelectItem::ScalarOverAggregate` with
  `declared_type: declared_type_at(select_index)`), and the ungrouped counterpart is what this plan
  added. The stated hard failure is no longer reachable, so the comment misinforms any future reader
  about why the native ground-truth table exists.
- Fix: In crates/lakehouse-engine/tests/e2e_join_test.rs, rewrite the doc comment on
  `ensure_ground_truth_lineitem_table` (lines 883-899) so it no longer claims
  `detect_group_by_aggregates` declines a scalar-over-aggregate select list or that running the
  select list directly against the virtual `fact_lineitem` table hard-fails with "Expected number of
  columns is 5 but pushdown query has 6". State instead the reason the helper still exists: the
  ground truth must be computed by Exasol over native data so it is an oracle independent of the
  pushdown path under test. Keep the final paragraph about `CREATE OR REPLACE TABLE` idempotency
  unchanged.

### crates/lakehouse-engine/src/adapter/pushdown/mod.rs

#### [OUTDATED_COMMENT] Comment names the deleted `aggregate_exasol_types`
- Location: line 476
- Issue: `// `aggregate_exasol_types` keyed off top-level select items only and would misalign` names
  a function this plan deleted from `support.rs`. A reader grepping the name finds nothing.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/mod.rs, rewrite the comment at lines 474-477
  so it states the invariant without naming `aggregate_exasol_types`: the per-plan declared types
  must come from the detection-built `plan_types`, which is aligned 1:1 with `grouped_agg_plans`,
  never from a `selectList`-keyed lookup (which would misalign once nested aggregates join the plan
  list).

#### [UNTESTED_ERROR_PATH] The unassemblable-merge-SELECT fallback has no test
- Location: lines 607-622 (the `let Some(merge_select) = single_group_merge_select(...) else { ... }`
  arm)
- Issue: this new arm routes the whole request to `qualified_single_table_fallback_pushdown`, and it
  is reachable: `has_distinct` already returned above, so the surviving `None` cause is a
  `ScalarOverAggregate` item that `classify_scalar_over_aggregate` accepted (checked with
  `render_expression`, DataFusion dialect) but `render_scalar_over_merge` declines (rendered with
  `render_expression_exasol`, Exasol dialect — the two dialects differ, see the CHAR/VARCHAR note in
  `scalar_over_agg.rs:163-168`). No test drives `build_dispatch_sql` down this branch, so a
  regression here would silently emit a positionally-short select list (`04000`) rather than the
  wrapper.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs, add a test that drives
  `dispatch_sql_for_body` with a single-group select list whose only item is a scalar-over-aggregate
  that `classify_scalar_over_aggregate` accepts but `render_scalar_over_merge` declines, and assert
  the returned SQL is the qualified single-table wrapper (it contains `AS "LHS_T0"` and the original
  select-list expression rendered natively), not a partial/merge scan (no `PARTIAL_` column). If no
  such dialect-divergent node exists, instead assert the reachability boundary directly in
  `single_group_agg_tests.rs` and add a comment on the `else` arm in mod.rs recording that the only
  live producer of `None` is the dialect divergence.

### crates/lakehouse-engine/src/adapter/pushdown/empty_result.rs

#### [OUTDATED_COMMENT] Doc comment sources a type list from the deleted `aggregate_exasol_types`
- Location: line 226 (doc comment on `empty_grouped_sql`)
- Issue: "types from `group_key_exasol_types` / `aggregate_exasol_types`" names a deleted function as
  the source of `aggregate_types`. The actual source is now `detection.plan_types` (see the caller at
  lines 56-60).
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/empty_result.rs, change the doc comment on
  `empty_grouped_sql` to name `group_key_exasol_types` and `GroupedAggregateDetection::plan_types` as
  the two type sources, dropping `aggregate_exasol_types`.

### crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs

#### [OUTDATED_COMMENT] Three comments name the deleted `aggregate_exasol_types`
- Location: lines 105, 689, 728
- Issue: line 105 says `plan_types` "Replaces `aggregate_exasol_types` on the grouped path"; lines
  689 and 728 both say a declared type comes "from `aggregate_exasol_types`/`selectListDataTypes`".
  The function no longer exists anywhere in the crate, so all three point at nothing — and lines 689
  and 728 actively mislead about where `partial_emits_items` / `col_type_for` get their `declared`
  argument (the caller's per-plan `aggregate_types` list).
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/grouped_agg.rs, delete the sentence
  "Replaces `aggregate_exasol_types` on the grouped path, which keyed off top-level select items only
  and would misalign once nested aggregates join `plans`." from the `plan_types` doc comment (line
  105-107), and in the comments at lines 688-690 and 727-729 replace
  "`aggregate_exasol_types`/`selectListDataTypes`" with "the caller's per-plan `aggregate_types`
  list, resolved from `selectListDataTypes`".

### crates/lakehouse-engine/src/adapter/pushdown/empty_result_tests.rs

#### [OUTDATED_COMMENT] Test doc justifies itself by a deleted function
- Location: line 106 (doc comment on
  `empty_agg_sql_scalar_over_aggregate_item_does_not_shift_a_later_bare_aggregate_type`)
- Issue: "the very misalignment that made `aggregate_exasol_types` unusable here once this variant
  existed" names a function that no longer exists, so the test's stated rationale cannot be checked
  against any code.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/empty_result_tests.rs, reword the closing
  parenthetical of the doc comment at lines 103-108 to state the rule without naming
  `aggregate_exasol_types`: a type list compacted down to only the `function_aggregate`-typed select
  items shifts every index after a `ScalarOverAggregate` item, so each item's cast type must be
  looked up at its own `selectList` index.

### crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs

#### [OUTDATED_COMMENT] Module doc asserts an independence the module's own imports break
- Location: lines 8-9 ("This module owns that mechanism so the two planners cannot drift apart on it,
  and names neither of them"), contradicted by line 15 (`use super::single_group_agg::parse_agg_item;`)
- Issue: the module does name a planner — `single_group_agg` — and calls into it from both
  `classify_scalar_over_aggregate` (line 133) and `render_scalar_over_merge` (line 171). The stated
  invariant is what a reader will rely on when deciding whether a new dependency is allowed here, so
  a doc comment that claims it while the file violates it is worse than no claim. (The
  `parse_agg_item` dependency itself pre-dates this plan; only the claim is new.)
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs, rewrite the last sentence
  of the module doc (lines 7-9) to state the accurate invariant: this module owns the decomposition
  mechanism and takes the merged `PARTIAL_*` expressions as a parameter so it never depends on either
  planner's merge assembly, and note that its one remaining dependency on `single_group_agg` is the
  `parse_agg_item` AggKind-parsing primitive, which is not planner-specific and is tracked for
  relocation (see the Expert fix on the `single_group_agg` ↔ `grouped_agg` cycle).

### crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg_tests.rs

#### [VAGUE_TEST_NAME] "distinct aggregates" collides with the DISTINCT domain term
- Location: line 63, `fold_keeps_distinct_aggregates_in_separate_slots`
- Issue: the test folds `SUM("X")` and `COUNT(*)` — neither carries `distinct: true`. In this
  codebase DISTINCT is a loaded domain term (`SingleGroupItem::Distinct`, `parse_count_distinct`,
  `has_distinct`, and the sibling test `classify_declines_a_distinct_inner_aggregate` two screens
  below), so the name reads as being about DISTINCT aggregates when it is about structurally
  different ones.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg_tests.rs, rename
  `fold_keeps_distinct_aggregates_in_separate_slots` to
  `fold_keeps_structurally_different_aggregates_in_separate_slots`.

### crates/lakehouse-engine/src/adapter/pushdown/single_group_agg.rs

#### [UNUSED_VARIABLE] `ScalarOverAggregate::select_index` is written but never read
- Location: line 42 (field declaration); written at line 119; every production reader destructures it
  away (`single_group_agg.rs:...` `SingleGroupItem::ScalarOverAggregate { node, declared_type, .. }`,
  `empty_result.rs` likewise)
- Issue: no production code path reads the field. Items are always held in `selectList` order and
  every consumer that needs an ordinal enumerates the slice (`empty_agg_sql`'s
  `.iter().enumerate()`, `single_group_plan_types`' `items.iter().enumerate()`), so the stored copy
  is redundant and nothing keeps it in sync with the item's real position — a second home for a fact
  the slice order already carries.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/single_group_agg.rs, remove the
  `select_index` field from `SingleGroupItem::ScalarOverAggregate` and drop it from the construction
  site at lines 117-122. Then remove the `select_index: N` initializers from the variant literals in
  `empty_result_tests.rs` (lines 111, 188, 193, 227, 391, 395, 434, 437, 442) and
  `single_group_agg_tests.rs` (line 203). Run `cargo test -p lakehouse-engine` to confirm no other
  construction site remains.

### crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs

#### [IMPLEMENTATION_COUPLED_TEST] The decline golden fabricates the very signal it tests
- Location: `dispatch_sql_widened` (the `true` at the 5th positional argument) and
  `nested_aggregate_decline_matches_qualified_wrapper_golden`
- Issue: the golden harness hardcodes `projection_widened = true` and passes empty
  `proj_cols`/`proj_types` instead of deriving them from `project_columns`, so the test proves only
  that `build_dispatch_sql` honours a widening signal it was handed — not that the
  `ROUND(SUM(DISTINCT "AMOUNT"), 2)` fixture actually widens. The floor's two halves (the subtree
  probe in `support_tests.rs`, the wrapper routing here) are each covered but never joined, so
  deleting the `contains_aggregate_node` call in `project_columns` would leave this golden green.
  `pushdown_tests.rs`'s `dispatch_sql_for_body` shows the derived-input pattern this harness could
  use.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs, change
  `dispatch_sql_widened` to derive its dispatch inputs through the production helpers instead of
  hardcoding them: call `project_columns(pushdown_req, base_col_types())` and pass the returned
  projection items, projection types, and widening flag into `build_dispatch_sql`. Add an
  `assert!(widened, ...)` inside the helper so the fixture's own widening is asserted, and confirm
  `nested_aggregate_decline.sql` still matches byte-for-byte.

### crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs

#### [SHRINKABLE] Five near-identical widening tests repeat the same 20-line assertion block
- Location: `project_columns_widens_on_aggregate_nested_in_scalar_item`,
  `project_columns_top_level_aggregate_widening_is_unchanged_by_the_subtree_probe`,
  `project_columns_widens_on_aggregate_nested_in_function_scalar_cast_item`,
  `project_columns_widens_on_aggregate_nested_in_function_scalar_case_item`,
  `project_columns_widens_on_aggregate_nested_in_arithmetic_node`,
  `project_columns_widens_on_aggregate_nested_in_predicate_node`
- Issue: six tests each rebuild the same `expected_names` / `expected_types` vectors from
  `decimal_rewrite_col_types()` and repeat the same three assertions; the only variation is the
  select-list node and its `selectListDataTypes` entry. Well past the third occurrence, so the
  duplication should be extracted rather than tolerated.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs, add a helper
  `assert_widens_to_full_base_row(select_item: Json, declared_type: Json, why: &str)` that builds the
  `pushdown_req`, calls `project_columns` with `decimal_rewrite_col_types()`, and asserts `widened`,
  the full-base-row projection items, and the base-row EMITS types. Rewrite all six tests above to
  one call each, keeping their current names and doc comments so each node family stays an
  independently-named test.

#### [MISSING_DESIGN_INTENT] The issue-#198 rationale was deleted from the limit-render test
- Location: `aggregate_merge_renders_request_limit_when_some` (the doc comment above it was removed
  by this change)
- Issue: the four-line doc comment stating why the test exists — a pushed `LIMIT 0` over a one-row
  aggregate merge must return zero rows instead of being silently dropped, issue #198 — was deleted
  while the only substantive edit to that test was adding the `merge_select` argument. The test now
  asserts a `LIMIT` renders with nothing recording which live bug that guards.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs, restore the doc comment
  above `aggregate_merge_renders_request_limit_when_some`: "The aggregate merge SELECT renders
  `LIMIT n` on the outer wrapper when `request_limit` is `Some(n)` — the render site issue #198 needs
  so a pushed `LIMIT 0` over a one-row aggregate merge returns zero rows instead of being silently
  dropped."

## Expert fixes

### crates/lakehouse-engine/src/adapter/pushdown/single_group_agg.rs

#### [INFORMATION_LEAKAGE] The nested-only declared-type default has two homes that disagree, emitting a VARCHAR partial column for a numeric MIN/MAX
- Location: `single_group_plan_types`, `const DEFAULT_TYPE: &str = "VARCHAR(2000000)"` and the
  `vec![DEFAULT_TYPE.to_string(); plans.len()]` seed
- Issue: "what type does a plan slot get when it is reached ONLY through a nested
  scalar-over-aggregate" is one design decision that now lives in two modules and they answer
  differently. `scalar_over_agg::fold_aggregate_plan` answers `DOUBLE PRECISION` (line 44), and the
  grouped planner's `plan_types` doc comment (`grouped_agg.rs:102-107`) documents that answer.
  `single_group_plan_types` answers `VARCHAR(2000000)`. The list is not only an outer-cast source —
  `build_aggregate_scan_sql` passes it straight to `partial_emits_items` as the `EMITS` type list, so
  the default is load-bearing for the scan's column types. Traced path for
  `SELECT ROUND(MIN(A * B), 2) FROM t`:
  `detect_aggregates` → `classify_scalar_over_aggregate` → `parse_agg_item` accepts MIN via
  `EXPR_CAPABLE_AGG_KINDS` with `column: None, arg_expr: Some(...)` → `validate_agg_col_types` does
  not gate MIN/MAX (`grouped_agg.rs:789-796`, `needs_numeric` covers only Sum/Var*/Stddev*) →
  `single_group_plan_types` gives the nested-only slot `VARCHAR(2000000)` → `partial_emits_items`'
  `Min | Max` arm calls `col_type_for(None, Some(expr), col_types, Some("VARCHAR(2000000)"))`, which
  returns the declared type verbatim (`grouped_agg.rs:736-741`) with no `sum_emit_type` normalisation
  to rescue it → the shard emits `"PARTIAL_min_0" VARCHAR(2000000)` and the merge renders
  `MIN("PARTIAL_min_0")` over VARCHAR, i.e. a lexicographic minimum of a numeric expression. The
  grouped planner on the identical select list yields `DOUBLE PRECISION` and a numeric minimum. The
  failure mode is a wrong value, not an error, and
  `single_group_plan_types_defaults_when_reached_only_through_scalar_wrapper`
  (single_group_agg_tests.rs:891) currently asserts the `VARCHAR(2000000)` answer, so the defect is
  pinned green by a passing test. A top-level `MIN(A * B)` is unaffected (its own
  `selectListDataTypes` entry supplies a numeric type), so only the nested case this plan introduced
  is exposed.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/single_group_agg.rs, split the two defaults in
  `single_group_plan_types`: seed `plan_types` for a slot reached only through a nested
  `ScalarOverAggregate` with `"DOUBLE PRECISION"` (matching
  `scalar_over_agg::fold_aggregate_plan`'s own default and the grouped planner's documented
  behaviour), and keep `"VARCHAR(2000000)"` only as the fallback when a top-level
  `SingleGroupItem::Aggregate` has no `selectListDataTypes` entry at its ordinal (that value is what
  `cast_to_declared_type` reads as "emit no cast", and `detect_aggregates` uses the same fallback).
  Update the function's doc comment to name both defaults and why they differ. Update
  `single_group_plan_types_defaults_when_reached_only_through_scalar_wrapper` (line 891),
  `single_group_plan_types_prefers_top_level_declared_type_for_shared_slot` (line 914), and
  `single_group_plan_types_resolves_both_ends_of_an_interleaved_list` (line 938) to the new expected
  values. Add a regression test asserting that
  `SELECT ROUND(MIN(<expr>), 2)` over an expression-argument MIN emits a NUMERIC
  `"PARTIAL_min_0"` EMITS type, never `VARCHAR(2000000)`. Verify all six new and eighteen existing
  `testdata/dispatch_golden/` fixtures stay byte-identical (the nested SUM/COUNT slots in the new
  fixtures resolve through the column map or the hardcoded stat/count types, so none should move).

#### [DEPENDENCY_CYCLE] The single-group planner now names the grouped planner, closing a planner-to-planner cycle
- Location: line 12, `use super::grouped_agg::{cast_merge_items, merge_select_items};`
- Issue: `grouped_agg.rs:16` imports `single_group_agg::parse_agg_item` and `single_group_agg.rs:12`
  now imports `grouped_agg::{cast_merge_items, merge_select_items}` — a two-module cycle between the
  two planners, plus a three-module one through the new shared owner
  (`grouped_agg` → `scalar_over_agg` → `single_group_agg` → `grouped_agg`). This directly inverts the
  plan's own stated pattern ("the shared module never names either planner and no module cycle
  forms"): the merge-expression primitives every single-group aggregate needs live inside the GROUP BY
  planner, so the single-group path cannot be read, moved, or changed without the grouped planner. The
  edge is new — before this change `support.rs` owned the call to `cast_merge_items` and
  `single_group_agg.rs` named no sibling planner at all.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/, move `merge_select_items` and
  `cast_merge_items` (and the `stddev_of` / statistical-merge helpers they need) out of
  `grouped_agg.rs` into `scalar_over_agg.rs`, the module already designated the single owner of the
  partial-to-merge rewrite, keeping `pub(super)` visibility. Repoint `grouped_agg.rs`
  (`build_grouped_aggregate_scan_sql`, `render_having_over_merge`, `render_having_operand`),
  `single_group_agg.rs` (`single_group_merge_select`), and `support_tests.rs` (which imports
  `cast_merge_items` for `build_agg_sql`) at the new location, and delete the
  `single_group_agg` → `grouped_agg` import. Then move `parse_agg_item` together with
  `EXPR_CAPABLE_AGG_KINDS`, `STAT_AGG_KINDS`, `arg_column_or_expr`, and `column_from_first_arg` into
  `scalar_over_agg.rs` as well and repoint `grouped_agg.rs` and `scalar_over_agg.rs`, so neither
  planner names the other and the shared module names neither. Prove the move changed nothing:
  `grouped_aggregate.sql`, `grouped_all_agg_kinds.sql`, `group_by_fallback.sql`,
  `single_group_all_agg_kinds.sql`, and all six new fixtures must match byte-for-byte, and every
  existing grouped and single-group unit test must pass unedited.

#### [INFORMATION_LEAKAGE] The `declared_type_at` select-list-type lookup is copy-pasted into a fourth and fifth home
- Location: `single_group_agg.rs:96-103` (in `detect_aggregates`), `single_group_agg.rs:185-192` (in
  `single_group_plan_types`), `empty_result.rs:186-193` (in `empty_agg_sql`); the same closure
  already exists at `grouped_agg.rs:201-208` and the same lookup is inlined at `support.rs:1256`
- Issue: "read `selectListDataTypes[i]`, map it with `exasol_type_from_json`, default to
  `VARCHAR(2000000)`" is a single decision about the pushdown wire format, and this change adds three
  more byte-identical copies of it across two modules — five sites in four modules in total. Changing
  the default, or handling a new `dataType` shape, now means editing every one of them, and a missed
  site produces a positional type mismatch (`04000`) rather than a compile error. This is precisely
  the back-door leakage the plan set out to eliminate for the decomposition quartet, reintroduced for
  the type lookup.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, add
  `pub(super) fn declared_select_type(pushdown_req: &Json, select_index: usize) -> String` that
  performs the `selectListDataTypes` → `exasol_type_from_json` lookup and returns
  `"VARCHAR(2000000)"` when the entry is absent, with a doc comment naming it the single owner of
  that default. Replace the closures in `single_group_agg.rs` (`detect_aggregates`,
  `single_group_plan_types`), `empty_result.rs` (`empty_agg_sql`), and `grouped_agg.rs` (line 201)
  with calls to it, and route the inline lookup at `support.rs:1256` through it too if its default
  matches. Coordinate with the nested-only-default fix above so the two defaults stay distinct.
  Confirm all twenty-four `testdata/dispatch_golden/` fixtures stay byte-identical.

### crates/lakehouse-engine/src/adapter/pushdown/support.rs

#### [TOO_MANY_ARGUMENTS] `build_scan_driving_sql` reaches ten arguments, three of them an unenforced aggregate-path bundle
- Location: `build_scan_driving_sql` (now 10 parameters) and `build_aggregate_scan_sql` (now 9)
- Issue: the guardrail is three. Beyond the count, the new `merge_select: &[String]` parameter joins
  `aggregate_types: &[String]` and `request_limit: Option<u64>` as a third argument that only the
  aggregate sub-path reads, with the contract stated in prose ("Row scans read neither: pass `&[]`")
  and nothing enforcing it. Because `build_aggregate_scan_sql` now does
  `let merge_select = merge_select.join(", ")`, an aggregate spec paired with an empty `merge_select`
  silently renders `SELECT  FROM (...)` — malformed SQL from a `pub` façade function, where the old
  code derived the merge from `aggregates` and could not be empty. Two adjacent `&[String]`
  parameters with different alignment rules (one per plan, one per select-list item) are also easy to
  transpose at a call site, and this signature's call sites span the e2e test crates that host
  `cargo test` does not compile.
- Fix: In crates/lakehouse-engine/src/adapter/pushdown/support.rs, introduce
  `pub struct AggregateMergeInputs { pub plan_types: Vec<String>, pub merge_select: Vec<String>,
  pub request_limit: Option<u64> }`, re-export it on the `pushdown` façade alongside
  `build_scan_driving_sql`, and replace the `request_limit`, `aggregate_types`, and `merge_select`
  parameters of both `build_scan_driving_sql` and `build_aggregate_scan_sql` with a single
  `Option<&AggregateMergeInputs>` — `None` on the row-scan path, so "row scans read neither" becomes
  unrepresentable instead of documented. Return a `Result`/`None` decline (or `debug_assert!`) when a
  spec carries `aggregates` but the inputs are absent or `merge_select` is empty, so a malformed
  `SELECT  FROM` can never be emitted. Update the one production caller in `mod.rs` and every test
  caller — `support_tests.rs`, `test_support_tests.rs`, `topn_tests.rs`, and the external
  `tests/scan_plan_shape.rs` — and confirm `tests/pushdown_public_surface.rs` and
  `src/adapter/pushdown_surface_probe_tests.rs` still compile (the `build_scan_driving_sql` name must
  survive; add `AggregateMergeInputs` to their `use` lists only if the probes enumerate façade
  items). Compile the e2e test crates explicitly (`cargo test --no-run --all-targets`) before
  declaring the census complete.
