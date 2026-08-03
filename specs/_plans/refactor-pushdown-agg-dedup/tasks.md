# Tasks: refactor-pushdown-agg-dedup

## Phase 2: Implementation (Group A)
- [x] 1.1 Capture pre-refactor golden baselines (dispatch_golden + scan-seam fixtures)
- [x] 1.2 DONE (captured 2026-07-31) — live Docker Exasol capture of the STDDEV/VARIANCE-over-expression failure on all four paths

## Phase 2: Implementation (Group B)
- [x] 1.3 Add PartialAggColumn + AggKind::partial_columns() to scan/spec.rs, rewire five contract sites [expert]
- [x] 1.4 Collapse parse_agg_item in single_group_agg.rs onto two name tables

## Phase 2: Implementation (Group C)
- [x] 1.5 Extract statistical merge fragments in grouped_agg.rs (numer/pop_denom/samp_denom + stddev_of) [expert]

## Phase 2: Implementation (Group D)
- [x] 1.6 One owner for the declared-type CAST rule (support.rs::cast_to_declared_type)

## Phase 2: Implementation (Group E)
- [x] 1.7 STDDEV/VARIANCE over expression argument declines the pushdown instead of erroring [expert]

## Phase 3: Verification
- [x] 1.8 Run verification checklist: cargo test, cargo clippy --all-targets, cargo fmt, make cross-musl-udf-build, make test-e2e

## Phase 4: Review Fixes
- [x] 4.1 In dispatch_golden.rs, add a `col_types: Vec<(String, String)>` parameter to `dispatch_sql_with_pushdown_req` (after `proj_types`), replace its hardcoded `base_col_types()` argument with that parameter, update its four call sites to pass `base_col_types()`, then reduce `dispatch_sql_with_col_types` to route through it instead of duplicating the `build_dispatch_sql` call
- [x] 4.2 In dispatch_golden.rs, remove the never-varied `filter: Option<String>` and `limit: Option<u64>` parameters from `dispatch_sql_with_col_types` and drop the `None, None` arguments from its two call sites (apply together with 4.1 so the final signature is `fn dispatch_sql_with_col_types(request: &Json, col_types: Vec<(String, String)>) -> String`)
- [x] 4.3 In single_group_agg.rs, delete the redundant four-line comment block above the `STAT_AGG_KINDS.iter().find(...)` lookup (lines ~292-295) that restates `parse_agg_item`'s doc comment and `STAT_AGG_KINDS`' doc comment; change no code and no doc comment
- [x] 4.4 In support.rs, extend `cast_to_declared_type`'s doc comment with one sentence stating why `VARCHAR(2000000)` is exempt from casting: it is the catch-all `crate::types::mapping` returns for any type it cannot map, so its presence signals "no usable declared type" rather than a type Exasol actually declared; change no code
- [x] 4.5 In file_resolution.rs, replace the outdated three-line comment on `empty_grouped_sql`'s `GroupedSelectItem::ScalarOverAggregate` arm (lines ~789-791) that claims it mirrors the `GroupKey`/`Aggregate` arms' unconditional cast, with a comment stating the actual rule: it routes through `cast_to_declared_type` and emits a bare `NULL` when the declared type is the `VARCHAR(2000000)` default, unlike the arms above which cast unconditionally; change no code
- [x] 4.6 Add `e2e_grouped_stddev_over_expression_falls_back_and_returns_correct_value` to `e2e_capability_test.rs`: assert the grouped `STDDEV(score + id)` over `GROUP BY MOD(id, 4)` pushes no `PARTIAL_stat_` and returns each group's correct sample standard deviation, referenced only from projected rows [expert]
