# Tasks: fix-join-filter-type-rewrites

## Phase 2: Implementation (Group A)
- [x] 1.1 Add failing unit tests in `joins/sql_builders.rs` for `render_broadcast_join`: LIKE over DECIMAL side column → `Ok(None)`; LIKE over DATE side column → `Ok(Some(..))` with CAST-to-VARCHAR; `INSTR(a,b,3)` → `Ok(None)`; absent/trivially-true filter stays eligible with no scan-spec filter.
- [x] 1.2 Add `join_col_types(request, join)` helper in `joins/rendering.rs`; wire `classify_where_filter` into `render_broadcast_join` after `disjoint_schema_guard`; update `extract_join_projection` to reuse `join_col_types`; update doc comments.
- [x] 1.3 Remove now-unused `datafusion_renderable` import from `joins/sql_builders.rs`; confirm clippy clean.

## Phase 2: Implementation (Group B)
- [x] 2.1 Add failing unit tests in `joins/rendering.rs` for new `type_screened_leg_filter`: all-accepted, all-declined, mixed, DATE LIKE (leg/CAST), DECIMAL LIKE (declined/RAW), type-accepted-but-unrenderable-rewrite (declined/RAW), two sides sharing a column name screened independently.
- [x] 2.2 Add `type_screened_leg_filter(side_local, col_types) -> (Option<Json>, Option<Json>)` in `joins/rendering.rs`, beside `renderable_only`/`declined_only`. [expert]
- [x] 2.3 Add failing unit tests in `joins/sql_builders.rs` for `build_n_scan_join_sql`: side-local LIKE/DECIMAL → outer WHERE not leg; side-local LIKE/DATE → leg as CAST not outer WHERE; mixed accepted/declined per side; total/disjoint conjunct coverage; golden SQL tests unchanged.
- [x] 2.4 Restructure `build_n_scan_join_sql` so per-side fan-out runs BEFORE residual assembly; wire `type_screened_leg_filter` per side; update doc comments on `build_n_scan_join_sql` and `build_side_fan_out_sql`. [expert]

## Phase 2: Implementation (Group C)
- [x] 3.1 E2E test: below-threshold two-table inner equi-join, WHERE LIKE over DECIMAL `O_CUSTKEY` → N-scan wrapper, ground-truth row match.
- [x] 3.2 E2E test: same join, WHERE LIKE over DATE `O_ORDERDATE` → broadcast join retained (CAST), ground-truth row match, format-independent.
- [x] 3.3 E2E test against `VS_NAME_LOW` (forced N-scan fallback): side-local LIKE over DECIMAL `O_CUSTKEY` → outer WHERE not leg, ground-truth row match.
- [x] 3.4 E2E test: `INSTR(C_NAME, <substr>, 3)` in join WHERE → native Exasol result, not start-position-ignoring result.
- [x] 3.5 Extend `tests/common/seed.rs` star-schema seeding: add scale>0 DECIMAL column to `fact_orders` (Iceberg schema + `make_orders_batch`); re-run `cargo test` and `make test-e2e` to confirm no fixture regressed.
- [x] 3.6 E2E test (#223 slice 2): join WHERE `LENGTH(<scale>0 DECIMAL col>) > n` matches native Exasol evaluation, at BOTH `VS_NAME` (broadcast) and `VS_NAME_LOW` (N-scan).

## Phase 2: Implementation (Group D)
- [x] 4.1 Run full verification checklist (build, unit tests, E2E against manually-started Docker stack, clippy, fmt, spec validation) and record results.

## Phase 4: Review Fixes
- [x] 4.2 Extract `pub(super) fn type_accepted_rewrite(expr, col_types) -> Option<Json>` in `adapter/pushdown/support.rs` as the sole owner of "a tree the DataFusion scan may be handed"; call it from both `classify_where_filter` (preserving its three-way absent/declined/rendered outcome) and `joins/rendering.rs::type_screened_leg_filter`, drop the now-unused imports, and replace the doc comment's `support.rs:1092` citation with a `[type_accepted_rewrite]` link. [expert]
- [x] 4.3 Replace `e2e_join_decimal_stringification_matches_native_at_both_surfaces`'s `expected_join_rows_with_fact_where` oracle with one computed in Rust from `order_totalprice_unscaled` + `O_TOTALPRICE_PS`'s scale (Exasol's trimmed-decimal text, keys whose text length exceeds 3, paired as `expected_full_join_rows` does), asserting exactly 4 expected members; export `O_TOTALPRICE_PS` as `pub`. [expert]
- [x] 4.4 In `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` `mod tests`, add `join_like_over_varchar_side_column_pushes_down_unchanged`, `join_decimal_stringification_renders_trimmed_at_both_join_sites`, and `join_instr_beyond_two_args_declines_at_both_join_sites` per the finding's exact spec; reconcile `plan.md` § Verification § Scenario Coverage and § Manual Testing with the shipped test names, replacing `broadcast_filter_runs_type_rewrites_over_union_of_side_columns` / `broadcast_type_decline_and_dialect_decline_share_one_route` with `broadcast_declines_like_over_decimal_side_column` / `broadcast_keeps_plan_and_casts_like_over_date_side_column`.
- [x] 4.5 In `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs`, rewrite `side_local_filter`'s doc comment (line ~216) to enumerate three consumers and the tree each receives: (a) `resolve_file_list` Iceberg manifest pruning — RAW, unscreened; (b) the fan-out `ScanSpec.filter` — screened by `renderable_only` AND screened/rewritten per side by `type_screened_leg_filter`; (c) the outer wrapper's residual `WHERE` — the RAW conjuncts `type_screened_leg_filter` hands back. Keep the existing rationale for (a).
- [x] 4.6 In `crates/lakehouse-engine/tests/e2e_join_test.rs` `e2e_join_instr_with_start_position_returns_native_result`, call `explain_virtual_sql` before running the query and assert: `has_two_scan_wrapper(&pushed)`, the pushed SQL contains verbatim `INSTR(` with literal `3` in the outer `WHERE` (verify exact substring against live EXPLAIN VIRTUAL first), and it does NOT contain `strpos(`. Keep the existing row-equality assertions.
- [x] 4.7 In `crates/lakehouse-engine/tests/e2e_join_test.rs`, delete every plan-task parenthetical (`(task 3.N)`, `(tasks 3.1, 3.2, 3.3, 3.6)`, `(plan `fix-join-filter-type-rewrites`, tasks 3.1-3.4, 3.6)`, `seeded by task 3.5` → `seeded in tests/common/seed.rs`) from the doc comments at the current occurrences of these markers; keep every `#NNN` issue reference and surrounding prose.
- [x] 4.8 In `crates/lakehouse-engine/tests/common/seed.rs`, restore `seed_star_schema`'s doc comment directly above `pub async fn seed_star_schema` (~line 997): "Seed the `dim_customer` and `fact_orders` star-schema tables into the `e2e_lakehouse` namespace. Idempotent." — extended to name the new `O_TOTALPRICE` DECIMAL column.
- [x] 4.9 In `crates/lakehouse-engine/tests/common/seed.rs` (~line 980), rewrite `O_TOTALPRICE_PS`'s doc comment to "Precision/scale of `fact_orders.O_TOTALPRICE`, the scale > 0 DECIMAL column whose stringified length differs between DataFusion's full-scale text and Exasol's trimmed form (#223 slice 2)." — dropping `task 3.5`, keeping the issue reference.

## Phase 5: Verification
- [x] 5.1 Automated checks (build/test/lint/format)
- [x] 5.2 Scenario coverage audit
- [x] 5.3 Manual verification steps
