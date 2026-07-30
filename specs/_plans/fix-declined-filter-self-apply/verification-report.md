# Verification Report: fix-declined-filter-self-apply

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | A DataFusion-declined WHERE predicate is now self-applied by the adapter (qualified `LHS_T0`/`LHS_T1`.. wrapper WHERE) at every dispatch shape — single-table, broadcast join, and N-scan join — instead of silently returning unfiltered rows. Confirmed by unit, integration, and live-Docker-Exasol manual verification; zero regressions. |
| Code review | 10 findings — standard: 8, expert: 2 — 10 fixed |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test --workspace`) | ✓ |
| Tests (e2e, `make test-e2e`) | ✓ |
| Lint (`cargo clippy --all-targets --all-features`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | Not measured (no coverage tool wired into this project); scenario coverage audited manually below — every plan scenario maps to a passing test |
| Integration | Not measured; see Scenario Coverage |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`lakehouse-engine` lib) | 667 | 667 | 0 |
| Unit (`vs-expression` lib) | 121 | 121 | 0 |
| Unit (`lakehouse-catalog` lib) | 72 | 72 | 0 |
| Unit (misc integration-test-binary unit checks: `boolean_to_string_casing_test`, `build_convention`, `catalog_session_signatures`, `catalog_crate_boundary`, `catalog_public_surface`) | 8 | 8 | 0 |
| Integration (e2e, `make test-e2e`, live Docker Exasol/MinIO/Iceberg-REST — 8 binaries: `e2e_scan_test`, `e2e_capability_test`, `e2e_count_distinct_test`, `e2e_join_test`, `e2e_positional_deletes_test`, `e2e_int96_timestamp_test`, `e2e_refresh_test`, `e2e_non_ascii_identifier_test`) | 200 | 200 | 0 |

Host `cargo test` (no `--features exasol-e2e`) correctly compiles the e2e test binaries but runs 0 tests in each (feature-gated), per this project's convention — the e2e feature run above is the real gate.

### Manual Tests

| Test | Result |
|------|--------|
| SECOND arity-3 declined filter, single-table: `COUNT(*)` under `WHERE SECOND(C_TS, 3) > 1` | ✓ 0 rows (was 12 before the fix) |
| LIKE-on-DECIMAL declined filter: `WHERE C_DECIMAL_A LIKE '1%' ORDER BY ID` | ✓ ids `1, 5, 7` (was all 12) |
| EXPLAIN VIRTUAL — declined filter emits self-applied wrapper, not a scan-spec filter | ✓ `PUSHDOWN_SQL` shows `AS "LHS_T0" WHERE (1 < SECOND("LHS_T0"."C_TS", 3))`; scan-spec JSON blob has no `"filter"` key |
| `SELECT *` under a declined filter projects the full base row | ✓ 0 rows, `numColumns` = 10, header lists all 10 columns (`ID,C_DECIMAL_A,C_DECIMAL_B,C_DOUBLE,C_VARCHAR,C_DATE,C_TS,C_BOOL,C_PRICE,C_QTY`) |
| Fast path unchanged: `WHERE SECOND(C_TS) > 1` (renders, no wrapper) | ✓ `PUSHDOWN_SQL` carries `"filter":"(1 < date_part('SECOND', \"C_TS\"))"` in the scan spec; no `LHS_T0` wrapper anywhere |
| Broadcast-eligible join, declined side-local conjunct → N-scan fallback | ✓ `PUSHDOWN_SQL` shows `LHS_T0`/`LHS_T1` two-leg N-scan wrapper (no common-blob broadcast join block), outer `WHERE ((SECOND("LHS_T0"."O_ORDERDATE", 3) = 0))` |
| Three-table N-scan join, mixed rendering + declined conjuncts → residual routing | ✓ 3-leg wrapper (`LHS_T0`/`LHS_T1`/`LHS_T2`); `fact_orders` leg (`LHS_T1`) scan-spec `"filter"` carries only the rendering `DATE '2024-01-05' <= O_ORDERDATE` conjunct; outer `WHERE` carries only the declined `SECOND("LHS_T1"."O_ORDERDATE", 3) = 0` conjunct |
| Both-dialects-unrenderable predicate (`CAST(... AS HASHTYPE)`) → clean terminal error | ✓ SQL state 22002, error text: `join pushdown declined: a declined WHERE predicate could be rendered by neither dialect, so it could be applied nowhere: {...predicate JSON...}`; no rows; no `minioadmin` credential leak in the message |
| CLAUDE.md records the corrected protocol fact | ✓ `grep -n "never independently re-checks" CLAUDE.md` → exactly 1 match (CLAUDE.md:58), no issue number attached (plain fact, per plan) |
| Decline-path memory cost (`TEMP_DB_RAM_PEAK` via `EXA_DBA_AUDIT_SQL` after `FLUSH STATISTICS`) | ✓ measured — see Notes; declined-path query: 21.7 MiB / 3.995s, rendering-path query: 21.7 MiB / 3.002s (identical at this fixture's scale — see Notes for why no divergence was expected here) |

## Tool Evidence

### Linter

```
cargo clippy --all-targets --all-features
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 7.62s
```
0 warnings, 0 errors.

### Formatter

```
cargo fmt --check
(no output, exit 0)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| Single-table | pushdown-declined-filter-self-apply | Declined WHERE filter is self-applied in the adapter's own outer WHERE | `tests/e2e_capability_test.rs` | `e2e_declined_filter_second_arity_returns_filtered_rows` | Pass |
| Single-table | pushdown-declined-filter-self-apply | Declined WHERE filter is self-applied in the adapter's own outer WHERE | `tests/e2e_capability_test.rs` | `e2e_declined_filter_like_on_decimal_returns_filtered_rows` | Pass |
| Single-table | pushdown-declined-filter-self-apply | Declined filter applied before aggregation/grouping/truncation | `tests/e2e_capability_test.rs` | `e2e_declined_filter_under_aggregate_filters_before_aggregating` | Pass |
| Single-table | pushdown-declined-filter-self-apply | Declined filter applied before aggregation/grouping/truncation | `tests/e2e_capability_test.rs` | `e2e_declined_filter_under_order_by_limit_filters_before_truncating` | Pass |
| Single-table | pushdown-planning | Rendering filter keeps the wrapper-free fast path unchanged | `src/adapter/pushdown/dispatch_golden.rs` | `rendering_filter_emits_unchanged_wrapper_free_scan` | Pass |
| Single-table | pushdown-declined-filter-self-apply | Trivially-true filter still omitted, no wrapper | `src/adapter/pushdown/mod.rs` | `trivially_true_filter_omitted_without_wrapper` | Pass |
| Broadcast join | pushdown-planning-join | Broadcast-eligible join whose filter declines falls back to N-scan | `tests/e2e_join_test.rs` | `e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters` | Pass |
| Broadcast join | pushdown-planning-join | Broadcast-eligible join whose filter declines falls back to N-scan | `src/adapter/pushdown/joins/sql_builders.rs` | `broadcast_declines_on_unrenderable_filter_stays_eligible_when_absent` | Pass |
| Broadcast join | pushdown-planning-join | Declined filter excludes rows (row-level, discriminating) | `tests/e2e_join_test.rs` | `e2e_broadcast_declined_filter_excludes_rows` | Pass |
| N-scan join | pushdown-planning-join-fallback | Declined side-local conjunct becomes a residual outer-WHERE conjunct | `tests/e2e_join_test.rs` | `e2e_n_scan_declined_side_local_conjunct_applied_in_outer_where` | Pass |
| N-scan join | pushdown-planning-join-fallback | Declined side-local conjunct becomes a residual outer-WHERE conjunct | `src/adapter/pushdown/joins/rendering.rs` | `declined_side_local_conjunct_partitions_to_residual` | Pass |
| N-scan join | pushdown-planning-join-fallback | Render decline leaves each side's Iceberg manifest-pruning input unchanged | `src/adapter/pushdown/joins/rendering.rs` | `join_side_pruning_input_unchanged_when_df_render_declines` | Pass |
| N-scan join | pushdown-planning-join-fallback | Trivially-true residual emits no outer WHERE, no error | `src/adapter/pushdown/joins/sql_builders.rs` | `trivially_true_residual_emits_no_outer_where_and_does_not_error` | Pass |
| Terminal case | vs-expression-translator-cast | Predicate unrenderable under both dialects returns a clean, named error | `tests/e2e_capability_test.rs` | `e2e_both_dialects_unrenderable_predicate_errors_without_rows` | Pass |
| Cross-cutting | pushdown-declined-filter-self-apply | Absent filter distinguished from declined filter at every site | `src/adapter/pushdown/support.rs` | `datafusion_renderable_separates_absent_declined_and_trivially_true` | Pass |
| Cross-cutting | pushdown-declined-filter-self-apply | Absent filter distinguished from declined filter at every site | `src/adapter/pushdown/dispatch_golden.rs` | `filterless_request_emits_unchanged_sql_at_all_three_sites` | Pass |
| Single-table (CHANGED) | pushdown-declined-filter-self-apply | LIKE on DECIMAL declines the whole filter | `tests/e2e_capability_test.rs` | `e2e_declined_filter_like_on_decimal_returns_filtered_rows` | Pass |
| Single-table (CHANGED) | pushdown-declined-filter-self-apply | LIKE on integer column declines the whole filter | `src/adapter/pushdown/support.rs` | `declined_like_on_integer_column_routes_to_wrapper_where` | Pass |
| Single-table (CHANGED) | pushdown-declined-filter-self-apply | LIKE on unresolvable-type column declines the whole filter | `src/adapter/pushdown/support.rs` | `declined_like_on_unresolvable_column_routes_to_wrapper_where` | Pass |
| Single-table (CHANGED) | pushdown-declined-filter-self-apply | Nested non-string LIKE declines the entire enclosing filter | `src/adapter/pushdown/support.rs` | `nested_like_decline_routes_to_wrapper_where` | Pass |
| Single-table (CHANGED) | pushdown-declined-filter-self-apply | Non-coercible resolvable column type in a WHERE string function declines the whole filter | `tests/e2e_capability_test.rs` | `e2e_declined_filter_instr_three_arg_returns_filtered_rows` | Pass |
| Broadcast join (CHANGED) | pushdown-planning-join | Broadcast join projection/filter rendered per involved table | `tests/e2e_join_test.rs` | `e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters` | Pass |
| N-scan join (CHANGED) | pushdown-planning-join-fallback | Join conditions attach greedily by table-name set; side-local filters push into each leg | `tests/e2e_join_test.rs` | `e2e_n_scan_declined_side_local_conjunct_applied_in_outer_where` | Pass |
| Single-table (CHANGED) | pushdown | Untranslatable conjunct disables pruning for that conjunct only | `src/adapter/pushdown/mod.rs` | `iceberg_pruning_input_unchanged_when_df_render_declines` | Pass |
| Single-table (CHANGED) | pushdown | Filter predicate pushed into the scan spec or self-applied in the wrapper | `src/adapter/pushdown/mod.rs` | `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper` | Pass |
| Terminal case (CHANGED) | vs-expression-translator-cast | CAST renders the mapped target type per dialect | `tests/e2e_capability_test.rs` | `e2e_both_dialects_unrenderable_predicate_errors_without_rows` | Pass |
| Single-table (NEW) | pushdown-declined-filter-self-apply | Declined WHERE filter routes the single-table request to the qualified wrapper | `src/adapter/pushdown/mod.rs` | `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper` | Pass |
| Single-table (NEW) | pushdown-declined-filter-self-apply | Declined WHERE filter routes the single-table request to the qualified wrapper (absent select list) | `src/adapter/pushdown/mod.rs` | `declined_filter_with_absent_select_list_projects_full_row` | Pass |
| Single-table (NEW) | pushdown-declined-filter-self-apply | Declined WHERE filter routes the single-table request to the qualified wrapper (SELECT *) | `tests/e2e_capability_test.rs` | `e2e_declined_filter_select_star_returns_full_row_shape` | Pass |
| Single-table (NEW) | pushdown-declined-filter-self-apply | Declined WHERE filter under an aggregate is applied ahead of the aggregate | `tests/e2e_capability_test.rs` | `e2e_declined_filter_under_aggregate_filters_before_aggregating` | Pass |
| Single-table (NEW) | pushdown-declined-filter-self-apply | Declined single-table WHERE predicate is an Exasol-dialect wrapper position | `src/adapter/pushdown/joins/sql_builders.rs` | `single_table_wrapper_renders_declined_predicate_in_exasol_dialect` | Pass |
| Translator (NEW) | vs-expression | Refused argument count declines for DataFusion, renders for Exasol | `crates/vs-expression/src/lib.rs` | `second_with_precision_declines_for_datafusion_renders_for_exasol` | Pass |

## Notes

- **Review fixes**: all 10 code-review findings (8 standard, 2 expert) were fixed under `## Phase 4: Review Fixes` in `tasks.md` (tasks 4.1–4.10). No outstanding findings.
- **Golden-SQL stability**: `git diff -- crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` shows only new fixtures added (`filterless_broadcast_join.sql`, `filterless_n_scan_join.sql`, `filterless_single_table.sql`, `rendering_broadcast_join.sql`, `rendering_n_scan_join.sql`, `rendering_single_table.sql`); zero existing fixture changed — the wrapper-free fast path for a rendering filter is byte-for-byte unchanged.
- **Memory-cost manual test**: the declined-path and rendering-path queries measured identical `TEMP_DB_RAM_PEAK` (21.7 MiB) on the seeded `typed_distinct_probe` fixture (12 rows). This fixture is far too small to expose the scanned-vs-result-row scaling difference the plan's row anticipates (both queries fit well inside baseline UDF/session overhead) — the check confirms the declined path completes with bounded, non-runaway temp memory, not a demonstrated cost delta. A meaningful before/after cost-scaling comparison would need a much larger seeded table and is out of scope for this bug fix.
- **`e2e_both_dialects_unrenderable_predicate_errors_without_rows`** was tightened per review finding 4.4/4.5 area (standard fix, `tasks.md` 4.4) to assert the single phrase `"neither dialect"` rather than a loose HASHTYPE/`"applied nowhere"` OR; the live manual HASHTYPE-CAST check above independently confirms the exact phrase survives Exasol's error wrapping unmodified.
- No known issues or limitations remain open for this plan.
