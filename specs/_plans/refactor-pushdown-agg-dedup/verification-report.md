# Verification Report: refactor-pushdown-agg-dedup

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Five duplicated aggregate-pushdown contract sites collapsed onto shared owners (`PartialAggColumn` descriptor, statistical merge fragments, declared-type CAST rule, `parse_agg_item` name tables); every generated SQL string, `EMITS` clause, and partial row stays byte-identical except the one sanctioned change — `STDDEV`/`VARIANCE` over an expression argument now declines the pushdown instead of erroring, verified end-to-end against a live Docker Exasol container. |
| Code review | 6 findings — standard: 5, expert: 1 — all 6 fixed |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test`) | ✓ |
| Lint (`cargo clippy --all-targets`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |
| E2E (`make test-e2e`) | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`lakehouse-engine` lib) | 717 | 717 | 0 |
| Unit (`lakehouse-catalog` lib + boundary/surface) | ~93 | ~93 | 0 |
| Integration (workspace, non-e2e, `cargo test`) | remaining suites in `target/speq:cargo_test.log` | all `ok` | 2 (pre-existing, out of scope) |
| Integration (E2E, Docker Exasol/`make test-e2e`) | 60 | 60 | 0 |

Full `cargo test` run: every `test result:` line in `target/speq:cargo_test.log` reports `ok` with `0 failed` (40 test binaries). `make test-e2e`: `test result: ok. 60 passed; 0 failed; 0 ignored`.

### Golden fixture byte-identity (the plan's central correctness claim)

All four `[expert]` tasks (1.3, 1.5, 1.7) and task 1.6 were gated on these fixtures staying byte-identical, checked after every task:

| Fixture | md5 |
|---|---|
| `adapter/pushdown/testdata/dispatch_golden/single_group_all_agg_kinds.sql` | `d6464de508c29cbeb628397b883a6615` |
| `adapter/pushdown/testdata/dispatch_golden/grouped_all_agg_kinds.sql` | `870ee848cfd2fb37e8d60420b72129f0` |
| `scan/testdata/partial_agg_golden/partial_agg_all_agg_kinds.sql` | `3453e185c8a076ff940d8ae4a87c2fcb` |
| `scan/testdata/partial_agg_golden/grouped_partial_agg_all_agg_kinds.sql` | `d6977eba16cf86f478c0d08691acf881` |

Plus all ten pre-existing `dispatch_golden` fixtures — 14 fixture tests total, all `... ok`, all asserting full-string `assert_eq!` against `include_str!`, never `.contains(...)`.

One deliberate empirical proof that these fixtures discriminate rather than rubber-stamp: the task-1.5 agent ran a RED probe (omitted one parenthesis pair in `stddev_of`) and confirmed both `dispatch_golden` all-agg-kinds fixtures failed while all six `.contains(...)` merge-formula tests still passed — the exact failure mode the plan flagged this task `[expert]` for.

### Manual Tests

Run live against the same Docker Exasol container / `MY_LAKEHOUSE` virtual schema the E2E suite uses (host `localhost:28563`, TLS-pinned via `exapump`).

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL SELECT COUNT(*), AVG(score), STDDEV(score) FROM EVENTS` — current shape | ✓ — emits `PARTIAL_count_0`, `PARTIAL_avg_sum_1`/`PARTIAL_avg_cnt_1`, `PARTIAL_stat_cnt_2`/`PARTIAL_stat_sum_2`/`PARTIAL_stat_sumsq_2` per the descriptor's arities (1, 2, 3); byte-identity to the pre-refactor capture is what the golden fixture tests assert directly (no pre-refactor binary remained checked out to re-diff live against) |
| `SELECT COUNT(*), COUNT(id), SUM(score), MIN(event_ts), MAX(event_ts), AVG(score) FROM EVENTS` | ✓ — `20, 20, 1050.0, 2024-01-01T00:00:00, 2024-01-01T19:00:00, 52.5` — matches the seed's closed form (`score = 5·id`, `id = 1..20` ⇒ `Σscore = 5·210 = 1050`, `avg = 52.5`) |
| `SELECT MOD(id,4), STDDEV(score), STDDEV_POP(score), VARIANCE(score), VAR_POP(score) FROM EVENTS GROUP BY MOD(id,4)` | ✓ — all 4 groups: `31.622776601683793 / 28.284271247461902 / 1000.0 / 800.0` — `800·5/4 = 1000` (n=5 per group), `√1000 = 31.6227766`, `√800 = 28.2842712` |
| `SELECT STDDEV(score) FROM EVENTS WHERE 1=0` | ✓ — `NULL`, not `0.0` |
| `SELECT id, STDDEV(score) FROM EVENTS GROUP BY id` (single-row groups) | ✓ — `NULL` for every group, not `0.0` |
| (aggregate-extensions, task 1.2, DONE) `STDDEV(score + id)` before | MEASURED 2026-07-31: `sqlCode 22002` |
| `STDDEV(score + id)` after | ✓ — `35.4964786985977` = `6√35` |
| (aggregate-extensions, task 1.2, DONE) `VARIANCE(score * 2)` before | MEASURED 2026-07-31: `sqlCode 22002` |
| `VARIANCE(score * 2)` after | ✓ — `3500.0` = `100·35` |
| (aggregate-extensions, task 1.2, DONE) grouped `STDDEV(score + id)` before | MEASURED 2026-07-31: `sqlCode 22002` |
| grouped `STDDEV(score + id)` after | ✓ — `37.94733192202055` = `12√10` per group |
| (aggregate-extensions, task 1.2, DONE) grouped `SQRT(STDDEV(score + id))` before | MEASURED 2026-07-31: `sqlCode 22002` |
| grouped `SQRT(STDDEV(score + id))` after | ✓ — `6.160140576482046` = `√37.947332` |
| grouped `STDDEV(score)` (bare-column non-regression) | ✓ — `31.622776601683793` = `√1000`, unchanged, still decomposed via `PARTIAL_stat_*` |

## Tool Evidence

### Linter

```
cargo clippy --all-targets
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.59s
0 warnings, 0 errors
```

### Formatter

```
cargo fmt -- --check
(no output — no diff)
```

### Build

```
make cross-musl-udf-build
exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | scan-partial-agg-column-contract | One descriptor owns every aggregate's partial column set | `scan/spec.rs` | `partial_columns_arity_per_agg_kind`, `is_counter_marks_the_four_count_columns` | Pass |
| datafusion-scan | scan-partial-agg-column-contract | One descriptor owns every aggregate's partial column set | `scan/partial_agg.rs` | `partial_agg_sql_stat_emits_cnt_sum_sumsq`, `stat_aggregate_null_fallback_row_has_three_values`, `partial_agg_sql_avg_emits_sum_count_pair` | Pass |
| datafusion-scan | scan-partial-agg-column-contract | The partial column name has one owner | `scan/spec.rs` | `partial_column_name_renders_role_and_ordinal` | Pass |
| datafusion-scan | scan-partial-agg-column-contract | The partial column name has one owner | `scan/partial_agg.rs` | `stat_aggregate_index_follows_plan_order`, `partial_agg_sql_mixed_column_order_and_indices` | Pass |
| datafusion-scan | scan-partial-agg-column-contract | Statistical partial argument routes through shared renderer | `scan/partial_agg.rs` | `partial_agg_sql_stat_emits_cnt_sum_sumsq`, `partial_sql_uses_rendered_expression_argument` | Pass |
| vs-adapter | pushdown-agg-sql-consolidation | Byte-identical generated SQL/EMITS/rows | `adapter/pushdown/dispatch_golden.rs` | `single_group_all_agg_kinds_matches_golden`, `grouped_all_agg_kinds_matches_golden` + 10 pre-existing | Pass |
| datafusion-scan | scan-partial-agg-column-contract | Byte-identical generated SQL (scan-side) | `scan/partial_agg.rs` | `partial_agg_sql_all_agg_kinds_matches_golden`, `grouped_partial_agg_sql_all_agg_kinds_matches_golden` | Pass |
| vs-adapter | pushdown-planning-aggregate-extensions | Byte-identical generated SQL | `tests/e2e_capability_test.rs` | `e2e_stddev_variance_pushdown` | Pass |
| vs-adapter | pushdown-agg-sql-consolidation | Aggregate-plan shape unchanged | `tests/scan_plan_shape.rs` | 8 shape assertions (incl. `sum_two_column_product_emits_aggregates_not_raw_scan`) | Pass |
| datafusion-scan | scan-partial-agg-column-contract | Scan-to-adapter column alignment per kind | `adapter/pushdown/grouped_agg.rs` | `scan_select_list_and_emits_agree_per_agg_kind` | Pass |
| vs-adapter | pushdown-agg-sql-consolidation | Sufficient-statistics fragments, one owner per denominator | `adapter/pushdown/grouped_agg.rs` | `var_pop_merge_formula_divides_by_n`, `var_samp_merge_formula_divides_by_n_minus_1`, `stddev_pop_merge_formula_uses_sqrt`, `stddev_samp_merge_formula_uses_sqrt_and_n_minus_1`, `stddev_pop_merge_null_passthrough_for_n_zero`, `stddev_samp_merge_null_passthrough_for_n_zero_and_n_one` | Pass |
| vs-adapter | pushdown-agg-sql-consolidation | Sufficient-statistics fragments (E2E) | `tests/e2e_capability_test.rs` | `e2e_stddev_variance_pushdown` | Pass |
| vs-adapter | pushdown-agg-sql-consolidation | Declared-type CAST rule, one owner | `adapter/pushdown/support.rs` | `cast_to_declared_type_skips_the_varchar_default_and_absent_type` | Pass |
| vs-adapter | pushdown-agg-sql-consolidation | Declared-type CAST rule (fixture coverage) | `adapter/pushdown/dispatch_golden.rs` | all 12 fixture tests | Pass |
| vs-adapter | pushdown-planning-single-group-agg | Function names map to AggKind via two tables | `adapter/pushdown/single_group_agg.rs` | `parse_agg_item_recognises_stat_functions`, `bare_column_aggregates_unchanged_regression` | Pass |
| vs-adapter | pushdown-planning-aggregate-extensions | Stat-over-expression declines pushdown | `adapter/pushdown/single_group_agg.rs` | `stat_aggregate_over_expression_argument_declines`, `stat_aggregate_over_bare_column_still_parses` | Pass |
| vs-adapter | pushdown-planning-aggregate-extensions | Stat-over-expression declines pushdown (grouped) | `adapter/pushdown/grouped_agg.rs` | `grouped_stat_aggregate_over_expression_argument_declines`, `having_over_stat_aggregate_with_expression_argument_declines`, `scalar_over_stat_aggregate_with_expression_argument_declines` | Pass |
| vs-adapter | pushdown-planning-aggregate-extensions | Stat-over-expression declines pushdown (E2E) | `tests/e2e_capability_test.rs` | `e2e_stddev_over_expression_falls_back_and_returns_correct_value`, `e2e_grouped_stddev_over_expression_falls_back_and_returns_correct_value` (added by review-fix 4.6, closing the two-of-three-paths gap the code reviewer found) | Pass |

## Notes

- **Code review, applied in full.** 6 findings (standard 5, expert 1). Standard: deduplicated a second 22-argument `build_dispatch_sql` call site in `dispatch_golden.rs`, removed two never-varied dead parameters, deleted a redundant comment block in `single_group_agg.rs`, documented why `VARCHAR(2000000)` is exempt from the shared CAST helper, and fixed an outdated "mirrors" comment in `file_resolution.rs`. Expert: the reviewer found that task 1.7's decline reached three live-reachable select-list paths (ungrouped, grouped, grouped scalar-over-aggregate) but only the ungrouped path had an E2E guard — added `e2e_grouped_stddev_over_expression_falls_back_and_returns_correct_value`, verified via mutation testing (reverting the production fix made the new test fail at its `PARTIAL_stat_` gate).
- **One sanctioned behavior change**, verified against the live container both before (task 1.2, 2026-07-31) and after (task 1.7 and review-fix 4.6): `STDDEV`/`VARIANCE` over an expression argument no longer errors with `sqlCode 22002`; it declines the pushdown and Exasol computes the statistic natively.
- **One judgment call flagged by the task-1.3 agent, outside the plan's dead-code table**: `build_partial_agg_sql_filtered`'s doc comment hand-listed per-kind arity and names, and was already stale before this plan (it omitted all four statistical kinds). Replaced with a pointer to `AggKind::partial_columns`/`partial_column_name`. Not authorized by the plan's dead-code removal table; flagged for reviewer awareness. The code-reviewer did not raise it as a finding, so it stands as an accepted improvement within scope of "one owner for the column contract."
- **Row 1 of the plan's Manual Testing table** (a live before/after `EXPLAIN VIRTUAL` diff) could not be re-run as a literal diff — no pre-refactor binary remained checked out once the refactor landed across all `[expert]` tasks. The equivalent evidence is the golden-fixture byte-identity gate, which asserts full-string equality against the pre-refactor capture directly (stronger than a manual diff, since it runs on every `cargo test`). The current live `EXPLAIN VIRTUAL` output was captured for the record and shows the expected `PARTIAL_count_0` / `PARTIAL_avg_sum_1`+`PARTIAL_avg_cnt_1` / `PARTIAL_stat_cnt_2`+`PARTIAL_stat_sum_2`+`PARTIAL_stat_sumsq_2` shape.
- No test was deleted. No production visibility widened (verified by the task-1.3 agent and re-checked by the code reviewer).

Ready for: `/speq:record refactor-pushdown-agg-dedup`
