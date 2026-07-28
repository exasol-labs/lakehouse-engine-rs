# Verification Report: refactor-pushdown-collect-walk-dedup

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All three duplicated collect-walks folded onto `walk_column_nodes`; both duplicated accessor/resolver pairs folded into one each. No behavior change: every golden-SQL string and expected test value is unedited. All gates green. |
| Code review | 3 findings — standard: 2, expert: 1 — all 3 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-musl-udf-build`, `rust:1.94-bookworm`, release profile, exit 0) |
| Tests | ✓ (828 passed, 0 failed, 2 pre-existing ignored) |
| Lint | ✓ (`cargo clippy --all-targets`, 0 warnings) |
| Format | ✓ (`cargo fmt --check`, no diffs) |
| Scenario Coverage | ✓ (all listed scenarios have a passing test) |
| Manual Tests | ✓ (all grep/count checks match expected values) |

## Test Evidence

### Coverage

This is a pure structural refactor; no new production branches were introduced apart from the new `walk_column_nodes` primitive, which carries 2 dedicated unit tests (the plan's only new tests). All pre-existing scenarios are exercised by the pre-existing suite, unedited.

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (lib) | `cargo test` (lakehouse-engine + vs-expression lib targets) | 676 | 0 |
| Integration | `cargo test` (all `tests/*.rs` binaries) | 152 | 2 (pre-existing, unrelated) |
| **Total** | `cargo test` | **828** | **2** |

0 failures across the full run, both before and after the review-fix round.

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine golden_` — all 4 join golden assertions | ✓ |
| `cargo test -p lakehouse-engine walk_column_nodes` — both new primitive tests | ✓ |
| `cargo test -p lakehouse-engine adapter::pushdown` — 0 failures | ✓ |
| `grep -c 'Json::Array' joins/rendering.rs` → `2` (down from 4) | ✓ |
| `grep -A 12 'fn collect_column_tables\|fn collect_side_column_names' ... \| grep -c 'Json::'` → `0` | ✓ |
| `grep -rn 'fn walk_column_nodes' crates/` → exactly one definition | ✓ |
| `cargo test -p lakehouse-engine adapter::tests` — 10 resolver tests, names/values unchanged | ✓ |
| `cargo test -p lakehouse-engine connection` — credential suite | ✓ |
| `grep -rn 'fn str_prop\|fn str_field\|fn resolve_df_target_partitions\|fn resolve_df_threads_per_udf' crates/` → no hits | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets
    Checking lakehouse-engine v0.30.9 (.../crates/lakehouse-engine)
    Checking vs-expression v0.2.0 (.../crates/vs-expression)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 5.78s
EXIT:0
```

### Formatter

```
cargo fmt --check
FMT_EXIT:0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-module-structure | Primitive contract — visits every nested column node once | `adapter/pushdown/support.rs` | `walk_column_nodes_visits_every_nested_column_node_once` (new) | Pass |
| vs-adapter | pushdown-module-structure | Primitive contract — no-op on a non-container root (review-added) | `adapter/pushdown/support.rs` | `walk_column_nodes_never_invokes_callback_for_a_non_container_root` (new) | Pass |
| vs-adapter | pushdown-module-structure | `collect_column_tables` side attribution | `adapter/pushdown/joins/sql_builders.rs` | `golden_n_scan_join_sql_unchanged` | Pass |
| vs-adapter | pushdown-module-structure | `collect_side_column_names` per-side narrowing | `adapter/pushdown/joins/rendering.rs` | `referenced_side_columns_narrows_to_used_columns`, `referenced_side_columns_keeps_all_when_select_list_absent` | Pass |
| vs-adapter | pushdown-module-structure | `collect_all_column_names` wrapper projection | `adapter/pushdown/dispatch_golden.rs` | `group_by_fallback_matches_golden`, `multi_count_distinct_decline_matches_golden` | Pass |
| vs-adapter | pushdown-module-structure | `collect_all_column_names` hidden-column append order | `adapter/pushdown/topn.rs` | `declined_order_by_expression_appends_referenced_columns_as_hidden`, `declined_order_by_two_expression_keys_renders_both_and_leaks_none` | Pass |
| vs-adapter | pushdown-module-structure | Broadcast and grouped-qualified join SQL | `adapter/pushdown/joins/sql_builders.rs` | `golden_broadcast_join_sql_unchanged`, `golden_grouped_qualified_fallback_sql_unchanged` | Pass |
| vs-adapter | adapter-module-structure | `nonempty_str` — property side | `adapter/mod.rs` | four empty-string-to-default property tests + `set_properties` merge tests | Pass |
| vs-adapter | adapter-module-structure | `nonempty_str` — credential side | `adapter/connection.rs` | `parse_creds`/`read_connection` credential-field suite | Pass |
| vs-adapter | adapter-module-structure | `resolve_df_fixed_count` — both FIXED-mode properties | `adapter/mod.rs` | 10 resolver tests (`df_target_partitions_*`, `df_threads_per_udf_*`) | Pass |

## Notes

- **Code review**: 3 findings, all fixed.
  - Standard: `nonempty_str`'s parameter renamed `props` → `obj` (it now serves both property and credential JSON, not just properties).
  - Standard: added `walk_column_nodes_never_invokes_callback_for_a_non_container_root`, pinning the primitive's no-op behavior on a Null/scalar/empty-object root — a shape production reaches unguarded via `groupBy`/`orderBy`/`selectList`, but which the plan's original test fixture never exercised.
  - Expert: appended a paragraph to `walk_column_nodes`'s doc comment documenting the deliberate, unenforceable-by-test case-folding divergence between `collect_all_column_names` (Unicode `to_uppercase`) and the two joins closures (`to_ascii_uppercase`), so a future reader doesn't "clean up" the disagreement into a silent behavior change.
- No golden SQL string, expected test value, or assertion message was edited anywhere in the diff — confirmed both by the reviewer and by an independent `git diff` grep for `golden|SELECT |expected` (empty).
- `resolve_s3_max_connections` and `auto_threads_per_udf` were left untouched apart from doc-comment cross-reference renames, per the plan's explicit descope.
- Ready for `/speq:record refactor-pushdown-collect-walk-dedup`.
