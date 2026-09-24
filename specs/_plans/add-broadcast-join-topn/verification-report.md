# Verification Report: add-broadcast-join-topn

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | A zero-offset `ORDER BY … LIMIT n` over a broadcast-eligible join now bounds each shard to its own post-join top-`n`. Build, unit/integration tests, lint, format, and the live E2E suite are all green. |
| Code review | 13 findings — 13 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (covered by the live E2E suite; see Notes) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (`cargo test --workspace`) | 1804 | 1802 | 2 |
| E2E (`make test-e2e`, live Docker Exasol) | 382 | 382 | 0 |

Full logs: `target/speq-cargo-test.log`, `target/speq-clippy.log`, `target/speq-fmt.log`, `target/speq-e2e.log`.

## Tool Evidence

### Linter

```
cargo clippy --all-targets
Finished `dev` profile [unoptimized + debuginfo] target(s) in 5.79s
EXIT:0
```

### Formatter

```
cargo fmt --check
EXIT:0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-join | A pushed ordering over a broadcast-eligible join is served by an outer wrapper over the broadcast fan-out | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `broadcast_ordered_zero_offset_limit_bounds_each_shard_to_its_top_n`, `broadcast_ordered_renders_limit_and_offset_on_the_wrapper_only`, `broadcast_ordered_plan_rendering_no_order_by_is_a_programming_error` | Pass |
| vs-adapter | pushdown-planning-join | A pushed ordering over a broadcast-eligible join is served by an outer wrapper over the broadcast fan-out | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_order_by_limit_stays_broadcast_and_top_n_correct` | Pass |
| vs-adapter | pushdown-planning-join | A zero-offset ORDER BY with LIMIT over a broadcast-eligible join bounds each shard to its own post-join top-N | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `broadcast_ordered_zero_offset_limit_bounds_each_shard_to_its_top_n`, `broadcast_ordered_bounds_shards_with_keys_from_either_side`, `broadcast_zero_offset_request_takes_the_bounded_path` | Pass |
| vs-adapter | pushdown-planning-join | A zero-offset ORDER BY with LIMIT over a broadcast-eligible join bounds each shard to its own post-join top-N | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_order_by_limit_stays_broadcast_and_top_n_correct`, `e2e_broadcast_join_top_n_ranks_empty_string_as_null` | Pass |
| vs-adapter | pushdown-planning-join | An ordered broadcast window with a non-zero offset or no LIMIT leaves every shard unbounded | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `broadcast_ordered_without_limit_wraps_fan_out_with_no_window`, `broadcast_ordered_renders_limit_and_offset_on_the_wrapper_only` | Pass |
| vs-adapter | pushdown-planning-join | An ordered broadcast window with a non-zero offset or no LIMIT leaves every shard unbounded | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_order_by_without_limit_and_with_offset_stay_broadcast` | Pass |
| datafusion-scan | scan-execution-join | A post-join ordering and cap bound each join shard to its own top-N over the joined output | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_order_by_limit_emits_bounded_top_n_after_join` | Pass |
| datafusion-scan | scan-execution-join | A join shard ranks each sort key by the value it emits | `crates/lakehouse-engine/tests/scan_join_test.rs` | `join_top_n_ranks_an_empty_string_key_as_null` | Pass |
| datafusion-scan | scan-execution-join | A join shard ranks each sort key by the value it emits | `crates/lakehouse-engine/src/scan/join_scan_tests.rs` | `build_join_sql_ranks_a_json_rendered_key_by_its_emitted_text`, `build_join_sql_ranks_a_nan_float_key_as_null`, `build_join_sql_ranks_a_cast_fallback_key_by_its_emitted_text` | Pass |
| datafusion-scan | scan-execution-join | A join shard ranks each sort key by the value it emits | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_top_n_ranks_empty_string_as_null` | Pass |
| datafusion-scan | scan-execution-join | A join block without a post-join ordering keeps its encoding | `crates/lakehouse-engine/src/scan/spec_tests.rs`, `crates/lakehouse-engine/src/scan/join_scan_tests.rs` | `join_spec_omitting_post_join_order_by_deserializes_to_empty`, `build_join_sql_renders_no_order_by_without_a_post_join_ordering` | Pass |

The build-fix pass (task 4.12) also split out `e2e_broadcast_join_top_n_keeps_the_single_node_sort_keys_across_a_tie` from the plan's tie-case assertion block, covering the same scenario with a dedicated E2E test.

## Notes

- Group A (join scan post-join top-N, tasks 2.1-2.7) and Group B (broadcast planner, tasks 2.8-2.16) both landed clean, in sequence as planned.
- Code review found 13 standard findings (0 expert): a duplicated Arrow-type classification (now reuses `classify_exa_type`), a missing CAST-fallback ranking test, stale/duplicated doc comments across 3 files, a positional 4-tuple replaced with a named `BroadcastWindowPlacement` struct to remove a silent-swap risk, test fixture de-duplication (Rule of Three), a duplicate E2E helper function, a redundant query re-execution, an inline tie-case block promoted to its own named test, and a redundant constant. All 13 were applied and reverified (unit tests, clippy, fmt all still clean afterward).
- Manual Testing steps from the plan (`EXPLAIN VIRTUAL` against a live `exapump` session) are subsumed by the E2E suite's live-Exasol assertions on pushed SQL shape (`"post_join_order_by"`, `"post_join_limit"`, exact `LIMIT`/`ORDER BY` tails) in `e2e_join_test.rs`, run against the same Docker Exasol stack the manual steps target.
- No deviation from the plan's design. `binds_to_projection` and the superseded unbounded-shard test were removed as planned (Dead Code Removal section), not left behind.
