# Verification Report: fix-broadcast-join-limit-suppression

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | A bare `LIMIT` and a bare-column `ORDER BY` over a broadcast-eligible inner equi-join now stay broadcast; the bare-`LIMIT` case caps each shard's joined output at `n` via `JoinSpec.post_join_limit`. All suites green against the live Exasol Docker stack. |
| Code review | 3 findings — 3 fixed (all format/clippy-gate hygiene; core logic passed clean) |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + host integration (`cargo test`) | 1093 | 1091 | 2 |
| E2E (`make test-e2e`, live Exasol/MinIO/Iceberg-REST) | 244 | 244 | 0 |

E2E per-binary: `e2e_scan_test` 62, `e2e_capability_test` 77, `e2e_count_distinct_test` 19, `e2e_join_test` 29, `e2e_positional_deletes_test` 18, `e2e_int96_timestamp_test` 9, `e2e_refresh_test` 13, `e2e_non_ascii_identifier_test` 8, `e2e_harness_row_cap_test` 9 — all `0 failed`.

The 2 ignored unit tests are pre-existing and unrelated to this plan.

### Manual Tests

The plan's Manual Testing table lists `EXPLAIN VIRTUAL` / result checks against a live cluster. Each is covered by an automated E2E test asserting the same plan shape (`has_broadcast_join_block` / `has_two_scan_wrapper`) and result, which is stronger evidence than a one-off manual run.

| Manual scenario | Covering E2E test | Result |
|------|------|--------|
| Bare `LIMIT` join → broadcast block, ` LIMIT 3`, 3 rows from the join | `e2e_broadcast_join_bare_limit_stays_broadcast_and_truncates` | ✓ |
| `ORDER BY … LIMIT` join → broadcast block, exact top-N | `e2e_broadcast_join_order_by_limit_stays_broadcast_and_top_n_correct` | ✓ |
| Bare `ORDER BY` (no LIMIT) and `ORDER BY … LIMIT … OFFSET` → broadcast block, exact window | `e2e_broadcast_join_order_by_without_limit_and_with_offset_stay_broadcast` | ✓ |
| `COUNT(*)` over a join and `LIMIT … OFFSET` (no ORDER BY) → two-scan fallback | `e2e_join_offset_and_aggregate_shapes_still_use_two_scan_fallback` | ✓ |
| Scan reads the cap from the join block, not the input | `join_limit_bounds_joined_output_not_scanned_input` | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets  →  exit 0, 0 warnings (workspace-wide)
```

### Formatter

```
cargo fmt --all --check  →  exit 0, no diff
```

### Build

```
make cross-musl-udf-build  →  exit 0 (release .so, glibc 2.36 SLC-matched)
speq plan validate fix-broadcast-join-limit-suppression  →  exit 0 (only non-blocking AND-step style warnings)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning-join | Bare LIMIT served by broadcast with per-shard post-join cap | `tests/e2e_join_test.rs` | `e2e_broadcast_join_bare_limit_stays_broadcast_and_truncates` | Pass |
| vs-adapter | pushdown-planning-join | Bare LIMIT served by broadcast with per-shard post-join cap | `joins/sql_builders_tests.rs` | `broadcast_bare_limit_caps_each_shard_and_the_merge` | Pass |
| vs-adapter | pushdown-planning-join | Pushed ordering served by outer wrapper | `tests/e2e_join_test.rs` | `e2e_broadcast_join_order_by_limit_stays_broadcast_and_top_n_correct` | Pass |
| vs-adapter | pushdown-planning-join | Pushed ordering served by outer wrapper | `joins/sql_builders_tests.rs` | `broadcast_ordered_wraps_fan_out_and_leaves_shards_unbounded` | Pass |
| vs-adapter | pushdown-planning-join | Bare ORDER BY, no LIMIT | `joins/sql_builders_tests.rs` | `broadcast_ordered_without_limit_wraps_fan_out_with_no_window` | Pass |
| vs-adapter | pushdown-planning-join | ORDER BY + LIMIT + OFFSET on wrapper only | `joins/sql_builders_tests.rs` | `broadcast_ordered_renders_limit_and_offset_on_the_wrapper_only` | Pass |
| vs-adapter | pushdown-planning-join | Bare ORDER BY and ORDER BY+LIMIT+OFFSET stay broadcast (live) | `tests/e2e_join_test.rs` | `e2e_broadcast_join_order_by_without_limit_and_with_offset_stay_broadcast` | Pass |
| vs-adapter | pushdown-planning-join | Wrapper must render an ORDER BY (programming error otherwise) | `joins/sql_builders_tests.rs` | `broadcast_ordered_plan_rendering_no_order_by_is_a_programming_error` | Pass |
| vs-adapter | pushdown-planning-join-fallback | Every forcing/served shape classified | `joins/sql_builders_tests.rs` | `join_window_classification_covers_every_forcing_and_served_shape` | Pass |
| vs-adapter | pushdown-planning-join-fallback | Request-shape arms evaluated before the render | `joins/sql_builders_tests.rs` | `aggregate_over_join_classifies_before_the_render_that_would_error` | Pass |
| vs-adapter | pushdown-planning-join-fallback | Unprojected sort key downgraded at construction site | `joins/sql_builders_tests.rs` | `broadcast_ordered_unprojected_key_downgrades_to_the_fallback` | Pass |
| vs-adapter | pushdown-planning-join-fallback | Offset/aggregate shapes still use two-scan fallback | `tests/e2e_join_test.rs` | `e2e_join_offset_and_aggregate_shapes_still_use_two_scan_fallback` | Pass |
| vs-adapter | pushdown-planning-join-fallback | Aggregate over a join routes through unified wrapper | `joins/sql_builders_tests.rs` | `aggregate_over_join_renders_exasol_aggregate_over_unified_wrapper` | Pass |
| vs-adapter | pushdown-planning-join-fallback | Fallback leg spec never carries a limit/sort/join block | `joins/sql_builders_tests.rs` | `fallback_leg_fan_out_spec_never_carries_a_limit_or_sort` | Pass |
| datafusion-scan | scan-execution-join | LIMIT bounds joined output, not scanned input | `tests/scan_join_test.rs` | `join_limit_bounds_joined_output_not_scanned_input` | Pass |
| datafusion-scan | scan-execution-join | Cap wire field additive and defaulted | `scan/spec_tests.rs` | `join_spec_omitting_post_join_limit_deserializes_to_none` | Pass |

## Notes

- **Design realized as planned.** The per-shard cap lives on `JoinSpec.post_join_limit`, not `CommonScanSpec.limit`; the N-scan leg builder constructs no join block, so it structurally cannot express one. `common.limit` is now read on no join path. The reviewer verified `carries_aggregation_clause` preserves the deleted boolean's four forcing conditions byte-for-byte, and that unordered broadcast SQL stays byte-identical (golden tests plus an explicit diff assertion).
- **Two faithful adaptations of the plan's literal wording:**
  - `aggregate_over_join_classifies_before_the_render_that_would_error` was authored in `planning_tests.rs` by task 1, then relocated to `joins/sql_builders_tests.rs` to match the plan's Verification table.
  - `e2e_join_offset_and_aggregate_shapes_still_use_two_scan_fallback`'s offset-without-ORDER-BY arm asserts Exasol's grammar rejection (`sqlCode 42000` — `OFFSET` requires `ORDER BY`) rather than an `EXPLAIN VIRTUAL` plan shape, because that query never reaches the adapter. Verified against the order-by-capability and join-fallback specs.
- **One stale reference beyond the plan's enumerated sites** was fixed: `docs/debugging-pushdown.md` still named the deleted `join_requires_exasol_postprocessing` and stated the reversed "any limit disqualifies broadcast" claim. Rewritten to match post-fix behavior (bare `LIMIT`/`ORDER BY` stay broadcast; only aggregate/GROUP BY/HAVING fall back).
- The deleted test `e2e_broadcast_declined_by_explicit_limit_falls_back_to_n_scan` is confirmed absent from the E2E run.
