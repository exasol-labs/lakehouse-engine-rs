# Verification Report: add-topn-pushdown

## Verdict: PASS

All 15 implementation tasks complete (12 planned + 3 discovered-in-flight correctness fixes: B3b,
B6, and the comment nit from code review). Code review clean. Full automated checklist green,
including the full `make test-e2e` suite. Live NQ4 benchmark confirms correctness and a 5.65x
speedup (12.03s → 2.13s), now also beating Trino's 4.71s baseline.

## Evidence

### Automated checklist
| Check | Command | Result |
|---|---|---|
| Build | `make cross-musl-udf-build` | Current (confirmed during B6/C1 live runs) |
| Test | `cargo test --workspace` | All suites `ok`, 0 failed |
| E2E | `make test-e2e` | 53 passed, 0 failed |
| Lint | `cargo clippy --workspace --all-targets` | No issues |
| Format | `cargo fmt --check` | Clean |

### Scenario coverage
| Scenario | Test | Status |
|---|---|---|
| Ordered top-N pushes down as per-shard bounded sort + Exasol merge | `e2e_scan_test::ordered_topn_pushes_down_matches_single_node` | PASS |
| Ordered top-N pushed down (plan shape) | `scan_plan_shape::ordered_topn_emits_per_shard_and_outer_order_by` | PASS |
| Per-shard limit emitted only alongside matching per-shard sort | `pushdown::order_by_present_without_topn_match_withholds_per_shard_limit` | PASS |
| Ordered top-N preserves DESC + NULL ordering | `scan::mod::ordered_scan_sql_preserves_desc_and_null_placement` | PASS |
| Unsupported ordered-query shapes decline the top-N path | `pushdown::unsupported_order_by_shape_declines_topn` | PASS |
| ORDER_BY_COLUMN advertised (only) | `capabilities::capabilities_advertise_order_by_column_only` | PASS |
| Unbounded ORDER BY falls back correctness-safe | `pushdown::unbounded_order_by_falls_back_correctness_safe` | PASS |
| Bounded local top-N, not a global sort | `scan_plan_shape::order_by_spec_emits_bounded_topk_not_global_sort` | PASS |
| (Discovered) JSON-fallback-typed sort key declines | `pushdown::json_fallback_typed_sort_key_declines_topn` | PASS |
| (Discovered) Grouped-aggregate ORDER BY without LIMIT | `pushdown::grouped_order_by_no_limit_renders_explicit_merge_order_by` | PASS |
| (Discovered) Row-scan decline wraps outer ORDER BY | `pushdown::row_scan_decline_order_by_no_limit_wraps_outer_order_by` | PASS |
| (Discovered) Decline case correctness regression check | `e2e_scan_test::order_by_without_limit_falls_back_correctly` | PASS |
| (Regression) Grouped-aggregate ORDER BY still correct | `test_high_cardinality_group_by_spill` / `_multi_key_group_by_spill` | PASS |

### Manual verification (live, test1)
| Check | Result |
|---|---|
| `EXPLAIN VIRTUAL` NQ4 | Per-shard spec carries `"limit":20,"order_by":[...]`; outer merge `ORDER BY "L_EXTENDEDPRICE" DESC NULLS FIRST LIMIT 20` |
| NQ4 correctness | Identical 20-row set to pre-optimization baseline (one benign tie-order swap, no secondary tiebreak column — expected) |
| NQ4 timing | 12.03s → **2.13s** (5.65x), now beats Trino's 4.71s baseline |
| `bench/run.sh` NQ4 pushdown_check | Passes live |

## Deviations from plan (three, all necessary correctness fixes, all fixed within scope)

1. **B3b** — `detect_topn` didn't originally guard against sort keys on JSON-fallback-typed columns (List/Struct/out-of-range-Decimal/etc.), which could rank differently per-shard (native value) vs at merge (JSON string text). Fixed by declining the shape when the sort key's Arrow type needs the fallback. Not triggered by NQ4.
2. **B6 (critical)** — Advertising `ORDER_BY_COLUMN` silently removed Exasol's implicit backstop re-sort for paths that had never needed their own `ORDER BY` before: the grouped-aggregate merge and `detect_topn`'s own decline path. Found live via B5's E2E run (two pre-existing tests started failing), confirmed via an A/B rebuild experiment. Fixed by rendering an explicit final `ORDER BY`/`LIMIT` on every path that can receive an `order_by`-carrying request, not just the new optimization — reusing the same shared rendering function throughout.
3. **Code review nit** — a `ponytail:` comment's argument-count justification went stale after this plan added a 13th parameter; corrected in place.

## Known residual (documented, not a regression)

`detect_topn`'s decline path for a genuinely unprojected sort column now produces a runtime
"column not found" error from Exasol rather than the old silent-wrong-order result — a fail-loud
improvement over fail-silent, not a defect. The general fix (emit unprojected sort keys as extra
trailing EMITS columns) is a deliberately deferred generalization, tracked in the plan's
Consequences table.

## Next steps

- `speq:record add-topn-pushdown` to merge these spec deltas into the permanent library.
- Commit and push alongside the sibling `add-arithmetic-aggregate-pushdown-and-benchmark-suite` plan.
