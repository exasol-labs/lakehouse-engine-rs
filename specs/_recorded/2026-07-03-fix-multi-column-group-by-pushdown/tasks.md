# Tasks: fix-multi-column-group-by-pushdown

## Phase 2: Implementation

### Group A (capability + docs — parallel with 2.1)
- [x] 1.1 Add `"AGGREGATE_GROUP_BY_TUPLE"` to `CAPABILITIES` const array (capabilities.rs ~L132-134); update adjacent comment
- [x] 4.1 Update `docs/capabilities.md`: move `AGGREGATE_GROUP_BY_TUPLE` from unsupported (~L71) into supported "Group by" row (~L56)

### Verify N-key path (expert — blocks Group B and Group C)
- [x] 2.1 Spike/verify N≥2 group-key path across `detect_group_by_aggregates`, `group_key_exasol_types`, `build_grouped_aggregate_scan_sql`; fix real bugs found (pushdown.rs) [expert]

### Re-purpose capability tests (expert — after 1.1)
- [x] 1.2 Re-purpose the three capability tests asserting TUPLE absence to assert presence AND backed-by-multi-key-path invariant (capabilities.rs) [expert]

### Group B (unit tests — after 2.1)
- [x] 2.2 Unit test: all-expression multi-key tuple detected/rendered per element; one untranslatable element forces full fallback (pushdown.rs)
- [x] 2.3 Unit test: mixed-type multi-key result-type mapping resolves each `GK_{i}` by its own select index (DECIMAL + VARCHAR) (pushdown.rs)
- [x] 2.4 Unit test: multi-key grouped SQL build with HAVING+LIMIT places both only in outer wrapper (pushdown.rs)

### Group C (E2E — after 1.1 and 2.1)
- [x] 3.1 Add `EXPLAIN VIRTUAL` pushdown-occurred assertion to `test_group_by_interleaved_multi_key` (e2e_scan_test.rs)
- [x] 3.2 Add same pushdown-occurred assertion to `test_group_by_multi_key_with_filter` (e2e_scan_test.rs)
- [x] 3.3 New E2E test: expression-valued multi-key tuple GROUP BY, correct results + per-key types + EXPLAIN-verified pushdown (e2e_scan_test.rs)
- [x] 3.4 New E2E test: multi-key GROUP BY with HAVING+LIMIT (outer wrapper) (e2e_scan_test.rs)
- [x] 3.5 New E2E test: high-cardinality multi-key GROUP BY completes under bounded pool (e2e_scan_test.rs)

## Phase 4: Code Review
- [x] 4.2 Review all changed files
- [x] 4.3 Fix review findings: add pushdown assertion to HAVING+LIMIT E2E test; fix non-interpolating `.expect()` strings in pushdown unit test; soften expr-tuple E2E type comment

## Phase 4: Code Review (cont.)
- [x] 4.4 Fix 2 E2E failures (test-code defects, feature is correct): (a) `assert_group_by_pushed_down` helper wrongly requires `GROUP BY shard_key` (absent on single-shard/filter-pruned) — use scan-spec `group_keys` discriminator; (b) `test_group_by_expr_multi_key_tuple` used unpushable CAST+division key — switch to advertised-function expression tuple [expert]

## Phase 5: Verification
- [x] 5.1 Build (`make cross-musl-udf-build`) — .so built at 0.21.0
- [x] 5.2 Test (`cargo test`) — 351+ passed, 0 failed
- [x] 5.3 Lint (`cargo clippy --all-targets`) — clean
- [x] 5.4 Format (`cargo fmt --check`) — clean
- [x] 5.5 E2E (`make test-e2e`) — exit 0: 36 passed (e2e_scan_test) + 7 passed (e2e_capability_test), 0 failed
