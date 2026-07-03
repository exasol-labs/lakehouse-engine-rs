# Verification Report: fix-nested-aggregate-pushdown

**Generated:** 2026-07-03

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Issue #52 fixed: outer `COUNT(*)` over an inner `GROUP BY` no longer crashes with `Schema error: No field named "NULL"`; it now returns the correct distinct-group count via a corrected grouped scan. All host unit tests, clippy, fmt, and the full local-Docker E2E suite are green. |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (host `cargo test`) | ✓ (416 passed, 2 ignored) |
| Tests (`make test-e2e`) | ✓ (41 passed: 7 capability + 34 scan) |
| Lint (`cargo clippy --all-targets`) | ✓ (no issues) |
| Format (`cargo fmt --check`) | ✓ (clean) |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (host, `cargo test`) | 16 suites | 416 | 2 (pre-existing, unrelated) |
| E2E (`make test-e2e`, local Docker + MinIO + Iceberg REST) | 2 suites | 41 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT COUNT(*) FROM (SELECT id, COUNT(*) FROM events GROUP BY id) t` (unique-key case, expect 20) | ✓ |
| `SELECT COUNT(*) FROM (SELECT MOD(id,4) k, COUNT(*) FROM events GROUP BY MOD(id,4)) t` (duplicate-key discriminator, expect 4 not 20) | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets
No issues found
```

### Formatter

```
cargo fmt --check
(no output — clean)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-planning | Composed pushdown request never renders a scan spec that references a non-source column | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `composed_nested_aggregate_request_does_not_reference_phantom_column` | Pass |
| vs-adapter | pushdown-planning | Bare `literal_bool` selectList item classifies as Constant, not group-key (review-fix follow-up) | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `literal_bool_selectlist_item_classifies_as_constant_not_group_key` | Pass |
| packaging | e2e-harness | End-to-end nested aggregate over a grouped sub-select returns the correct outer count (incl. duplicate-key discriminator) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_nested_aggregate_over_grouped_subselect_returns_correct_count` | Pass |

## Notes

- **Root cause** (decision-log entries [1], [4], [4a]): Exasol does not send a nested sub-select for `COUNT(*) FROM (SELECT k, COUNT(*) FROM t GROUP BY k)`. It sends one flat `pushdownRequest` with `aggregationType=group_by`, a real `groupBy`, and a `selectList` containing only a bare literal (its own "count the groups" rewrite — it doesn't need the inner columns, just row-per-group). The adapter's `detect_group_by_aggregates` didn't recognize a bare-literal selectList item, aborted grouped-aggregate detection, and fell through to a row-scan path that pushed the rendered literal `"NULL"` in as a projection column name — which DataFusion rejected as an unknown identifier.
- **Fix (family (a), correct-parsing):** `crates/lakehouse-engine/src/adapter/pushdown.rs` — a new `Constant` variant on `GroupedSelectItem`, a shared `is_literal_selectlist_item` helper (covering all 8 literal node types the `vs-expression` renderer supports, including `literal_bool` added during code review), and a `constant_projection_sql` helper that projects a correctly-typed constant over the still-grouped scan. The emitted `ScanSpec` now has real `group_keys` and an empty aggregate `plans` list, so Exasol's own outer `COUNT(*)` counts actual groups. A defence-in-depth guard in `extract_projection` prevents a bare literal from ever being pushed into a row-scan projection's column-name list.
- **Fix family (b) (fallback-to-row-scan) was explicitly rejected**: it would return one row per source row, not per group — coincidentally correct on the seeded `events` table (unique `id`) but silently wrong on any duplicate-key group column (e.g. the issue's actual `LINEITEM.L_ORDERKEY`). The E2E test's `MOD(id,4)` duplicate-key case (expect 4, not 20) is the regression guard that actually discriminates the correct fix from that unsafe fallback.
- One code-review finding was fixed before this report: the initial literal-type list omitted `literal_bool`; it's now included via a single shared helper so the two call sites (`detect_group_by_aggregates`, `extract_projection`) can't drift apart again.
- No temporary diagnostic instrumentation from the Task 1 spike remains in the tree (confirmed by the reviewer and by `git diff`/`grep`).
- Live-cluster (AWS Glue) re-verification was out of scope per decision-log [5] (treated as generic Exasol pushdown-composition behavior, reproducible on local Docker) — flagged here again in case the user wants it before closing #52.
