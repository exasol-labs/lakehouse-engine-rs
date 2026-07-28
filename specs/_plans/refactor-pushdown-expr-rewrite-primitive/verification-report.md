# Verification Report: refactor-pushdown-expr-rewrite-primitive

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 11 plan tasks implemented, both commit gates green, code review findings fixed, full test/lint/format/UDF-build/E2E suite green. |
| Code review | 7 findings — standard: 7, expert: 0 — 7 fixed |

| Check | Status |
|-------|--------|
| Build (`cargo test` compile) | ✓ |
| Tests (`cargo test`) | ✓ |
| Lint (`cargo clippy --all-targets`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Census (`cargo test --features exasol-e2e --no-run`) | ✓ |
| Build (UDF `.so`, `make cross-musl-udf-build`) | ✓ |
| E2E (`make test-e2e`, live Exasol/MinIO/Iceberg stack) | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (with 1 noted plan-inconsistency, 3 rows not independently re-run — see Notes) |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit + integration (`cargo test`) | full workspace | 680 (lib) + integration binaries, all green | 0 |
| Feature-gated compile census (`cargo test --features exasol-e2e --no-run`) | compile only | compiles | 0 |
| E2E (`make test-e2e`, 7 binaries) | live stack | 50+16+7+15+16+11+59 = 174 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p lakehouse-engine --lib adapter::pushdown` — 0 failures, no edited assertion | ✓ (392 passed, 0 failed) |
| `git diff --stat` shows `support.rs` net line delta after commit 1 | ✗ as literally stated — see Notes |
| Exasol Docker stack + `make test-e2e` — 0 failures | ✓ (174 passed, 0 failed across 7 binaries) |
| Deployed-VS LIKE-in-CASE query over a DECIMAL subject (`L_QUANTITY`) | not independently re-run — see Notes |
| Deployed-VS LIKE-in-CASE query over a DATE subject (`L_SHIPDATE`) | not independently re-run — see Notes |
| Deployed-VS `UPPER(L_QUANTITY) = '17'` unchanged-behavior check | not independently re-run — see Notes |

## Tool Evidence

### Linter

```
cargo clippy --all-targets
(clean — 0 warnings, 0 errors)
```

### Formatter

```
cargo fmt --check
(clean — no diff)
```

### Build

```
make cross-musl-udf-build
Finished `release` profile [optimized] target(s) in 27m 42s
target/release/liblakehouse_engine.so (163.5M)
```

### E2E

```
make test-e2e
e2e_join_test:              50 passed; 0 failed
e2e_capability_test:        16 passed; 0 failed
e2e_count_distinct_test:     7 passed; 0 failed
e2e_int96_timestamp_test:   15 passed; 0 failed
e2e_positional_deletes_test:16 passed; 0 failed
e2e_refresh_test:           11 passed; 0 failed
e2e_scan_test:              59 passed; 0 failed
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-module-structure | Shared post-order primitive | `support.rs` | `decimal_rewrite_passes_through_non_object_node` + existing corpus (`rewrite_reaches_decimal_inside_case_then_branch`, `rewrite_nested_concat_wraps_only_inner_decimal`, `string_fn_guard_passes_through_non_object_node`, `string_fn_guard_reaches_function_under_comparison_predicate`, `string_fn_guard_nested_decline_propagates_to_root`, `string_fn_guard_coerces_inner_nested_string_function`) | Pass |
| vs-adapter | pushdown-module-structure | Byte-identity, rendered-SQL half | `mod.rs` | `where_filter_decimal_stringification_rewritten_to_trim`, `filter_decimal_comparison_not_rewritten`, `where_filter_string_fn_under_comparison_predicate_coerced`, `where_filter_string_fn_over_double_declines`, `where_filter_upper_decimal_inside_like_subject_coerced` | Pass |
| vs-adapter | pushdown-planning-like-type-coercion | Nested non-string LIKE declines whole filter | `support.rs` | `like_guard_nested_decimal_declines_whole_filter`, `like_guard_not_wrapped_decimal_declines`, `like_guard_decimal_inside_case_declines` | Pass |
| vs-adapter | pushdown-planning-like-type-coercion | LIKE nested inside CASE is type-guarded | `support.rs` | `like_guard_decimal_inside_case_declines`, `like_guard_date_inside_case_wraps_cast`, `like_guard_varchar_inside_case_unchanged` (added by review fix 4.5) | Pass |
| vs-adapter | pushdown-planning-like-type-coercion | LIKE nested inside CASE, wired chain | `mod.rs` | `where_filter_like_decimal_inside_case_declines_whole_filter` | Pass |
| vs-adapter | pushdown-planning-string-fn-type-coercion | Composes with LIKE guard + decimal rewriter, no double coercion | `mod.rs` | `where_filter_decimal_stringification_rewritten_to_trim`, `where_filter_upper_decimal_inside_like_subject_coerced` | Pass |
| vs-adapter | pushdown-planning-decimal-string-format | Implicit CONCAT over DECIMAL renders trimmed form, incl. nested | `support.rs` | `rewrite_nested_concat_wraps_only_inner_decimal`, `selectlist_nested_concat_decimal_arg_rewritten` | Pass |

All 19 named tests individually confirmed present and passing (`cargo test -p lakehouse-engine --lib <name>` → 1 passed each).

## Notes

- **Two-commit structure**: the plan specifies commit 1 (tasks 1-6, byte-identical refactor) and
  commit 2 (tasks 7-11, LIKE-guard behavior change) as separate commits for provability. This
  verification ran against the combined uncommitted working tree (all 11 tasks applied). The
  actual git commit split, if desired, happens at the `implement-pr` orchestrator's commit step —
  not lost, just not yet materialized as two commits at this point in the pipeline.
- **Manual-testing row 2 discrepancy**: the plan's Manual Testing table expects
  `git diff --stat crates/.../pushdown/` "after commit 1" to show `support.rs` losing more lines
  than it gains. The actual diff (support.rs + mod.rs combined, full branch vs `main`) is
  `+542/-203` — net growth, not shrinkage. This was flagged during implementation (task 3-6's
  agent) and holds through the full diff as well: growth comes from `rewrite_expr_tree`'s own doc
  comment (a plan requirement — 4 mandated points), from new tests added by tasks 1, 2, 7, and by
  review fix 4.5, and from the stale-documentation sweep (tasks 9-10) which replaced short false
  claims with longer, correct ones. No dead code was left behind — the plan's own Dead Code
  Removal table's four items were each independently confirmed removed by the implementing
  agents. This is a plan-checklist wording issue, not an implementation defect: the qualitative
  goal ("one traversal, one field-list declaration" replacing three duplicated copies) was met;
  the literal line-count prediction was wrong given how much of the growth is required
  documentation and required new tests.
- **Deployed-VS manual queries not independently re-run**: the plan's three "Against the deployed
  VS" rows (LIKE-in-CASE over a DECIMAL subject, over a DATE subject, and an unchanged
  `UPPER(...)` check) call for issuing SQL against a purpose-deployed VS with a TPC-H-shaped
  `LINEITEM` table. This execution environment has only incidental leftover schemas from prior,
  unrelated E2E test runs (e.g. `MY_LAKEHOUSE.FACT_LINEITEM`, missing `L_SHIPDATE`) — not a
  reliable stand-in for the plan's scenario. Per the plan's own text ("The end-to-end behavior of
  commit 2 is additionally exercised by `make test-e2e`"), the automated equivalent is the
  E2E suite (174 tests, 0 failures) plus the wired-chain regression test added in task 7/8
  (`where_filter_like_decimal_inside_case_declines_whole_filter`), which reproduces the exact
  production chokepoint (`mod.rs:210-214`) over a decimal-column LIKE nested in a
  `function_scalar_case`. These three manual rows are the one item in the plan not independently
  re-executed against a live deployed VS in this run.
- No test assertion was edited anywhere in the implementation (confirmed via `git diff` review at
  each gate); all new tests are additive.
