# Verification Report: refactor-pushdown-type-rewrite-pipeline

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Two pipeline functions (`apply_filter_type_rewrites`, `apply_select_item_type_rewrites`) now own the type-rewrite pass order; the three passes are private; every gate is green with zero test-assertion edits. |
| Code review | 5 findings — standard: 5 fixed, expert: 0 |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (5/7; 2 deferred — see Notes) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (workspace, `cargo test`) | 683 (lib) + smaller integration binaries | 683 passed, 0 failed | 2 ignored (pre-existing, unrelated) |
| Unit (`adapter::pushdown` filter) | 395 | 395 passed, 0 failed | 0 |
| E2E (`make test-e2e`, 7 binaries, live Docker Exasol/MinIO/Iceberg-REST stack) | 174 | 174 passed, 0 failed | 0 |
| E2E compile gate (`cargo test --features exasol-e2e --no-run`) | n/a | compiles, exit 0 | n/a |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: exit 0, 0 warnings/errors
```

### Formatter

```
cargo fmt --check: exit 0, no diff
```

### Rustdoc (narrowing gate)

```
cargo doc --no-deps -p lakehouse-engine: exit 0
grep for private-link warnings on like_subject_type_guard / string_function_arg_type_guard /
rewrite_decimal_stringifications / apply_filter_type_rewrites / apply_select_item_type_rewrites: no matches
```

### UDF build

```
make cross-musl-udf-build: exit 0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| vs-adapter | pushdown-module-structure | Filter pipeline, rendered-SQL byte-identity | `mod.rs` | 6 chain tests (`where_filter_decimal_stringification_rewritten_to_trim`, `filter_decimal_comparison_not_rewritten`, `where_filter_string_fn_under_comparison_predicate_coerced`, `where_filter_string_fn_over_double_declines`, `where_filter_upper_decimal_inside_like_subject_coerced`, `where_filter_like_decimal_inside_case_declines_whole_filter`) | Pass |
| vs-adapter | pushdown-module-structure | Select-list pipeline, decline-to-full-row path | `support.rs` | 9 `selectlist_*`/`stringify_*` tests | Pass |
| vs-adapter | pushdown-module-structure | Two pass lists stay distinct; select-list LIKE omission tracked as #219 gap | `support.rs` | `select_list_pipeline_omits_like_pass_pending_219` (new) | Pass |
| vs-adapter | pushdown-module-structure | Each pass keeps its per-node decision unchanged | `support.rs` | `like_guard_*`, `rewrite_*`/`decimal_rewrite_*`, `string_fn_guard_*` corpus | Pass |
| vs-adapter | pushdown-module-structure | No dispatch-shape or join SQL regression | `pushdown/` | `testdata/dispatch_golden/` + join golden-SQL assertions | Pass |
| vs-adapter | pushdown-planning-string-fn-type-coercion-composition | Guard composes with LIKE guard and decimal rewriter without double coercion | `mod.rs` | `where_filter_decimal_stringification_rewritten_to_trim`, `where_filter_upper_decimal_inside_like_subject_coerced` | Pass |

## Notes

- **Byte-identity confirmed by diff inspection, not just test pass:** `git diff crates/lakehouse-engine/src/adapter/pushdown/` shows only 2 new `assert_eq!` lines (both from the one new test, `select_list_pipeline_omits_like_pass_pending_219`) — no existing assertion or expected value was touched. Per-file diff: `mod.rs` +101/−? net, `support.rs` net addition — both confined to the chain-to-call swap, import update, and comment relocation described in the plan.
- **Narrowing verified, not assumed:** `git grep` for the three pass names outside `support.rs` (in `mod.rs` and `joins/`) returns prose-comment mentions only — no call, no import. The narrowing to private compiled cleanly on the first attempt, confirming Groups A/B had already rewired every external caller.
- **Rustdoc gate:** `cargo doc` produced 32 pre-existing private-link warnings, all in unrelated modules (`scan/raw_scan.rs`, `scan/join_scan.rs`, `scan/mod.rs`, `scan/spec.rs`); none reference the three narrowed passes or the two new pipeline functions, so the narrowing introduced zero new warnings.
- **Code review:** 5 findings, all comment/doc-quality (redundant restatement, a stale "wired into" claim left over from before the rewiring, a duplicate inline comment). None required touching a test assertion or executable logic; all 5 fixed and re-verified (695/395 pushdown tests, clippy, fmt all clean after fixes).
- **Manual Testing table — 2 of 7 scenarios deferred:** the two scenarios requiring a query against a deployed VS with live TPC-H-style Iceberg data (`LENGTH(L_QUANTITY) > 5`, `UPPER(L_QUANTITY)` against a LINEITEM table) were not re-run — this headless environment's Docker stack has no LINEITEM/PART virtual schema registered (that setup exists only on the staging environment per prior session memory). The exact code paths those two scenarios exercise (DECIMAL argument coerced through `string_function_arg_type_guard` then declined by `rewrite_decimal_stringifications`, for both a WHERE-clause and a SELECT-list argument) are covered byte-for-byte by the automated unit tests in the Scenario Coverage table above (`where_filter_decimal_stringification_rewritten_to_trim` for the filter side, `selectlist_length_decimal_arg_rewritten`/`selectlist_upper_decimal_arg_coerced_not_full_row` for the select-list side), all passing. The other 5 manual scenarios (unit run, diff inspection, grep census, Docker-stack E2E) were executed directly, as recorded above.
- Dead code, YAGNI, and error-handling checks: none flagged by review beyond the 5 doc/comment findings — no new abstractions, no registry, no configuration parameter, matching the plan's explicit non-goals.
