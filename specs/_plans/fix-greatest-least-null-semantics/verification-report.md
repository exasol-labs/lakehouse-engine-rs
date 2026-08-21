# Verification Report: fix-greatest-least-null-semantics

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Pushed-down `GREATEST`/`LEAST` now NULL-guard the DataFusion rendering; Exasol dialect and `capabilities.rs` unchanged; the false in-repo `GREATEST` NULL-contract claim is corrected in all four locations with zero SQL change; a live E2E regression test for issue #202 passes against the Docker Exasol container. |
| Code review | 2 findings — 2 fixed |

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

| Type | Run | Passed | Failed | Ignored |
|------|-----|--------|--------|---------|
| Unit + integration (`cargo test`, host) | ~1379 | ~1379 | 0 | 0 |
| E2E (`make test-e2e`, live Docker Exasol) | 304 | 304 | 0 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT GREATEST(0.0, NULL), LEAST(1.0, NULL), GREATEST(5) FROM dual` → empty, empty, `5` | ✓ |
| `SELECT COUNT(*) FROM MY_LAKEHOUSE.EVENTS WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL` → `4` | ✓ |
| `SELECT id, GREATEST(id, NULLIF(MOD(id, 5), 0)) FROM MY_LAKEHOUSE.EVENTS ORDER BY id` → NULL at `id` 5/10/15/20, own `id` otherwise | ✓ |
| `EXPLAIN VIRTUAL` on the `LEAST(...) IS NULL` predicate query → pushed scan-spec filter carries `CASE WHEN … IS NULL … THEN NULL ELSE least(...)` | ✓ |
| `SELECT STDDEV_POP(score) FROM MY_LAKEHOUSE.EVENTS WHERE id < 0` → empty (NULL) for zero-row group | ✓ |

## Tool Evidence

### Build

```
make cross-musl-udf-build: EXIT_CODE=0
```

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.78s
EXIT_CODE=0
```

### Formatter

```
cargo fmt --all -- --check
EXIT_CODE=0 (no diff)
```

### E2E (relevant excerpt)

```
test test_greatest_least_propagate_null_argument ... ok
test result: ok. 76 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 163.99s
[... 11 other e2e test binaries, all "0 failed" ...]
EXIT_CODE=0
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| sql-comprehension | vs-expression-translator-scalar-fns | GREATEST/LEAST translate to DataFusion greatest/least (guarded) | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least` | Pass |
| sql-comprehension | vs-expression-translator-scalar-fns | Single-argument degenerate guard | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least_single_argument_guard` | Pass |
| sql-comprehension | vs-expression-translator-scalar-fns | Literal NULL argument | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least_with_literal_null_argument` | Pass |
| sql-comprehension | vs-expression-translator-scalar-fns | Nested argument rendered once, referenced twice | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least_nested_argument_once_referenced_twice` | Pass |
| sql-comprehension | vs-expression-translator-scalar-fns | Exasol dialect stays byte-identical/verbatim | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least_verbatim_in_exasol_dialect` | Pass |
| sql-comprehension | vs-expression-translator-scalar-fns | Declaration-driven verbatim sweep unaffected | `crates/vs-expression/src/lib_tests.rs` | `exasol_dialect_renders_declared_verbatim_surface` | Pass |
| sql-comprehension | vs-expression-translator-scalar-fns | Live pushdown NULL propagation (predicate + value position) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_greatest_least_propagate_null_argument` | Pass |
| vs-adapter | pushdown-agg-sql-consolidation | Sufficient-statistics fragments, one owner per denominator (doc-only correction) | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs` | `stddev_pop_merge_null_passthrough_for_n_zero` | Pass |
| vs-adapter | pushdown-agg-sql-consolidation | Sufficient-statistics fragments, one owner per denominator (doc-only correction) | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs` | `stddev_samp_merge_null_passthrough_for_n_zero_and_n_one` | Pass |
| vs-adapter | pushdown-agg-sql-consolidation | Golden fixtures unchanged | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `testdata/dispatch_golden/*` (byte-identical) | Pass |

Two additional review-fix tests extend coverage beyond the plan's table: `greatest_least_without_arguments_key_errors` (missing-`arguments`-key error path) and `renders_nested_greatest_guard_referencing_the_inner_case_twice` (compounding nested-guard duplication shape) — both in `crates/vs-expression/src/lib_tests.rs`, both passing.

## Notes

- `capabilities.rs` was not touched; `FN_GREATEST`/`FN_LEAST` stay advertised, confirmed live via `EXPLAIN VIRTUAL`'s capabilities list.
- The two code-review findings were both test-coverage gaps in `vs-expression`'s new guard logic (an untested missing-`arguments`-key error arm, and an untested compounding-duplication shape for a nested `GREATEST`/`LEAST` argument); both were fixed by adding the missing tests, no production code changed as part of the fix.
- The E2E suite (`make test-e2e`) was run once, covering both this skill's Phase 5 checklist and later serving as the test evidence for `/speq:implement-pr`'s record gate — deliberately not re-run a second time.
- `sweep.timestamp`, an untracked stray file unrelated to this plan, was left untouched and excluded from all diffs/commits.
