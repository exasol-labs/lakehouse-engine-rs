# Verification Report: fix-regexp-like-pushdown

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The adapter no longer advertises `FN_PRED_REGEXP_LIKE`, the translator no longer renders it, and every automated check passes. The manual `EXPLAIN VIRTUAL` and `exapump` steps were not run separately. |
| Code review | 7 findings, 7 fixed (all comment-only) |

| Check | Status |
|-------|--------|
| Build | ✓ (`make test-e2e` runs `make cross-udf-build`, exit 0) |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ for the two unit-test steps. The three Docker steps are covered by E2E tests, not run by hand. |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Changed production lines | 100% (2 of 2 executable lines; the rest are deletions and comments). Gate: 90%. |

### Test Results

| Type | Result |
|------|--------|
| `cargo test --workspace` | exit 0, 0 failed (rerun after the review fixes) |
| `cargo test -p vs-expression regexp` | 2 passed, 0 failed |
| `make test-e2e` | exit 0, 15 test binaries passed, 0 `FAILED` in `target/e2e-final.log` |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p vs-expression regexp` | ✓ |
| `cargo test -p lakehouse-engine --lib -- like_guard selectlist_ filter_in_common_arg` | ✓ (48 passed) |
| `capabilities_doc_lists_exactly_the_advertised_set` | ✓ (failed on 10 undocumented names before the docs edit, passes after) |
| `REGEXP_LIKE 'event-1'` count, look-around count, `EXPLAIN VIRTUAL` filter | Not run by hand. Asserted by `e2e_regexp_like_returns_native_rows` and `e2e_regexp_like_conjunct_stays_in_exasol_while_sibling_pushes`, which passed. |

## Tool Evidence

### Linter

`cargo clippy --all-targets`: exit 0, 0 warnings. The same run with `--features exasol-e2e` also gave 0 warnings.

### Formatter

`cargo fmt --all --check`: exit 0, no changes.

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| sql-comprehension | vs-expression-translator-predicates | REGEXP_LIKE predicate is not translated in either dialect | `crates/vs-expression/src/lib_tests.rs` | `regexp_like_predicate_declines_in_both_dialects` | Pass |
| sql-functions | vs-expression-translator-scalar-fns | Regexp scalar functions are deliberately not translated | `crates/vs-expression/src/lib_tests.rs` | `regexp_scalar_functions_decline_in_both_dialects` | Pass |
| pushdown-types, pushdown, pushdown-joins | LIKE type coercion and select-list | the nine existing LIKE and select-list scenarios in plan.md | `support_tests.rs`, `e2e_join_test.rs` | `like_guard_*`, `declined_like_on_*`, `selectlist_*`, `filter_in_common_arg`, `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` | Pass |
| pushdown-capabilities | string-conversion-capability | Regular-expression capabilities are not advertised | `capabilities_tests.rs`, `tests/e2e_capability_test.rs` | `reports_audited_capability_set`, `e2e_regexp_like_conjunct_stays_in_exasol_while_sibling_pushes` | Pass |
| pushdown-capabilities | string-conversion-capability | REGEXP_LIKE returns the rows native Exasol returns | `tests/e2e_capability_test.rs` | `e2e_regexp_like_returns_native_rows` | Pass |
| pushdown-capabilities | string-conversion-capability | A REGEXP_LIKE conjunct stays in Exasol while its sibling conjunct is pushed | `tests/e2e_capability_test.rs` | `e2e_regexp_like_conjunct_stays_in_exasol_while_sibling_pushes` | Pass |
| vs-adapter | create-virtual-schema | Adapter reports its pushdown capabilities | `capabilities_tests.rs` | `capabilities_doc_lists_exactly_the_advertised_set`, `reports_audited_capability_set` | Pass |

## Notes

- The new E2E tests were not run against the pre-fix `.so`. Their ability to detect the defect is based on the measurements in plan.md (defect: 10, 20 and a scan error; native: 0, 0 and 3).
- The E2E run happened before the 7 review fixes. Those fixes only changed comments, and `cargo test --workspace`, clippy and fmt were rerun afterwards.
- The E2E run used an isolated Docker project (`lhe-regexp`), because the stack already running did not match this tree's `docker-compose.yml`. The isolated project was torn down. The existing stack was left alone.
- `specs/testing.md`, referenced by `AGENTS.md`, does not exist in this tree.
- No GitHub issue exists for this defect. The deltas and plan still carry `(#TBD)`, and the implementing commit needs `Closes #<n>`.
