# Verification Report: fix-string-function-integer-args

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The eight string functions take an arithmetic, DECIMAL, or DOUBLE argument in the scan and match native Exasol, including halves, negative counts, and out-of-range errors |
| Code review | 6 findings — 6 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | 93.9% of changed lines (155/165); workspace 92.05% lines |
| Integration | Not measured (E2E runs the `.so` in Exasol) |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit | 2140 | 2138 | 2 |
| Integration | 389 | 389 | 0 |

- Unit: `cargo llvm-cov --workspace`, every workspace unit test. The 2 ignored tests predate this change. The plan's filter `cargo test --workspace --lib exasol_numeric` passed 8 of 8, and `--lib scan::` passed 408 of 408.
- Integration: `make test-e2e` (`e2e_local`), 389 passed in 654.84s. No other worktree ran an E2E suite or a `.so` build during the run.

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT ID, C_DOUBLE, RPAD(C_VARCHAR, 2 + MOD(ID, 2)), SUBSTR('abcd', C_DOUBLE), CHR(64 + C_DOUBLE)` over `MY_LAKEHOUSE.TYPED_DISTINCT_PROBE` and `FILTER_ORACLE.NATIVE_COPY` | ✓ 12 rows each, identical output; row `ID = 1` (`C_DOUBLE = 0.5`) shows `abcd` and `A` |
| `EXPLAIN VIRTUAL SELECT RPAD(C_VARCHAR, ID) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE` | ✓ scan expression `rpad("C_VARCHAR", "ID")`, unchanged |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Checking lakehouse-engine v0.58.4 (.../crates/lakehouse-engine)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 14.03s
```

### Formatter

```
make fmt-check: exit 0, no changes
```

### Build

```
make cross-udf-build
  LAKEHOUSE_SCAN: ABI version 12, fingerprint 0.33.0:rustc_1.98.1__48a229cea_2026-09-01_ — OK
  LAKEHOUSE_ADAPTER: ABI version 12, fingerprint 0.33.0:rustc_1.98.1__48a229cea_2026-09-01_ — OK
  LAKEHOUSE_VERSION: ABI version 12, fingerprint 0.33.0:rustc_1.98.1__48a229cea_2026-09-01_ — OK
  LAKEHOUSE_EXPORT: ABI version 12, fingerprint 0.33.0:rustc_1.98.1__48a229cea_2026-09-01_ — OK
✓ 4 UDF(s) validated in 'target/release/liblakehouse_engine.so'
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| scan-types | scan-numeric-typing | A string function's numeric argument rounds to an integer as Exasol does | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `string_function_numeric_arguments_match_native` | Pass |
| scan-types | scan-numeric-typing | A string function's numeric argument rounds to an integer as Exasol does | `crates/lakehouse-engine/src/scan/string_argument_tests.rs` | `string_function_numeric_argument_rounds_to_int64` | Pass |
| scan-types | scan-numeric-typing | A string function's numeric argument rounds to an integer as Exasol does | `crates/lakehouse-engine/src/scan/string_argument_tests.rs` | `dictionary_argument_converts_and_unsigned_beyond_int64_fails` | Pass |
| scan-types | scan-numeric-typing | A string function's numeric argument outside Exasol's range fails the query | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `string_function_numeric_arguments_match_native` | Pass |
| scan-types | scan-numeric-typing | A string function's numeric argument outside Exasol's range fails the query | `crates/lakehouse-engine/src/scan/string_argument_tests.rs` | `string_function_numeric_argument_outside_exasols_range_fails` | Pass |
| scan-types | scan-numeric-typing | A string function's numeric argument outside Exasol's range fails the query | `crates/lakehouse-engine/src/scan/string_argument_tests.rs` | `dictionary_argument_converts_and_unsigned_beyond_int64_fails` | Pass |
| scan-types | scan-numeric-typing | A negative count or length makes a string function return NULL as Exasol does | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `string_function_numeric_arguments_match_native` | Pass |
| scan-types | scan-numeric-typing | A negative count or length makes a string function return NULL as Exasol does | `crates/lakehouse-engine/src/scan/string_argument_tests.rs` | `negative_count_or_length_returns_null_or_refuses_repeat` | Pass |

## Notes

- The join-condition position of the first scenario is covered by a self-join case in `string_function_numeric_argument_rounds_to_int64`, not by the E2E test.
- The repo's `changed_line_coverage.py` diffs `merge-base..HEAD`, so it reports 0/0 on uncommitted work. The 93.9% figure comes from a copy that diffs the merge base against the working tree. Rerun the script after committing.
- The pre-fix failure was not reproduced by a deliberate rebuild of the old `.so`. An earlier E2E run that another worktree's 0.58.3 `.so` overwrote showed it: every case of `string_function_numeric_arguments_match_native` failed with `Failed to coerce arguments to satisfy a call to 'rpad' function`, the error from #604.
- The `.so` upload is shared across worktrees on one stack. Two earlier E2E runs were discarded because another worktree's run replaced the `.so` mid-run.
