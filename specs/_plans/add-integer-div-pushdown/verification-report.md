# Verification Report: add-integer-div-pushdown

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The adapter advertises `FN_DIV`, and the scan computes `DIV` over integer operands through `vs_checked_int_div`. Unit, E2E, lint, and manual checks pass. |
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
| Unit | not measured |
| Integration | not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace --lib`) | 1975 | 1975 | 0 |
| Integration (local E2E, `e2e_local`) | 364 | 364 | 0 |

The E2E suite ran on a private stack (compose project `lakehouse-engine-rs-2`, network `lakehouse-engine-2`, subnet `10.85.0.0/16`, Exasol port 38563). The first full run passed 358 tests and failed the 6 `statement_reuse` tests. These 6 tests count lines in the forward-proxy log. The harness finds the proxy container by the hard-coded network name `lakehouse-engine`, so on a parallel stack it reads the proxy log of a different stack. With the lookup pointed at `lakehouse-engine-2` for the run only, all 8 `statement_reuse` tests passed on the same build. The run-only edits to `tests/common/stack.rs` and `tests/e2e_local/proxy.rs` were reverted.

### Manual Tests

Run against the private stack, virtual schema `MY_LAKEHOUSE`, table `TYPED_DISTINCT_PROBE`.

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL SELECT ID, DIV(ID - 7, 2) ...`: pushdown SQL contains `vs_checked_int_div((\"ID\" - 7), 2)` | ✓ |
| `SELECT ID, DIV(ID - 7, 2), DIV(ID - 7, -2) ... ORDER BY ID`: ID 2 gives `-2`, `2`; ID 8 gives `0`, `0`; ID 10 gives `1`, `-1` | ✓ |
| `SELECT ID, DIV(ID, ID - ID) ...`: fails with `data exception - division by zero: vs_checked_int_div(1, 0) has no result`, without `assigned data could not be read` | ✓ |
| `EXPLAIN VIRTUAL SELECT ID, DIV(C_DECIMAL_A, 2) ...`: no `vs_checked_int_div`; the wrapper SQL computes `CAST(DIV("LHS_T0"."C_DECIMAL_A", 2) AS DECIMAL...)` | ✓ |
| `EXPLAIN VIRTUAL SELECT ID ... WHERE DIV(ID, 2) = 3`: the scan filter is `(vs_checked_int_div(\"ID\", 2) = 3)` | ✓ |
| `EXPLAIN VIRTUAL SELECT CAST(ID AS VARCHAR(20)), -ID ...`: Exasol delegates both. `FN_CAST` and `FN_NEG` are advertised. The CAST to `VARCHAR` routes to the wrapper SQL, as the delta scenario requires. `SELECT CAST(ID AS DECIMAL(10,0)), -ID` puts both expressions in the scan projection | ✓ (see Notes) |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s)
```

### Formatter

```
cargo fmt --all -- --check
(no output, exit 0)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| sql-comprehension | vs-expression-translator-scalar-ops | Arithmetic operators translate to binary SQL expressions | `crates/vs-expression/src/lib_tests.rs`, `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `arithmetic_operators_render_identically_in_both_dialects`, `reports_audited_capability_set` | Pass |
| sql-comprehension | vs-expression-translator-scalar-ops | Integer division over integer operands runs in the scan | `crates/lakehouse-e2e/tests/e2e_local/scan.rs`, `crates/lakehouse-engine/src/scan/checked_div_tests.rs` | `e2e_integer_div_matches_native_over_a_native_copy`, the `checked_int_div_*` unit tests | Pass |
| sql-comprehension | vs-expression-translator-scalar-ops | Integer division over a decimal or DOUBLE operand runs in Exasol | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_integer_div_matches_native_over_a_native_copy` | Pass |
| sql-comprehension | vs-expression-translator-scalar-ops | A pushed-down integer division by zero fails the query | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_integer_div_by_zero_fails_with_division_by_zero` | Pass |
| pushdown-capabilities | pushdown-planning-string-conversion-capability | Conversion and unary-negation capabilities are advertised so CAST and unary-minus expressions push down | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs`, `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `reports_audited_capability_set`, `cast_to_a_non_emittable_target_matches_native` | Pass |

The capability-extensions delta changes only its Background, so it maps no scenario.

## Notes

- The plan's manual step for the string-conversion capability expected `CAST(ID AS VARCHAR(20))` in the scan projection. The delta scenario routes a CAST to a target the scan does not compute to the wrapper, and that is the observed behavior. The step's expected text was wrong. The code matches the spec.
- Review fix 4.6 replaced the unchecked `i128` `/` with `checked_div`. A stored `Decimal128` value outside its declared precision can reach `i128::MIN / -1`, and that case now fails the query with a numeric-overflow error instead of a panic.
- The E2E harness finds the forward-proxy container by the fixed network name `lakehouse-engine`. A parallel stack needs a run-only override for the `statement_reuse` tests.
