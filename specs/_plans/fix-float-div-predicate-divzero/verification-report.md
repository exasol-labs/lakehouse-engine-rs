# Verification Report: fix-float-div-predicate-divzero

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | A pushed `FLOAT_DIV` by zero now fails the query in every position (projection, predicate, join fact-leg, aggregate argument), with a clean division-by-zero message and no storage-read framing, matching native Exasol's behavior. All plan tasks complete, all verification checks green, live against the local Docker Exasol stack. |
| Code review | 15 findings — 15 fixed (14 standard, 1 expert). The expert fix's first attempt (composing errors unconditionally) regressed two live E2E tests; a follow-up fix (task 4.27) narrowed composition to the one case structurally distinguishable without message-text matching (`ResourcesExhausted`), and full E2E is green again. |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-udf-build`, exit 0) |
| Tests | ✓ (`cargo test`, 0 failures) |
| Lint | ✓ (`cargo clippy --all-targets`, 0 warnings) |
| Format | ✓ (`cargo fmt --check`, no changes) |
| Scenario Coverage | ✓ (every scenario-coverage test name resolves to a real `fn`) |
| Manual Tests | ✓ (3/3, run live) |

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit + integration (`cargo test`) | all workspace targets | 1200+ (see log) | 0 |
| E2E (`make test-e2e`, live Docker) | 12 binaries | 310 | 0 |

### Manual Tests

| Test | Command | Result |
|------|---------|--------|
| Checked-division rendering | `EXPLAIN VIRTUAL SELECT L_ORDERKEY / L_LINENUMBER FROM MY_LAKEHOUSE.FACT_LINEITEM;` | ✓ `PUSHDOWN_SQL` projection reads `vs_checked_float_div("L_ORDERKEY", "L_LINENUMBER")`, `emit_exa_types` is `["DOUBLE PRECISION"]` |
| Predicate division by zero fails | `SELECT COUNT(*) FROM MY_LAKEHOUSE.FACT_LINEITEM WHERE 0 < L_ORDERKEY / (L_LINENUMBER - L_LINENUMBER);` | ✓ Fails: `data exception - division by zero: vs_checked_float_div(6, 0) has no finite result...` (SQL state `22002`), no `assigned data could not be read` framing |
| Correct division unaffected | `SELECT L_ORDERKEY / L_LINENUMBER FROM MY_LAKEHOUSE.FACT_LINEITEM WHERE L_ORDERKEY = 7 AND L_LINENUMBER = 2;` | ✓ Returns `3.5` |

## Tool Evidence

### Linter

```
cargo clippy --all-targets: Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.02s
0 warnings, 0 errors
```

### Formatter

```
cargo fmt --check: exit 0, no diff
```

### Tracked-exception gate

```
grep -rn 'TODO-suppression\|TODO-stored-nan\|TODO-scalar-fns' specs/_plans/fix-float-div-predicate-divzero/ --include='spec.md'
(no output)
```

## Scenario Coverage

Every row of `plan.md` § Verification § Scenario Coverage was checked against the live source tree: each named test function exists (`grep -rn 'fn <name>'` across `crates/`) and the full suite run above passed. Highlights:

| Scenario | Test Location | Test Name | Passes |
|----------|---------------|-----------|--------|
| FLOAT_DIV renders a checked call in the DataFusion dialect | `crates/vs-expression/src/lib_tests.rs` | `float_div_calls_checked_division_for_*` family (10 tests) | Pass |
| Exasol dialect keeps rendering FLOAT_DIV as a bare `/` | `crates/vs-expression/src/lib_tests.rs` | `float_div_renders_checked_division_call_only_in_the_datafusion_dialect` | Pass |
| Predicate-position division by zero fails, no storage framing | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_by_zero_in_filter_fails_like_native_exasol` | Pass |
| Guarded division matches the measured native outcome (both conjunct orders, including the over-raise direction) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_guarded_by_a_non_zero_conjunct_matches_the_measured_outcome` | Pass |
| Broadcast-join fact-leg filter fails | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_float_div_by_zero_in_fact_leg_filter_fails` | Pass |
| Aggregate-argument division by zero fails | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_by_zero_in_aggregate_argument_fails` | Pass |
| Checked division: type-based error recognition, session-scoped failure recording | `crates/lakehouse-engine/src/scan/checked_div_tests.rs`, `crates/lakehouse-engine/src/scan/emit_tests.rs` | `checked_float_div_*`, `classify_scan_error_*`, `reframe_checked_division_*` | Pass |
| Plan shape unchanged for a division-free spec | `crates/lakehouse-engine/tests/scan_parquet_pruning.rs`, `datafusion-scan/scan-execution-plan-shape` tests | existing, unedited | Pass, no golden diff |

## Notes

- **Live measurement (Group A, tasks 1.1-1.2):** pre-fix, native Exasol raises `22012` for the unguarded shape and does NOT raise for the guarded shape (`(L_LINENUMBER - 1) <> 0 AND ... / (L_LINENUMBER - 1)`) in either conjunct order, returning 10 of 20 rows. Post-fix, the checked division matches this in the guard-first conjunct order (parity) but raises in the division-first order (an over-raise, covered by tracked exception `#392`) — the Parquet row filter's sequential, textual-order conjunct evaluation is the actual protection mechanism, not `datafusion-physical-expr`'s `PRE_SELECTION_THRESHOLD`, and it depends on `datafusion.execution.parquet.reorder_filters` defaulting to `false`.
- **Three tracked-exception GitHub issues filed** (orchestrator-filed, per decision-log's explicit assignment, not by an implementer agent): `#392` (suppression/over-raise, both directions), `#393` (stored-NaN comparison semantics), `#394` (non-finite values from scalar functions other than `FLOAT_DIV`). All three cited inline in both spec deltas and in decision-log.
- **One implementation deviation from the plan's original design**, discovered mid-flight and code-reviewed: the Parquet row filter flattens a predicate-position error's type before it reaches `classify_scan_error`, so the checked division additionally records its first failure as a typed value on the registered UDF instance (scoped to one session), read back once by the scan dispatcher. Decision-log entry [13] records this, its rejected alternatives, and the accepted limitation (an unrelated concurrent storage failure sharing the same scan as a division failure may be masked by the division's own message) that keeps the plan's primary, tested guarantee intact.
- No E2E test was skipped; the Docker Exasol stack was up and used for every live check in this report.

Ready for: `/speq:record fix-float-div-predicate-divzero`
