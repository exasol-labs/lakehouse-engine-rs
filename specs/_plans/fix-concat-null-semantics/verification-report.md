# Verification Report: fix-concat-null-semantics

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Pushed-down `CONCAT` now renders `nullif(concat(<args>), '')` in the DataFusion dialect, restoring Exasol's NULL-as-empty-string semantics (issue #374); the Exasol dialect is byte-identical. All checks green, including the live E2E regression test and manual verification against a real Exasol container. |
| Code review | 3 findings — 3 fixed (standard: 2, expert: 1) |

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

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + Integration (host, `cargo test`) | full workspace | 1189 + 168 + 15 + 1 + 3 + 2 + 9 + ... (all suites reported `0 failed`) | 0 |
| Unit (`vs-expression`, isolated) | `cargo test -p vs-expression --lib` | 147 | 0 |
| E2E (`make test-e2e`, live Exasol + MinIO + Iceberg REST) | 12 test binaries, `--test-threads=1` | every `test result: ok`, `0 failed` across all 12 binaries | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `NULL \|\| 'abc'`, `'x' \|\| NULL`, `CONCAT(NULL, 'abc')` on native `dual` | ✓ — `abc, x, abc` |
| VALUE position: `name \|\| NULLIF(name, name) \|\| '-suffix'`, `id <= 3` | ✓ — `event-01-suffix`, `event-02-suffix`, `event-03-suffix` |
| FILTER position: `COUNT(*) WHERE (name \|\| NULLIF(name, name)) = name` | ✓ — `20` |
| All-NULL FILTER: `COUNT(*) WHERE (NULLIF(name, name) \|\| NULLIF(name, name)) IS NULL` | ✓ — `20` |
| `EXPLAIN VIRTUAL` on the FILTER query | ✓ — pushed scan spec's `filter` carries `nullif(concat(\"NAME\", (CASE \"NAME\" WHEN \"NAME\" THEN NULL ELSE \"NAME\" END)), '') = \"NAME\"` — delegated to DataFusion, not evaluated by Exasol |
| `greatest-least` unchanged: `COUNT(*) WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL` | ✓ — `4` (confirms the `greatest-least` prose-only delta moved no behavior) |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
(clean — no warnings or errors)
```

### Formatter

```
cargo fmt --all -- --check
(clean after one fixup: cargo fmt --all applied a single blank-line removal
introduced ahead of the new concat_missing_arguments_or_null_argument_errors_in_both_dialects
test in lib_tests.rs)
```

### Build

```
make cross-musl-udf-build
Finished `release` profile [optimized] target(s) — exit 0
```

## Scenario Coverage

| Scenario | Test Type | Test Location | Test Name | Passes |
|----------|-----------|---------------|-----------|--------|
| CONCAT translates to a NULL-skipping DataFusion concat call | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_concat_as_nullif_wrapped_concat_call` | Pass |
| CONCAT translates to a NULL-skipping DataFusion concat call | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_concat_bool_operand_as_exasol_case` | Pass |
| CONCAT translates to a NULL-skipping DataFusion concat call | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_concat_as_chained_pipe_operator_in_exasol_dialect` | Pass |
| CONCAT translates to a NULL-skipping DataFusion concat call | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_nested_concat_wrapper_per_level` | Pass |
| CONCAT translates to a NULL-skipping DataFusion concat call | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_concat_single_argument` | Pass |
| CONCAT translates to a NULL-skipping DataFusion concat call | Unit | `crates/vs-expression/src/lib_tests.rs` | `concat_empty_argument_list_errors_in_both_dialects` | Pass |
| CONCAT translates to a NULL-skipping DataFusion concat call (review addition) | Unit | `crates/vs-expression/src/lib_tests.rs` | `concat_missing_arguments_or_null_argument_errors_in_both_dialects` | Pass |
| CONCAT translates to a NULL-skipping DataFusion concat call | Unit | `crates/vs-expression/src/lib_tests.rs` | `exasol_dialect_renders_declared_verbatim_surface` (unedited) | Pass |
| CONCAT translates to a NULL-skipping DataFusion concat call | Integration | `crates/lakehouse-engine/tests/boolean_to_string_casing_test.rs` | `concat_predicate_matches_exasol_uppercase_not_datafusion_lowercase`, `concat_group_by_key_uses_exasol_uppercase_labels` (both unedited) | Pass |
| A pushed-down CONCAT over a NULL operand concatenates the non-NULL parts on the cluster | Integration/E2E | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_concat_null_operand_concatenates_non_null_parts` | Pass |
| String scalar functions translate to DataFusion string calls | Unit | `crates/vs-expression/src/lib_tests.rs` | existing string-function tests (unchanged) | Pass |
| GREATEST and LEAST translate to DataFusion greatest/least | Unit | `crates/vs-expression/src/lib_tests.rs` | existing `GREATEST`/`LEAST` tests (unchanged) | Pass |
| A pushed-down GREATEST or LEAST over a NULL-producing argument returns NULL on the cluster | Integration/E2E | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_greatest_least_propagate_null_argument` (unchanged) | Pass |

## Notes

- Task 2.2 of the plan (confirm `boolean_to_string_casing_test.rs` passes with ZERO edits) is folded into
  this verification rather than tracked as a separate implementation task: `git diff --exit-code` on that
  file is clean and both its `CONCAT`-relevant tests pass, holding the gate that the `nullif`-wrapper is
  structurally required (a bare `concat(...)` would break `concat_group_by_key_uses_exasol_uppercase_labels`).
- The E2E regression test (`test_concat_null_operand_concatenates_non_null_parts`) was actually driven
  RED→GREEN during implementation: it failed against the pre-fix `.so` (`None` vs `Some("event-01-suffix")`
  at the VALUE assertion), then passed after `make cross-musl-udf-build` rebuilt the `.so` with the fix.
  Code review additionally required (and got) live confirmation that both FILTER-position queries are
  delegated to the DataFusion scan rather than evaluated natively by Exasol, closing a gap where those two
  assertions could pass vacuously.
- Per an explicit cost-control directive for this implementation run, the full `make test-e2e` suite was
  run exactly once, as the single authoritative gate — not repeated per task or per fix. All prior
  per-task E2E verification used a single filtered test invocation
  (`cargo test --features exasol-e2e --test e2e_scan_test <name>`) against a targeted `.so` rebuild instead.
- Coverage percentages are not reported: the plan's Verification > Checklist does not request a coverage
  metric, and running `cargo llvm-cov` was judged an unjustified extra build/run for this change.
- No dead code, capability withdrawal, or API/DDL/wire-format change. `FN_CONCAT` remains advertised, and
  `crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs` was not touched, per the plan's
  Non-Goals.
