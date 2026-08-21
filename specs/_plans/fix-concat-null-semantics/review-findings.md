# Code Review Findings: fix-concat-null-semantics

## Summary
- Files reviewed: 3
- Total findings: 3 (standard: 2, expert: 1)

Verification run during review (evidence, not findings):
- `cargo test -p vs-expression` → 146 passed, 0 failed.
- `cargo test -p lakehouse-engine --test boolean_to_string_casing_test` → 3 passed; `git diff --exit-code` on that file is clean (task 2.2 gate holds).
- `cargo clippy --workspace --all-targets -- -D warnings` → clean (so the new E2E test compiles).
- `cargo fmt --all -- --check` → clean.
- DataFusion 54.1.0 contract checks against the vendored source: `ConcatFunc::signature` is `Signature::variadic([Utf8View, Utf8, LargeUtf8, Binary])` and `coerced_from` has `(Utf8 | LargeUtf8, _) => Some(Utf8)` ("any type can be coerced into strings"), so moving from the `||` operator to `concat(...)` does NOT narrow the accepted argument types; `NullIfFunc::signature` is `Signature::comparable(2, …)`, so `nullif(<Utf8View>, '')` coerces cleanly for Parquet view-typed string columns. No finding raised on either.
- The nested/partial-NULL algebra of `nullif(concat(...), '')` was checked by hand against Exasol's captured contract for the two-, three-, and all-NULL operand cases and for the NULL-boolean `CASE` operand; it matches in every case. No finding raised.

## Standard fixes

### crates/vs-expression/src/lib.rs

#### [OUTDATED_COMMENT] `TRANSLATED_SCALAR_FNS` row comment asserts that DataFusion's `concat()` propagates NULL
- Location: lines 137-139
- Issue: the new row comment reads "the arm owns both dialects because Exasol's `||` does not propagate NULL the way DataFusion's `concat()` does (#374)", which asserts that DataFusion's `concat()` propagates NULL. It does not — `datafusion-functions-54.1.0/src/string/concat.rs:106` documents "NULL arguments are ignored", and the same file contradicts itself two other places: the module doc bullet (lines 47-51) and the arm's own comment (lines 1162-1164) both correctly say `concat()` treats NULL as the empty string. This is the same class of refuted-claim comment task 1.3 was written to remove, and it survives task 1.3's acceptance check only because the false claim is now attached to `concat()` instead of to Exasol's `||`. The stated reason is also wrong on a second count: the arm is `Shaped` because Exasol's form is the `||` operator rather than a call (the gate cannot derive it), not because of the NULL contract — the NULL contract is why the two dialects *diverge* inside the arm.
- Fix: In `crates/vs-expression/src/lib.rs`, rewrite the `CONCAT` row comment in `TRANSLATED_SCALAR_FNS` (lines 137-139) so it no longer claims DataFusion's `concat()` propagates NULL. State that `CONCAT` is `Shaped` because Exasol's form is the `||` operator, not a call, and that the two dialects additionally diverge on the NULL contract: Exasol's `||` treats a NULL operand as the empty string and yields NULL only for an all-empty result, which the DataFusion side reproduces as `nullif(concat(...), '')` (#374). Keep it to at most three lines, matching the neighbouring `MOD` row comment at lines 133-135.

### crates/vs-expression/src/lib_tests.rs

#### [UNTESTED_ERROR_PATH] Two of the rewritten `CONCAT` arm's three error paths have no test
- Location: `crates/vs-expression/src/lib_tests.rs` lines 1666-1682 (`concat_empty_argument_list_errors_in_both_dialects`); the untested paths are `crates/vs-expression/src/lib.rs` lines 1174-1176 and 1185-1187
- Issue: task 1.1 rewrote the whole `"CONCAT"` arm, and the golden rule requires existing untested behavior in code being changed to be pinned by a test. The new arity floor (`function_scalar CONCAT requires at least 1 argument, got 0`) is covered in both dialects and both safe variants, but the arm's two other failure paths are not covered anywhere in the crate: `function_scalar CONCAT missing 'arguments'` (lib.rs:1174-1176) and `CONCAT argument rendered to null` (lib.rs:1185-1187, reachable when an argument element is JSON `null`, which `render_expression_inner` turns into `Ok(None)` at lib.rs:613). A grep for `CONCAT` in `lib_tests.rs` returns no assertion on either string.
- Fix: In `crates/vs-expression/src/lib_tests.rs`, add a unit test immediately after `concat_empty_argument_list_errors_in_both_dialects` covering the `CONCAT` arm's other two error paths in both dialects: (a) a `{"type": "function_scalar", "name": "CONCAT"}` node with no `arguments` key must fail with exactly `function_scalar CONCAT missing 'arguments'`; (b) a CONCAT node whose `arguments` array contains a JSON `null` element alongside a valid column argument must fail with exactly `CONCAT argument rendered to null`. Assert the exact strings via `render_expression(&expr).unwrap_err().to_string()` and `render_expression_exasol(&expr).unwrap_err().to_string()`, and assert `render_expression_safe` and `render_expression_exasol_safe` return `None` for both inputs. Name the test so it states the condition and the expected behavior, e.g. `concat_missing_arguments_or_null_argument_errors_in_both_dialects`.

## Expert fixes

### crates/lakehouse-engine/tests/e2e_scan_test.rs

#### [IMPLEMENTATION_COUPLED_TEST] The two FILTER-position assertions can pass without the translator being exercised at all
- Location: lines 4243-4271 (`test_concat_null_operand_concatenates_non_null_parts`, the `filter_sql` and `all_null_filter_sql` blocks)
- Issue: the test calls `explain_virtual_pushdown_sql` only for `value_sql`. The two filter queries assert row counts (`20` and `20`) with no delegation check, so both assertions pass identically whether the adapter pushed the expression into the DataFusion scan or Exasol evaluated `name || NULLIF(name, name)` natively — Exasol's own semantics already produce `20` in both cases. That makes them a green test over unproven behavior: if the filter shape ever stops being delegated (a capability change, a pushdown-planning rejection, an Exasol planner change), the test keeps passing while proving nothing about the renderer. It also voids the test's own documented purpose: the doc comment (lines 4212-4218) calls the all-NULL filter "the assertion that discriminates the `nullif`-wrapped rendering from a bare `concat(...)`", and that discrimination only holds while the expression is delegated. The plan's task 2.1 acceptance is explicit that the `EXPLAIN VIRTUAL` assertion exists "so the test proves the translator rather than Exasol evaluating the expression itself", and its Context section records the filter shape as delegated on this checkout's container (`EXPLAIN VIRTUAL` carried `("NAME" || (CASE …)) = "NAME"`).
- Fix: In `crates/lakehouse-engine/tests/e2e_scan_test.rs`, inside `test_concat_null_operand_concatenates_non_null_parts`, add an `explain_virtual_pushdown_sql` assertion for `filter_sql` and another for `all_null_filter_sql`, each asserting the pushed scan spec contains `nullif(concat(` — mirroring the existing `value_pushed` assertion and the two-position pattern in `test_greatest_least_propagate_null_argument` (lines 4158-4163 and 4196-4201). Reuse the same failure message shape, including the captured `{pushed}` text. If the live `EXPLAIN VIRTUAL` shows either filter shape is not delegated to the adapter, replace that query with a delegated equivalent that still discriminates the same semantics (non-NULL parts concatenated; all-NULL operands collapsing to NULL rather than the empty string) rather than dropping or weakening the delegation check.
