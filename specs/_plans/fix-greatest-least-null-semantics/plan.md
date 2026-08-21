# Plan: fix-greatest-least-null-semantics

## Summary

NULL-guard the DataFusion-dialect rendering of `GREATEST`/`LEAST` so a pushed-down call returns
NULL when any argument is NULL, matching Exasol and fixing issue #202's silent wrong results. The
same change corrects a live-refuted claim about Exasol's `GREATEST` NULL contract that this
repository had recorded as normative in a second feature.

## Design

### Context

Exasol's `GREATEST`/`LEAST` return NULL if ANY argument is NULL. DataFusion's `greatest`/`least`
return NULL only if ALL arguments are NULL — they skip NULLs and return the largest or smallest
remaining value. The translator maps the Exasol names 1:1 onto the DataFusion functions, so every
pushed-down call over a nullable argument diverges. Both halves were captured for this plan rather
than recalled:

| Side | Evidence |
|---|---|
| Exasol propagates NULL | `SELECT GREATEST(0.0, NULL), LEAST(1.0, NULL), GREATEST(1, 2, NULL), SQRT(GREATEST(0.0, NULL)), GREATEST(CAST(NULL AS DOUBLE)), LEAST(NULL, NULL), GREATEST('a', NULL) FROM dual` returns NULL in every column on the Exasol 2025.2.1 container pinned in `docker-compose.yml`; `GREATEST(5)` returns `5` |
| DataFusion skips NULL | Pinned DataFusion 54.1.0 documents both names as "Returns _null_ if all expressions are _null_" (`datafusion-functions-54.1.0/src/core/greatest.rs:40`, `.../least.rs:40`) |
| End-to-end divergence | Issue #202: `WHERE LEAST(l_tax, l_discount, NULL) IS NULL` matched 9965 rows natively and 0 through the virtual schema; `GREATEST(c_acctbal, NULLIF(c_acctbal, c_acctbal))` returned NULL natively and `711.56` through the virtual schema |

`capabilities.rs` advertises `FN_GREATEST` and `FN_LEAST`, and Exasol never re-checks or re-applies
an advertised capability. The adapter therefore owns generating an equivalent of Exasol's own
semantics, and a NULL contract is part of those semantics.

- **Goals** — restore Exasol's NULL-propagates semantics for pushed-down `GREATEST`/`LEAST` in
  value and predicate position; keep both capabilities advertised and both calls pushed down; leave
  the Exasol-dialect rendering byte-identical; make the library state one contract for Exasol's
  `GREATEST` instead of two contradictory ones.
- **Non-Goals** — withdrawing `FN_GREATEST`/`FN_LEAST`; adding a guard to any other translated
  scalar function (audited, none needs one); changing any generated aggregate merge SQL; teaching
  the translator column nullability.

### Decision

Render the DataFusion dialect as `CASE WHEN <a1> IS NULL[ OR <a2> IS NULL]... THEN NULL ELSE
greatest(<a1>, <a2>, ...) END`, in the `"GREATEST" | "LEAST"` arm of `render_expression_inner`
(`crates/vs-expression/src/lib.rs:1289-1300`) only.

#### Architecture

No component or boundary changes. One arm of one existing dispatch changes on one of its two
dialect paths:

```
function_scalar GREATEST/LEAST
   │
   ├─ Dialect::Exasol ──▶ VerbatimCall gate (lib.rs:986)  ──▶ GREATEST(a, b)   [UNCHANGED]
   │
   └─ Dialect::DataFusion ─▶ per-name arm (lib.rs:1289)   ──▶ CASE WHEN a IS NULL OR b IS NULL
                                                                THEN NULL ELSE greatest(a, b) END
```

The Exasol path never reaches the arm: the `ExasolForm::VerbatimCall` gate returns ahead of the
whole `match fn_name.as_str()`. Both names keep that declaration, so the declaration-driven verbatim
sweep test (`exasol_dialect_renders_declared_verbatim_surface`) stays green with no edit — the
structural proof that the guard cannot leak onto the Exasol-parsed path.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Guard the semantics at the render site | `render_expression_inner`'s `GREATEST`/`LEAST` arm | The adapter owns equivalence for every advertised capability; Exasol provides no fallback |
| Render once, reference twice | The guard chain and the call share one `render_args` result | An argument's two occurrences cannot diverge, and no sub-expression is walked twice |
| Keep the call in `ELSE` | `ELSE greatest(...)` rather than a bare NULL | The call pins the result TYPE; an all-NULL CASE would yield a Null-typed column |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| NULL-guard the DataFusion rendering | Withdraw `FN_GREATEST`/`FN_LEAST` from `capabilities.rs` | Rejected in the interview. Withdrawal loses the pushdown and the scan-side projection/filter narrowing it enables; the guard fixes the semantics at the source and matches how `CONCAT` was already fixed for the same class of divergence |
| `CASE WHEN … IS NULL … THEN NULL ELSE <call> END` | `coalesce`/`nvl`-based rewrite; a DataFusion UDF implementing Exasol semantics | Fixed by the interview. A `coalesce` trick cannot express "NULL if ANY argument is NULL"; a custom UDF adds a scan-side registration and a second place the contract lives |
| Duplicate each argument's rendered text | Bind the argument once via a subquery or a DataFusion `WITH` | No translated `function_scalar` name is non-deterministic — `CURRENT_DATE`, `SYSDATE`, `CURRENT_TIMESTAMP`, `SYSTIMESTAMP` are deliberately undeclared — so both copies always evaluate equal, and DataFusion's common-subexpression elimination handles the cost. A binding form would restructure the whole expression for no correctness gain |
| Emit the guard unconditionally | Skip it when every argument is provably non-nullable | The translator receives no nullability metadata for a `column` node. Inferring it would be a guess whose failure mode is silent wrong results — the exact failure this plan removes |
| Correct the aggregate-merge doc comments and keep the guard | Leave the false claim; or delete the now-redundant `CASE` guard | A live-refuted claim recorded as normative would have the library asserting two opposite contracts for one engine and invites re-litigating #202. Deleting the guard would change SQL that golden fixtures pin byte-for-byte, for no correctness gain |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| `sql-comprehension/vs-expression-translator-scalar-fns` | CHANGED | `specs/_plans/fix-greatest-least-null-semantics/sql-comprehension/vs-expression-translator-scalar-fns/spec.md` |
| `vs-adapter/pushdown-agg-sql-consolidation` | CHANGED | `specs/_plans/fix-greatest-least-null-semantics/vs-adapter/pushdown-agg-sql-consolidation/spec.md` |

## Impact

Pushed-down `GREATEST`/`LEAST` results change for rows where any argument is NULL: they now return
NULL instead of the largest or smallest non-NULL argument. Filters over such expressions therefore
return different row sets, and projected values return NULL where they previously returned a number
or string. Both are corrections — the new results match what native Exasol returns for the same
query — so a user comparing the virtual schema against a native table stops seeing a discrepancy.
No API, capability, DDL, connection property, or wire format changes; `FN_GREATEST` and `FN_LEAST`
stay advertised. Generated aggregate merge SQL is unchanged. Version bump: PATCH (`fix`).

## Dependencies

None. No new crate, no dependency version change. The pinned DataFusion 54.1 and the Exasol 2025.2.1
Docker image already in use supply all evidence and all test surface.

## Implementation Tasks

- [ ] 1.1 Render the DataFusion-dialect `GREATEST`/`LEAST` as a NULL-guarded `CASE` in the
      `"GREATEST" | "LEAST"` arm of `render_expression_inner`
      (`crates/vs-expression/src/lib.rs:1289-1300`), keeping the existing missing-`arguments` and
      empty-argument-list errors and calling `render_args` exactly once so each argument's rendered
      text is reused in its `IS NULL` clause and in the call. Update the two existing tests in
      `crates/vs-expression/src/lib_tests.rs` that assert the old bare rendering —
      `renders_greatest_least` (`:1623-1648`) and the two `render_expression` assertions inside
      `renders_greatest_least_verbatim_in_exasol_dialect` (`:2951-2988`) — and add unit tests for:
      the multi-argument guard's exact SQL text, the one-argument degenerate guard, a `literal_null`
      argument (whose `NULL IS NULL` clause makes the whole expression NULL, as Exasol's
      `LEAST(x, y, NULL)` does), a nested `function_scalar` argument rendered once and referenced
      twice identically, and the empty-argument-list error. **Acceptance:** the exact rendered
      strings are asserted, not `.contains(...)` probes; the two
      `render_expression_exasol` assertions in `renders_greatest_least_verbatim_in_exasol_dialect`
      stay BYTE-IDENTICAL and `exasol_dialect_renders_declared_verbatim_surface` passes with no
      edit; `capabilities.rs` is not touched; `cargo test -p vs-expression` green.
- [ ] 1.2 Correct the false Exasol `GREATEST` NULL-contract claim wherever it is recorded in code:
      `stddev_of`'s and `merge_select_items`' doc comments in
      `crates/lakehouse-engine/src/adapter/pushdown/scalar_over_agg.rs` (the
      `Exasol's GREATEST(0.0, NULL) = 0.0` sentence at `:393-396` and the `stddev_of` doc at
      `:366-371`), and the doc comments of `stddev_pop_merge_null_passthrough_for_n_zero` and
      `stddev_samp_merge_null_passthrough_for_n_zero_and_n_one` in
      `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs` (`:2933-2936`,
      `:2957-2960`). State the live-captured contract — Exasol returns NULL if ANY argument is NULL,
      so `SQRT(GREATEST(0.0, NULL))` is already NULL — and give the retained `CASE WHEN … IS NULL`
      guard its honest reason: it states the NULL path explicitly and is pinned byte-for-byte by
      golden fixtures, while the `GREATEST(0.0, …)` clamp keeps its own unchanged purpose of
      stopping a tiny negative rounding artifact from reaching `SQRT`. **Acceptance:** no comment,
      test name, or doc string in the repository claims Exasol's `GREATEST` skips NULLs; ZERO
      characters of generated SQL change — every `testdata/dispatch_golden/` fixture is byte-identical
      and all six `.contains(...)` merge tests pass with no edit to any expected value.
- [ ] 2.1 Add a live E2E regression test for issue #202 to
      `crates/lakehouse-engine/tests/e2e_scan_test.rs`, following the file's existing
      `setup_e2e()` / `exa_conn()` / `vs_table()` / `explain_virtual_sql` conventions and deriving a
      NULL for some rows only via `NULLIF(MOD(id, 5), 0)` (the technique
      `test_group_by_null_key_grouping` already uses, since the seed fixture has no nullable column).
      Assert the predicate position — `WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL` returns
      exactly 4 of the 20 seeded rows, where the unguarded rendering returns 0 — and the value
      position — `SELECT id, GREATEST(id, NULLIF(MOD(id, 5), 0)) … ORDER BY id` returns NULL for
      exactly the four multiples of 5 and the row's own `id` for the other sixteen. Assert via
      `explain_virtual_sql` that the guarded form reached the scan spec, so the test proves the
      translator rather than Exasol evaluating the expression itself. **Acceptance:** the fixture is
      discriminating in both directions (some rows NULL, some not) and the assertions are exact
      values, not row counts alone; the test FAILS rather than skips with no reachable Exasol
      container, using the file's existing fail-fast helpers; the doc comment records the pre-fix
      values (`0` rows and a non-NULL projection) so the regression stays legible.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2 |
| Group B | 2.1 |

Sequential dependencies:
- Group A → Group B (the E2E test asserts the fixed behavior, so it lands after the fix)

Within Group A, 1.1 touches only `crates/vs-expression/` and 1.2 touches only
`crates/lakehouse-engine/src/adapter/pushdown/`, so the two run concurrently. Both agents share one
working tree: neither may run `git stash`, `git reset`, or `git checkout`, which would transiently
revert the sibling's work.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| — | — | None. The fix rewrites one `format!` expression in place and corrects four doc comments; no function, test, module, or fixture becomes unreachable. The two existing `GREATEST`/`LEAST` unit tests are UPDATED rather than deleted — each still covers a live scenario clause, and the Exasol-dialect half of one is the regression guard that the guard did not leak dialects. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| GREATEST and LEAST translate to DataFusion greatest/least | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least` (updated: exact guarded SQL for a 3-argument `GREATEST` and a 2-argument `LEAST`) |
| GREATEST and LEAST translate to DataFusion greatest/least | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least_single_argument_guard` |
| GREATEST and LEAST translate to DataFusion greatest/least | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least_with_literal_null_argument` |
| GREATEST and LEAST translate to DataFusion greatest/least | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least_nested_argument_once_referenced_twice` |
| GREATEST and LEAST translate to DataFusion greatest/least | Unit | `crates/vs-expression/src/lib_tests.rs` | `renders_greatest_least_verbatim_in_exasol_dialect` (updated: Exasol half byte-identical, DataFusion half guarded) |
| GREATEST and LEAST translate to DataFusion greatest/least | Unit | `crates/vs-expression/src/lib_tests.rs` | `exasol_dialect_renders_declared_verbatim_surface` (existing, must stay green with NO edit) |
| A pushed-down GREATEST or LEAST over a NULL-producing argument returns NULL on the cluster | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `test_greatest_least_propagate_null_argument` |
| The sufficient-statistics fragments have one owner per denominator | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs` | `stddev_pop_merge_null_passthrough_for_n_zero` (existing, unchanged assertions, corrected doc comment) |
| The sufficient-statistics fragments have one owner per denominator | Unit | `crates/lakehouse-engine/src/adapter/pushdown/grouped_agg_tests.rs` | `stddev_samp_merge_null_passthrough_for_n_zero_and_n_one` (existing, unchanged assertions, corrected doc comment) |
| The sufficient-statistics fragments have one owner per denominator | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | existing `testdata/dispatch_golden/` full-string fixtures (must stay BYTE-IDENTICAL — the proof task 1.2 changed no SQL) |

Expression rendering is a total function of a request-JSON node with no I/O, so unit tests are the
right instrument for the rendered shape. The one integration test is what this repository's
verification discipline requires on top: the rendered SQL is only correct if the live Exasol engine
and the live DataFusion scan agree with it, which no unit test can establish. The
`pushdown-agg-sql-consolidation` scenario needs no new test — its change is documentation, and its
existing tests plus the byte-identical golden fixtures are exactly the evidence that nothing else
moved.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| `vs-expression-translator-scalar-fns` | `exapump sql --profile docker "SELECT GREATEST(0.0, NULL), LEAST(1.0, NULL), GREATEST(5) FROM dual"` | Empty, empty, `5` — the native Exasol contract this fix reproduces |
| `vs-expression-translator-scalar-fns` | `exapump sql --profile docker "SELECT COUNT(*) FROM MY_LAKEHOUSE.EVENTS WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL"` | `4`, not the pre-fix `0` |
| `vs-expression-translator-scalar-fns` | `exapump sql --profile docker "SELECT id, GREATEST(id, NULLIF(MOD(id, 5), 0)) FROM MY_LAKEHOUSE.EVENTS ORDER BY id"` | NULL at `id` 5, 10, 15, 20; the row's own `id` at the other sixteen |
| `vs-expression-translator-scalar-fns` | `exapump sql --profile docker -f csv "EXPLAIN VIRTUAL SELECT COUNT(*) FROM MY_LAKEHOUSE.EVENTS WHERE LEAST(id, NULLIF(MOD(id, 5), 0)) IS NULL"` | The pushed SQL's scan spec carries `CASE WHEN` and `least(` — the expression is delegated to DataFusion, not left to Exasol |
| `pushdown-agg-sql-consolidation` | `exapump sql --profile docker "SELECT STDDEV_POP(score) FROM MY_LAKEHOUSE.EVENTS WHERE id < 0"` | Empty (NULL) for the zero-row group — unchanged by this plan, confirming the retained guard still behaves |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `EXASOL_CONTAINER=lakehouse-engine-rs-2-exasol-1 make test-e2e > /tmp/e2e.log 2>&1; echo "rc=$?"` then read `/tmp/e2e.log` | `rc=0`, 0 failures. Do not judge the run from a piped `tail` — capture the exit code and read the log |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors/warnings |
| Format | `cargo fmt --all -- --check` | No changes |
