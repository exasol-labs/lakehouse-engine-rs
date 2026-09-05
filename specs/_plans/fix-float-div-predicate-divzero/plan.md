# Plan: fix-float-div-predicate-divzero

> **Status:** blocked — see open-questions.md

## Summary

Fix issue #370 by rendering a DataFusion-dialect `FLOAT_DIV` as a checked-division function call instead of the `/` operator, so a zero divisor raises where the division happens rather than only where the value reaches an emit-time check. The fix removes the silent wrong row count in predicate position and, as a consequence, replaces the projection path's infinity complaint and its `0/0` silent NULL with one division-by-zero error.

## Design

### Context

A pushed `FLOAT_DIV` by zero has three different outcomes today for one user error. Issue #370 measured all three live on `exasol/docker-db:2025.2.1`, with every pushed filter read out of the `EXPLAIN VIRTUAL` `PUSHDOWN_SQL` ScanSpec.

| Position | Value produced | Outcome today | Native Exasol |
|---|---|---|---|
| Projection, `x/0` | `+Inf` | Query fails, `22002`, `numeric value out of range: value inf ...` | Fails, `22012` |
| Projection, `0/0` | `NaN` | Query succeeds, silent NULL per row (`#246`) | Fails, `22012` |
| Predicate, `x/0` | `+Inf` | Query succeeds, 20 of 20 rows or 0 of 20 | Fails, `22012` |
| Predicate, `0/0` | `NaN` | Query succeeds, 0 of 20 or 20 of 20 | Fails, `22012` |

The predicate row is issue #370. The comparison consumes the non-finite value mid-plan, so no emit-boundary check ever sees it. The broadcast-join fact-leg filter reproduces the single-table result exactly and adds no divergence of its own.

The single cause is that the rendered SQL uses the `/` operator, and an operator has no way to fail. `crates/vs-expression` renders `(CAST(<left> AS DOUBLE) / <right>)` for the DataFusion dialect (`crates/vs-expression/src/lib.rs`, the `ADD | SUB | MULT | FLOAT_DIV` arm). Every scan path then splices that text: the raw-row `WHERE` and select list (`scan/raw_scan.rs`), the broadcast-join `WHERE` (`scan/join_scan.rs`), and both partial-aggregate SQL builders (`scan/partial_agg.rs`).

The recorded ADR `079-fix-float-div-truncation` already rejected the two obvious repairs. Widening `arrow_value_at`'s `is_nan()` check to `!is_finite()` fails because that boundary cannot tell a computed non-finite value from one stored in the source table, and it "misses the predicate case entirely". Rendering `NULLIF(<right>, 0)` fails because NULL is the wrong answer already observed. Neither ADR considered acting at the rendering layer.

- **Goals** — a pushed `FLOAT_DIV` by zero fails the query in every position; one code path decides the outcome; the check carries none of the stored-value ambiguity that ruled out the emit-boundary fix.
- **Non-Goals** — closing `#246` (it stays open for every other `NaN` at the emit boundary); comparison semantics for a `NaN` read from a column, which issue #370 reports as an uninvestigated second observation; a non-finite value produced in predicate position by a pushed scalar function other than `FLOAT_DIV`, which tracked exception 3 below covers and names; any change to the Exasol dialect; any change to advertised capabilities.

### Decision

Render the DataFusion-dialect `FLOAT_DIV` as `vs_checked_float_div(<left>, <right>)`. `crates/vs-expression` exports the function name as one public constant carrying the contract. `crates/lakehouse-engine` implements the function as a DataFusion `ScalarUDF` and registers it in `build_session_context`, the one production session builder.

The function coerces both operands to `Float64`, divides, propagates NULL, and raises when its own result is not finite. A zero divisor raises with a message naming a division by zero. Any other non-finite result raises with a message naming a numeric value out of range.

This is safe in a way the emit-boundary widening was not. The check sees only the two operands of a division the pushdown itself synthesised, so it never inspects a value read straight out of a column. A plain `SELECT <double_col>` over a table storing `NaN` reaches no checked division at all.

The `CAST(<left> AS DOUBLE)` wrapper goes away. The function owns the always-`DOUBLE` coercion for both operands, so a SQL-level cast for one of them would state the same decision twice.

#### Architecture

```
Exasol pushdown JSON
  │
  ▼
crates/vs-expression  ── Dialect::Exasol ──▶  "(<l> / <r>)"        ─▶ Exasol engine (raises 22012 itself)
  │  FLOAT_DIV arm
  └──────────────────── Dialect::DataFusion ─▶ "vs_checked_float_div(<l>, <r>)"
                                                        │
                        exports CHECKED_FLOAT_DIV_FN ────┤ (one name, one owner)
                                                        ▼
crates/lakehouse-engine  scan/checked_div.rs   ── ScalarUDF impl
                         scan/object_store.rs  ── build_session_context registers it
                         scan/emit.rs          ── classify_scan_error surfaces it by type
                                                        │
                        raw scan · join scan · partial agg (all share one session)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Session-registered scalar function named by an exported constant | `crates/vs-expression` constant, `scan/checked_div.rs` implementation | The name has one owner, so the crate that emits it and the crate that registers it cannot drift |
| Registration at the single session builder | `scan/object_store.rs` `build_session_context` | All three run paths take their session from it, so one call reaches every pushed expression |
| Error recognised by type, not by message text | `scan/emit.rs` `classify_scan_error` | A string match on an error message is a silent coupling that breaks on any wording change |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Checked-division function in the DataFusion dialect | Widen `arrow_value_at` to `!is_finite()` | Rejected already in ADR `079`: the emit boundary cannot tell a computed non-finite from a stored one, and it never sees a predicate at all |
| Checked-division function | Render `NULLIF(<right>, 0)` | Rejected already in ADR `079`: NULL is the wrong answer already observed, and it conflates a zero divisor with a NULL divisor |
| Checked-division function | Stop pushing any predicate containing a division, apply it in the Exasol wrapper instead | Gives exact `22012` parity, but costs filter pushdown on every division predicate to fix a pathological case, and needs the scan to project the operand columns |
| Checked-division function | Accept the gap and file a tracked exception | Rejected: a fix exists that is safe, small, and free of the false-positive risk that justified accepting the emit-boundary gap |
| Name owned by `crates/vs-expression`, implementation owned by `crates/lakehouse-engine` | Implement the `ScalarUDF` inside `crates/vs-expression` | Rejected: `vs-expression` depends only on `exasol-udf-sdk` and `serde_json` by design, and a DataFusion dependency would be a large change to a crate meant for sibling reuse |
| Drop the `CAST(<left> AS DOUBLE)` wrapper | Keep it and let the function coerce only the right operand | Rejected: the double-coercion decision would then live in two modules |
| Raise on ANY non-finite result, not only on a zero divisor | Check only `<right> == 0` | A finite numerator over a tiny divisor overflows to `±Inf` and reproduces #370's exact defect class; Exasol can represent no non-finite `DOUBLE`, so no non-finite result is ever a correct answer |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator-float-div | CHANGED | `specs/_plans/fix-float-div-predicate-divzero/sql-comprehension/vs-expression-translator-float-div/spec.md` |
| datafusion-scan/scan-execution-expression-pushdown | CHANGED | `specs/_plans/fix-float-div-predicate-divzero/datafusion-scan/scan-execution-expression-pushdown/spec.md` |

## Impact

Queries change behaviour for users. A pushed division by zero that used to succeed now fails.

- A `WHERE` predicate containing a division by zero fails instead of returning a row count that disagrees with Exasol. This is the fix.
- A projected `0/0` fails instead of returning NULL for each affected row.
- A projected `x/0` still fails. The message changes from `numeric value out of range: value inf ...` to a division-by-zero message, and the raising layer moves from the Exasol engine to the scan.
- A query dividing a column that legitimately stores `±Inf` or `NaN` now fails where a predicate over it used to succeed. Projecting the same value already fails today at `22002`.
- A GUARDED division can start failing. `WHERE <d> <> 0 AND <n> / <d> > 0` succeeds today and may raise after this change, because a guard conjunct does not stop DataFusion from evaluating the division over the rows the guard excluded. `datafusion-physical-expr` 54.1 pre-selects the surviving rows only when the left conjunct's true ratio over the batch is at or below `PRE_SELECTION_THRESHOLD` (0.2), and a division in the LEFT conjunct is never protected at all. The outcome therefore depends on per-batch selectivity and on conjunct order. Task 1.2 measures native Exasol's own answer for this shape before any code changes.
- No advertised capability changes. No Exasol-dialect SQL changes. No `ScanSpec` field changes. No pushdown is refused that was accepted before.

This is not a breaking API change. It is a correctness change that converts silent wrong answers into loud failures, with the guarded-division shape above as the one case where a correct answer can become an intermittent failure instead.

### Tracked exceptions this plan does not fix

Three named gaps remain. Per CLAUDE.md, each is recorded as an accurately scoped tracked exception rather than a silent gap. **All three GitHub issues MUST be filed before this plan ships**, and their numbers cited inline in the spec deltas the way `(#27)` is cited in `specs/datafusion-scan/scan-execution-field-id-projection/spec.md`. Each delta carries a greppable placeholder token that task 4.10 replaces with the filed number. Decision-log entries [7], [8], and [12] carry the proposed titles and scopes.

1. `(#TODO-suppression)` The division-by-zero error is a per-row side effect of an expression DataFusion evaluates over a row set of its own choosing, so it diverges from native Exasol in BOTH directions. It may be SUPPRESSED, because predicate evaluation order, file pruning, row-group pruning, and an applied LIMIT may skip the division for a row. It may also be RAISED for a row an adjacent guard conjunct already excluded, which is the guarded-division regression named above. The rows a successful query returns are unaffected in either direction.
2. `(#TODO-stored-nan)` Comparison semantics for a `NaN` read from a source column stay unmeasured. Issue #370 observed non-IEEE ordering and states the mechanism was not investigated. After this fix a pushed `FLOAT_DIV` cannot produce a `NaN`, so #370's own reproducer no longer reaches it.
3. `(#TODO-scalar-fns)` A non-finite value produced in predicate position by a pushed scalar function OTHER than `FLOAT_DIV` keeps the exact gap this plan closes. `crates/lakehouse-engine/src/adapter/capabilities.rs` advertises `FN_SQRT`, `FN_LN`, `FN_LOG`, `FN_ACOS`, `FN_ASIN`, `FN_EXP`, `FN_POWER`, and `FN_MOD`, each translated into a pushed predicate and each able to yield `NaN` or `±Inf`. `WHERE SQRT(<negative_col>) > 0` reproduces #370's mechanism with no division involved.

## Requirements

| Requirement | Details |
|-------------|---------|
| Performance | The function evaluates one whole Arrow array per call, not one row per call. Per-row cost stays a cast, a divide, and a finiteness test. No benchmark run is required, and none is claimed. |
| Plan shape | A spec whose SQL contains no division MUST produce a byte-identical plan. `datafusion-scan/scan-execution-plan-shape` and `tests/scan_parquet_pruning.rs` MUST pass unedited. |
| Pruning | A conjunct containing a checked division derives no min/max pruning bound, exactly as the `/` operator's conjunct derived none: neither shape is a column-against-literal comparison. `iceberg_predicate.rs` and `delta_predicate.rs` read the pushdown JSON tree, not the rendered SQL, and drop a node they cannot translate soundly, so plan-time file pruning is unchanged. |
| Security | The new error passes through the same redaction the other scan errors use. A credential value MUST NOT appear in the surfaced message. |
| Migration | None. No `ScanSpec` field changes, so an in-flight spec from an older adapter still parses. |
| Concurrency | None. The function is stateless and `Immutable`. |

## Dependencies

None new. The implementation uses `datafusion` 54.1 and `arrow` 58, both already pinned in `[workspace.dependencies]`.

## Implementation Tasks

### 1. Reproduce the defect

Both tasks in this section MUST run against an unmodified tree, before any task in section 2 or 3 lands. A reproduction run after the rendering change would call an unregistered function and return an unresolved-function error instead of the pre-fix measurement.

- [ ] 1.1 Bring up the local Docker stack and reproduce issue #370's single-table predicate case: run the `FACT_LINEITEM` query with the `(L_LINENUMBER - L_LINENUMBER)` divisor, confirm the pushed filter from `EXPLAIN VIRTUAL` `PUSHDOWN_SQL`, and record the row count against the native oracle. Record the same for the `0/0` shape and for the broadcast-join fact-leg shape.
- [ ] 1.2 Measure the GUARDED shape live against the `LHVS.GT_LINEITEM_SCAN` native oracle, in BOTH conjunct orders, and record whether native Exasol raises: `SELECT COUNT(*) FROM ... WHERE L_LINENUMBER <> 0 AND 0 < L_ORDERKEY / (L_LINENUMBER - L_LINENUMBER)` and the same conjuncts reversed. Confirm from `EXPLAIN VIRTUAL` `PUSHDOWN_SQL` that both conjuncts reach the scan in one pushed filter, and record the pre-fix row count for each order. This is the shape a correct query today can start failing on after the fix, so the native answer decides whether the post-fix behaviour is a parity gain or a regression. Write the measured outcome into the `A division by zero inside a filter predicate fails the query rather than changing the row count` scenario of `specs/_plans/fix-float-div-predicate-divzero/sql-comprehension/vs-expression-translator-float-div/spec.md`, replacing the clause that says the outcome SHALL be measured.

### 2. Checked-division rendering in `crates/vs-expression`

- [ ] 2.1 Add a failing unit test asserting the DataFusion dialect renders `vs_checked_float_div("A", 1)` for a `FLOAT_DIV` node, and that the Exasol dialect still renders `("A" / 1)`.
- [ ] 2.2 Export the function name as one public constant with a doc comment stating the full contract the registered implementation must satisfy: two arguments, both coerced to `Float64`, `Float64` result, NULL propagated, error when the result is not finite.
- [ ] 2.3 Change the `FLOAT_DIV` arm so the DataFusion dialect renders the call from that constant and no longer wraps the left operand in `CAST(... AS DOUBLE)`. Leave the Exasol dialect byte-identical.
- [ ] 2.4 Update the nine `float_div_*` rendering tests and retarget the divergence guard `float_div_casts_to_double_only_in_the_datafusion_dialect` to assert the new dialect pair.
- [ ] 2.5 Add the checked-division function name to the sweep test's banned-token list, and confirm the sweep's Exasol expectation for `FLOAT_DIV` stays `("A" / "B")`.
- [ ] 2.6 Remove `cast_to_double`, now unreachable. Keep `DOUBLE_TYPE`, which `render_cast_target` still uses.
- [ ] 2.7 Update the pushdown-text assertion in `e2e_float_div_pushes_double_cast_projection` to the new rendering, and rename it to match.

### 3. Checked-division evaluation in `crates/lakehouse-engine`

- [ ] 3.1 Add failing unit tests for the scalar function over one batch: `Int64/Int64`, `Decimal128/Int64`, `Int64/Decimal128`, `Decimal128/Decimal128`, `Float32/Float64`, `Float64/Float64`; a NULL in either operand; a NULL numerator over a zero divisor; a zero divisor with a non-zero numerator; `0/0`; a `-0.0` divisor; an overflow to `+Inf` from finite operands; and a stored `NaN` operand.
- [ ] 3.2 Implement the `ScalarUDFImpl` in a new `crates/lakehouse-engine/src/scan/checked_div.rs`, modelled on `NestedJsonRenderUdf` in `raw_scan.rs`: `Signature::any(2, Volatility::Immutable)`, `Float64` return type, both operands cast to `Float64` inside `invoke_with_args`, NULL preserved through the null mask, and an error raised for a non-finite result. Distinguish a zero divisor from any other non-finite cause in the message. [expert]
- [ ] 3.3 Register the function in `build_session_context` (`scan/object_store.rs`), reading the name from the `vs-expression` constant, next to the existing `register_nested_json_render_udf` call. Add a unit test asserting a session built by that function resolves the name.
- [ ] 3.4 Add the classification arm to `classify_scan_error` (`scan/emit.rs`) so the error surfaces without the `scan failed: assigned data could not be read` prefix. Recognise it by type through a dedicated error carried on the DataFusion error chain, never by matching message text. Add a unit test that a credential value passed in `secrets` is still absent from the surfaced message.
- [ ] 3.5 Establish which route a two-literal division takes out of the scan. DataFusion may fold it during optimization, and a planning failure surfaces through `UdfError::User("DataFusion SQL error: {e}")` in `raw_scan.rs` / `join_scan.rs` and `"partial aggregate SQL error: {e}"` in `partial_agg.rs`, bypassing `classify_scan_error`. Add a test pinning the observed route, and extend the framing to it if the fold raises at plan time.

### 4. End-to-end verification

- [ ] 4.1 Rewrite `e2e_float_div_by_zero_projected_fails_with_inf_out_of_range` to assert the query fails with a division-by-zero message, and record the SQL state observed live rather than assuming it.
- [ ] 4.2 Rewrite `e2e_zero_div_zero_projected_returns_silent_null` to assert the query now fails with the same division-by-zero message.
- [ ] 4.3 Add an E2E test in `tests/e2e_scan_test.rs` for the single-table predicate position: assert the query fails for `> 0` and for `< 0`, for the `x/0` shape and the `0/0` shape, and assert the filter reaching the scan is the pushed one by reading `EXPLAIN VIRTUAL`.
- [ ] 4.4 Add an E2E test in `tests/e2e_scan_test.rs` asserting a NULL divisor in a predicate still returns no rows and does not fail.
- [ ] 4.5 Add an E2E test in `tests/e2e_scan_test.rs` for the GUARDED shape task 1.2 measured, `e2e_float_div_guarded_by_a_non_zero_conjunct_matches_the_measured_outcome`: run both conjunct orders against the native oracle, assert the outcome task 1.2 recorded in the spec, and assert from `EXPLAIN VIRTUAL` that both conjuncts reach the scan in one pushed filter. If the outcome is a raise, the test pins the over-raise direction of tracked exception 1 rather than a parity claim.
- [ ] 4.6 Add an E2E test in `tests/e2e_join_test.rs` for the broadcast-join fact-leg filter: assert broadcast is retained (a `"join":{` common blob, no `LHS_T0` two-scan wrapper) and that the query fails.
- [ ] 4.7 Add an E2E test in `tests/e2e_scan_test.rs` for a division by zero inside a pushed aggregate argument (`SUM(L_ORDERKEY / (L_LINENUMBER - L_LINENUMBER))`): assert the aggregate is pushed by reading `EXPLAIN VIRTUAL`, and assert the query fails with the same division-by-zero message rather than reaching `arrow_value_at`'s separate check.
- [ ] 4.8 Confirm `e2e_float_div_int_over_int_matches_native_oracle` and `e2e_float_div_filter_row_count_matches_native_oracle` still pass unchanged, so the fix does not regress a correct division.
- [ ] 4.9 Confirm both `dispatch_golden` fixtures carrying a translated `FLOAT_DIV` are unchanged, that `datafusion-scan/scan-execution-plan-shape`'s tests and `tests/scan_parquet_pruning.rs` pass unedited, and that `cargo test` reports no golden diff.
- [ ] 4.10 File the three tracked-exception issues from decision-log entries [7], [8], and [12], then replace every `(#TODO-suppression)`, `(#TODO-stored-nan)`, and `(#TODO-scalar-fns)` token in `plan.md` and in both spec deltas with the filed issue number in the `(#NNN)` form. FAIL this task if `grep -rn '(#TODO-' specs/_plans/fix-float-div-predicate-divzero/` returns any line.
- [ ] 4.11 Run the verification checklist below.

## Parallelization

| Group | Tasks | Depends on | Knowledge |
|-------|-------|------------|-----------|
| A: Live pre-fix reproduction | 1.1-1.2 | — | This group MUST run against an UNMODIFIED tree: no task from group B or C may have landed, or the pushed SQL calls a function no session registers and the query returns an unresolved-function error instead of the pre-fix measurement. Issue #370's body; both spec deltas as the record the measurement is written back into; `crates/lakehouse-engine/tests/e2e_scan_test.rs` and `crates/lakehouse-engine/tests/e2e_join_test.rs` for the fixture and `EXPLAIN VIRTUAL` helpers |
| B: Checked-division rendering | 2.1-2.7 | A (the reproduction must precede the fix, per CLAUDE.md verification discipline) | spec delta `sql-comprehension/vs-expression-translator-float-div`; `crates/vs-expression/src/lib.rs`, `crates/vs-expression/src/lib_tests.rs`, `crates/lakehouse-engine/tests/e2e_scan_test.rs` |
| C: Checked-division evaluation and end-to-end proof | 3.1-3.5, 4.1-4.11 | B (imports the exported function-name constant; both groups edit `tests/e2e_scan_test.rs`) | spec delta `datafusion-scan/scan-execution-expression-pushdown`; `crates/lakehouse-engine/src/scan/checked_div.rs`, `crates/lakehouse-engine/src/scan/object_store.rs`, `crates/lakehouse-engine/src/scan/emit.rs`, `crates/lakehouse-engine/tests/e2e_scan_test.rs`, `crates/lakehouse-engine/tests/e2e_join_test.rs` |

The three groups run in sequence, not in parallel. Group A is the live pre-fix measurement and owns the whole of it, because CLAUDE.md's verification discipline requires the reproduction to precede the fix and the pre-fix behaviour stops being observable the moment group B lands. Groups B and C are the two crates and the two spec deltas: group C imports the constant group B exports, and both touch `tests/e2e_scan_test.rs`. Only group C carries an `[expert]` task.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Function | `crates/vs-expression/src/lib.rs` `cast_to_double` | Its only caller is the `FLOAT_DIV` DataFusion arm, which no longer emits a cast |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| FLOAT_DIV renders true float division in the DataFusion dialect | Unit | `crates/vs-expression/src/lib_tests.rs` | `float_div_renders_a_checked_call_against_column_right_operand` (plus the eight sibling `float_div_*` operand-shape tests) |
| FLOAT_DIV renders true float division in the DataFusion dialect (name has one owner) | Unit | `crates/vs-expression/src/lib_tests.rs` | `float_div_rendering_reads_the_exported_function_name_constant` |
| FLOAT_DIV renders true float division in the DataFusion dialect (pushed text, live) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_pushes_checked_division_projection` |
| The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator | Unit | `crates/vs-expression/src/lib_tests.rs` | `float_div_renders_a_checked_call_only_in_the_datafusion_dialect` |
| The Exasol dialect keeps rendering FLOAT_DIV as a bare division operator (consumer SQL frozen) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden_tests.rs` | `single_group_scalar_over_aggregate_dedup` and `single_group_scalar_over_aggregate_interleaved` golden comparisons |
| A pushed-down division by zero fails the query rather than returning a wrong value | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_by_zero_projected_fails_with_division_by_zero` |
| A division by zero inside a filter predicate fails the query rather than changing the row count | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_by_zero_in_filter_fails_like_native_exasol` |
| A division by zero inside a filter predicate fails the query rather than changing the row count (NULL divisor) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_null_divisor_in_filter_returns_no_rows` |
| A division by zero inside a filter predicate fails the query rather than changing the row count (join leg) | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_join_float_div_by_zero_in_fact_leg_filter_fails` |
| A division by zero inside a filter predicate fails the query rather than changing the row count (guarded shape, both conjunct orders) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_guarded_by_a_non_zero_conjunct_matches_the_measured_outcome` |
| Zero divided by zero fails the query instead of reaching the NaN-at-emit gap | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_zero_div_zero_projected_fails_with_division_by_zero` |
| FLOAT_DIV stays outside the verbatim rule in both dialects | Unit | `crates/vs-expression/src/lib_tests.rs` | `exasol_dialect_renders_declared_verbatim_surface` |
| The scan session registers the checked float-division function every pushed expression needs | Unit | `crates/lakehouse-engine/src/scan/object_store_tests.rs` | `build_session_context_registers_the_checked_float_div_function` |
| The scan session registers the checked float-division function every pushed expression needs (plan shape unchanged) | Integration | `crates/lakehouse-engine/tests/scan_parquet_pruning.rs` and the `datafusion-scan/scan-execution-plan-shape` tests | existing tests, unedited |
| A checked float division raises rather than producing a non-finite value (aggregate argument) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_float_div_by_zero_in_aggregate_argument_fails` |
| A checked float division raises rather than producing a non-finite value (plan-time fold route) | Unit | `crates/lakehouse-engine/src/scan/checked_div_tests.rs` | `checked_float_div_over_two_literals_surfaces_a_division_by_zero_message` |
| A checked float division raises rather than producing a non-finite value | Unit | `crates/lakehouse-engine/src/scan/checked_div_tests.rs` | `checked_float_div_divides_every_operand_pairing_as_double`, `checked_float_div_propagates_null_in_either_operand`, `checked_float_div_raises_on_a_zero_divisor`, `checked_float_div_raises_on_zero_over_zero`, `checked_float_div_treats_negative_zero_as_zero`, `checked_float_div_raises_on_an_overflow_to_infinity`, `checked_float_div_raises_on_a_stored_non_finite_operand` |
| A checked float division raises rather than producing a non-finite value (error framing) | Unit | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `classify_scan_error_names_a_checked_division_failure_without_the_storage_prefix`, `classify_scan_error_redacts_secrets_from_a_checked_division_failure` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| sql-comprehension/vs-expression-translator-float-div | `EXPLAIN VIRTUAL SELECT L_ORDERKEY / L_LINENUMBER FROM MY_LAKEHOUSE.FACT_LINEITEM;` | The `PUSHDOWN_SQL` ScanSpec projection reads `vs_checked_float_div("L_ORDERKEY", "L_LINENUMBER")`, and `emit_exa_types` stays `["DOUBLE PRECISION"]` |
| datafusion-scan/scan-execution-expression-pushdown | `SELECT COUNT(*) FROM MY_LAKEHOUSE.FACT_LINEITEM WHERE 0 < L_ORDERKEY / (L_LINENUMBER - L_LINENUMBER);` | The query fails with a message naming a division by zero, instead of returning 20 |
| datafusion-scan/scan-execution-expression-pushdown | `SELECT L_ORDERKEY / L_LINENUMBER FROM MY_LAKEHOUSE.FACT_LINEITEM WHERE L_ORDERKEY = 7 AND L_LINENUMBER = 2;` | Returns `3.5`, unchanged, so a correct division still works |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures, and it FAILS rather than skips if the Docker stack is down |
| Lint | `cargo clippy --all-targets` | 0 errors, 0 warnings |
| Format | `cargo fmt` | No changes |
| Tracked-exception issues filed | `grep -rn '(#TODO-' specs/_plans/fix-float-div-predicate-divzero/` | No output. Every placeholder token has been replaced by task 4.10 with the filed issue number in the `(#NNN)` form |
</content>
</invoke>
