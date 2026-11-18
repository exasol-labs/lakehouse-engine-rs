# Plan Review Findings: fix-string-function-integer-args

## Summary
- Axes checked: 4/4
- Total findings: 6 (Blockers: 1, Findings: 5)
- Intent Fidelity blockers: 0

Live checks run for this review on the local Docker Exasol (native `FROM DUAL`):

| Query | Native result |
|---|---|
| `SUBSTR('abcd', -2)`, `SUBSTR('abcd', -1.5)`, `SUBSTR('abcd', -0.5)` | `'cd'`, `'cd'`, `'d'` (negative halves round away from zero; a negative start counts from the end) |
| `SUBSTR('abcd', 2, -1)`, `LEFT('abcd', -1)` | NULL, NULL |
| `RPAD('ab', 1e20)`, `LEFT`, `SUBSTR` start and length, `CHR`, `REPEAT` with `1e20` | `data exception - numeric value out of range: value 100000000000000000000 is not in [ -999999999 .. 999999999 ]` (SQL state 22003) |

Through the deployed virtual schema, `SELECT SUBSTR(C_VARCHAR, -1) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE` pushes `substr("C_VARCHAR", -1)` and fails with `string data, right truncation: max length: 1, emitted: 2`.

## Intent Fidelity

#### [INTENT_GAP] FINDINGS
- Location: plan.md § Requirements, "Conversion by Arrow type"
- Issue: The issue's proposal says "An argument whose type the translator cannot tell declines, and Exasol computes the function." The plan says "Any other type: unchanged, so DataFusion's own coercion error stands." The user chose the scan rule, so the scan cannot decline, but no direction says which argument types still fail the query after the fix (for example a `Utf8` argument, or a dictionary-encoded integer column) and whether the `vs-expression` typed pass already declines them before the scan.
- Fix: Add a direction to intent.md that names the argument types the rule leaves unchanged, says whether each can reach the scan, and states that such a call still fails as before (not a new regression).

No other objection, axis checked: the eight functions, the three argument shapes, and the select list / WHERE / GROUP BY positions from #604 all appear in the scenario and the E2E plan; the ROUND/TRUNC follow-up and the scan-rule location are interview decisions; D4's reframing of "pins the rendered scan SQL" follows from the interview's scan-rule choice.

## Feasibility

#### [UNSTATED_ASSUMPTION] BLOCKER
- Location: plan.md § Requirements, "Out of range"; intent.md § Directions D3; spec delta § Scenario: A string function's numeric argument beyond BIGINT fails the query
- Issue: The plan assumes the valid range is BIGINT and that Exasol's error wording cannot be matched ("Exasol's error wording is not reproduced (D3)"). Live, native Exasol rejects every one of the eight functions' numeric arguments outside `[-999999999 .. 999999999]` with `data exception - numeric value out of range` (SQL state 22003). So for a value between 10^9 and `i64::MAX` the planned scan returns a result (or, for `RPAD`/`LPAD`/`REPEAT`, tries to allocate a multi-gigabyte string per row and risks a VM crash) where native Exasol fails the query. That breaks result parity (mission Core Capability 12) for a reachable input such as `RPAD(C_VARCHAR, C_DOUBLE * 1e9)`. D3's wording premise is also wrong: the scan already has `numeric_overflow` and the session failure slot that produce exactly that message prefix.
- Fix: In plan.md § Requirements, replace "Out of range" with: after rounding, an argument outside `[-999999999, 999999999]` fails the query with a `numeric_overflow` message recorded in the session's failure slot (so it survives the Parquet row filter), as the "Exact overflow" scenario already does. Rewrite the second scenario's GIVEN to "outside Exasol's argument range of ±999,999,999" and its THEN to "SHALL fail with a message that starts with `data exception - numeric value out of range`". Rewrite D3 accordingly, and change `string_function_numeric_argument_beyond_bigint_fails` to use `1e9` and `-1e9` (just outside) plus `999999999` (just inside, succeeds). State in a direction whether an existing `Int64` column argument is range-checked too (it reaches the same arm only if the rule checks `Int64`, which the plan leaves unchanged).

#### [HIDDEN_DEPENDENCY] FINDINGS
- Location: plan.md § Verification, Scenario Coverage bullet ("`SUBSTR(C_VARCHAR, C_DOUBLE - 1, C_DOUBLE)`"); plan.md § Requirements, "Rounding"; intent.md D2
- Issue: The scan's `substr` does not match native for a negative start or a negative length, independent of this fix. DataFusion clamps a negative start to the string's beginning and raises `negative count not allowed` for a negative length; native Exasol counts a negative start from the end (`SUBSTR('abcd', -2) = 'cd'`) and returns NULL for a negative length. The planned E2E example `SUBSTR(C_VARCHAR, C_DOUBLE - 1, C_DOUBLE)` gives a start of `-0.5 → -1` for the rows with `C_DOUBLE = 0.5`, so it will mismatch native for that pre-existing reason. D2's probe `SUBSTR('abcd', 2, -0.5)` hits the negative-length error the same way. `LEFT` with a negative count also differs (native NULL, DataFusion "all but the last n").
- Fix: In plan.md § Verification, keep every `SUBSTR` start at or above 1 and every length, count, and `LEFT`/`RIGHT` argument at or above 0 after rounding (for example `SUBSTR(C_VARCHAR, C_DOUBLE + 0.5, C_DOUBLE)`). Update D2 to record the live result above (negative halves round away from zero: `-1.5 → -2`, `-0.5 → -1`) and drop its "check before implementing" step; pin the negative half in the unit test only. Add an open question listing the negative-start/negative-length deviation as a follow-up `(#TBD)`.

No other objection on NFRs or ordering: DataFusion 55.2's `round` accepts `Decimal32/64/128/256` and `Float32/64` (checked in `datafusion-functions-55.2.0/src/math/round.rs`), and `Expr::Cast` executes with a checked cast.

## Requirement Quality

#### [COMPLETENESS_GAP] FINDINGS
- Location: spec delta § Scenario: A string function's numeric argument rounds to an integer as Exasol does; plan.md § Verification, Scenario Coverage
- Issue: The WHEN step lists "a select list, a filter, a GROUP BY key, an aggregate argument, or a join condition", and the THEN claims parity for "any integer, DECIMAL, or DOUBLE expression". No planned test covers a join condition, and per the finding above a negative `SUBSTR` start or negative length does not match native.
- Fix: Either add a broadcast-join E2E case whose join condition calls one of the functions with a DECIMAL argument (and confirm with `EXPLAIN VIRTUAL` that it is pushed), or drop "or a join condition" from the WHEN step. Narrow the GIVEN to "whose rounded argument is a valid non-negative length or a positive start", or add a sentence that negative `SUBSTR` arguments are outside this scenario.

#### [COMPLETENESS_GAP] FINDINGS
- Location: plan.md § Verification, Scenario Coverage row for "A string function's numeric argument beyond BIGINT fails the query"
- Issue: The failure scenario is proved only by a unit test. `specs/testing.md` § Coverage rule requires an E2E test for a scenario a user sees through Exasol SQL; a failed query is observable, and `assert_cases_match_native` already takes `error_cases` that assert native and the virtual schema both fail.
- Fix: Add an `ERROR_CASES` entry to `string_function_numeric_arguments_match_native`, such as `SELECT ID, RPAD(C_VARCHAR, C_DOUBLE * 1e9) FROM {T}` with `Has(SCAN_ITEM)`, and list it in the Scenario Coverage table against the second scenario.

#### [UNSTATED_ASSUMPTION] FINDINGS
- Location: plan.md § Requirements, "Conversion by Arrow type" ("`Int64` and `Null`: unchanged")
- Issue: Leaving a `Null`-typed argument unchanged assumes DataFusion coerces `Null` into `lpad`'s, `rpad`'s, `left`'s, and `right`'s exact `Int64` signature. Nothing in the plan checks it.
- Fix: Either cast `Null` to `Int64` with the integer arm (a no-cost cast), or add a unit case `rpad(s, NULL)` that proves the unchanged path plans.

No objection on Background, axis checked: the delta's Background and feature description are byte-identical to the recorded `specs/scan-types/scan-numeric-typing/spec.md`, and `speq plan validate` passes. No conflict with recorded scenarios: the existing "Exact overflow" scenario is about exact arithmetic and SUM, and the BLOCKER fix above aligns the new failure scenario with its message.

## Design Depth

No objection, axis checked: the change adds one match arm to the existing `ExasolNumericTyping::rewrite_expr`, which already rewrites `greatest`/`least`/`coalesce` arguments by Arrow type, so no new module or interface appears. intent.md has no `ADR: yes` direction and the plan has no architecture delta; the plan states no new component, boundary, or data flow. Accepted ADRs `typed-pass-infers-exasol-types` and `declared-types-from-typed-pass` govern the `vs-expression` typed pass and declared types; the plan leaves the scan SQL and declared types unchanged, so it conflicts with neither.
