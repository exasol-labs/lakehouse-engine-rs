# Plan: add-bitwise-function-pushdown

## Summary

Investigate the eleven `FN_BIT_*` bitwise operator functions from issue #108 against pinned
DataFusion 54.0.0 and decline all eleven, recording the decline as a cited tracked exception with
regression tests so the gap reads as investigated-and-declined rather than an open omission.

## Design

### Context

Issue #108 reports that Exasol's bitwise operator functions (`FN_BIT_AND`, `FN_BIT_OR`,
`FN_BIT_XOR`, `FN_BIT_NOT`, `FN_BIT_LSHIFT`, `FN_BIT_RSHIFT`, `FN_BIT_LROTATE`, `FN_BIT_RROTATE`,
`FN_BIT_CHECK`, `FN_BIT_SET`, `FN_BIT_TO_NUM`) are not advertised, so bitwise expressions fall back
to raw row scanning. The issue's own backing-path bar requires a verified `crates/vs-expression`
translation whose DataFusion result matches Exasol's semantics before any capability is advertised.
The planning duty is to determine per function whether a faithful translation exists at the pinned
versions — not to assume the DataFusion equivalents exist or behave identically.

- **Goals** — Determine faithfulness per function against pinned DataFusion 54.0.0; record the
  determination; advertise only functions that pass the backing-path and semantic-parity bar; cite
  issue #108 inline in both governing specs as a tracked exception.
- **Non-Goals** — Advertising any bitwise capability that diverges from Exasol; adding an
  unsigned-cast rendering mechanism; touching `FN_BIT_LENGTH` (an Exasol string function, out of
  scope per the issue); changing any other translator or capability behavior.

### Decision

Decline all eleven functions. None has a faithful DataFusion 54.0.0 translation over Exasol's
unsigned 64-bit bit-function domain. The eleven stay unadvertised; the adapter falls back to row
scanning and Exasol evaluates the function. This mirrors the recorded regexp decline
(`039-add-fn-regexp-pushdown`, issue #106) and the `DIV`/`TO_CHAR`/`TO_NUMBER` declines in
`sql-comprehension/vs-expression-translator-scalar-ops`.

#### Evidence (read from pinned sources, not memory)

Exasol semantics — confirmed against Exasol's published function documentation (docs.exasol.com):
bit functions operate on unsigned 64-bit integers, argument range `0`–`18446744073709551615`,
result type `DECIMAL(20,0)`; `BIT_NOT(0) = 18446744073709551615`; shift and rotate counts are
`0`–`63`.

Iceberg type domain — confirmed against the Apache Iceberg spec Primitive Types table
(`format/spec.md`): `int` is "32-bit signed integers", `long` is "64-bit signed integers"; the spec
defines no unsigned integer primitive. Per the project's Iceberg-compliance rule, this is a
deliberate, named trade-off driven by the source-type domain, not a silent gap.

DataFusion 54.0.0 — read from the pinned crate sources in the cargo registry:

| Function(s) | DataFusion 54.0.0 evidence | Blocker |
|---|---|---|
| `BIT_AND`, `BIT_OR`, `BIT_XOR` | Operators `&`/`|`/`#` exist (`datafusion-expr` `Operator::BitwiseAnd/Or/Xor`, `datafusion-sql` `binary_op.rs`) | Act on signed operand type; a bit-63-set result is unsigned-large in Exasol but negative under signed `Int64`, and `Int64` → `DECIMAL(20,0)` carries the negative value. Type/value-blind translator cannot restrict to the safe subset (same limitation as `DIV`) |
| `BIT_LSHIFT`, `BIT_RSHIFT` | Operators `<<`/`>>` exist (`Operator::BitwiseShiftLeft/Right`) | Same signed/unsigned result divergence; `>>` on signed integers is arithmetic (sign-extend) vs Exasol's logical (zero-fill), diverging unconditionally on any bit-63-set operand |
| `BIT_NOT` | SQL planner `parse_sql_unary_op` (`datafusion-sql/src/expr/unary_op.rs`) handles only `Not`/`Plus`/`Minus`; unary `~` → `not_impl_err` | No operator or function; `~x` signed = `-(x+1)` ≠ Exasol `2^64-1-x` unsigned |
| `BIT_LROTATE`, `BIT_RROTATE`, `BIT_CHECK`, `BIT_SET`, `BIT_TO_NUM` | `datafusion-functions` 54.0.0 registers no rotate / bit-test / bit-set / bits-to-number scalar function (only string `bit_length`) | No builtin exists |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Decline all eleven; document and cite #108 in both specs | Advertise `BIT_AND`/`BIT_OR`/`BIT_XOR`/shifts, decline the rest | The operator-backed functions diverge on the unsigned-64-bit domain and the type/value-blind translator cannot restrict to the safe subset — the recorded `DIV` limitation |
| Spec + regression-test + citation change only, no production arm | Add an unsigned-cast rendering (`arrow_cast(a,'UInt64') & arrow_cast(b,'UInt64')` → `DECIMAL(20,0)`) | Fails the backing-path bar: casting a genuinely negative signed source to `UInt64` reinterprets it and masks the error Exasol raises on negative arguments; needs new per-function result-type/EMITS machinery; still covers zero of the six no-builtin functions |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |

## Implementation Tasks

1. Apply the two CHANGED spec deltas into the permanent library (recorder), preserving the
   surrounding scenarios and the existing `FN_ADD`/`FN_SUB`/`FN_MULT`/`FN_FLOAT_DIV`/`FN_NEG`/
   `FN_CAST` advertisements and `DIV`/`TO_CHAR`/`TO_NUMBER` declines.
2. Add a capability regression test asserting the eleven `FN_BIT_*` names are NOT advertised, in the
   `declined translations must stay unadvertised` block of `reports_audited_capability_set` in
   `crates/lakehouse-engine/src/adapter/capabilities.rs`, with an inline `#108` citation. [expert]
3. Add a translator decline-lock test in `crates/vs-expression/src/lib.rs` (modelled on
   `regexp_scalar_functions_fall_through`) asserting each of the eleven `BIT_*` `function_scalar`
   nodes returns an error naming the function in raising mode and `None` in safe mode, with an
   inline `#108` citation recording the two blocker classes. [expert]
4. Confirm no production decline arm is added — the eleven names fall through to the existing
   unsupported-`function_scalar` path (`Unsupported node type returns error in raising mode` /
   `Safe variant returns None for unsupported nodes`); the tests pin the decline behavior.
5. Close issue #108 as investigated-and-declined, linking this plan's decision-log and the two
   updated scenarios.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 2, Task 3 |
| Group B | Task 1, Task 4, Task 5 |

Sequential dependencies:
- Group A → Group B (recorder applies deltas and issue closes after the tests pin the behavior).

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| — | — | None. This plan adds no production code and removes none; the decline is a fall-through with no dedicated arm. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Bitwise operator function capabilities remain absent | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` (extended `FN_BIT_*` decline assertions) |
| Bitwise operator functions are deliberately not translated | Unit | `crates/vs-expression/src/lib.rs` | `bitwise_operator_functions_fall_through` |

Both scenarios assert deterministic, pure-computation decline behavior (capability-list contents;
translator return values) with no I/O, so unit tests are the correct form — matching the existing
`regexp_scalar_functions_fall_through` and `reports_audited_capability_set` decline tests.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-adapter/pushdown-planning-capability-extensions | `cargo test -p lakehouse-engine reports_audited_capability_set` | Pass — none of the eleven `FN_BIT_*` names appear in the advertised set |
| sql-comprehension/vs-expression-translator-scalar-ops | `cargo test -p vs-expression bitwise_operator_functions_fall_through` | Pass — each `BIT_*` node errors in raising mode and is `None` in safe mode |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
