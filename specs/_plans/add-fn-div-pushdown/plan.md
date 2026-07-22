# Plan: add-fn-div-pushdown

## Summary

Correct the factual basis of the `FN_DIV` decline: live Exasol verification shows `DIV` truncates
toward zero (not floor division as recorded), so the existing ADR and spec rationale are wrong even
though the decline outcome stands. This plan corrects the record — no capability is advertised and
no runtime behavior changes.

## Design

### Context

GitHub issue #105 flagged two unadvertised scalar operators, `FN_NEG` and `FN_DIV`. `FN_NEG` shipped
in PR #115 and is locked in by test and spec. `FN_DIV` was declined in ADR
`exclude-fn-div-no-faithful-datafusion-floor-division` on the premise that "Exasol `DIV` is floor
division" diverging from DataFusion truncation. Independent live verification refutes that premise.

Verified against the live Exasol engine (2026-07-22):

| Expression | Result | Interpretation |
|------------|--------|----------------|
| `DIV(-7, 2)` | `-3` | truncation toward zero (floor would give `-4`) |
| `DIV(7, -2)` | `-3` | truncation toward zero |
| `DIV(15.7, 6.2)` | `2` | `TRUNC(m/n)`, applies to decimal operands too |
| `DIV(5, 0)` | error 22012 | division by zero raises |

DataFusion 54 (docs verified): integer `/` truncates toward zero; float division by zero yields
infinity (32.0.0 changelog); no `div` builtin exists.

So Exasol `DIV` equals `TRUNC(m/n)`, and DataFusion integer `/` already matches Exasol `DIV` for
integer operands. The old ADR's stated divergence — floor vs. truncation on negative operands — does
not exist. The false premise is dangerous: a future planner could "fix" the supposed divergence by
shipping `FN_DIV` via `FLOOR`, which would itself diverge from Exasol.

- **Goals** — Replace the false floor-division rationale with the verified truncation semantics in
  the spec, the ADR, and the one stale code comment. Keep `FN_DIV` unadvertised.
- **Non-Goals** — Advertise `FN_DIV`; change the translator's decline behavior; change any test
  assertion; touch `FN_NEG` (already shipped), `FN_CAST`, or the other issue-104/106/107 declines.

### Decision

Keep `FN_DIV` unadvertised, restated on the verified reason. DataFusion 54 has no `div` builtin. A
`TRUNC(m/n)` emulation would render every operand type, but for DOUBLE operands it diverges from
Exasol on division by zero — Exasol raises SQL state 22012 while DataFusion float division yields
infinity. Unlike CAST, whose explicit `dataType` field lets `render_cast_target` decline only the
unsupported target subset while still advertising `FN_CAST`, DIV's operand types are not carried in
the expression node — the arithmetic operator arm renders operands via recursive calls into opaque
SQL strings without ever inspecting their types, so the translator cannot identify a safe
integer-only case to render selectively. The decline therefore stands; only its rationale is
corrected.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Per-function decline with verified rationale | `crates/vs-expression` | Matches the `FN_TO_CHAR`/regexp/date-fn decline pattern: advertise only when the DataFusion result is confirmed to match Exasol for every argument shape |
| Superseding ADR | `decision-log.md` | The old ADR's factual premise is wrong; the corrected ADR preserves the same outcome for the right reason |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Keep `FN_DIV` declined; correct the rationale to truncation + DOUBLE div-by-zero divergence | Advertise `FN_DIV` via `TRUNC(m/n)` | Rejected — DOUBLE-operand division by zero yields infinity in DataFusion vs. an Exasol error, and DIV's operand types aren't carried in the expression node so the translator cannot selectively render only the safe integer case; no verified-safe rendering exists |
| Correct spec/ADR/comment now | Close #105 with no spec change | Rejected — the permanent spec and ADR assert a live-refuted premise ("floor division"); leaving it risks a future wrong "fix" |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |

The delta corrects the Background paragraph and the "Integer division DIV is deliberately not
translated" scenario. The decline behavior and its test assertions are unchanged.

## Implementation Tasks

1. Correct the stale doc comment in `crates/vs-expression/src/lib.rs` (the `div_falls_through_as_unsupported` test, lines ~2129–2131) — replace "Exasol DIV is floor division; DataFusion 54 `/` truncates … (diverges on negative operands)" with the verified reason: Exasol `DIV` truncates toward zero and matches DataFusion integer `/`, but DataFusion has no `div` builtin and a `TRUNC(m/n)` emulation diverges on DOUBLE division by zero.
2. Confirm `cargo test -p vs-expression div_falls_through_as_unsupported` still passes unchanged (behavior is identical; only the comment moved).

## Parallelization

Single-file change; no parallel groups.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| — | — | None. No code, test, or spec becomes obsolete; the change is a rationale correction. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Integer division DIV is deliberately not translated | Unit | `crates/vs-expression/src/lib.rs` | `div_falls_through_as_unsupported` |

The scenario's assertions (translator declines `DIV` in raising and safe modes) are unchanged; the
existing test already covers them. Only the scenario's rationale prose and the test's doc comment
change. Exasol's runtime `DIV` semantics are verified live (see Design) and cannot be asserted from
the Rust crate.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| sql-comprehension/vs-expression-translator-scalar-ops | `cargo test -p vs-expression div_falls_through_as_unsupported` | Test passes; `DIV` declines in both modes |
| sql-comprehension/vs-expression-translator-scalar-ops | `cargo test -p lakehouse-engine capabilities` | `FN_DIV` stays in the "declined translations must stay unadvertised" list |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |

## Follow-up

After this plan records, close GitHub issue #105: `FN_NEG` shipped in PR #115; `FN_DIV` declined per
the corrected ADR `exclude-fn-div-no-faithful-datafusion-truncated-division`. No residual scope
remains unless DataFusion adds a faithful `div` with matching division-by-zero semantics.
