# Plan: add-fn-regexp-pushdown

## Summary

Re-verify the four scalar regexp pushdown gaps raised in issue #106 against the pinned
dependency versions, affirm the recorded decision to leave them unadvertised, and strengthen the
citation trail so the gap reads as investigated-and-declined rather than an open omission.

## Design

### Context

Issue #106 reopens a gap already investigated and promoted to ADR: `FN_REGEXP_REPLACE`,
`FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, and `FN_REGEXP_COUNT` are not advertised, so queries using
them fall back to raw row scanning. Recorded decision `034-add-pushdown-capability-gaps` entry [5]
declined all four on three grounds: the Rust `regex` dialect, the missing `regexp_substr`, and
divergent argument shapes. The issue asks whether a literal-pattern validation gate changes that
calculus. The planning duty is to re-verify the blockers against the versions actually pinned and
either overturn or affirm the decision with fresh evidence — not to re-litigate from memory.

- **Goals** — Confirm each blocker against pinned DataFusion 54.0.0 and `regex` 1.12.4; record the
  re-verification; cite issue #106 inline in both governing specs.
- **Non-Goals** — Advertising any regexp scalar capability; adding a regex-dialect-validation
  dependency; changing translator or capability behavior; touching `FN_PRED_REGEXP_LIKE`.

### Decision

Affirm the recorded decision. All three blockers hold at the pinned versions, and the
literal-pattern validation alternative fails the project's own backing-path bar.

Re-verification evidence (read from the pinned sources in this repo, not from memory):

| Blocker | Evidence at pinned version | Status |
|---------|----------------------------|--------|
| Rust `regex` dialect | `regex` 1.12.4 (`Cargo.lock`); DataFusion `compile_regex` returns `regex::Regex`; the crate rejects backreferences and lookaround by design | Holds |
| No `regexp_substr` | `datafusion-functions` 54.0.0 registers `regexp_count`, `regexp_instr`, `regexp_like`, `regexp_match`, `regexp_replace` — no `regexp_substr` | Holds |
| Argument-shape divergence | `regexp_replace(str, pattern, replacement[, flags])` has no position/occurrence; `regexp_instr(str, regexp[, start[, N[, flags[, subexpr]]]])` carries `subexpr`, not Exasol's return-option; Exasol carries position/occurrence/return-option | Holds |

The literal-pattern validation gate (the issue's open question, and decision [5]'s rejected
alternative) fails for a reason stronger than decision [5]'s "cannot embed a regex engine": the
`regex` crate is already a transitive dependency, so a `Regex::new` compile check is cheap — but
compile-success does not prove match parity with Exasol's PCRE. POSIX leftmost-longest versus PCRE
leftmost-first, Unicode-versus-ASCII character classes, and dot-newline handling all diverge on
patterns that both engines accept. The issue's own backing-path bar requires the DataFusion result
to match Exasol's semantics; a compile check certifies syntax, not semantics, so it cannot meet
that bar. `regexp_substr` and the argument-shape gaps are unaffected by any pattern check.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Affirm the decline; document and cite #106 | Advertise the three functions that exist in DataFusion and gate on literal-pattern validation | Compile-success does not prove match parity; `regexp_substr` is absent and argument shapes diverge regardless of the pattern |
| Spec + citation change only, no behavior change | Add a regex-dialect-validation crate as a pushdown-time gate | The gate cannot certify semantic parity and leaves two blockers unaddressed; adds a dependency for no correctness gain |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator-scalar-fns | CHANGED | `sql-comprehension/vs-expression-translator-scalar-fns/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |

## Implementation Tasks

1. Apply the two CHANGED spec deltas into the permanent library (recorder), preserving the
   unchanged surrounding scenarios and the `FN_PRED_REGEXP_LIKE` advertisement.
2. Add the `#106` citation to the two backing-code comments that already document the decline —
   the decline arm comment in `crates/vs-expression/src/lib.rs` and the capability-decline comment
   in `crates/lakehouse-engine/src/adapter/capabilities.rs` — so the code, spec, and issue agree.
   Documentation-comment change only; no behavior change.
3. Close issue #106 as investigated-and-declined, linking the re-verification in this plan's
   decision-log and the two updated scenarios.

## Parallelization

Tasks 1 and 2 are independent and MAY run concurrently. Task 3 runs after both land.

## Dead Code Removal

None. No code path is removed; behavior is unchanged.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Regexp scalar functions are deliberately not translated | Unit | `crates/vs-expression/src/lib.rs` | `regexp_scalar_functions_fall_through` |
| Regexp scalar functions are deliberately not translated (REGEXP_LIKE unaffected) | Unit | `crates/vs-expression/src/lib.rs` | `regexp_scalar_exclusion_leaves_regexp_like_untouched` |
| Regexp scalar function capabilities remain absent | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` |

Both scenarios assert absence of behavior (decline / non-advertisement) over pure in-memory
computation with no I/O; unit tests are the correct form and the tests already exist. The deltas
change spec prose and the inline issue citation only, so the existing assertions continue to pass
unchanged; no new test is required.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-expression-translator-scalar-fns | `cargo test -p vs-expression regexp` | `regexp_scalar_functions_fall_through` and `regexp_scalar_exclusion_leaves_regexp_like_untouched` pass |
| pushdown-planning-capability-extensions | `cargo test -p lakehouse-engine reports_audited_capability_set` | Test passes; `FN_REGEXP_*` absent from the advertised set |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
