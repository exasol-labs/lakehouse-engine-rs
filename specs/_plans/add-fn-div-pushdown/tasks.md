# Tasks: add-fn-div-pushdown

## Phase 2: Implementation (Group A)
- [x] 2.1 Correct the stale doc comment in `crates/vs-expression/src/lib.rs` (the `div_falls_through_as_unsupported` test, lines ~2129–2131) — replace the false "Exasol DIV is floor division ... diverges on negative operands" claim with the verified reason: Exasol `DIV` truncates toward zero and matches DataFusion integer `/`, but DataFusion has no `div` builtin and a `TRUNC(m/n)` emulation diverges on DOUBLE division by zero.
- [x] 2.2 Reword the "capability advertisement is per function, not per operand type" rationale in `plan.md` (§ Decision), `decision-log.md` ([1]), and the spec delta (`sql-comprehension/vs-expression-translator-scalar-ops/spec.md` scenario AND-clause) — this framing is contradicted by the existing `FN_CAST` precedent (`render_cast_target` in `crates/vs-expression/src/lib.rs`, which IS advertised while declining a subset of target types at translation time). Replace with the accurate blocker: DIV operand types are not carried in the expression node (unlike CAST's explicit `dataType` field), so the translator cannot identify and render only the safe integer-operand case; combined with the DOUBLE div-by-zero divergence, no verified-safe partial rendering exists. (plan-reviewer round-1 ADVISORY finding, PR #166)
- [x] 2.3 Add an explicit `**ID:** exclude-fn-div-no-faithful-datafusion-truncated-division` field to `decision-log.md` entry [1] so the recorder produces the ADR ID the plan's Follow-up section already promises. (plan-reviewer round-1 ADVISORY finding, PR #166)

## Phase 3: Verification
- [x] 3.1 Run `cargo test -p vs-expression div_falls_through_as_unsupported` — confirm unchanged pass (behavior identical, only comment moved)
- [x] 3.2 Run `cargo test -p lakehouse-engine capabilities` — confirm `FN_DIV` stays in the "declined translations must stay unadvertised" list
- [x] 3.3 Run full checklist: `cargo test` (0 failures), `cargo clippy --all-targets` (0 errors/warnings), `cargo fmt --check` (no changes)
