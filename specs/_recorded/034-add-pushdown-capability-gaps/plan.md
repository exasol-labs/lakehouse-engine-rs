# Plan: add-pushdown-capability-gaps

## Summary

Close four pushdown-capability-gap issues (#104, #105, #106, #107) by advertising the three capabilities whose translation is backed and Exasol-parity-confirmable — `FN_CAST`, `FN_NEG`, `FN_WEEK` — and recording deliberate, evidence-based exclusions for every remaining function whose DataFusion 54 semantics diverge from Exasol. Advertisement stays gated on the project's backing-path bar: a capability ships only once a `crates/vs-expression` translator arm renders it and the DataFusion result matches Exasol.

## Design

### Context

Each issue found its gap by diffing the advertised capability set against Exasol's capability vocabulary. Every listed function is a valid Exasol VS capability that Exasol's compiler pushes once advertised; today none are advertised, so matching queries fall back to raw row scanning instead of the node-local DataFusion scan. The forcing question is per-function: does DataFusion 54 have a faithful translation, or would advertising cause a silent wrong result (Exasol trusts a pushed-down capability) or a node-local scan failure?

DataFusion is pinned at version 54 (Arrow 58) in the workspace manifest, not the 58 named in one line of `mission.md`; parity was verified against DataFusion 54 documentation and source.

- **Goals** — Advertise every gap function that has a verified, Exasol-matching DataFusion 54 translation. Record a reasoned, durable exclusion for every function that does not, so the gaps are named rather than silent and are not re-litigated.
- **Non-Goals** — Emulating divergent Exasol semantics (floor division, Oracle format models, Oracle calendar arithmetic) with multi-step DataFusion expressions. Revisiting the pre-existing `FN_PRED_REGEXP_LIKE` advertisement. Any Iceberg scan, pushdown-planning, or schema/type-handling change.

The Iceberg-spec compliance gate in `CLAUDE.md` does not apply to this plan. These are Exasol SQL-expression-pushdown capabilities — the VS layer translating Exasol scalar and date functions into DataFusion SQL fragments. They touch neither Iceberg file-format handling nor schema/type mapping, so no normative Iceberg-spec section governs them.

### Decision

Advertise `FN_CAST`, `FN_NEG`, and `FN_WEEK`. Exclude the rest. Each advertised capability is backed by a `crates/vs-expression` arm and stays coherent with the translator the same way the arithmetic-operator and GROUP-BY-tuple advertisements are gated on their backing paths. Each excluded capability leaves the current correctness-safe fallback intact: the translator returns an error (raising mode) or `None` (safe variants), so the adapter omits the clause and Exasol post-processes it.

#### Architecture

```
Exasol getCapabilities ──▶ capabilities.rs CAPABILITIES
                                │  advertises FN_CAST, FN_NEG, FN_WEEK (+ existing set)
                                ▼
Exasol pushdown request ──▶ adapter ──▶ crates/vs-expression translator
                                                │  renders CAST / NEG / WEEK arms
                                                │  declines DIV, TO_CHAR, TO_NUMBER,
                                                │  regexp-scalars, ADD_*, *_BETWEEN,
                                                │  ADD_MONTHS/YEARS, DAYOFWEEK, LAST_DAY,
                                                │  CONVERT_TZ  → Exasol post-processes
                                                ▼
                                         DataFusion 54 scan SQL
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Advertise-only-when-backed | `capabilities.rs` + `vs-expression` | An advertised capability Exasol cannot see the adapter decline is a silent wrong result; each advertised name maps to a translator arm |
| Per-node correctness-safe fallback | `vs-expression` decline → adapter omit | An untranslatable node falls back to Exasol post-processing rather than a divergent scan |
| Documented deliberate exclusion | spec deltas + decision-log | Names each gap the plan does not close and why, so it is not re-litigated |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Advertise `FN_CAST` | Keep unadvertised | Highest-impact gap: a CAST currently blocks pushdown of its whole containing expression. Arm exists; unsupported targets (INTERVAL, GEOMETRY, HASHTYPE, TIMESTAMP WITH LOCAL TIME ZONE) return an error and fall back |
| Advertise `FN_NEG` | Keep unadvertised | Unary minus is exact in DataFusion. Arm exists; composes with the arithmetic-aggregate decomposition path |
| Advertise `FN_WEEK` only from #107 | Advertise the whole date group | Exasol `WEEK` and DataFusion `date_part('week', …)` are both ISO-8601; parity is confirmable by test. The rest diverge |
| Exclude `FN_DIV` | Emulate floor division | Exasol `DIV` is floor division; DataFusion `/` truncates toward zero (diverges on negatives) and has no `div`. Div-by-zero and decimal-rounding parity are unverified |
| Exclude `FN_TO_CHAR`, `FN_TO_NUMBER` | Advertise no-format case | DataFusion `to_char` uses strftime masks (not Oracle masks) and rejects numeric formatting; DataFusion has no `to_number`. No-format string→number is already reachable via `FN_CAST` |
| Exclude regexp scalars | Advertise literal-pattern subset | DataFusion runs the Rust `regex` crate (no pattern backreferences, no lookaround), lacks `regexp_substr`, and its argument shapes differ. A pushed Exasol PCRE pattern the translator cannot pre-validate would fail the node-local scan |
| Exclude `ADD_*`, `*_BETWEEN`, `ADD_MONTHS/YEARS`, `MONTHS/YEARS_BETWEEN`, `DAYOFWEEK`, `LAST_DAY`, `CONVERT_TZ` | Advertise per function | Variable×INTERVAL scaling is unverified in DataFusion 54; date-diff needs divergent multi-step emulation; month/year arithmetic needs Oracle end-of-month clamping DataFusion lacks; `DAYOFWEEK` numbering differs; `CONVERT_TZ` is session-timezone dependent |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator-scalar-ops | CHANGED | `sql-comprehension/vs-expression-translator-scalar-ops/spec.md` |
| sql-comprehension/vs-expression-translator-scalar-fns | CHANGED | `sql-comprehension/vs-expression-translator-scalar-fns/spec.md` |
| sql-comprehension/vs-expression-translator-date-fns | CHANGED | `sql-comprehension/vs-expression-translator-date-fns/spec.md` |
| vs-adapter/pushdown-planning-capability-extensions | CHANGED | `vs-adapter/pushdown-planning-capability-extensions/spec.md` |

## Dependencies

- DataFusion 54 (Arrow 58) — the target execution engine whose function set bounds what is translatable.
- Exasol Docker container — required for the E2E capability-alignment tests (fail, never skip, when unavailable).

## Implementation Tasks

1. [expert] Audit `render_cast_target` (`crates/vs-expression/src/lib.rs`) against the Exasol CAST target-type set; confirm every faithful target (VARCHAR, CHAR, DECIMAL(p,s), DOUBLE, BOOLEAN, DATE, TIMESTAMP) renders and every unsupported target (INTERVAL, GEOMETRY, HASHTYPE, TIMESTAMP WITH LOCAL TIME ZONE) returns an error so the adapter falls back; verify number→VARCHAR formatting parity and narrow the target set (fall back) on any target whose DataFusion output diverges from Exasol. Add unit tests. (#104)
2. Add a unit test proving `NEG` composes with the arithmetic-aggregate decomposition path (e.g. `SUM(-col)` renders). (#105)
3. [expert] Confirm Exasol `DIV` (floor division) has no faithful DataFusion 54 translation; add a unit test asserting a `DIV` node falls through (error in raising mode, `None` in safe variants); document the emulation approach and the div-by-zero / negative-operand divergence in the scalar-ops spec. (#105)
4. [expert] Confirm the Rust-`regex`-crate dialect and argument-shape divergence for `REGEXP_REPLACE`, `REGEXP_SUBSTR`, `REGEXP_INSTR`, `REGEXP_COUNT`; add unit tests asserting each falls through; document the exclusion in the scalar-fns spec. (#106)
5. [expert] Add a `WEEK` arm rendering `date_part('week', <arg>)`; verify ISO-8601 parity with Exasol `WEEK` including year-boundary dates (unit test, plus the E2E test in task 9). If parity fails, do not advertise `FN_WEEK`, exclude it, and record a superseding decision. (#107)
6. Add unit tests asserting the excluded #107 functions (`ADD_DAYS`, `ADD_HOURS`, `ADD_MINUTES`, `ADD_SECONDS`, `ADD_WEEKS`, `ADD_MONTHS`, `ADD_YEARS`, `DAYS_BETWEEN`, `HOURS_BETWEEN`, `MINUTES_BETWEEN`, `SECONDS_BETWEEN`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, `LAST_DAY`, `CONVERT_TZ`) fall through; update the date-fns "Unsupported date functions fall through" scenario example set. (#107)
7. Add unit tests asserting `TO_CHAR` and `TO_NUMBER` fall through; document the exclusion in the scalar-ops spec. (#104)
8. Advertise `FN_CAST`, `FN_NEG`, and `FN_WEEK` in `crates/lakehouse-engine/src/adapter/capabilities.rs`; extend the inline capability tests to assert the three present and the excluded names (`FN_TO_CHAR`, `FN_TO_NUMBER`, `FN_DIV`, `FN_REGEXP_REPLACE`, `FN_REGEXP_SUBSTR`, `FN_REGEXP_INSTR`, `FN_REGEXP_COUNT`, `FN_ADD_*`, `FN_*_BETWEEN`, `FN_ADD_MONTHS`, `FN_ADD_YEARS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_LAST_DAY`, `FN_CONVERT_TZ`) absent; assert no new join or cross-join capability was introduced. (#104, #105, #106, #107)
9. Extend `crates/lakehouse-engine/tests/e2e_capability_test.rs` with capability-alignment tests exercising CAST, unary-minus, and WEEK in filter and select-list positions against the live Exasol stack. (#104, #105, #107)

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1, Task 2, Task 3, Task 4, Task 5, Task 6, Task 7 |
| Group B | Task 8 |
| Group C | Task 9 |

Sequential dependencies:
- Group A → Group B (advertisement in the single `capabilities.rs` file depends on every backing/exclusion outcome from Group A; serialised to avoid conflicting edits to one file).
- Group B → Group C (E2E alignment tests exercise the advertised capabilities).

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | This plan adds capability advertisements, translator coverage, and tests; it removes no existing behavior |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| CAST translates to DataFusion CAST syntax (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `cast_faithful_targets_render` |
| Unsupported CAST target falls back (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `cast_unsupported_target_falls_through` |
| Integer division DIV is deliberately not translated (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `div_falls_through` |
| Conversion format functions TO_CHAR and TO_NUMBER are deliberately not translated (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `to_char_to_number_fall_through` |
| Unary negation composes with aggregate decomposition (CHANGED arithmetic scenario) | Unit | `crates/vs-expression/src/lib.rs` | `neg_composes_with_aggregate` |
| Regexp scalar functions are deliberately not translated (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `regexp_scalars_fall_through` |
| WEEK translates to the DataFusion date_part('week') ISO-8601 call (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `week_translates_to_iso_date_part` |
| Unsupported date functions fall through as unsupported nodes (CHANGED) | Unit | `crates/vs-expression/src/lib.rs` | `unsupported_date_fn_falls_through` |
| Conversion and unary-negation capabilities are advertised (NEW) | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `advertises_cast_and_neg_capabilities` |
| ISO week capability is advertised (NEW) | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `advertises_iso_week_capability` |
| Regexp scalar function capabilities remain absent (NEW) | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `regexp_scalar_capabilities_absent` |
| Advertised CAST/NEG/WEEK execute end-to-end (NEW) | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_cast_neg_week_pushdown` |

Translator and capability scenarios are pure string computation with no I/O, so they are covered by unit tests per the mission testing standard. End-to-end execution parity (including WEEK ISO year-boundary parity against Exasol) is covered by the integration test.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-expression-translator-scalar-ops | `cargo test -p vs-expression cast` | CAST faithful-target and fall-through tests pass |
| vs-adapter/pushdown-planning-capability-extensions | `cargo test -p lakehouse-engine capabilities` | Advertised FN_CAST/FN_NEG/FN_WEEK present, excluded names absent |
| date-fns / capability alignment | `make cross-musl-udf-build && make test-e2e` | `SELECT WEEK(event_date), CAST(id AS VARCHAR), -score FROM <vs>.<table>` returns via the DataFusion scan path with results matching Exasol |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
