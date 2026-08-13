# Verification Report: fix-decimal-precision-scale-guard

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Both catalog kinds now read one shared Exasol-decimal-domain predicate; the live Exasol capture confirms `DECIMAL(0,0)` and `DECIMAL(5,10)` are real rejections, not documentation claims. All checklist items pass. |
| Code review | 4 findings — standard: 2, expert: 2 — all fixed |

| Check | Status |
|-------|--------|
| Build (`make cross-musl-udf-build`) | ✓ |
| Tests (`cargo test`) | ✓ |
| Lint (`cargo clippy --workspace --all-targets -- -D warnings`) | ✓ |
| Format (`cargo fmt --check`) | ✓ |
| Spec validation (`speq plan validate fix-decimal-precision-scale-guard`) | ✓ (2 non-blocking step-count warnings) |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Notes — scope extension beyond the plan, needs explicit human sign-off

The expert review-fix pass (finding 4.1, information-leakage) found that the Exasol-decimal-domain
predicate had a **third** copy the plan did not name, in `iceberg_primitive_to_arrow` (the Iceberg→Arrow
logical-tag mapping used for `initial-default` encoding), not only the two Exasol-string producers
`plan.md` scoped. Fixing that third copy surfaced a **fourth** copy in
`crates/lakehouse-engine/src/adapter/pushdown/file_resolution.rs`'s `encode_initial_default`, guarded
by the same stale `precision <= 36 && scale <= 36` condition kept in sync only by convention.

Left unfixed, that fourth copy would have caused a **silent wrong-value defect**: an Iceberg
`decimal(5,10)` field with an `initial-default` would still take the numeric-tag branch there while
`iceberg_primitive_to_arrow`'s tag became `"utf8"` after the guard-sharing fix, so
`reconstruct_initial_default` would hand back the raw `i128` mantissa's digits as the column's string
default — a wrong value, not a clean fallback. The expert agent widened
`exasol_representable_catalog_decimal` from private to `pub(crate)` and routed
`encode_initial_default` through it too, pinned by a new failing-first test,
`build_logical_schema_omits_default_for_decimal_outside_exasol_domain`
(`crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs`), which stays red against the
`mapping.rs`-only fix and green after the `file_resolution.rs` fix.

This is a correct, test-pinned fix for a real defect, but it touches a file
(`adapter/pushdown/file_resolution.rs`) and widens a visibility (`private` → `pub(crate)`) that
`plan.md` did not scope or approve. Per this command's instruction, `/speq:record` was **not** run —
this report and the PR are for human review before any spec is merged. The reviewer should decide
whether this fourth-site fix belongs in this plan's delta, a follow-up issue, or a plan amendment
before recording.

One related spec-consistency edit made by the orchestrator, not a sub-agent: the plan's own
`datafusion-scan/type-mapping-module-structure` spec delta (untracked, not yet recorded) still named
the pre-rename `decimal_to_exasol` symbol after the expert fix renamed it to
`catalog_decimal_to_exasol` / `exasol_representable_catalog_decimal`. Updated both references so the
delta describes the code that actually exists; re-ran `speq plan validate` afterward (still passes,
same two pre-existing step-count warnings).

## Test Evidence

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Full workspace (`cargo test`) | all suites | 805 (lib) + all other suites | 0 |
| `types::mapping` (targeted) | `cargo test -p lakehouse-engine --lib types::mapping` | 34 | 0 |
| `exasol_type_to_json` (targeted) | `cargo test -p lakehouse-engine --lib exasol_type_to_json` | 6 | 0 |
| Fourth-site regression (targeted) | `cargo test -p lakehouse-engine --lib build_logical_schema_omits_default_for_decimal_outside_exasol_domain` | 1 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT CAST(1 AS DECIMAL(0,0))` against live Docker Exasol | ✓ Rejected, SQL state 42000 "illegal precision value: 0" |
| `SELECT CAST(1 AS DECIMAL(5,10))` against live Docker Exasol | ✓ Rejected, SQL state 42000 "illegal scale value: 10" |
| `SELECT CAST(1 AS DECIMAL(1,0))` (control) | ✓ Succeeded |
| `SELECT CAST(1 AS DECIMAL(36,36))` (control) — re-verified with `CAST(0.5 AS DECIMAL(36,36))` after the literal `1` control hit an unrelated value-range exception | ✓ Succeeded |
| `cargo test -p lakehouse-engine types::mapping` | ✓ All pass |
| `cargo test -p lakehouse-engine exasol_type_to_json` | ✓ All pass, unchanged |

Full verbatim probe output is in `specs/_plans/fix-decimal-precision-scale-guard/decision-log.md` § Live Captures.

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets -- -D warnings
exit 0, no warnings
```

### Formatter

```
cargo fmt --check
exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | type-mapping | A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `catalog_decimal_guard_is_shared_by_both_source_kinds` | Pass |
| vs-adapter | unity-catalog-create-virtual-schema | Unity Catalog Spark column types map to Exasol types sufficient for listing | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `unity_spark_types_map_to_exasol` | Pass |
| vs-adapter | unity-catalog-create-virtual-schema | An incompatible Unity Catalog column type is declared as VARCHAR rather than failing | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `incompatible_unity_types_declared_varchar` | Pass |
| datafusion-scan | type-mapping-module-structure | One DECIMAL parser serves every Exasol type-string consumer | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `exasol_type_to_json_out_of_range_decimal_args_become_varchar` and sibling `exasol_type_to_json_*` tests (unchanged) | Pass |
| — (not in original plan; added by expert review fix) | — | Fourth duplicate guard in `encode_initial_default` no longer diverges from the shared predicate | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `build_logical_schema_omits_default_for_decimal_outside_exasol_domain` | Pass |

## Review Findings Summary

- **Standard (2, both fixed):** pinned two off-by-one boundary cases (`(37,0)`, `(5,6)`) into
  `catalog_decimal_guard_is_shared_by_both_source_kinds`; corrected `CLAUDE.md`'s DECIMAL domain
  description from `p≤36, s≤36` to `1≤p≤36, 0≤s≤p` and reattributed it as Exasol's own domain
  rather than a catalog-path-specific extra rule.
- **Expert (2, both fixed):** extracted the shared predicate into `exasol_representable_catalog_decimal`
  and routed a third producer (`iceberg_primitive_to_arrow`) through it, which led to discovering and
  fixing the fourth-site defect described above; named the magic `36` as `EXASOL_DECIMAL_MAX_PRECISION`.

Full findings: `specs/_plans/fix-decimal-precision-scale-guard/review-findings.md`.
