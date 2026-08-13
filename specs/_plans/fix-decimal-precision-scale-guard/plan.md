# Plan: fix-decimal-precision-scale-guard

## Summary

Give both catalog kinds ONE shared decimal guard in `types/mapping.rs` that admits a `DECIMAL(p,s)` only when `1 ≤ p ≤ 36` and `s ≤ p`, so a `p = 0` or `s > p` from either catalog falls back to `VARCHAR(2000000)` instead of declaring an Exasol type Exasol rejects. Fixes issue #329.

## Design

### Context

`iceberg_primitive_to_exasol` and `unity_type_name_to_exasol` (both in `crates/lakehouse-engine/src/types/mapping.rs`) each carry their own copy of the predicate `precision <= 36 && scale <= 36`. Two precision/scale pairs pass it and produce an invalid Exasol column type in the `createVirtualSchema` response: `p = 0` yields `DECIMAL(0,0)`, and `s > p` yields a shape such as `DECIMAL(5,10)`. Exasol's `DECIMAL(p,s)` domain is `1 ≤ p ≤ 36` and `0 ≤ s ≤ p`, so both declarations fail the enumeration outright — the exact failure the `VARCHAR` fallback exists to prevent.

The duplication is the deeper defect. One decision — which decimals Exasol can express — has two homes with nothing enforcing agreement, the back-door leakage shape `/speq:design-philosophy` names. Fixing one copy and not the other is a live possibility precisely because they are separate.

- **Goals** — one owner for the Exasol-decimal-domain decision; both catalog kinds provably identical; no invalid Exasol type reachable from catalog input; the fix gated on a live capture rather than on documented limits.
- **Non-Goals** — no fail-loud path, no `Result` threading, no change to the Arrow `Decimal128` path, no change to `exasol_type_from_json`, no new dependency, no wire-format or scan-path change.

### Decision

Add one private function to `types/mapping.rs` and collapse both call sites onto it.

```rust
/// Decide the Exasol type for a catalog-declared decimal.
///
/// Exasol's DECIMAL domain is 1 <= p <= 36 and 0 <= s <= p; anything outside it
/// is declared VARCHAR(2000000) and carried as a JSON string, the same fallback
/// an out-of-range precision already took. Both catalog kinds read the decision
/// here so a Unity `DECIMAL(0,0)` and an Iceberg `decimal(0,0)` cannot diverge.
/// `s >= 0` needs no test: both catalog-sourced fields are unsigned.
fn decimal_to_exasol(precision: u32, scale: u32) -> String {
    if (1..=36).contains(&precision) && scale <= precision {
        format!("DECIMAL({precision},{scale})")
    } else {
        "VARCHAR(2000000)".to_string()
    }
}
```

Both call sites collapse onto it:

- `iceberg_primitive_to_exasol` — the guarded arm and the separate `Decimal { .. } => "VARCHAR(2000000)"` arm merge into one unguarded arm, `Decimal { precision, scale } => decimal_to_exasol(*precision, *scale),`. The `// Out-of-range Decimal` comment above the deleted arm goes with it.
- `unity_type_name_to_exasol` — the guarded arm becomes `"DECIMAL" => decimal_to_exasol(precision, scale),`. The arm is now unconditional, so a bad-argument `DECIMAL` reaches the helper instead of falling through to the trailing `_ => "VARCHAR(2000000)"` wildcard. The returned string is the same either way; the difference is that the decision is now stated once rather than reached by two routes.

Both call sites drop the `scale <= 36` half, which becomes dead: `s ≤ p` and `p ≤ 36` imply it.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Single owner for one decision | `decimal_to_exasol` in `types/mapping.rs` | Removes the back-door leakage of the Exasol-decimal-domain predicate across two functions |
| Private to its module | `fn`, not `pub` / `pub(crate)` | Its only two consumers are in the same file; hiding the predicate entirely is what makes the module deep |
| Total function, no `Result` | returns `String` | Keeps `column_source_type_to_exasol` and `build_listing_virtual_tables` infallible |

`/speq:design-philosophy` Quick Diagnostic, answered for the one new abstraction: a one-sentence summary names its responsibility ("decide the Exasol type for a catalog-declared decimal"); calling it is easier than restating a two-clause predicate plus two literals; changing the predicate forces no edit outside `types/mapping.rs`; the doc comment states why the guard is what it is rather than restating the name; it is the sole owner of that decision; the module boundary is unchanged; no tactical shortcut is taken; and it depends on nothing — it is pure computation over two integers.

It is not a pass-through: it does not forward the same arguments to another function, it holds the predicate and both output strings.

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Silent `VARCHAR(2000000)` fallback | Fail loud: thread `Result` through `column_source_type_to_exasol` → `build_listing_virtual_tables`, add error variants and tests | Confirmed in the clarifying interview. The fallback already absorbs `p > 36`; adding a second, louder treatment for two neighbouring pairs splits one policy in two. Fail-loud costs new error variants, new tests, and a fallible signature on a path whose every other type maps totally |
| One shared private helper | Fix each guard in place | Two correct copies still agree by coincidence; the next reader can fix one and not the other. The duplication is the defect |
| Guard reads `(1..=36).contains(&precision) && scale <= precision` | `precision >= 1 && precision <= 36 && scale <= precision && scale <= 36` | `s ≤ p ≤ 36` makes `s ≤ 36` dead; a redundant fourth condition invites the halves to drift |
| Leave `arrow_to_exasol_type` / `compatible_exasol_type` alone | Extend the helper to cover them | `CompatibleExaType::Decimal(u8, i8)` — the Arrow scale is SIGNED and legitimately negative, with no `s ≤ p` analogue, and the input is DataFusion's own schema rather than catalog wire input. A `(u32, u32)` helper cannot express that domain; folding them in needs casts and buys nothing |
| Leave `exasol_type_from_json` alone | Extend the helper to cover it | Its `u64` precision and scale come from Exasol's own `dataType` JSON for a type Exasol already accepted. Once this fix lands no invalid decimal can be declared for Exasol to echo back |
| Live-capture the Exasol rejection before coding | Rely on documented Exasol limits | CLAUDE.md § Verification discipline forbids asserting a SQL capability or limitation from documentation alone. If Exasol accepted either shape the plan's premise would be wrong, so the capture gates the fix rather than following it |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/type-mapping | CHANGED | `datafusion-scan/type-mapping/spec.md` |
| vs-adapter/unity-catalog-create-virtual-schema | CHANGED | `vs-adapter/unity-catalog-create-virtual-schema/spec.md` |
| datafusion-scan/type-mapping-module-structure | CHANGED | `datafusion-scan/type-mapping-module-structure/spec.md` |

`vs-adapter/create-virtual-schema` gets NO delta and the omission is deliberate: its recorded Background delegates the mapping wholesale ("Schema mapping MUST use the same mapping as the scan, defined in the `datafusion-scan/type-mapping` feature") and names the fallback class generically as "out-of-range decimal", which the widened definition satisfies without an edit.

## Impact

No behavior changes for any table any catalog in this repo's fixtures serves. Every decimal in every fixture satisfies the new guard — a repo-wide sweep of `DECIMAL(p,s)` literals found one violating pair, `DECIMAL(10,200)`, and it appears only in `exasol_type_to_json`'s divergence tests and their spec, on a path this plan does not touch.

What changes is the failure mode for a catalog that declares a decimal Exasol cannot express. Previously `createVirtualSchema` failed with an Exasol type error naming the invalid `DECIMAL`; now the column is declared `VARCHAR(2000000)` and the enumeration succeeds. Operators see a column typed `VARCHAR` where they might expect a numeric — the same trade-off a `DECIMAL(38,10)` column already carries.

No breaking change. No wire-format, scan-spec, capability, or generated-SQL change.

## Dependencies

None added; no dependency version changes.

## Apache Iceberg spec compliance

Required by CLAUDE.md § Iceberg specification compliance because this change touches schema/type handling. Quoted from the Apache Iceberg table spec, not from memory:

- Primitive Types table: `decimal(P,S)` — "Fixed-point decimal; precision P, scale S" · "Scale is fixed, precision must be 38 or less".
- Schema evolution, type promotion: `decimal(P, S)` → `decimal(P', S)` if `P' > P` · "Widen precision only".
- Appendix A (Avro): `decimal(P,S)` → `{ "type": "fixed", "size": minBytesRequired(P), "logicalType": "decimal", "precision": P, "scale": S }` · "Stored as fixed using the minimum number of bytes for the given precision."
- Appendix A (Parquet): `decimal(P,S)` → `P <= 9`: `int32`, `P <= 18`: `int64`, `fixed` otherwise, annotated `DECIMAL(P,S)`.
- Appendix C (JSON type serialization): `decimal(P, S)` → `JSON string: "decimal(<P>,<S>)"`.

**Finding, which corrects issue #329's stated premise.** The normative row constrains only `P ≤ 38`. It states no lower bound on `P` and no relation between `S` and `P`. Issue #329 asserts "the Iceberg spec constrains p and s the same way"; the quoted text does not. A catalog serving `decimal(0,0)` or `decimal(5,10)` therefore violates nothing in the spec's own wording, so "only a misbehaving catalog produces it" cannot justify the guard. The Exasol target-type limitation is the whole justification, which makes the fix more necessary rather than less.

**Deviation classification.** Mapping a spec-legal `decimal(P,S)` to `VARCHAR(2000000)` when `P = 0` or `S > P` is an Exasol target-type limitation — Exasol has no such `DECIMAL`, exactly as it has none with `P > 36`, whose identical fallback is already recorded. Per CLAUDE.md that is not a gap, but it is named as a deliberate trade-off in the `datafusion-scan/type-mapping` delta rather than left unstated. No tracked-exception issue is opened: the column stays queryable as a JSON `VARCHAR` string, so nothing is dropped or left untyped.

**Reachability, verified in the vendored source rather than assumed.** `iceberg 0.10.0`'s `Type::decimal` and `Type::decimal_required_bytes` (`~/.cargo/registry/.../iceberg-0.10.0/src/spec/datatypes.rs`) both assert `precision > 0 && precision <= MAX_DECIMAL_PRECISION`, but `deserialize_decimal` in the same file bypasses both — it splits the `decimal(P,S)` string and builds `PrimitiveType::Decimal { precision, scale }` straight from two `u32` parses with no bound check. So `"decimal(0, 0)"` in table metadata deserializes cleanly. Because both fields are `u32`, a negative scale is unrepresentable and needs no guard. On the Unity side `neutral_column` (`crates/lakehouse-catalog/src/unity/client.rs`) resolves an absent `type_precision` through `.unwrap_or(0)`, so `p = 0` needs only an omitted wire field, not a misbehaving catalog.

## Implementation Tasks

1. **Verification gate — capture Exasol's real DECIMAL domain before any code changes.**
   1.1 Bring up the Docker Exasol container (`docker compose up -d --wait exasol`; the `make` targets never start it) and run four probes through `exapump sql … -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"`: `SELECT CAST(1 AS DECIMAL(0,0))` and `SELECT CAST(1 AS DECIMAL(5,10))` MUST both be rejected, and the two controls `SELECT CAST(1 AS DECIMAL(1,0))` and `SELECT CAST(1 AS DECIMAL(36,36))` MUST both succeed — the controls are what distinguish a real rejection from a broken probe. Record each verbatim error text and each control's result in `decision-log.md` under a new `## Live Captures` heading. If EITHER bad shape is accepted, STOP and report: the plan's premise is wrong and tasks 2 onward must not run.

2. **One shared decimal guard.**
   2.1 Add the failing unit tests to `crates/lakehouse-engine/src/types/mapping_tests.rs`, per § Test Disposition. Append cases only — no existing assertion may be edited or removed. Confirm they fail against the unmodified guards before task 2.2.
   2.2 Add the private `fn decimal_to_exasol(precision: u32, scale: u32) -> String` to `crates/lakehouse-engine/src/types/mapping.rs` with the doc comment from § Design, then repoint both call sites: replace `iceberg_primitive_to_exasol`'s guarded `Decimal` arm AND its separate `Decimal { .. } => "VARCHAR(2000000)"` arm (deleting the `// Out-of-range Decimal` comment with them) by the single arm `Decimal { precision, scale } => decimal_to_exasol(*precision, *scale),`; replace `unity_type_name_to_exasol`'s `"DECIMAL" if precision <= 36 && scale <= 36 =>` arm by the unguarded `"DECIMAL" => decimal_to_exasol(precision, scale),`. Update `unity_type_name_to_exasol`'s doc comment so its "an out-of-range `DECIMAL(p,s)`" sentence names the shared guard and its actual domain rather than implying the `≤ 36` pair alone. Leave `arrow_to_exasol_type`, `compatible_exasol_type`, and `exasol_type_from_json` untouched.

3. **Documentation alignment.**
   3.1 In `CLAUDE.md` § Data types, add one sentence beneath the Arrow-to-Exasol table recording that the table's `Decimal128(p,s) where p≤36 and s≤36` row governs the Arrow direction, and that a CATALOG-declared decimal (Iceberg or Unity) additionally requires `1 ≤ p` and `s ≤ p` and otherwise maps to `VARCHAR(2000000)`. Do not edit the table rows themselves — they remain accurate for the direction they describe.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 |
| Group B | 2.1 → 2.2 · 3.1 |

Sequential dependencies:
- Group A → Group B. Task 1.1 gates everything: it is what makes the guard's premise a capture rather than a claim.
- Within Group B: 2.1 → 2.2, strictly sequential (failing-test-first). Task 3.1 touches only `CLAUDE.md` and is independent of both.

No task is tagged `[expert]`. The change is one pure two-integer predicate plus two call-site substitutions in a single file, with no concurrency, no ordering hazard, no cross-file refactor, and no novel algorithm.

## Test Disposition

| Test | File | Disposition |
|---|---|---|
| `catalog_decimal_guard_is_shared_by_both_source_kinds` | `crates/lakehouse-engine/src/types/mapping_tests.rs` | NEW. Drives `column_source_type_to_exasol` over the pair matrix `(0,0)`, `(0,5)`, `(5,10)`, `(1,0)`, `(18,4)`, `(36,36)`, `(38,10)`, `(18,37)` for BOTH `ColumnSourceType::Iceberg(Type::Primitive(PrimitiveType::Decimal{..}))` and `ColumnSourceType::Unity { type_name: "DECIMAL", .. }`, asserting each pair's expected string AND that the two kinds return the identical string for every pair. The cross-kind equality is the assertion no per-path test carries: it fails if a later change fixes one guard and not the other |
| `iceberg_types_map_to_exasol_type` | `crates/lakehouse-engine/src/types/mapping_tests.rs` | AMENDED, additive only. Append `Decimal { precision: 0, scale: 0 }` → `VARCHAR(2000000)` and `Decimal { precision: 5, scale: 10 }` → `VARCHAR(2000000)`. Its existing `(18,4)` → `DECIMAL(18,4)` and `(38,10)` → `VARCHAR(2000000)` assertions stay byte-identical |
| `incompatible_unity_types_declared_varchar` | `crates/lakehouse-engine/src/types/mapping_tests.rs` | AMENDED, additive only. Append Unity `DECIMAL` cases `precision: 0, scale: 0` → `VARCHAR(2000000)` and `precision: 5, scale: 10` → `VARCHAR(2000000)`. Its existing `(38,10)` and `(18,37)` assertions stay byte-identical, as does the non-DECIMAL loop (whose `precision: 0` is ignored by every type name it passes) |
| `unity_spark_types_map_to_exasol` | `crates/lakehouse-engine/src/types/mapping_tests.rs` | UNCHANGED. Its two DECIMAL cases `(10,2)` and `(36,36)` both satisfy the new guard, so no assertion moves. Verified, not assumed — `(36,36)` is the boundary the `s ≤ p` half could plausibly have broken |
| `column_source_type_maps_to_exasol_in_one_home` | `crates/lakehouse-engine/src/types/mapping_tests.rs` | UNCHANGED. Uses `LONG`, not `DECIMAL` |
| `decimal128_in_range_maps_to_decimal`, `decimal128_out_of_range_maps_to_varchar_json` | `crates/lakehouse-engine/src/types/mapping_tests.rs` | UNCHANGED. Arrow path, out of scope. `Decimal128(36, 36)` still maps to `DECIMAL(36,36)` |
| `exasol_type_to_json_out_of_range_decimal_args_become_varchar` and every other `exasol_type_to_json` / `exasol_type_from_json` / `parse_decimal_args` test | `crates/lakehouse-engine/src/types/mapping_tests.rs` | UNCHANGED. Those functions are untouched; their `DECIMAL(10,200)` input reaches them as a literal string, never through either catalog producer |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Match arm | `iceberg_primitive_to_exasol`'s `Decimal { .. } => "VARCHAR(2000000)".to_string()` (`types/mapping.rs`) | The shared helper returns the fallback itself; the separate catch-all arm becomes unreachable |
| Guard clause | the `*scale <= 36` half of `iceberg_primitive_to_exasol`'s arm guard, and the `scale <= 36` half of `unity_type_name_to_exasol`'s (`types/mapping.rs`) | Dead: `s ≤ p` and `p ≤ 36` imply it |
| Comment | `// Out-of-range Decimal` above the deleted Iceberg arm (`types/mapping.rs`) | Annotates a deleted arm |
| Spec prose | "or `type_text`" in `specs/vs-adapter/unity-catalog-create-virtual-schema/spec.md`, both occurrences (Background paragraph and the Spark-column-types scenario) | Names a recovery path the code does not implement: `ColumnInfo` declares no `type_text` field |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| datafusion-scan/type-mapping — A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `catalog_decimal_guard_is_shared_by_both_source_kinds` |
| vs-adapter/unity-catalog-create-virtual-schema — Unity Catalog Spark column types map to Exasol types sufficient for listing | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `unity_spark_types_map_to_exasol` |
| vs-adapter/unity-catalog-create-virtual-schema — An incompatible Unity Catalog column type is declared as VARCHAR rather than failing | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `incompatible_unity_types_declared_varchar` |
| datafusion-scan/type-mapping-module-structure — One DECIMAL parser serves every Exasol type-string consumer | Unit | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `exasol_type_to_json_out_of_range_decimal_args_become_varchar` and the sibling `exasol_type_to_json_*` tests, all unchanged — the amended clause is a reachability argument over producers, and the narrowed producer guard is pinned by `catalog_decimal_guard_is_shared_by_both_source_kinds` |

Unit rather than integration for every scenario, which the speq default permits only for pure computation with no I/O: `decimal_to_exasol` and both call sites are total functions over integers and a string, reading no state. No integration or E2E test is writable for these scenarios at all — reaching them end to end requires a catalog that serves `decimal(0,0)` or `decimal(5,10)`, and neither Lakekeeper, the Iceberg REST fixture, nor Unity Catalog OSS will emit one. That absence is stated here rather than left as an apparent coverage gap.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/type-mapping | `docker compose up -d --wait exasol` then `exapump sql "SELECT CAST(1 AS DECIMAL(0,0))" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | Exasol rejects the statement with a type error; the verbatim text is recorded in `decision-log.md` § Live Captures |
| datafusion-scan/type-mapping | `exapump sql "SELECT CAST(1 AS DECIMAL(5,10))" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | Exasol rejects the statement with a type error |
| datafusion-scan/type-mapping | `exapump sql "SELECT CAST(1 AS DECIMAL(1,0)), CAST(1 AS DECIMAL(36,36))" -d "exasol://sys:exasol@localhost:28563?validateservercertificate=0"` | Both controls succeed, proving the two rejections above are the type domain and not a broken probe |
| vs-adapter/unity-catalog-create-virtual-schema | `cargo test -p lakehouse-engine types::mapping` | All `types::mapping` tests pass, including the three new or amended ones |
| datafusion-scan/type-mapping-module-structure | `cargo test -p lakehouse-engine exasol_type_to_json` | Unchanged: every `exasol_type_to_json` test passes with no edit |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
| Spec validation | `speq plan validate fix-decimal-precision-scale-guard` | pass |
