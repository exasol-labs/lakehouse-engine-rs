# Plan: change-name-mapping-fallback

## Summary

Honor the Iceberg `schema.name-mapping.default` table property when the field-id column
projection resolves a data file whose physical fields carry no embedded `PARQUET:field_id`,
inserting it as a new resolution step between the embedded-field-id match and the existing
physical-name fallback (Iceberg column-projection rule #2). Implements GitHub issue #28.

## Design

### Context

Field-id-based projection (#26) binds each logical column to a physical Parquet column by
`PARQUET:field_id`, falling back to a physical-name match when a file field carries no
embedded field-id. The Iceberg spec defines a stronger fallback for that exact case: the
table property `schema.name-mapping.default` maps physical column names → field-ids for
data files written without embedded field-ids. Today that property is ignored, so a rename
that the name-mapping would resolve is missed and columns can bind incorrectly under schema
evolution.

- **Goals** — For a physical field with no embedded field-id, resolve it via the table's
  `schema.name-mapping.default` (name → field-id → logical field) before falling back to a
  physical-name match; resolve/parse the property once per query in the VS and thread it into
  the scan spec; preserve all current behavior when no name-mapping is present or does not
  cover a field.
- **Non-Goals** — Nested `fields` name-mapping entries for struct/map/list children (deferred
  to #83); Iceberg column-projection rule #1 (partition Identity-Transform substitution) and
  rule #3 (`initial-default` values), neither implemented anywhere in this engine; the
  drop+rename-into-a-reused-name collision case (see Decision [7]); join-side changes beyond
  mirroring the existing per-side logical-schema threading.

### Decision

Resolve `schema.name-mapping.default` once in the VS planning layer, parse it with the
`iceberg` crate's own `NameMapping`/`MappedField` deserializer, flatten the top-level entries
to a compact `Vec<NameMappingEntry { name, field_id }>`, and thread it — shard-invariant —
through `CommonScanSpec` into each `ScanSpec` (and the join dimension side), exactly mirroring
how `logical_schema` is already threaded. In the scan UDF, `rename_physical_to_logical` gains a
name-mapping lookup used only for physical fields that lack an embedded field-id.

#### Architecture

```
VS planning (pushdown.rs)                     Scan UDF (scan/mod.rs)
┌───────────────────────────┐                 ┌─────────────────────────────────┐
│ resolve_file_list         │  ScanSpec        │ register_files                  │
│  table.metadata()         │  .name_mapping   │  → PositionalDeleteScanTable    │
│   .properties()           │ ───────────────▶ │   → FieldIdExprAdapterFactory   │
│  ["schema.name-mapping..."]│ (via Common-    │    → rename_physical_to_logical │
│  → iceberg::NameMapping   │  ScanSpec)       │       (name-mapping step)       │
│  → Vec<NameMappingEntry>  │                 │                                 │
└───────────────────────────┘                 └─────────────────────────────────┘
```

Resolution order for a physical field in `rename_physical_to_logical`:
1. Embedded `PARQUET:field_id` present AND in the logical schema → adopt that logical name (unchanged).
2. NEW: no embedded field-id, but name-mapping maps the physical name → a field-id present in the logical schema → adopt that logical name.
3. Else → keep the physical name (existing physical-name fallback), DefaultPhysicalExprAdapter then resolves by name / NULL-fills / errors as today.

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Resolve-once in VS, thread into shard-invariant `CommonScanSpec` | `resolve_file_list` → `CommonScanSpec.name_mapping` | Repo rule: metadata resolved once per query, never per UDF invocation |
| Reuse `iceberg::spec::NameMapping` deserializer | `pushdown.rs` parse step | Prefer the pinned crate's spec-accurate parser over hand-rolled JSON |
| Augment, do not replace, the shipped fallback | `rename_physical_to_logical` step 2 vs 3 | Preserve current behavior for the no-mapping / uncovered-field cases |
| Fail loud at plan time on malformed metadata | VS parse of a present-but-invalid property | Mirrors `ensure_supported_delete_mechanisms` correctness-gate discipline |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Parse with `iceberg::spec::NameMapping` | Hand-rolled serde struct | Crate already ships a spec-accurate, tested deserializer (kebab-case `field-id`, nested `fields`, `DefaultOnNull`); reuse over reinvention |
| Store flat `Vec<NameMappingEntry{name,field_id}>` in the spec | Raw JSON string re-parsed in UDF; the nested `NameMapping` type | Parse once in VS per repo rule; flat name→id is the exact lookup shape the resolver needs; nested entries are unused (Decision [4]) |
| Name-mapping only for fields lacking an embedded field-id | Consult name-mapping for every field | Iceberg rule #2 scopes name-mapping to files "without field id information"; an embedded id is authoritative |
| Malformed present property → clean plan-time error | Silently ignore and fall through | Repo fails loud on metadata correctness issues; a malformed mapping is a real config error, surfaced once in the VS |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-field-id-projection | CHANGED | `datafusion-scan/scan-execution-field-id-projection/spec.md` |

## Dependencies

- `iceberg` crate (git tag `v0.10.0-rc.2`, rev `be6cc96`) — CONFIRMED to export
  `iceberg::spec::{NameMapping, MappedField}` (serde `Deserialize`, kebab-case, nested
  `fields`) and `iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING` (= `"schema.name-mapping.default"`).
  No new dependency required.

## Implementation Tasks

1. **Wire type + spec threading**
   - [ ] 1.1 Add `NameMappingEntry { name: String, field_id: i32 }` (serde) to `scan/spec.rs`; add `name_mapping: Vec<NameMappingEntry>` (`#[serde(default, skip_serializing_if = "Vec::is_empty")]`) to `ScanSpec`, `CommonScanSpec`, and `JoinSpec`; copy it in `ScanSpec::to_common`, `ScanSpec::from_parts`, and the join-side builders that already clone `logical_schema`.
   - [ ] 1.2 Unit test in `scan/spec.rs`: `name_mapping` round-trips through `ScanSpec`/`CommonScanSpec` JSON when populated, is absent from JSON when empty, and a legacy payload lacking the field deserializes to empty (backward-compat), mirroring `logical_schema_round_trips_and_defaults_to_empty`.
2. **VS resolve-once parse + threading**
   - [ ] 2.1 In `resolve_file_list` (`adapter/pushdown.rs`), read `table.metadata().properties().get(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)`; when present, parse via `serde_json::from_str::<iceberg::spec::NameMapping>` and flatten top-level entries to `Vec<NameMappingEntry>` (one per name; skip entries with no `field_id`; do NOT recurse into nested `fields`); on parse failure return a clean, credential-free plan-time `UdfError`. Extend the return tuple and thread the value into the built `ScanSpec` and into `resolve_one_join_side` for the dimension side. `[expert]`
   - [ ] 2.2 Unit test in `adapter/pushdown.rs`: a representative property JSON (multi-name entries + a nested `fields` entry + an entry with no `field-id`) flattens to the expected top-level `{name, field_id}` set with nested/id-less entries excluded; an absent property yields an empty mapping; malformed JSON yields a clean error.
3. **Scan-side resolution rewire**
   - [ ] 3.1 Thread `name_mapping` from `ScanSpec` through `register_files` → `PositionalDeleteScanTable::new` → `FieldIdExprAdapterFactory` (add a field holding the parsed mapping / a `HashMap<&str,i32>` built once per file open), mirroring how `use_field_id_adapter`/`logical_schema` already flow; do the same for the join dimension side's `register_files` call.
   - [ ] 3.2 Rewire `rename_physical_to_logical` to accept the name-mapping and insert the name-mapping resolution step: for a physical field with NO embedded field-id, look its physical name up in the name-mapping to obtain a field-id, and if that field-id is in `logical_name_by_id` adopt that logical name; otherwise keep the existing physical-name fallback. Embedded field-id resolution stays strictly first. `[expert]`
   - [ ] 3.3 Unit tests in the `field_id_adapter` mod of `scan/mod.rs`: (a) a no-field-id physical field resolves via name-mapping to its logical name; (b) an embedded field-id wins over a conflicting name-mapping entry; (c) name-mapping absent → physical-name identity preserved; (d) name-mapping present but not covering a field → physical-name identity preserved.
4. **Docker-free integration test of the read path**
   - [ ] 4.1 Add `tests/scan_name_mapping.rs` (reusing the `scan_no_head_test.rs` harness: local `file://` Parquet via `ArrowWriter`, `run_raw_scan_with_session`, Arrow-batch decode). Write a Parquet file whose column carries NO `PARQUET:field_id` and whose physical name differs from the current logical name; build a `ScanSpec` with `logical_schema` + `name_mapping` mapping the physical name to the field-id; assert the renamed column emits real values (never NULL) under the logical name. Add a companion case with an empty `name_mapping` asserting the physical-name fallback still binds.
5. **Correct the misleading comment (issue-scope clarification, Q1)**
   - [ ] 5.1 In `rename_physical_to_logical`'s doc comment, replace the claim that drop+rename-into-a-reused-name collisions "belong to the name-mapping work tracked in issue #28" with an accurate note: `schema.name-mapping.default` maps current-state physical names to field-ids and cannot disambiguate a dropped column whose old physical name was later reused, so this collision is a distinct, still-open concern unrelated to (and not resolved by) name-mapping support.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 |
| Group B (after A) | 2.1, 3.1, 5.1 |
| Group C (after B) | 3.2 |
| Group D (tests, after their code) | 1.2, 2.2, 3.3, 4.1 |

Sequential dependencies:
- 1.1 → Group B (2.1, 3.1 depend on the new spec field; 5.1 is independent doc work, grouped for scheduling)
- 3.1 → 3.2 (the factory must carry the mapping before the resolver can consume it)
- Each test task depends on its corresponding implementation task.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none) | — | The physical-name fallback is retained (name-mapping augments it); no code is obsoleted. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Column projection binds by Iceberg field-id across physical layouts | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_renamed_column_resolves_by_field_id` (existing, unchanged) |
| Field-id resolution honors schema.name-mapping.default for a file field without an embedded field-id | Integration | `crates/lakehouse-engine/tests/scan_name_mapping.rs` | `name_mapping_resolves_no_field_id_column` |
| Field-id resolution honors schema.name-mapping.default (embedded id precedence) | Unit | `crates/lakehouse-engine/src/scan/mod.rs` (`field_id_adapter` mod) | `embedded_field_id_wins_over_name_mapping` |
| Field-id resolution falls back to physical name when no name-mapping resolves a file field | Unit | `crates/lakehouse-engine/src/scan/mod.rs` (`field_id_adapter` mod) | `no_name_mapping_falls_back_to_physical_name`, `uncovered_name_mapping_falls_back_to_physical_name` |
| Field-id resolution falls back to physical name (read path) | Integration | `crates/lakehouse-engine/tests/scan_name_mapping.rs` | `empty_name_mapping_preserves_physical_name_binding` |
| The VS resolves schema.name-mapping.default once per query into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `resolves_name_mapping_flat_entries_once`, `absent_name_mapping_is_empty`, `malformed_name_mapping_errors_cleanly` |
| The VS threads name-mapping through the scan spec (round-trip) | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `name_mapping_round_trips_and_defaults_to_empty` |
| Added nullable column absent from an older file is NULL-filled | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | existing coverage (unchanged) |
| Added required column missing from an older file errors cleanly | Unit | `crates/lakehouse-engine/src/scan/mod.rs` (`field_id_adapter` mod) | existing coverage (unchanged) |
| Scan without a logical schema falls back to first-file inference | Integration | `crates/lakehouse-engine/tests/scan_no_head_test.rs` | existing coverage (unchanged) |

Note: `rename_physical_to_logical` is pure computation (`Schema` → `Schema`), so its
resolution scenarios are covered by unit tests in the `field_id_adapter` mod; the end-to-end
read behavior is covered by the Docker-free `run_raw_scan_with_session` integration test in
`tests/scan_name_mapping.rs`.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-execution-field-id-projection | `cargo test -p lakehouse-engine --test scan_name_mapping` | Both tests pass: the no-field-id column resolves to real values via the name-mapping; the empty-mapping case binds by physical name |
| scan-execution-field-id-projection | `cargo test -p lakehouse-engine field_id_adapter` | Name-mapping resolution / precedence / fallback unit tests pass |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
