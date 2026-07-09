# Plan: fix-iceberg-spec-gaps

## Summary

Close the two remaining Iceberg column-projection missing-field gaps by converting the two
cases where the scan silently returns wrong data — an identity-partition source column absent
from a data file (rule #1) and an added column with a non-null `initial-default` absent from a
data file (rule #3) — into clean, fail-loud errors via a shard-invariant no-null-fill guard
resolved once per query; full value reconstruction is deferred and tracked (issue #99 / #27,
backlog BL-003 / BL-004).

## Design

### Context

Iceberg's column-projection resolution order for a field-id absent from a data file is
(1) reconstruct from the data file's identity-partition metadata, (2) `schema.name-mapping.default`,
(3) the field's `initial-default`, (4) null. Rule #2 shipped in `change-name-mapping-fallback`
(#28). Rules #1 and #3 are implemented nowhere:

- **Gap 1 (rule #1, untracked):** `FileEntry` carries only `{path, size, deletes}` and
  `plan_files_from_table` drops each file's partition tuple. An identity-partitioned table
  whose data files omit the partition source column (permitted by the spec — e.g. metadata-only
  Hive migrations) returns a silent wrong NULL for the optional case, or a misattributed
  "required missing" error for the required case.
- **Gap 2 (rule #3, tracked but mis-scoped as #27):** `initial-default` applies to ANY added
  field, optional or required. `LogicalField` carries no `initial_default`, and the field-id
  adapter unconditionally NULL-fills any absent nullable column, so an optional column added
  with a non-null `initial-default` silently returns NULL instead of the default for pre-add rows.

Both defects are **silent wrong data**. The mission makes correctness and safety guards
first-class ("usable engine … returns a clean error rather than OOM-crashing"). The minimal
mission-aligned fix is to make both cases fail loud; materializing the correct value is a
larger feature (per-file partition tuples / typed-literal synthesis) that shares one seam and
is deferred with tracking, per the repo's Iceberg-spec-compliance rule.

- **Goals** — Detect, per file, that a logical field-id which resolves to no physical column is
  one where a substituted NULL (or a bare required-missing error) is *known wrong*, and return a
  clean, credential-free error naming the accurate reason. Resolve the guard set once per query
  in the VS from table metadata and thread it shard-invariant into the scan spec, exactly
  mirroring how `logical_schema` and `name_mapping` are threaded. Preserve all current behavior
  when the guard set is empty (the common case).
- **Non-Goals** — Materializing the correct value: reconstructing the partition value from a
  file's partition tuple (deferred, #99 / BL-003) or synthesizing the `initial-default` literal
  (deferred, #27 / BL-004). Neither adds per-file partition tuples nor typed-default literals to
  the wire format in this plan. Nested `fields` name-mapping (#83). Non-identity partition
  transforms (only identity is reconstructable by value substitution; others are not affected).

### Decision

Add ONE shard-invariant guard threaded through the same seam as `name_mapping`, consumed at the
same null-fill point in the scan.

- **VS (plan time, `resolve_file_list`):** from `table.metadata()`, collect a
  `Vec<FieldFillGuard { field_id, reason }>`: `reason = IdentityPartition` for every `source_id`
  of an Identity-Transform `PartitionField` across `partition_specs_iter()`; `reason =
  InitialDefault` for every current-schema `NestedField` whose `initial_default` is `Some`. A
  field-id qualifying under both records IdentityPartition once. Thread it into `CommonScanSpec`
  (and the join dimension side), exactly like `name_mapping`.
- **Scan (`FieldIdExprAdapterFactory::create`, per file):** after computing the
  logical-name-renamed physical schema (existing `rename_physical_to_logical`), determine which
  logical field-ids resolve to NO physical column in this file. For each such absent field-id
  present in the guard set, return `Err(UdfError/DataFusionError)` with an accurate, redacted
  message BEFORE constructing the inner `DefaultPhysicalExprAdapter`. Absent field-ids NOT in the
  guard set keep today's behavior (nullable → NULL, required → default adapter's clean error).

#### Architecture

```
VS planning (pushdown.rs)                        Scan UDF (scan/mod.rs)
┌──────────────────────────────┐                 ┌───────────────────────────────────┐
│ resolve_file_list            │  ScanSpec         │ register_files                    │
│  table.metadata()            │  .field_fill_     │  → PositionalDeleteScanTable      │
│   .partition_specs_iter()    │   guards          │   → FieldIdExprAdapterFactory     │
│    (Transform::Identity       │ ───────────────▶ │    create(): logical field-id     │
│     → source_id)             │ (via Common-     │     absent from file ∧ in guard   │
│   .current_schema()          │  ScanSpec)       │     set → clean error (no NULL)    │
│    (initial_default.is_some) │                 │    else → DefaultPhysicalExprAdapter│
│  → Vec<FieldFillGuard>       │                 │                                     │
└──────────────────────────────┘                 └───────────────────────────────────┘
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Resolve-once in VS, thread into shard-invariant `CommonScanSpec` | `resolve_file_list` → `CommonScanSpec.field_fill_guards` | Repo rule: metadata resolved once per query, never per UDF invocation |
| Fail loud instead of silent wrong data | `FieldIdExprAdapterFactory::create` guard | Mission: correctness/safety first-class; a wrong NULL is worse than a clean error |
| Reuse the `name_mapping` threading seam verbatim | spec.rs, pushdown.rs, scan/mod.rs, join side | Precedent `change-name-mapping-fallback`; minimizes new surface |
| Accurate reason enum (`IdentityPartition` / `InitialDefault`) | `FieldFillGuard.reason` | Error names the real cause and the deferred follow-on issue |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Fail-loud guard now; defer value materialization | Full rule-#1 partition reconstruction + rule-#3 default synthesis in this plan | Reconstruction needs per-file partition tuples on `FileEntry` + typed-literal synthesis intercepting the default adapter — a genuine feature, not a gap fix; the guard removes the *silent-wrong-data* defect with a fraction of the surface and leaves a clean follow-on. Mission prioritizes bounded correctness over completeness. |
| One guard set with a `reason` tag | Two separate `Vec<i32>` fields | One field keeps the wire compact and lets one code path emit accurate per-reason errors; extensible if rule ordering changes |
| Guard both optional AND required absent fields | Guard only optional (silent-NULL) fields | For a required guarded field the current error is *misattributed*; naming the real reason (partition / initial-default) is strictly better and costs nothing |
| Collect identity sources across ALL partition specs | Only `default_partition_spec()` | A file may have been written under an older spec; over-guarding fails loud (safe), under-guarding risks a silent wrong NULL |
| Broaden #27 to any-nullability (not just required) | Leave #27 required-only | Iceberg `initial-default` applies to any added field; the current spec/#27 scope is a factual error the intent calls out |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-field-id-projection | CHANGED | `datafusion-scan/scan-execution-field-id-projection/spec.md` |

## Dependencies

- `iceberg` crate (git rev `662ac7b`, current pin) — CONFIRMED to expose
  `NestedField.initial_default: Option<Literal>` (`spec/datatypes.rs`),
  `PartitionField { source_id, transform }` + `Transform::Identity` (`spec/partition.rs`), and
  `TableMetadata::partition_specs_iter()` / `current_schema()` (`spec/table_metadata.rs`). No new
  dependency required.

## Implementation Tasks

1. **Wire type + spec threading**
   - [ ] 1.1 Add `FieldFillGuard { field_id: i32, reason: FillGuardReason }` and
     `enum FillGuardReason { IdentityPartition, InitialDefault }` (serde, lowercase tag) to
     `scan/spec.rs`; add `field_fill_guards: Vec<FieldFillGuard>`
     (`#[serde(default, skip_serializing_if = "Vec::is_empty")]`) to `ScanSpec`, `CommonScanSpec`,
     and `JoinSpec`; copy it in `ScanSpec::to_common`, `from_parts`, and the join-side builders
     that already clone `name_mapping`.
   - [ ] 1.2 Unit test in `scan/spec.rs`: `field_fill_guards` round-trips through
     `ScanSpec`/`CommonScanSpec` JSON when populated, is absent from JSON when empty, and a legacy
     payload lacking the field deserializes to empty (backward-compat) — mirroring
     `name_mapping_round_trips_and_defaults_to_empty`.
2. **VS resolve-once guard-set construction + threading**
   - [ ] 2.1 In `resolve_file_list` (`adapter/pushdown.rs`), add `build_field_fill_guards(&TableMetadata)`:
     for each `PartitionField` with `Transform::Identity` across `partition_specs_iter()`, emit
     `{source_id, IdentityPartition}`; for each `current_schema()` field with `initial_default.is_some()`,
     emit `{field_id, InitialDefault}`; dedup so a field-id under both records IdentityPartition once.
     Extend the return tuple and thread the value into the built `ScanSpec` and into the join
     dimension side, mirroring `name_mapping`. Reads only metadata already loaded — no credential path. `[expert]`
   - [ ] 2.2 Unit test in `adapter/pushdown.rs`: a synthetic `TableMetadata` with an identity partition
     on field A, a bucket (non-identity) partition on field B, a schema field C with a non-null
     `initial_default`, and a field D that is both an identity source and has an initial-default →
     guard set = {A:identity, C:initial_default, D:identity}, B excluded; a table with neither yields empty.
3. **Scan-side guard enforcement**
   - [ ] 3.1 Thread `field_fill_guards` from `ScanSpec` through `register_files` →
     `PositionalDeleteScanTable::new` → `FieldIdExprAdapterFactory` (add a field), mirroring how
     `name_mapping` already flows; do the same for the join dimension side's `register_files` call.
   - [ ] 3.2 In `FieldIdExprAdapterFactory::create`, after `rename_physical_to_logical`, compute the
     set of logical field-ids with NO resolving physical column in this file; for each such field-id
     found in `field_fill_guards`, return a clean `DataFusionError` naming the column and the accurate
     reason (identity-partition reconstruction / `initial-default` materialization not implemented),
     redacted of credentials, BEFORE building the inner `DefaultPhysicalExprAdapter`. Non-guarded
     absent field-ids keep current behavior. `[expert]`
   - [ ] 3.3 Unit tests in the `field_id_adapter` mod of `scan/mod.rs`: (a) an absent guarded
     identity-partition field-id → `create` errors with the partition reason; (b) an absent guarded
     initial-default field-id → errors with the initial-default reason; (c) an absent NON-guarded
     nullable field-id → `create` succeeds (default adapter NULL-fills, unchanged); (d) a guarded
     field-id that IS present in the file → `create` succeeds (guard only fires on absence);
     (e) a field-id in both categories → error names the partition reason.
4. **Docker-free integration test of the read path**
   - [ ] 4.1 Add `tests/scan_fill_guard.rs` (reusing the `scan_name_mapping.rs` / `scan_no_head_test.rs`
     harness: local `file://` Parquet via `ArrowWriter`, `run_raw_scan_with_session`). Write a Parquet
     file missing a field-id present in the logical schema; run once with that field-id in
     `field_fill_guards` (assert the scan fails with the accurate, credential-free message) and once
     with an empty guard set (assert the column NULL-fills as today). Cover both reasons.
5. **Track the deferred value materialization (fail-loud → full-fill follow-ons)**
   - [ ] 5.1 Add `BL-003` (identity-partition value reconstruction, references new issue #99) and
     `BL-004` (any-nullability `initial-default` materialization, cross-references #27) to
     `specs/backlog.md`, using the entry text recorded in `decision-log.md`.
   - [ ] 5.2 Create GitHub issue #99 ("feat(scan): reconstruct identity-partition source column values
     from data-file partition metadata — Iceberg rule #1") per the repo's issue-tracking rule, and
     update issue #27's body to broaden its scope from required-only to any-nullability `initial-default`
     fill. Reference #99 / #27 in the implementing commit. (Read-only-git constraint: issue creation is a
     GitHub write performed at implementation start, not during planning.)

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 |
| Group B (after A) | 2.1, 3.1 |
| Group C (after B) | 3.2 |
| Group D (tests, after their code) | 1.2, 2.2, 3.3, 4.1 |
| Group E (tracking, independent) | 5.1, 5.2 |

Sequential dependencies:
- 1.1 → Group B (2.1, 3.1 depend on the new spec field)
- 3.1 → 3.2 (the factory must carry the guard set before `create` can enforce it)
- Each test task depends on its corresponding implementation task.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| (none) | — | The guard is additive; the default NULL-fill / required-missing paths are retained for non-guarded fields. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Column projection binds by Iceberg field-id across physical layouts | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | existing coverage (unchanged) |
| Field-id resolution honors schema.name-mapping.default for a file field without an embedded field-id | Integration | `crates/lakehouse-engine/tests/scan_name_mapping.rs` | existing coverage (unchanged) |
| Field-id resolution falls back to physical name when no name-mapping resolves a file field without an embedded field-id | Integration/Unit | `crates/lakehouse-engine/tests/scan_name_mapping.rs`, `crates/lakehouse-engine/src/scan/mod.rs` | existing coverage (unchanged) |
| The VS resolves schema.name-mapping.default once per query into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | existing coverage (unchanged) |
| The VS resolves the no-null-fill guard set once per query into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `builds_field_fill_guards_from_metadata`, `empty_guard_set_when_no_identity_or_default` |
| Added nullable column absent from an older file is NULL-filled only when no guard applies | Integration | `crates/lakehouse-engine/tests/scan_fill_guard.rs` | `unguarded_absent_nullable_column_null_fills` |
| Optional identity-partition source column missing from a file errors cleanly instead of NULL-filling | Integration/Unit | `crates/lakehouse-engine/tests/scan_fill_guard.rs`, `crates/lakehouse-engine/src/scan/mod.rs` | `guarded_identity_partition_absent_errors`, `create_errors_on_absent_identity_partition_guard` |
| Optional column with a non-null initial-default missing from a file errors cleanly instead of NULL-filling | Integration/Unit | `crates/lakehouse-engine/tests/scan_fill_guard.rs`, `crates/lakehouse-engine/src/scan/mod.rs` | `guarded_initial_default_absent_errors`, `create_errors_on_absent_initial_default_guard` |
| Added required column missing from an older file errors cleanly | Unit | `crates/lakehouse-engine/src/scan/mod.rs` (`field_id_adapter` mod) | `required_missing_errors` (existing) + `guarded_required_names_accurate_reason` |
| Scan without a logical schema falls back to first-file inference | Integration | `crates/lakehouse-engine/tests/scan_no_head_test.rs` | existing coverage (unchanged) |
| Guard set round-trips through the scan spec (backward-compat) | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `field_fill_guards_round_trips_and_defaults_to_empty` |

Note: `FieldIdExprAdapterFactory::create` guard enforcement is pure computation over
`Schema` + guard set, so its resolution scenarios are covered by unit tests in the
`field_id_adapter` mod; the end-to-end read behavior is covered by the Docker-free
`run_raw_scan_with_session` integration test in `tests/scan_fill_guard.rs`.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-execution-field-id-projection | `cargo test -p lakehouse-engine --test scan_fill_guard` | A guarded absent field-id fails with an accurate, credential-free message; the empty-guard case NULL-fills as before |
| scan-execution-field-id-projection | `cargo test -p lakehouse-engine field_id_adapter` | Guard-enforcement (identity / initial-default / present / non-guarded / both) unit tests pass |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt --check` | No changes |
