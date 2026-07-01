# Plan: fix-scan-field-id-projection

## Summary

Make the scan engine's column projection field-id based (with a simple physical-name fallback) so renamed, dropped, and added-nullable columns return correct results across Iceberg schema evolution. Closes #26.

## Design

### Context

The scan engine currently binds columns by physical Parquet column **name**, which diverges from the Iceberg column-projection spec (field-id based, name-mapping fallback) and returns wrong/incomplete results under schema evolution. `register_files` (`crates/lakehouse-engine/src/scan/mod.rs` ~669–708) infers ONE Arrow schema from the FIRST assigned file and builds a single `ListingTable`; `build_scan_sql` (~757–802) aliases/projects by physical name. Under a rename, physical `score` and current logical `rating` share field-id 2 but not a name, so binding fails; and one `ListingTable` inferred from the first file cannot represent files with divergent physical layouts.

- **Goals** — Bind columns by Iceberg field-id (primary) with a physical-name fallback (physical name == current logical name) when a file field carries no embedded `PARQUET:field_id`. Correctly handle renamed columns (bound by id), dropped columns (ignored), added nullable columns absent from older files (NULL-filled per file), and added required columns missing from an older file (clean error, never wrong data).
- **Non-Goals** — Filling an added REQUIRED column from its Iceberg `initial-default` (tracked as **#27** — no `initial_default` is carried in the ScanSpec and no custom missing-column fill logic beyond DataFusion's default adapter). Honoring the `schema.name-mapping.default` table property for files without embedded field-ids (tracked as **#28** — the fallback stays a plain physical-name match). Reimplementing the whole schema adapter, or using iceberg-rust's ArrowReader / iceberg-datafusion (ruled out by the two-Arrow-versions constraint).

### Decision

Do NOT reimplement the schema adapter. Override ONLY the column-RESOLUTION step to be field-id-first, and REUSE `DefaultPhysicalExprAdapter`'s existing behavior for everything else (nullable-missing → NULL-fill, type divergence → cast, required-missing → error). The adapter is installed via `ListingTableConfig::with_expr_adapter_factory(Arc<dyn PhysicalExprAdapterFactory>)` — the supported replacement for the deprecated no-op `with_schema_adapter_factory`. The Parquet opener applies the expr adapter **per file**, so a single `ListingTable` over files with divergent physical layouts is handled correctly.

The per-column data carried in the spec is just `{field_id, name, arrow_type, nullable}` — no defaults.

#### Architecture

```
VS adapter (resolve_file_list seam, pushdown.rs)
  current_schema()  ──▶  logical schema [{field_id, name, arrow_type, nullable}]
                              │  (serde ScanSpec, JSON VARCHAR across UDF boundary)
                              ▼
Scan UDF (register_files, scan/mod.rs)
  build logical Arrow schema (each field tagged PARQUET:field_id, faithful nullability)
  ListingTableConfig::with_expr_adapter_factory(FieldIdExprAdapterFactory)
                              │  per-file
                              ▼
  FieldIdExprAdapter::rewrite  ── resolve logical Column → physical column BY field-id
       (fallback: physical-name match) ── then delegate DefaultPhysicalExprAdapter
       for null-fill / cast / required-missing-error
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Resolve-once metadata extraction | `resolve_file_list` seam (pushdown.rs) | Logical schema (with field-ids) is derived once per query, never per node — matches the project's resolve-once rule |
| `#[serde(default)]` optional spec field | `ScanSpec` (scan/spec.rs) | New logical-schema field is backward-compatible; absent → UDF falls back to name-based inference |
| Override resolution, reuse default | `FieldIdExprAdapter` wraps `DefaultPhysicalExprAdapter` | Field-id binding is the only new behavior; null-fill / cast / required-missing-error are reused verbatim, keeping the change small |
| Per-file expr adapter | Parquet opener calls `factory.create(logical, physical)` | Divergent physical layouts within one shard each bind correctly |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Field-id `PhysicalExprAdapter` + logical-schema registration | iceberg-rust ArrowReader / iceberg-datafusion | Ruled out: iceberg 0.9.1 uses arrow 57 (aliased), the DataFusion session uses arrow/parquet 58; iceberg types cannot cross into the DataFusion session. DataFusion 54 dictates the `PhysicalExprAdapter` mechanism (`with_schema_adapter_factory` is a deprecated no-op) |
| Carry no defaults in the spec | Carry Iceberg `initial-default` to fill added required columns | Deferred to **#27**. Core reuses the default adapter: added nullable absent → NULL-filled for free; added required missing → clean error |
| Simple physical-name fallback | Parse `schema.name-mapping.default` | Deferred to **#28**. Modern writers embed field-ids, so the fallback rarely fires |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| datafusion-scan/type-mapping | CHANGED | `datafusion-scan/type-mapping/spec.md` |

## Dependencies

- No new crates. Uses `datafusion_physical_expr_adapter` traits (`PhysicalExprAdapter`, `PhysicalExprAdapterFactory`) already available via DataFusion 54, and `parquet::arrow::PARQUET_FIELD_ID_META_KEY` on the arrow-58 side.
- Related out-of-scope follow-ups, referenced but NOT planned here: **#27** (initial-default fill for added required columns), **#28** (`schema.name-mapping.default` property).

## Implementation Tasks

1. **Type mapping: Iceberg → Arrow logical type**
   - [ ] 1.1 Add an `iceberg_type_to_arrow` mapping (model on the existing `iceberg_type_to_exasol` in `crates/lakehouse-engine/src/types/mapping.rs`): primitives → their Arrow equivalents; complex / out-of-range types → a string-family Arrow type (surfaced as JSON VARCHAR). Keep it in agreement with the `createVirtualSchema` mapping.
   - [ ] 1.2 Unit-test the mapping across primitive, out-of-range decimal, and complex Iceberg types.

2. **ScanSpec: carry the logical schema**
   - [ ] 2.1 Add a `#[serde(default)]` logical-schema field to `ScanSpec` (`crates/lakehouse-engine/src/scan/spec.rs`), a list of `{field_id: i32, name: String, arrow_type, nullable: bool}` carrying the FULL current schema (every column, not just projected ones — the adapter must bind filter / GROUP BY / aggregate columns too; see decision-log [6]). Kept as a distinct field alongside `projection`, NOT merged into it. Absent → backward-compatible (UDF falls back to first-file inference). Define a serde-friendly representation for `arrow_type` (e.g. a string tag consumable by the UDF), keeping it credential-free.
   - [ ] 2.2 Unit-test round-trip serde: a spec WITH the logical schema and a legacy spec WITHOUT it both deserialize correctly.

3. **VS adapter: extract the logical schema at the resolve-once seam**
   - [ ] 3.1 In `resolve_file_list` (`crates/lakehouse-engine/src/adapter/pushdown.rs` ~1628), after loading the `Table`, read `table.metadata().current_schema()` and build the logical schema (per field: field-id, current name, Arrow type via task 1, nullability via required/optional). Populate the new ScanSpec field at both `spec_template` sites (~1533, ~1575).
   - [ ] 3.2 Integration test: a pushdown request produces a scan spec whose logical schema carries the expected field-ids, current names, and nullability.

4. **Scan UDF: FieldId expression adapter**
   - [ ] 4.1 Implement `FieldIdExprAdapter` + `FieldIdExprAdapterFactory` (`datafusion_physical_expr_adapter` traits): resolve each logical `Column` to a physical column by matching the logical field's `PARQUET:field_id` against physical fields' `PARQUET:field_id`; fall back to a physical-name match when a file field lacks a field-id; delegate to `DefaultPhysicalExprAdapter` for null-fill (nullable → NULL literal), type-diff → cast, and required-missing → error. [expert]
   - [ ] 4.2 Wire `register_files` (`crates/lakehouse-engine/src/scan/mod.rs` ~669) to, WHEN the logical schema is present, build the logical Arrow schema (each field tagged with `PARQUET:field_id` metadata + faithful nullability) and register the `ListingTable` via `ListingTableConfig` with `.with_expr_adapter_factory(FieldIdExprAdapterFactory)` INSTEAD of `infer_schema`; when absent, keep the existing first-file-inference path unchanged.
   - [ ] 4.3 Verify `build_scan_sql`'s uppercase-alias inner-SELECT wrapper still works over the logical (current-name) schema; adjust only if the alias step misbehaves over the registered logical schema.

5. **Tests**
   - [ ] 5.1 Add integration tests for `FieldIdExprAdapter` using LOCAL Parquet files (no S3/catalog) with divergent physical names but matching field-ids, via `build_raw_scan_physical_plan` (`crates/lakehouse-engine/src/scan/mod.rs` ~719). Cover: rename (bind by id), dropped column (ignored), added-nullable column absent from an older file (NULL-filled), fallback (file field has no field-id → name match), added-required missing (clean error). Do NOT add initial-default or name-mapping-property tests (#27/#28).
   - [ ] 5.2 Flip `crates/lakehouse-engine/tests/e2e_scan_test.rs::e2e_renamed_column_resolves_by_field_id` from the xfail asserting the bug to assert the spec-compliant result: `EVO_TOTAL_ROWS` (10) rows, `rating = 10*id`, no NULLs.
   - [ ] 5.3 Rename `e2e_renamed_column_resolves_by_field_id` → `e2e_renamed_column_resolves_by_field_id` and rewrite its doc-comment from repro/xfail framing to a plain description of what it asserts (schema evolution with a renamed column, field-id projection, 10 rows, `rating = 10*id`, no NULLs). Update the `seed_renamed_column` call-site comment accordingly.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | Task 1 (type mapping), Task 2 (ScanSpec field) |
| Group B | Task 3 (adapter extraction), Task 4 (scan UDF adapter) |
| Group C | Task 5 (tests) |

Sequential dependencies:
- Group A → Group B (Task 3 needs the mapping from 1 and the field from 2; Task 4 needs the field from 2)
- Group B → Group C (tests exercise the end-to-end path)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Comment (xfail note) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` (~564) | The "PASSES while the bug exists" xfail note is removed when 5.2 flips the test to assert correctness |
| Function name (repro framing) | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_renamed_column_resolves_by_field_id` → `e2e_renamed_column_resolves_by_field_id`; doc-comment rewritten from repro/xfail framing to correctness-assertion description (task 5.3) |

No production code is deleted: the first-file-inference path in `register_files` is retained as the backward-compatible fallback for specs without a logical schema.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| scan-execution / Scan registers only its assigned files and returns matching rows | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_projection_filter_limit_returns_correct_rows` |
| scan-execution / Column projection binds by Iceberg field-id across physical layouts | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_renamed_column_resolves_by_field_id` |
| scan-execution / Field-id resolution falls back to physical name when a file field carries no field-id | Integration | `crates/lakehouse-engine/src/scan/mod.rs` (tests) | `field_id_adapter_falls_back_to_name_without_field_id` |
| scan-execution / Added nullable column absent from an older file is NULL-filled | Integration | `crates/lakehouse-engine/src/scan/mod.rs` (tests) | `field_id_adapter_null_fills_added_nullable_column` |
| scan-execution / Added required column missing from an older file errors cleanly | Integration | `crates/lakehouse-engine/src/scan/mod.rs` (tests) | `field_id_adapter_errors_on_missing_required_column` |
| scan-execution / Scan without a logical schema falls back to first-file inference | Integration | `crates/lakehouse-engine/src/scan/mod.rs` (tests) | `register_files_falls_back_without_logical_schema` |
| pushdown-planning / Pushdown resolves the file list once and builds a scan-driving query | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `e2e_pushdown_resolves_files_once_multi_table` |
| pushdown-planning / Projection is pushed into the scan-driving query | Integration | `crates/lakehouse-engine/src/adapter/pushdown.rs` (tests) | `pushdown_spec_carries_logical_schema_field_ids` |
| type-mapping / Iceberg logical schema maps to Arrow types for scan registration | Unit | `crates/lakehouse-engine/src/types/mapping.rs` (tests) | `iceberg_type_to_arrow_maps_all_families` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| datafusion-scan/scan-execution | `make test-e2e` then in the Exasol session: `SELECT id, rating FROM EVO_VS.EVO ORDER BY id` | 10 rows, `rating = 10*id`, no NULLs (the renamed column resolves by field-id) |
| vs-adapter/pushdown-planning | `cargo test -p lakehouse-engine pushdown_spec_carries_logical_schema_field_ids` | Scan spec JSON carries the logical schema with expected field-ids and nullability |
| datafusion-scan/type-mapping | `cargo test -p lakehouse-engine iceberg_type_to_arrow` | Mapping test passes for primitive, out-of-range decimal, and complex Iceberg types |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test (unit) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures (`e2e_renamed_column_resolves_by_field_id` asserts the fix) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
