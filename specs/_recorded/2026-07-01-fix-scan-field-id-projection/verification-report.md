# Verification Report: fix-scan-field-id-projection

## Verdict: ✅ PASS

Field-id-based column projection is implemented and verified end-to-end. All automated
checks pass (build, 312 host tests, 35 E2E tests, clippy, fmt), and every plan scenario
has a passing test. The flagship schema-evolution scenario — a renamed Iceberg column
resolved by field-id across divergent physical Parquet layouts — is confirmed against the
live Exasol Docker cluster.

Version bumped `0.16.0 → 0.16.1` (correctness fix). Closes #26.

## Automated Checks

| Step | Command | Expected | Result |
|------|---------|----------|--------|
| Build | `make cross-musl-udf-build` | Exit 0 | ✅ exit 0 |
| Test (unit) | `cargo test` | 0 failures | ✅ 312 passed, 0 failed |
| Test (E2E) | `make test-e2e` | 0 failures | ✅ 35 passed (7 capability + 28 scan), 0 failed |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings | ✅ 0 |
| Format | `cargo fmt --check` | No changes | ✅ clean |

## Scenario Coverage

| Scenario | Test | Result |
|----------|------|--------|
| Scan registers assigned files, returns matching rows | `e2e_projection_filter_limit_returns_correct_rows` | ✅ |
| Column projection binds by Iceberg field-id across physical layouts | `e2e_renamed_column_resolves_by_field_id` (E2E) + `field_id_adapter_reads_renamed_column_rows` / `field_id_adapter_reads_divergent_layouts_across_files` (local row-level) | ✅ |
| Field-id resolution falls back to physical name when a file field carries no field-id | `field_id_adapter_falls_back_to_name_without_field_id` | ✅ |
| Added nullable column absent from an older file is NULL-filled | `field_id_adapter_null_fills_added_nullable_column` | ✅ |
| Added required column missing from an older file errors cleanly | `field_id_adapter_errors_on_missing_required_column` | ✅ |
| Scan without a logical schema falls back to first-file inference | `register_files_falls_back_without_logical_schema` | ✅ |
| Pushdown resolves the file list once and builds a scan-driving query | `e2e_pushdown_resolves_files_once_multi_table` | ✅ |
| Pushdown scan spec carries the logical schema field-ids | `pushdown_spec_carries_logical_schema_field_ids` | ✅ |
| Iceberg logical schema maps to Arrow types for scan registration | `iceberg_type_to_arrow_maps_all_families` | ✅ |

## Key Findings & Corrections

1. **The `FieldIdExprAdapter` wrapper is load-bearing in DataFusion 54 — not dead code.**
   The code review's "simplification" (removing the wrapper so the factory returned a bare
   `DefaultPhysicalExprAdapter`) was a regression the host unit tests could not catch, because
   none of them collected rows from a physical file whose field names diverge from the logical
   table schema. The E2E caught it: DF54's Parquet opener applies the `PhysicalExprAdapter` to
   the projection, but resolves the adapter's output `Column`s **by name against the real
   physical file schema** — so a projected `Column("rating")` failed name lookup against a file
   whose fields are `[id, score]`. The wrapper was re-introduced; its `rewrite` renames the
   default's output columns back to real physical names at their (order-preserved) indices.
   The `PhysicalExprAdapter` (via `with_expr_adapter_factory`) IS the correct and sufficient DF54
   mechanism — the plan's premise that projection remapping needs a `SchemaAdapterFactory` was
   incorrect.

2. **Regression guard added.** `field_id_adapter_reads_renamed_column_rows` and
   `field_id_adapter_reads_divergent_layouts_across_files` now execute the production
   `register_files` + `build_scan_sql` path against local Parquet files with divergent physical
   names and **collect rows**, reproducing the E2E without Docker. These fail if the wrapper is
   removed again.

## Out-of-Scope Follow-ups (referenced, not implemented)

- **#27** — `initial-default` fill for added required columns (added-required-missing errors cleanly).
- **#28** — `schema.name-mapping.default` table property (fallback stays a plain physical-name match;
  also the name-collision-after-drop+rename edge case).
