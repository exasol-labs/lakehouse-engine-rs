# Tasks: fix-scan-field-id-projection

## Phase 2: Implementation (Group A)
- [x] 1.1 Add `iceberg_type_to_arrow` mapping in `types/mapping.rs`
- [x] 1.2 Unit-test the mapping across primitive, out-of-range decimal, and complex Iceberg types
- [x] 2.1 Add `#[serde(default)]` logical-schema field to `ScanSpec` (`scan/spec.rs`)
- [x] 2.2 Unit-test round-trip serde: spec WITH and legacy WITHOUT the logical schema

## Phase 2: Implementation (Group B)
- [x] 3.1 Extract logical schema at `resolve_file_list` seam (`adapter/pushdown.rs`)
- [x] 3.2 Integration test: pushdown request produces scan spec with expected field-ids/names/nullability
- [x] 4.1 Implement `FieldIdExprAdapter` + `FieldIdExprAdapterFactory` (`scan/mod.rs`) [expert]
- [x] 4.2 Wire `register_files` to build logical Arrow schema + `.with_expr_adapter_factory`
- [x] 4.3 Verify `build_scan_sql` uppercase-alias wrapper works over the logical schema

## Phase 2: Implementation (Group C)
- [x] 5.1 Integration tests for `FieldIdExprAdapter` using LOCAL Parquet files
- [x] 5.2 Flip `e2e_renamed_column_resolves_by_field_id` from xfail to assert correctness
- [x] 5.3 Rewrite doc-comment + seed call-site comment from repro/xfail framing to correctness

## Phase 2.5: Code Review Fixes
- [x] R.1 Remove dead `FieldIdExprAdapter` passthrough wrapper; factory returns `DefaultPhysicalExprAdapter` directly; move doc to factory/`rename_physical_to_logical`
- [x] R.2 Delete 3 duplicate short-named adapter tests (keep `field_id_adapter_*` variants)
- [x] R.3 Trim restating inline comments in `mapping.rs`; add name-uniqueness note to `rename_physical_to_logical` doc; fix `evo_schema` doc drift in seed.rs

## Phase 3: Verification
- [x] V.1 Build (`make cross-musl-udf-build`) — exit 0
- [x] V.2 Test unit (`cargo test`) — 310 pass, 0 fail
- [x] V.3 Test E2E (`make test-e2e`) — 35 pass, 0 fail (7 capability + 28 scan); flagship
      `e2e_renamed_column_resolves_by_field_id` ok after F.1 fix.
- [x] V.4 Lint (`cargo clippy --all-targets`) — 0 warnings
- [x] V.5 Format (`cargo fmt`) — clean

## Phase 3.5: E2E Fix
- [x] F.1 Fix projected-column field-id mapping so a physical file [id,score] read through a logical [id,rating] table returns `rating` values. Add a LOCAL row-collecting integration test reproducing the E2E (no Docker) before/with the fix. [expert]
