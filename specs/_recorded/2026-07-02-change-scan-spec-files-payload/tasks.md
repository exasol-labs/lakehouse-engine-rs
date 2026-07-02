# Tasks: change-scan-spec-files-payload

## Phase 2: Implementation (Group A — spec types + tests)
- [x] 1.1 Add `table_root: String` (`#[serde(default)]`, `skip_serializing_if` empty) to `CommonScanSpec` and `ScanSpec`; thread through `to_common`, `from_parts`, `from_parts_json` (`scan/spec.rs`)
- [x] 1.2 Change `ScanSpec.files` `Vec<String>` → `Vec<(String, u64)>`; retype `files_json`/`files_from_json`; keep credential-safe error redaction (`scan/spec.rs`)
- [x] 1.3 Unit tests in `scan/spec.rs`: `(path,size)` tuple round-trip; `table_root` round-trip + legacy-empty default; common blob has no `files` key; malformed JSON never leaks creds; `catalog` still absent. Update `sample_spec()` and all fixtures

## Phase 2: Implementation (Group B — sharding size propagation) — depends on A
- [x] 2.1 [expert] Change `partition_files_by_bytes(...) -> Vec<Vec<(String,u64)>>` so shards carry `(path,size)`; keep LPT byte-balancing, 0-byte→1-byte, clamp `[1,files.len()]`, disjoint-cover UNCHANGED (`adapter/sharding.rs`)
- [x] 2.2 Update `adapter/sharding.rs` tests: sizes travel with paths; balance/coverage/zero-size assertions still hold

## Phase 2: Implementation (Group C — adapter table_root + relative paths) — depends on A, B
- [x] 3.1 Return table root from `resolve_file_list` (add to returned tuple, sourced from `table_s3_location = result.metadata.location()` at `pushdown.rs:~1901`)
- [x] 3.2 [expert] In `handle_pushdown`, populate `table_root` on both `spec_template` sites (grouped + row/single-group); strip `table_root` when `path.starts_with(table_root)` else keep absolute, as shards are built
- [x] 3.3 Retype fan-out/SQL builders (`build_fan_out_inner`, `build_row_scan_sql`, `build_aggregate_scan_sql`, `build_grouped_aggregate_scan_sql`, `build_scan_driving_sql`) to take `shards: &[Vec<(String,u64)>]`; serialize via retyped `files_json`; common blob includes `table_root`
- [x] 3.4 Update `pushdown.rs` SQL-builder unit tests + fixtures: root once in common literal, never per-shard; per-shard literals `[[path,size],...]`; not-under-root stays absolute, under-root relative

## Phase 2: Implementation (Group D — scan UDF reconstruct + no-HEAD) — depends on A
- [x] 4.1 [expert] In `register_files` (`scan/mod.rs`): reconstruct absolute URI — `://` → parse as-is; else join onto `spec.table_root` (normalize trailing `/`). Preserve logical-schema + `FieldIdExprAdapterFactory` branch and first-file-inference fallback
- [x] 4.2 [expert] Wrap registered `ObjectStore` so `head(&Path)` returns `ObjectMeta{location, last_modified: Utc.timestamp_nanos(0), size, e_tag: None, version: None}` from spec size, delegating other methods; register in `RuntimeEnv` `ObjectStoreRegistry`; keep `with_expr_adapter_factory` wiring
- [x] 4.3 Fix `extract_bucket` (`scan/mod.rs:~665`): first entry may be relative; derive bucket from absolute URI (reconstruct via `table_root` or parse `table_root` host)
- [x] 4.4 Update `scan/mod.rs` fixtures building `spec.files = vec![...]` to `(path,size)` shape; set `table_root` where relative entry exercised

## Phase 2: Implementation (Group E — integration + E2E) — depends on C, D
- [x] 5.1 [expert] Host integration test (local Parquet, no S3) driving `register_files` + raw scan: identical rows whether sizes via spec or discovered; relative-entry + table_root reconstitution resolves to same file (`crates/lakehouse-engine/tests/`)
- [x] 5.2 E2E: multi-file scan through VS returns correct rows with new payload; spot-check fan-out SQL carries root once + per-shard `(path,size)` literals (`tests/e2e_scan_test.rs`)

## Phase 4: Code Review Fixes
- [x] R.1 [expert] Fix strip/reconstruct non-`/`-boundary asymmetry: `relativize_path_to_root` (pushdown.rs) strips `table_root` only at a real path-segment boundary, else emits absolute. Add regression test.

## Phase 3: Verification
- [x] V.1 Build: `make cross-musl-udf-build` → exit 0
- [x] V.2 Test: `cargo test` → 0 failures
- [x] V.3 E2E: `make test-e2e` → 0 failures
- [x] V.4 Lint: `cargo clippy --all-targets` → 0 warnings
- [x] V.5 Format: `cargo fmt` → no changes

## Notes
- Group F (6.1 decision log) is deferred to /speq:record time per plan task 6.1.
