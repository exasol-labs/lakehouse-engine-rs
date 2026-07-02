# Plan: change-scan-spec-files-payload

## Summary

Reshape the per-shard `ScanSpec.files` payload from bare absolute URI strings into compact
`(relative-or-absolute-path, byte-size)` 2-tuples, and carry the Iceberg table root ONCE in
the shard-invariant common spec — so the pushdown SQL stops repeating the table-location
prefix per file (#45) and the scan UDF builds each file's `ObjectMeta` from the spec instead
of issuing a redundant per-file object-store `HEAD` (#29). Closes #45 and #29.

## Design

### Context

The generated fan-out SQL embeds each shard's file list as a JSON array of fully-qualified S3
URIs. Every path repeats the same ~40–70-char table-location prefix (#45), and the scan UDF's
`ListingTable`, given exact paths, issues one object-store `HEAD` per assigned file at plan
time just to recover a byte size the VS layer ALREADY resolved from the Iceberg manifest and
then discarded in `partition_files_by_bytes` (#29). Both problems live on the same
`resolve_file_list → partition_files_by_bytes → ScanSpec → register_files` path and both are
solved by reshaping the same per-shard `files` payload, so they are planned together.

- **Goals** — carry the table root once (not per file); emit per-shard paths relative to it
  when it is an actual prefix; carry each file's byte size through the shard into the spec;
  build the scan's per-file metadata from the spec so no per-file HEAD is issued. Preserve
  byte-balanced sharding, the CommonScanSpec/ScanSpec split, credential-safe errors, the
  no-catalog guarantee, and field-id projection exactly.
- **Non-Goals** — no change to sharding math (G computation, byte-balancing, 0-byte→1-byte,
  disjoint-cover); no change to pushdown translation, aggregate decomposition, memory sizing,
  or credential resolution; no cross-version wire-compat decoder; not addressing the
  `initial-default` / name-mapping field-id gaps (#27/#28, still out of scope).

### Decision

#### Architecture

```
resolve_file_list ──▶ (files: Vec<(path, size)>, storage, logical_schema, table_root)
        │                       (table_root = result.metadata.location(), already computed)
        ▼
partition_files_by_bytes(files, G) ──▶ Vec<Vec<(path, size)>>   (size PROPAGATED, not dropped)
        │
        ▼  per shard, adapter side:
   strip table_root from path IFF path.starts_with(table_root); else keep absolute
        │
        ▼
   CommonScanSpec { ..., table_root }  serialized ONCE  ─┐
   shard files [[rel_or_abs_path, size], ...] per VALUES ─┴─▶ fan-out SQL
        │
        ▼  scan UDF (register_files):
   entry has "://"  → absolute, ListingTableUrl::parse as-is
   else             → join onto table_root (normalize trailing '/'), then parse
   build ObjectMeta{ size } from the (path,size) entry → ListingTable issues NO HEAD
   keep with_expr_adapter_factory(FieldIdExprAdapterFactory)  (field-id projection intact)
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Compact `(path, size)` tuple | `ScanSpec.files: Vec<(String,u64)>`, `files_json`/`files_from_json`, `partition_files_by_bytes` | Carry both facts in minimal bytes; serde-native tuple |
| Serialize-invariant-once | `table_root` in `CommonScanSpec` | Ship the table-location prefix once, not per file |
| Strip-if-prefix / absolute-passthrough | adapter path emit + UDF `register_files` reconstruct | Correct for `write.data.path` / object-storage / migrated layouts |
| Spec-backed metadata (no HEAD) | UDF file registration | Skip N per-file object-store HEADs on the pre-scan path |

#### Register path (supplying sizes while keeping field-id projection)

CONFIRMED (DataFusion 54.0.0 + object_store 0.13.2 source): keep the existing `ListingTable` +
`with_expr_adapter_factory(FieldIdExprAdapterFactory)` registration and wrap the `AmazonS3`
store in a thin `ObjectStore` that answers `head(&Path)` from the spec's known sizes
(delegating all other calls to the inner store), registered in the session `RuntimeEnv`'s
`ObjectStoreRegistry`. For an exact-file URL DF 54 calls `store.head(&path)` per path and does
NOT cache that branch, so the override is consulted every query and issues no network HEAD.
Build `ObjectMeta { location, last_modified: Utc.timestamp_nanos(0), size, e_tag: None, version: None }`
(`size` is `u64`; the epoch `last_modified` is exactly what `PartitionedFile::new` uses and is
not read for scan correctness). Fallback (also verified viable, retains field-id via
`FileScanConfigBuilder::with_expr_adapter`): `PartitionedFile::new(path, size)` +
`FileScanConfigBuilder` — larger change, use only if `ListingTable` must be abandoned. See
decision-log entry [4].

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| `Vec<(String,u64)>` compact tuple | struct-per-file objects; parallel path/size arrays | Minimal bytes, serde-native, cannot desync |
| `table_root` in the COMMON blob | repeat prefix per file (bug); staged prefix table | Shard-invariant; already resolved as the vended anchor; free to forward |
| Strip only on real prefix; else absolute | always strip / always join | Iceberg paths not guaranteed under `metadata.location()` |
| ObjectStore `head()` wrapper keeps ListingTable | PartitionedFile/FileScanConfig rewrite | Additive; leaves field-id projection path untouched |
| No dual-format `files` decoder | accept both old strings and new tuples | Stateless + single-`.so`: writer and reader are the same version |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| parallelism/work-unit-sharding | CHANGED | `parallelism/work-unit-sharding/spec.md` |
| datafusion-scan/scan-execution-spec-reconstitution | CHANGED | `datafusion-scan/scan-execution-spec-reconstitution/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |

## Dependencies

- DataFusion `54`, `object_store` `0.13.2`, `arrow`/`parquet` `58` (workspace-pinned; no
  version bump). Register-path mechanism confirmed against these versions.

## Migration

| Current | New |
|---------|-----|
| `ScanSpec.files: Vec<String>` (absolute URIs) | `ScanSpec.files: Vec<(String, u64)>` (relative-or-absolute path + byte size) |
| `CommonScanSpec` / `ScanSpec` (no root) | `+ table_root: String` (`#[serde(default)]`, empty ⇒ all-absolute) |
| `files_json`/`files_from_json` over `[String]` | over `[[path, size]]` 2-tuples |
| `partition_files_by_bytes(...) -> Vec<Vec<String>>` | `-> Vec<Vec<(String, u64)>>` (size propagated) |
| `register_files`: `ListingTableUrl::parse(f)` per absolute path; `ListingTable` HEADs each file | reconstruct absolute (join relative onto root; `://` passthrough); build `ObjectMeta` from spec size; no HEAD |

## Implementation Tasks

1. **Spec types: table_root + `(path, size)` files**
   1.1 Add `table_root: String` (`#[serde(default)]`, `skip_serializing_if` empty) to
       `CommonScanSpec` and `ScanSpec`; thread it through `to_common`, `from_parts`,
       `from_parts_json` (`scan/spec.rs`).
   1.2 Change `ScanSpec.files` from `Vec<String>` to `Vec<(String, u64)>`; retype
       `files_json(&[(String,u64)])` and `files_from_json(&str) -> Vec<(String,u64)>`
       (`scan/spec.rs`). Keep credential-safe error redaction (never echo raw input).
   1.3 Unit tests (`scan/spec.rs`): `(path,size)` files round-trip as compact 2-tuple arrays;
       `table_root` round-trips and defaults to empty on a legacy payload; common blob still
       carries no `files` key; malformed common/files JSON never leaks credentials; `catalog`
       still absent from all serialized JSON. Update `sample_spec()` and every existing
       `files: vec![...]` / legacy-JSON fixture in this module to the new shapes.

2. **Sharding: propagate size through the shard**
   2.1 Change `partition_files_by_bytes(files: Vec<(String,u64)>, n) -> Vec<Vec<(String,u64)>>`
       so each shard carries `(path, size)` entries; keep the LPT byte-balancing, the
       0-byte→1-byte rule, the clamp to `[1, files.len()]`, and disjoint-cover UNCHANGED
       (`adapter/sharding.rs`). [expert]
   2.2 Update `adapter/sharding.rs` tests to assert sizes travel with paths into the shards
       while the existing balance/coverage/zero-size assertions still hold.

3. **Adapter: carry table_root, emit relative-or-absolute paths**
   3.1 Return the table root from `resolve_file_list` (add it to the returned tuple, sourced
       from the `table_s3_location` = `result.metadata.location()` already computed at
       `pushdown.rs:~1901`) (`adapter/pushdown.rs`).
   3.2 In `handle_pushdown`, populate `table_root` on both `spec_template` construction sites
       (grouped + row/single-group). For each file, strip `table_root` when
       `path.starts_with(table_root)`, else keep absolute — do this as the shards are built
       from `partition_files_by_bytes` output so the per-shard payload carries relative (or
       absolute) `(path, size)` entries. [expert]
   3.3 Retype the fan-out/SQL builders (`build_fan_out_inner`, `build_row_scan_sql`,
       `build_aggregate_scan_sql`, `build_grouped_aggregate_scan_sql`, `build_scan_driving_sql`)
       to take `shards: &[Vec<(String,u64)>]` and serialize each shard via the retyped
       `files_json`. The common blob (already serialized once) now includes `table_root`.
   3.4 Update `pushdown.rs` SQL-builder unit tests + the `files_with_sizes`/`partition_*`
       fixtures: assert the table root appears once in the common literal and never in a
       per-shard `VALUES` literal; assert per-shard literals are `[[path,size],...]`; assert a
       not-under-root file stays absolute while under-root files are relative.

4. **Scan UDF: reconstruct absolute paths, build metadata from spec (no HEAD)**
   4.1 In `register_files` (`scan/mod.rs`), reconstruct each absolute URI: entry contains `://`
       → parse as-is; else join onto `spec.table_root` (normalize trailing `/`) before
       `ListingTableUrl::parse`. Preserve the logical-schema + `FieldIdExprAdapterFactory`
       branch and the first-file-inference fallback branch unchanged. [expert]
   4.2 Supply caller-known sizes so `ListingTable` issues no per-file HEAD: wrap the registered
       `ObjectStore` so `head(&Path)` returns `ObjectMeta { location, last_modified:
       Utc.timestamp_nanos(0), size (u64 from the spec), e_tag: None, version: None }`, keyed by
       the reconstructed absolute path, delegating every other method to the inner `AmazonS3`
       store. Register the wrapper in the session `RuntimeEnv`'s `ObjectStoreRegistry` under the
       same `ObjectStoreUrl`. Keep the `with_expr_adapter_factory(FieldIdExprAdapterFactory)`
       wiring untouched so field-id projection is preserved. (Confirmed against DF 54 /
       object_store 0.13.2; `PartitionedFile`+`FileScanConfigBuilder::with_expr_adapter` is the
       documented fallback.) [expert]
   4.3 Fix `extract_bucket` (`scan/mod.rs:~665`): the first file entry may now be RELATIVE, so
       derive the bucket from an absolute URI (reconstruct via `table_root` first, or parse the
       `table_root` host) rather than `Url::parse`-ing a possibly-relative `files[0]`.
   4.4 Update `scan/mod.rs` unit/integration fixtures that build `spec.files = vec![...]` to the
       `(path,size)` shape and set `table_root` where a relative entry is exercised.

5. **Integration + E2E coverage**
   5.1 Host integration test (local Parquet, no S3) driving `register_files` + a raw scan that
       asserts identical rows whether sizes are supplied via the spec or discovered — and that
       a relative-entry + table_root reconstitution resolves to the same file
       (`crates/lakehouse-engine/tests/`). [expert]
   5.2 E2E: confirm a multi-file scan through the VS still returns correct rows with the new
       payload shape; spot-check the generated fan-out SQL carries the root once and per-shard
       `(path,size)` literals (`tests/e2e_scan_test.rs`).

6. **Decision log**
   6.1 Add the plan's ADR entries (2-tuple encoding; table_root-once; strip-if-prefix /
       absolute-passthrough; ObjectStore head() wrapper preserving field-id projection;
       stateless/single-`.so` no-back-compat) to `specs/decision-log.md` at record time.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3 (spec types + tests) |
| Group B | 2.1, 2.2 (sharding size propagation) — depends on the `(String,u64)` element type from A |
| Group C | 3.1–3.4 (adapter table_root + relative paths) — depends on A, B |
| Group D | 4.1–4.4 (scan UDF reconstruct + no-HEAD) — depends on A |
| Group E | 5.1, 5.2 (integration + E2E) — depends on C, D |
| Group F | 6.1 (decision log) — independent |

Sequential dependencies:
- Group A → B, C, D (all consume the retyped `files` / `table_root`)
- Group B → C (adapter shards over the retyped partitioner output)
- Groups C, D → E (E2E exercises both the emitted SQL and the UDF reconstruction)
- Group F is independent

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Size drop | `partition_files_by_bytes` `.map(|(paths, _)| paths)` (`adapter/sharding.rs:41`) | Size must now be propagated, not discarded |
| Bare-string files assumption | `ScanSpec.files: Vec<String>` + `files_json`/`files_from_json` string forms | Replaced by `(String,u64)` tuple forms |
| Absolute-only `extract_bucket` | `scan/mod.rs:~665` `Url::parse(files[0])` | First entry may be relative; bucket must come from an absolute source |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| Pushdown resolves the file list once and builds a scan-driving query (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `pushdown_carries_table_root_and_sizes_in_common_and_shards` |
| Table root is carried once and paths under it are emitted relative (NEW) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `table_root_stripped_from_under_root_paths_and_carried_once` |
| A data-file path not under the table root is carried as an absolute path (NEW) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `path_not_under_root_stays_absolute` |
| File list is partitioned into G byte-balanced disjoint shards covering every file (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/sharding.rs` | `partition_by_bytes_propagates_size_into_shards` |
| Scan-driving query fans the SET UDF across shards via GROUP BY shard_key (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `fan_out_carries_root_once_and_path_size_tuples_per_shard` |
| Scan reconstitutes the ScanSpec from the common and per-shard arguments (CHANGED) | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `from_parts_reconstitutes_files_tuples_and_table_root` |
| A file-list argument that predates the size and relative-path encoding still reconstitutes (NEW) | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `legacy_empty_root_treats_paths_as_absolute` |
| Scan registers only its assigned files and returns matching rows (CHANGED) | Integration | `crates/lakehouse-engine/tests/e2e_scan_test.rs` | `scan_registers_assigned_files_with_path_size_payload` |
| Scan builds file metadata from the spec and issues no per-file HEAD (NEW) | Integration | `crates/lakehouse-engine/tests/scan_no_head_test.rs` | `scan_uses_spec_size_and_issues_no_head` |
| Relative paths resolve against the table root and absolute paths pass through (NEW) | Integration | `crates/lakehouse-engine/tests/scan_no_head_test.rs` | `relative_and_absolute_entries_resolve_to_same_files` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| Adapter emits root-once + `(path,size)` shards | `cargo test -p lakehouse-engine table_root_stripped_from_under_root_paths_and_carried_once -- --nocapture` | Generated SQL shows the table root exactly once in the common literal; per-shard `VALUES` literals are `[[relpath,size],...]` with no repeated prefix |
| Scan issues no per-file HEAD | `make cross-musl-udf-build && make test-e2e` | E2E passes; a multi-file scan returns correct rows with the new payload; store trace shows no per-file HEAD before scanning |
| Path not under root stays absolute | `cargo test -p lakehouse-engine path_not_under_root_stays_absolute -- --nocapture` | A file outside `metadata.location()` is emitted as a full absolute URI and reconstructs unchanged |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures (fails, not skips, without a DB) |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt` | No changes |
