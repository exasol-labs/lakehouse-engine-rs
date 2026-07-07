# Plan: add-positional-delete-application

## Summary

Fix issue #11's silent-correctness bug for the **positional-delete** case (tracked as #68) by
applying Iceberg merge-on-read Parquet positional deletes on read — keeping DataFusion's own
`ParquetSource` as the scan engine and attaching a per-data-file base `ParquetAccessPlan` so
deletes compose with pushdown/pruning — while failing loud at plan time on every delete mechanism
this engine cannot apply (equality deletes, Puffin/v3 deletion vectors, ORC/Avro).

## Design

### Context

The VS silently ignores Iceberg merge-on-read positional deletes: `plan_files_from_table`
(`adapter/pushdown.rs:2458+`) collapses each iceberg `FileScanTask` to a bare `(path, size)` pair
and discards its `.deletes`, so any query over a MOR table returns pre-delete rows with no error.
The scan then registers those files as a DataFusion `ListingTable` (`scan/mod.rs:1057-1107`) with
the `FieldIdExprAdapterFactory`. Neither path ever sees delete files. This is a correctness bug
against the mission's "correctness and safety are first-class" constraint. The rejected broader
plan (`add-iceberg-delete-application`) fixed it by swapping in iceberg-rust's `ArrowReader`, which
loses DataFusion pushdown/pruning/streaming and re-plans files; this narrower plan keeps
`ParquetSource` and covers positional deletes only.

- **Goals** — Apply Parquet positional deletes on read for both `write.delete.granularity=file`
  and `partition`; keep DataFusion `ParquetSource` as the scan engine (projection/filter/LIMIT
  pushdown, row-group + page pruning, statistics, streaming, and the existing `FieldIdExprAdapter`
  all preserved); keep the ScanSpec wire surface minimal (per-file positional-delete refs only);
  make plan-time fail-loud the authoritative correctness gate; comprehensive full-stack coverage.
- **Non-Goals** — Equality deletes; v3 / Puffin deletion vectors; ORC or Avro data or delete
  files (all fail loud at plan time, deferred under #11); swapping in iceberg-rust's `ArrowReader`;
  a native Rust position-delete writer (blocked on iceberg-rust #340); join pushdown or any new
  query capability.

### Decision

Apply positional deletes through DataFusion 54's native access-plan seam. At plan time the adapter
preserves each data file's associated positional-delete files (path, size, content type) into the
per-shard files argument and fails loud on any unsupported delete mechanism (detected at the
manifest/`DataFile` level, where the Puffin discriminator and file format are still visible). At
scan time a custom `TableProvider` over DataFusion's `ParquetSource` (replacing `ListingTable` in
`register_files`) reads each associated positional-delete Parquet file, filters its rows to the
data file being read (required for `partition` granularity), unions the `pos` values into a
per-data-file delete set, converts that set + the data file's per-row-group row counts into a
per-row-group `RowSelection`, and attaches it as a base `ParquetAccessPlan` via
`PartitionedFile::with_extensions`. The Parquet opener reads it as the base plan and intersects
predicate/bloom/row-group/page pruning on top, so deletes compose with pushdown rather than
defeating it. `logical_schema` + `FieldIdExprAdapter` are kept exactly as-is.

#### Architecture

```
plan time (adapter, resolve-once)                 read time (scan UDF, per shard)
┌──────────────────────────────┐                  ┌──────────────────────────────────────┐
│ plan_files() → FileScanTask[] │                  │ per-file entry {path,size,deletes[]}   │
│  • KEEP .deletes (path,size,  │──wire (2 args)──▶│  → custom TableProvider over            │
│    content_type) per data file│  common: schema, │    DataFusion ParquetSource            │
│  • FAIL LOUD at manifest/     │  proj, filter,   │  → read pos-delete parquet, filter by  │
│    DataFile level on equality/│  limit, creds,   │    file_path, union pos → DeleteVector  │
│    Puffin-DV/ORC/Avro ────────┤  table root      │  → RowSelection from row-group counts  │
│  • minimal surface (no schema,│  per-shard: files│  → ParquetAccessPlan on PartitionedFile │
│    no BoundPredicate added)   │  + deletes       │    .extensions (base plan)             │
└──────────────────────────────┘                  │  → opener intersects pruning ON TOP    │
                                                   │  → Filter → Projection → Coalesce      │
                                                   │  → emit_batch (unchanged)              │
                                                   │  read-time backstop: reject non-pos    │
                                                   └──────────────────────────────────────┘
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Custom `TableProvider` over DataFusion `ParquetSource` (replaces `ListingTable`) | `scan/mod.rs` `register_files` | `ListingTable` won't let us build a `FileScanConfig` and attach per-file `ParquetAccessPlan`s; a thin custom provider over the SAME `ParquetSource` keeps all pushdown/pruning/streaming |
| Base `ParquetAccessPlan` via `PartitionedFile::with_extensions` | scan reader | DataFusion's opener reads it as the base plan and intersects pruning on top, so deletes compose with (never disable) pushdown |
| Vendored `build_deletes_row_selection` (positions + row-group meta → `RowSelection`) | scan reader | Reuse iceberg-rust's verified row-group-boundary algorithm without depending on `pub(super)` visibility; attribution + upstream-tracking comment |
| `file_path`-filter of each delete file | scan reader | REQUIRED for `partition` granularity where one delete file references many data files |
| Fail-loud at manifest/`DataFile` level (plan time) + read-time backstop | adapter + scan | Puffin DV discriminator / file format is visible at the manifest level but dropped by `plan_files`; plan-time is the authoritative gate, scan-time is cheap defense-in-depth |
| Minimal ScanSpec surface (per-file delete refs only) | `scan/spec.rs`, adapter | Keep `logical_schema` + `FieldIdExprAdapter`; do NOT add a serialized Iceberg `Schema` or `BoundPredicate` (the divergence from the rejected plan) |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Keep DataFusion `ParquetSource`; apply deletes via a per-file base `ParquetAccessPlan` | Swap in iceberg-rust `ArrowReader`/`IcebergTableScan` (the rejected plan) | `ArrowReader` loses DataFusion projection/filter/LIMIT pushdown, row-group/page pruning, statistics, and streaming, and `IcebergTableScan` re-plans files inside the scan (breaks file-level work assignment + resolve-once); the access-plan seam preserves all of it (cf. apache/iceberg-rust#2376 perf concerns) |
| Unified custom provider on all paths, gated by a plan-shape/pruning test | Conditional (`ListingTable` for delete-free, custom provider only when deletes present) | Unified is cleaner and simpler; the plan-shape/pruning-preservation test (Task 4.2) is the gate — if it shows a noticeable regression on the delete-free path, fall back to conditional |
| Positional deletes only; fail loud on everything else at plan time | Silently return pre-delete rows (current #11 bug); handle equality/DV now | Narrower, correct, shippable scope; invalid results are never returned; equality + DV remain future work under #11 |
| Plan-time detection at manifest/`DataFile` level is authoritative | Read-time-only detection on `FileScanTaskDeleteFile` | `plan_files` drops the Puffin discriminator, so a DV is indistinguishable from a Parquet positional delete at read time; reliable detection needs manifest-level access |
| Minimal wire surface: per-file delete refs (path, size, content type) only | Carry serialized iceberg `Schema` + `BoundPredicate` (the rejected plan) | DataFusion does its own pushdown from the SQL filter and the existing `logical_schema`/`FieldIdExprAdapter` already handle schema evolution; adding schema+predicate is unnecessary weight |
| Reuse iceberg-rust `build_deletes_row_selection` via a vendored copy | Depend on it directly | It is `pub(super)` in iceberg (verify during implementation); a small vendored copy with attribution + upstream-tracking comment avoids a visibility dependency |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-positional-deletes | NEW | `datafusion-scan/scan-execution-positional-deletes/spec.md` |
| datafusion-scan/scan-execution | CHANGED | `datafusion-scan/scan-execution/spec.md` |
| datafusion-scan/scan-execution-spec-reconstitution | CHANGED | `datafusion-scan/scan-execution-spec-reconstitution/spec.md` |
| datafusion-scan/scan-execution-file-metadata | CHANGED | `datafusion-scan/scan-execution-file-metadata/spec.md` |
| datafusion-scan/scan-execution-memory-and-credentials | CHANGED | `datafusion-scan/scan-execution-memory-and-credentials/spec.md` |
| vs-adapter/pushdown-file-pruning | CHANGED | `vs-adapter/pushdown-file-pruning/spec.md` |
| vs-adapter/pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| vs-adapter/pushdown-planning-file-encoding | CHANGED | `vs-adapter/pushdown-planning-file-encoding/spec.md` |
| packaging/positional-delete-fixtures | NEW | `packaging/positional-delete-fixtures/spec.md` |
| packaging/e2e-harness-positional-deletes | NEW | `packaging/e2e-harness-positional-deletes/spec.md` |

> `datafusion-scan/scan-execution-field-id-projection` is intentionally UNCHANGED — the
> `FieldIdExprAdapter` is preserved exactly as-is (the key divergence from the rejected plan).

## Dependencies

- DataFusion 54 (`ParquetAccessPlan`, `PartitionedFile::with_extensions`,
  `ParquetSource::with_parquet_file_reader_factory`), arrow/parquet 58, iceberg-rust 0.10.0-rc.2 —
  all already pinned. Verified seams: `datafusion-datasource-54.0.0` `src/mod.rs:307`
  (`with_extensions`); `datafusion-datasource-parquet-54.0.0` `src/opener/mod.rs:896` / `:1348`
  (base plan from extensions) / `:1097-1121` (pruning intersects on top) / `:2303-2323` (upstream
  test of this exact pattern); `src/access_plan.rs:228-236` (`scan_selection` intersects);
  `source.rs:386` (`with_parquet_file_reader_factory`).
- iceberg-rust `crates/iceberg/src/arrow/reader/positional_deletes.rs::build_deletes_row_selection`
  (~110 lines, `pub(super)` — verify visibility, vendor with attribution) and
  `crates/iceberg/src/delete_vector.rs` (`DeleteVector` over `RoaringTreemap`).
- New E2E stack service in `docker-compose.yml`: **Apache Spark** (Iceberg Spark runtime,
  `write.delete.mode=merge-on-read`) for positional-delete fixtures at `file` and `partition`
  granularity.
- Upstream tracking (code comments + drop conditions): apache/iceberg-rust **#340** (position
  writer), **#2681 / #2580 / #2411** (v3 deletion-vector read); apache/iceberg-rust **#2376**
  (the perf concern motivating the `ParquetSource` approach).

## Implementation Tasks

### Group A — Wire format + adapter carry-through (foundation)

- [ ] 1.1 Extend the per-shard file entry in `scan/spec.rs` (`files: Vec<(String, u64)>`) to carry
      each data file's associated positional-delete file refs (path, byte size, delete content
      type); backward-compatible serde so legacy `(path, size)` entries deserialize with an empty
      delete list (document the chosen shape — struct-per-file with untagged legacy fallback, or a
      parallel structure).
- [ ] 1.2 Adapter: stop discarding `.deletes` in `plan_files_from_table` (`adapter/pushdown.rs:2458+`);
      associate each data file's Parquet positional-delete files and relativize their paths exactly
      like data-file paths.
- [ ] 1.3 Adapter: fail loud at plan time on any unsupported delete mechanism (equality delete,
      Puffin/v3 deletion vector, ORC/Avro data or delete file), detected at the manifest/`DataFile`
      level BEFORE building scan-driving SQL; clean credential-redacted error. [expert]

### Group B — Scan reader (custom TableProvider + delete application)

- [ ] 2.1 Implement a custom `TableProvider` over DataFusion's `ParquetSource` (build a
      `FileScanConfig` directly) replacing the `ListingTable` in `register_files`
      (`scan/mod.rs:1057-1107`); preserve the logical schema, the `FieldIdExprAdapter`, and the lean
      single-partition plan (one output partition, no repartition/coalesce). [expert]
- [ ] 2.2 Vendor/reimplement `build_deletes_row_selection` (deleted positions + per-row-group row
      counts → per-row-group `RowSelection`, handling row-group boundaries and skipped row groups),
      consuming a `DeleteVector` (`RoaringTreemap`) iterated ascending; attribution +
      upstream-tracking comment (#340). [expert]
- [ ] 2.3 Read each associated positional-delete Parquet file (columns `file_path`/`pos`, field-ids
      2147483546 / 2147483545), filter rows to the data file being read (required for `partition`
      granularity), and union `pos` values into a per-data-file `DeleteVector`; union multiple
      delete files.
- [ ] 2.4 Build a `ParquetAccessPlan` per delete-carrying data file from its row-group metadata +
      the `RowSelection` and attach via `PartitionedFile::with_extensions`; verify it composes with
      predicate/row-group/page pruning (the opener intersects on top). [expert]
- [ ] 2.5 Double-footer-read mitigation: install a shared `ParquetFileReaderFactory` / cached
      metadata reader so each delete-carrying data file's footer parses once for both access-plan
      construction and the opener (preferred), or accept one extra footer range GET — never a HEAD.
      [expert]
- [ ] 2.6 Read-time backstop: reject any assigned delete file that is not a Parquet positional
      delete (Puffin/DV/equality/unknown content type) with a clean, credential-redacted error
      before emitting any row for the affected data file.

### Group C — Fixtures (Apache Spark)

- [ ] 3.1 Add an Apache Spark service (Iceberg Spark runtime, `write.delete.mode=merge-on-read`) to
      `docker-compose.yml` with readiness/wait wiring; add a fixture step producing a
      `write.delete.granularity=file` MOR table against the shared REST catalog + MinIO, recording
      the deleted rows.
- [ ] 3.2 Add a fixture step producing a `write.delete.granularity=partition` MOR table laid out
      across at least two partitions each holding at least two data files, issuing a `DELETE`/`MERGE`
      whose committed positional-delete file(s) reference data files spanning multiple partitions
      (not just multiple files within one partition); record the deleted rows.
- [ ] 3.3 Add an upstream-tracking comment on the Spark fixtures (native position-delete writer
      #340) with the explicit drop condition.

### Group D — Tests

- [ ] 4.1 Scan-level no-container test (`tests/scan_positional_deletes.rs`): write a data Parquet +
      a positional-delete Parquet locally, hand-build the files spec, drive the scan, assert deleted
      rows are gone — covering `file` granularity, `partition` granularity (file_path filtering),
      multi-delete-file union, and a fully-deleted file.
- [ ] 4.2 Plan-shape / pruning-preservation GATE test (`tests/scan_plan_shape.rs`): assert the
      raw-scan physical plan keeps the lean single-partition shape (no repartition/coalesce) AND
      that row-group/predicate pruning still occurs with a base `ParquetAccessPlan` attached — this
      is the gate for the unified-vs-conditional provider decision (Task 2.1). [expert]
- [ ] 4.3 Reconstitution / no-HEAD test updates (`tests/scan_two_arg.rs`, `tests/scan_no_head_test.rs`):
      delete entries reconstitute; legacy entries reconstitute with empty deletes; no HEAD for data
      or delete files; footer read via a single range GET.
- [ ] 4.4 Adapter unit tests (`adapter/pushdown.rs` `#[cfg(test)]`): positional deletes preserved
      into the scan spec (`file` and `partition` granularity); delete-file path relative/absolute
      encoding; delete content type carried; fail-loud on equality/DV/ORC/Avro at plan time.
- [ ] 4.5 E2E delete matrix (`tests/e2e_positional_deletes_test.rs`): one test per
      `packaging/e2e-harness-positional-deletes` scenario — including the multi-partition-spanning
      post-delete correctness test and a fan-out-invariance test that deterministically forces both
      the same-shard and different-shard placements of the affected data files by controlling the
      shard count / parallelism factor (not relying on hash luck) and asserts identical post-delete
      results; suite FAILS (not skips) when the Exasol/Spark stack is unavailable.

### Group E — Commit

- [ ] 5.1 Commit referencing `Closes #68` (refs #11) per CLAUDE.md.

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A (foundation) | 1.1, 1.2, 1.3 |
| Group B (scan reader) | 2.1, 2.2, 2.3, 2.4, 2.5, 2.6 |
| Group C (fixtures) | 3.1, 3.2, 3.3 |
| Group D (tests) | 4.1, 4.2, 4.3, 4.4, 4.5 |

Sequential dependencies:
- Group A → Group B (reader needs the wire types) and → Group C/D fixtures/tests that assert carry-through
- Group A, B, C → Group D (tests exercise the built reader/adapter/fixtures)
- Group C runs concurrently with Group B
- Group D → Task 5.1 (commit)
- Within Group B: 2.1 precedes 2.4/2.5 (access plan attaches to the provider's `FileScanConfig`); 2.2/2.3 feed 2.4

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Code path | `scan/mod.rs:1057-1107` (`register_files` `ListingTable` construction) | Replaced by the custom `ParquetSource`-backed `TableProvider` (unified path). Retain the `ListingTable` path ONLY if Task 4.2 forces the conditional fallback. |

> No other removals: `FieldIdExprAdapter`/`FieldIdExprAdapterFactory` and the no-HEAD size machinery
> are PRESERVED (the divergence from the rejected plan, which deleted them).

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| positional-deletes: file-granularity removes flagged rows | Integration | `tests/scan_positional_deletes.rs` | `scan_applies_file_granularity_positional_deletes` |
| positional-deletes: partition-granularity filtered by file_path | Integration | `tests/scan_positional_deletes.rs` | `scan_filters_partition_delete_by_file_path` |
| positional-deletes: multiple delete files unioned | Integration | `tests/scan_positional_deletes.rs` | `scan_unions_multiple_delete_files` |
| positional-deletes: fully deleted file yields no rows | Integration | `tests/scan_positional_deletes.rs` | `scan_fully_deleted_file_yields_no_rows` |
| positional-deletes: composes with projection/filter/LIMIT/pruning | Integration | `tests/scan_positional_deletes.rs` | `scan_deletes_compose_with_pushdown_and_pruning` |
| positional-deletes: unapplicable delete clean error (backstop) | Integration | `tests/scan_positional_deletes.rs` | `scan_rejects_unapplicable_delete_file` |
| positional-deletes: delete-free file scans unchanged | Integration | `tests/scan_positional_deletes.rs` | `scan_delete_free_file_unchanged` |
| scan-execution: registers only assigned files via ParquetSource provider | Integration | `tests/scan_two_arg.rs` | `scan_registers_assigned_files_via_parquet_provider` |
| scan-execution: raw plan no repartition/coalesce + pruning preserved | Integration | `tests/scan_plan_shape.rs` | `raw_plan_lean_and_prunes_with_access_plan` |
| reconstitution: reconstitutes ScanSpec with delete entries | Integration | `tests/scan_two_arg.rs` | `spec_reconstitutes_with_delete_entries` |
| reconstitution: legacy no-delete entry reconstitutes empty | Unit | `crates/lakehouse-engine/src/scan/spec.rs` (`#[cfg(test)]`) | `legacy_file_entry_reconstitutes_empty_deletes` |
| file-metadata: no HEAD for delete files | Integration | `tests/scan_no_head_test.rs` | `scan_issues_no_head_for_delete_files` |
| file-metadata: footer via range GET, parsed once | Integration | `tests/scan_no_head_test.rs` | `scan_reads_footer_via_range_get_once` |
| file-metadata: delete-file relative/absolute path resolution | Unit | `crates/lakehouse-engine/src/scan/mod.rs` (`#[cfg(test)]`) | `delete_file_paths_resolve_relative_and_absolute` |
| memory-creds: delete files read with vended credentials | Integration | `tests/scan_positional_deletes.rs` | `scan_reads_delete_files_with_vended_credentials` |
| memory-creds: shared metadata reader avoids duplicate footer parse | Unit | `crates/lakehouse-engine/src/scan/mod.rs` (`#[cfg(test)]`) | `scan_installs_shared_parquet_metadata_reader` |
| file-pruning: positional deletes preserved into scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (`#[cfg(test)]`) | `adapter_preserves_positional_deletes_into_scan_spec` |
| file-pruning: unsupported delete fails loud at plan time | Integration | `tests/e2e_positional_deletes_test.rs` | `e2e_unsupported_delete_fails_loud` |
| pushdown-planning: delete refs carried in per-shard argument | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (`#[cfg(test)]`) | `adapter_carries_delete_refs_per_shard_minimal_common_spec` |
| file-encoding: delete-file paths relative/absolute | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (`#[cfg(test)]`) | `delete_file_paths_use_relative_absolute_encoding` |
| file-encoding: delete-file entry carries content type | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` (`#[cfg(test)]`) | `delete_file_entry_carries_content_type` |
| fixtures: Spark file-granularity fixture | Integration | `tests/e2e_positional_deletes_test.rs` | `fixture_spark_file_granularity_delete_table` |
| fixtures: Spark partition-granularity fixture | Integration | `tests/e2e_positional_deletes_test.rs` | `fixture_spark_partition_granularity_delete_table` |
| e2e: file-granularity returns post-delete rows | Integration | `tests/e2e_positional_deletes_test.rs` | `e2e_file_granularity_returns_post_delete_rows` |
| e2e: partition-granularity returns post-delete rows | Integration | `tests/e2e_positional_deletes_test.rs` | `e2e_partition_granularity_returns_post_delete_rows` |
| e2e: multi-partition-spanning delete returns exact post-delete set | Integration | `tests/e2e_positional_deletes_test.rs` | `e2e_partition_delete_spans_multiple_partitions` |
| e2e: post-delete result invariant across fan-out placement | Integration | `tests/e2e_positional_deletes_test.rs` | `e2e_partition_delete_invariant_across_fanout` |
| e2e: deletes × projection/filter/LIMIT | Integration | `tests/e2e_positional_deletes_test.rs` | `e2e_deletes_with_projection_filter_limit` |
| e2e: deletes × aggregation | Integration | `tests/e2e_positional_deletes_test.rs` | `e2e_deletes_with_single_and_grouped_agg` |
| e2e: unsupported delete fails loud | Integration | `tests/e2e_positional_deletes_test.rs` | `e2e_unsupported_delete_fails_loud` |
| e2e: delete-free non-regression | Integration | `tests/e2e_positional_deletes_test.rs` | `e2e_delete_free_table_no_regression` |
| e2e/fixtures: suite fails when stack unavailable | Integration | `tests/e2e_positional_deletes_test.rs` | `positional_delete_suite_fails_when_stack_unavailable` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-execution-positional-deletes (file) | `SELECT COUNT(*) FROM <vs>.mor_pos_file;` | Post-delete row count (fewer than the pre-delete count) |
| scan-execution-positional-deletes (partition) | `SELECT * FROM <vs>.mor_pos_partition WHERE <p>;` | Only non-deleted rows across the partition's data files |
| packaging/e2e-harness-positional-deletes (fan-out invariance) | Run the multi-partition delete query with a low then a high `parallelism_factor` (forcing same-shard then split-shard placement) | Identical post-delete row set in both runs |
| vs-adapter/pushdown-file-pruning (fail-loud) | `SELECT * FROM <vs>.equality_delete_tbl;` | Clean plan-time error naming the unsupported delete mechanism; no rows |
| vs-adapter/pushdown-planning | `EXPLAIN VIRTUAL SELECT * FROM <vs>.mor_pos_file;` | Scan-driving SQL whose per-shard arg carries the positional-delete file refs |
| packaging/e2e-harness-positional-deletes | `make test-e2e` | Positional-delete matrix passes; suite FAILS (not skips) when stack down |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 (`.so` built in `rust:1.94-bookworm`) |
| Test (host) | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures; positional-delete matrix green |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | No changes |
