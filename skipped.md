# Skipped `/simplify` findings — add-direct-storage-hive-partitioning

Context: `/simplify` pass (2026-09-23/24) over the Hive-partitioning feature on branch
`feat/add-direct-storage-hive-partitioning` (feature range `4f7e861..HEAD` before the cleanup
commit). The cleanup commit applied most findings; these were deliberately skipped because they
change specified behavior, need a spec decision, or sit outside the feature diff.

## 1. Fold all files' footers so the schema no longer depends on the filter (biggest win)

- **Where:** `crates/lakehouse-engine/src/adapter/parquet_directory.rs` (`resolve_parquet_directory`,
  `MergeMode::FoldEveryFile` path), `pushdown/format/parquet_format_reader.rs`
  (`declared_columns`, `absent_declared_fields`), `pushdown/format/mod.rs`
  (`ScanSource::DirectParquet { declared_columns }`), `pushdown/scan_resolution.rs`
  (`TableScanResolver::resolve(.., declared_columns)`), `pushdown/joins/mod.rs` (`side_columns`,
  `resolve_one_join_side`), `pushdown/joins/planning.rs`, `pushdown/mod.rs`.
- **Problem:** under `FoldEveryFile` the schema is folded over the *kept* (post-prune) files only,
  so a WHERE clause can drop columns or narrow a widened type vs. what enumeration advertised. The
  patch threads Exasol-declared columns down four shared signatures and re-adds missing columns as
  NULL, typed from Exasol metadata (`exasol_type_to_arrow(..).unwrap_or(Utf8)`), not from files.
  It contradicts the seam's own promise that enumeration and planning can't disagree on columns.
- **Deeper fix:** let `keep` narrow only the returned `files`, never the folded footer set (fold
  over all listed files in both modes, as `SampleOneFile` already does). Deletes
  `declared_columns` plumbing, `ScanSource::DirectParquet`'s field, the `resolve` parameter,
  `side_columns`, and `absent_declared_fields`.
- **Why skipped:** the spec (`specs/vs-adapter/direct-storage-hive-partitioning/spec.md`,
  `specs/vs-adapter/parquet-directory-seam/spec.md`) requires that pruned files' footers are not
  read. Trade-off: extra footer GETs for pruned files on pushdown (scan still skips them).
- **Fallback if the I/O is unacceptable:** make "logical_schema covers every declared column" a
  `ResolvedScan` invariant enforced once in `TableScanResolver::resolve` for every format arm,
  instead of a Parquet-reader-private patch.

## 2. Drop `ParquetFile.footer` until a consumer exists

- **Where:** `parquet_directory.rs` (`ParquetFile.footer`), footer-presence asserts in
  `parquet_directory_tests.rs`.
- **Problem:** written, never read in production.
- **Why skipped:** `specs/vs-adapter/parquet-directory-seam/spec.md` keeps it for future
  statistics pruning (tracked issue cited there). Spec-level decision. ~60 lines.

## 3. Positional partition values + predicate column binding

- **Where:** `parquet_directory.rs` `fill_partition_values` (a `BTreeMap` per listed file, key
  names cloned per file, even for pruned files); `pushdown/format/partition_predicate.rs`
  `partition_value` (linear, uppercasing key match per node per file).
- **Cheaper form:** store per-file values as `Vec<Option<String>>` indexed like
  `partition_columns` (key names shared via `Arc<[String]>`), and bind each predicate node's
  column to a key index once before the per-file loop.
- **Why skipped:** `FileEntry.partition_values` is the format-neutral `BTreeMap` shared with
  Delta; key counts are tiny. Worth it only if planning over very large file counts shows up in a
  profile.

## 4. `&move |values|` in `parquet_format_reader.rs`

- `move` can't be dropped: `PartitionKeepPredicate` is `dyn Fn(..) + Send + Sync`, so `&dyn`
  defaults to `'static` (E0373 without `move`). Removing it needs a lifetime on the public alias
  (`PartitionKeepPredicate<'a> = dyn Fn(..) + Send + Sync + 'a`). Cosmetic; skipped.

## 5. Parallel fixture uploads in E2E

- **Where:** `crates/lakehouse-engine/tests/common/raw_parquet.rs` `write_parquet_fixture` and
  `write_all_fixtures` in `tests/e2e_direct_storage_test.rs`.
- Each PUT builds its own S3 client + current-thread runtime, sequentially (~14 more PUTs added by
  the feature). Cheaper: one store + runtime, `try_join_all`. Skipped: pre-existing pattern
  outside the feature diff.

## 6. Out-of-diff reuse follow-ups

- `adapter/iceberg_predicate.rs` still has its own comparison/operand walking (`flip_operator`,
  `extract_column`, `resolve_column`); could lower onto the new
  `pushdown/format/filter_json.rs`. Note column matching semantics differ per backend (Delta
  `eq_ignore_ascii_case`, partition pruner Unicode uppercase, Iceberg `resolve_column`).
- `delta_predicate_tests.rs` / `iceberg_predicate_tests.rs` keep their own `col`/`str_lit`
  builders; could use `pushdown/test_support_tests.rs` `filter_json` builders.

## Not yet verified live

The cleanup commit passed host `cargo test --lib` (1337), clippy (incl. `exasol-e2e`) and fmt, but
was **not** run against Docker Exasol. Before merging, `make cross-udf-build` + direct-storage E2E,
and check:
- a request with several errors may now report a different one first (missing-segment check moved
  into `fold_schemas`' field loop); error texts unchanged;
- negative CREATE cases now also run `grant_connection_access_to_vs_owner`;
- `collision_override` fixture moved under `BASE_DIRECT`, so `VS_DIRECT_NARROW`
  (MERGE_SCHEMA=FALSE) now enumerates it too;
- merged pruning test in `parquet_format_reader_tests.rs` keeps the 2026 file readable, so it no
  longer proves that pruned file's footer is unread (the other two unreadable files still catch
  read-every-footer code).
