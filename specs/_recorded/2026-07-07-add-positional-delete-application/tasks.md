# Tasks: add-positional-delete-application

## Phase 2: Implementation — Group A (foundation: wire format + adapter carry-through)
- [x] 1.1 Extend per-shard file entry in `scan/spec.rs` to carry positional-delete refs (path, size, content type); backward-compatible serde (legacy `(path,size)` → empty deletes)
- [x] 1.2 Adapter: stop discarding `.deletes` in `plan_files_from_table`; associate + relativize positional-delete files per data file
- [x] 1.3 Adapter: fail loud at plan time on unsupported delete mechanisms (equality, Puffin/v3 DV, ORC/Avro) at manifest/DataFile level; credential-redacted error [expert]

## Phase 2: Implementation — Group B (scan reader: custom TableProvider + delete application)
- [x] 2.1 Custom `TableProvider` over DataFusion `ParquetSource` (FileScanConfig) replacing `ListingTable` in `register_files`; preserve logical schema, `FieldIdExprAdapter`, lean single-partition plan [expert]
- [x] 2.2 Vendor/reimplement `build_deletes_row_selection` (positions + row-group counts → `RowSelection`); attribution + upstream comment (#340) [expert]
- [x] 2.3 Read positional-delete Parquet (`file_path`/`pos`), filter to data file (partition granularity), union `pos` into per-file `DeleteVector`; union multiple delete files
- [x] 2.4 Build `ParquetAccessPlan` per delete-carrying data file, attach via `PartitionedFile::with_extensions`; verify composes with pruning [expert]
- [x] 2.5 Double-footer-read mitigation: shared `ParquetFileReaderFactory` / cached metadata reader (footer parsed once); never a HEAD [expert]
- [x] 2.6 Read-time backstop: reject non-Parquet-positional delete files with clean credential-redacted error before emitting rows

## Phase 2: Implementation — Group C (fixtures: Apache Spark)
- [x] 3.1 Add Apache Spark service (Iceberg runtime, MOR) to `docker-compose.yml` + readiness wiring; fixture producing `write.delete.granularity=file` MOR table; record deleted rows
- [x] 3.2 Fixture producing `write.delete.granularity=partition` MOR table (≥2 partitions, ≥2 data files each), delete spanning multiple partitions; record deleted rows
- [x] 3.3 Upstream-tracking comment on Spark fixtures (#340 native writer) with explicit drop condition

## Phase 2: Implementation — Group D (tests)
- [x] 4.1 Scan-level no-container test `tests/scan_positional_deletes.rs`: file/partition granularity, multi-delete-file union, fully-deleted file
- [x] 4.2 Plan-shape / pruning-preservation GATE test `tests/scan_plan_shape.rs`: lean single-partition shape + pruning with base access plan [expert]
- [x] 4.3 Reconstitution / no-HEAD test updates (`tests/scan_two_arg.rs`, `tests/scan_no_head_test.rs`): delete entries reconstitute; legacy empty; no HEAD; single range GET footer
- [x] 4.4 Adapter unit tests (`adapter/pushdown.rs` `#[cfg(test)]`): deletes preserved (file/partition); path encoding; content type carried; fail-loud on equality/DV/ORC/Avro
- [x] 4.5 E2E delete matrix `tests/e2e_positional_deletes_test.rs`: per-scenario incl. multi-partition-spanning + fan-out-invariance; suite FAILS when stack unavailable (authored + compiles)
- [x] 4.6 GAP: `e2e_unsupported_delete_fails_loud` targets a nonexistent `equality_delete_tbl`; add a Spark-producible unsupported-delete fixture (format-v3 Puffin deletion vector) + align the E2E test's target table so the fail-loud path is really exercised
- [x] 4.7 GAP (live-run finding, 2026-07-06; resolved 2026-07-07): `mor_pos_file` (`write.delete.granularity=file`) committed 2 position-delete files, and `resolve_file_list` showed BOTH referencing BOTH data files. `DELETE FROM` has no hint clause in Spark's SQL grammar (confirmed against the grammar — a literal `/*+ REPARTITION(1) */` on `DELETE FROM` is a syntax error), so the fix is the DELETE-compatible equivalent: added `'write.delete.distribution-mode' = 'none'` to `mor_pos_file`'s `TBLPROPERTIES` in `create_file_granularity_fixture.sql`, disabling Iceberg's default HASH write-shuffle so the delete write follows the natural one-task-per-small-file read partitioning. Verified via live re-run: Spark's `position_deletes` metadata table now shows each of the 2 delete files containing entries for exactly ONE data file (no cross-references) — the write side is now genuinely `file`-granularity-correct. HOWEVER `fixture_spark_file_granularity_delete_table` still showed each data file resolving 2 deletes: root-caused (via vendored `iceberg-rust` 0.10.0-rc.2 source, `delete_file_index.rs`) to an UPSTREAM READ-side gap, not the fixture — `DeleteFileIndex` has an explicit unclosed TODO ("we're not yet doing that here") and applies every partition-scoped position-delete file to every data file in the partition, since it doesn't yet gate by `referenced_data_file` (tracked by open, unmerged upstream PR apache/iceberg-rust#2532, pre-work for #340). This is a resolve_file_list/read-side limitation, not a correctness bug (`positional_deletes.rs` filters applied deletes by `file_path` per data file at scan time, so the over-broad association is safe, just less pruned) — confirmed by both correctness tests (`e2e_file_granularity_returns_post_delete_rows`, `e2e_partition_granularity_returns_post_delete_rows`) staying green throughout. Adjusted `fixture_spark_file_granularity_delete_table`'s shape assertion to the achievable invariant (each data file resolves both distinct delete files; exactly 2 distinct delete files total) with an UPSTREAM TRACKING comment + DROP CONDITION (tighten back to 1-per-file once a release containing #2532 is picked up). No `pos_delete_fixtures.rs` ground-truth constants changed (ids/counts unaffected — only delete-FILE shape). Full E2E suite (11/11) green; `fmt`/`clippy` clean.

## Phase 3: Verification
- [x] 6.1 Code review of all changed files — clean, no correctness defects; 1 efficiency finding applied (per-batch downcast in delete-scan loop)
- [x] 6.2 Build (`make cross-musl-udf-build`) exit 0 — `.so` built in rust:1.94-bookworm, fingerprint matched, loaded
- [x] 6.3 Test host (`cargo test`) 0 failures — 403 lib + all no-container integration
- [x] 6.4 Lint (`cargo clippy --all-targets`) 0 warnings
- [x] 6.5 Format (`cargo fmt --check`) no changes
- [x] 6.6 E2E (`make test-e2e`) — 67 passed, 0 failed (incl. all 11 positional-delete scenarios)
- [x] 6.7 Scenario coverage audit + verification report

## Phase 4: Commit
- [ ] 5.1 Commit referencing `Closes #68` (refs #11)
