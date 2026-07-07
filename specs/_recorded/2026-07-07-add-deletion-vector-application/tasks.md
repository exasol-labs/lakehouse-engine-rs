# Tasks: add-deletion-vector-application

Dependencies: Groups A, B, C are independent (may run concurrently). Group D depends on A + B + C.
Group E depends on D.

## Phase 2: Implementation (Group A — Wire format: normalized interned per-shard structure)
- [x] 2.A.1 Replace the per-shard files wire (`src/scan/spec.rs`) with the normalized `{deleteFiles, dataFiles}` object; retire the untagged `FileEntryWire` 2/3-tuple serde, the flat `DeleteFileRef`, and `DeleteFileContentType` [expert]
- [x] 2.A.2 `spec.rs` unit tests for the new shape (pool round-trip, interned dedup, DV ref with offset/length, mixed shard, single data file with both POS_DEL+DV, no-deletes compact form)
- [x] 2.A.3 Migrate `JoinSpec.files` (shard-invariant dimension side, arg 0) off `Vec<FileEntry>` onto the same normalized file-set shape; carry `join` through to_common/from_parts/sample_spec; extend join round-trip test for a dimension file carrying a positional delete [expert]

## Phase 2: Implementation (Group B — deletion-vector-v1 decoder)
- [x] 2.B.1 Re-fetch `format/puffin-spec.md` deletion-vector-v1 and cross-check against a real Spark-produced DV file's raw bytes (endianness, CRC polynomial vs crc32fast, no Puffin compression) [expert]
- [x] 2.B.2 Implement the `deletion-vector-v1` blob decoder in new `src/scan/deletion_vectors.rs` (BE length, magic, portable Roaring, RoaringTreemap, BE CRC-32, cardinality validation, fail-loud redacted errors) [expert]
- [x] 2.B.3 Decoder unit tests against known-good byte sequences (single-key, multi-key >2^32, empty, cardinality mismatch, corrupt magic, corrupt CRC) [expert]

## Phase 2: Implementation (Group C — Adapter DV-reference extraction)
- [x] 2.C.1 Relax `classify_manifest_file` (`src/adapter/pushdown.rs`) so PositionDeletes+Puffin returns Ok; keep equality/ORC/Avro rejected; drop the now-unreachable `DeletionVector` arm
- [x] 2.C.2 Turn the manifest/DataFile walk into a DV-reference producer (intern Puffin container once, add df-indexed deletes ref carrying offset/length, do NOT serialize referenced_data_file; union with positional-delete refs) [expert]
- [x] 2.C.3 `pushdown.rs` unit tests (classify accepts Puffin position deletes, rejects equality/ORC/Avro; DV-ref extraction populates correct data file with correct offset/length)

## Phase 2: Implementation (Group D — Scan-side DV application; depends on A, B, C)
- [x] 2.D.1 Add the DV branch to the read-time backstop `ensure_positional_delete` (`src/scan/positional_deletes.rs`); keep equality/ORC/Avro/unknown rejected; update rejection unit test
- [x] 2.D.2 In `access_plan_for_data_file`, resolve each deletes ref's df into the pool and dispatch on type: POS_DEL → existing union; DV → open Puffin, fetch blob at offset/length, decode (cross-check referenced-data-file), union into the same per-data-file RoaringTreemap before unchanged build_access_plan [expert]
- [x] 2.D.3 Puffin file open + blob fetch plumbing (build InputFile from pooled DV entry path + object store, no-HEAD size, select BlobMetadata at offset/length, credential-redacted errors)
- [x] 2.D.4 Integration tests `tests/scan_deletion_vectors.rs` (DV removes flagged rows, fully-deleted file empty, composes with projection/filter/LIMIT, mixed positional+DV shard resolves per file)

## Phase 2: Implementation (Group E — Fixtures + E2E; depends on D)
- [x] 2.E.1 Repurpose `scripts/spark-fixtures/create_deletion_vector_fixture.sql` into a positive DV fixture (10 rows, deleted id IN (3,7)); add a new mixed-mechanism fixture SQL
- [x] 2.E.2 Update `tests/common/pos_delete_fixtures.rs` (post-delete ground truth for mor_dv, consider rename off _unsupported; constants for the mixed fixture, in lockstep with SQL)
- [x] 2.E.3 New `tests/e2e_deletion_vectors_test.rs` (DV post-delete rows; composes with projection/filter/LIMIT; composes with single-group + grouped agg; mixed combined set; mixed invariant across fan-out; fixture-produced + stack-unavailable guards)
- [x] 2.E.4 Narrow `tests/e2e_positional_deletes_test.rs::e2e_unsupported_delete_fails_loud` to target a still-unsupported mechanism (equality or ORC/Avro), NOT the DV table

## Phase 2b: Code-review fixes
- [x] R.1 [expert] Fail loud in `attach_deletion_vectors` (pushdown.rs) when a collected DV ref matches no NON-pruned data file (silent-drop safety; refs to pruned files still dropped silently)
- [x] R.2 [expert] DV read path: reuse the opened Puffin container/`FileMetadata` across data files within a shard (avoid N re-opens for one shared container) and thread the pooled `size` to avoid a HEAD if iceberg's InputFile API allows; otherwise document the single HEAD
- [x] R.3 Update stale `unsupported_delete_error` text (pushdown.rs) — DVs are now supported
- [x] R.4 DRY the duplicated `redact` helper (positional_deletes.rs + puffin.rs) into one shared location
- [x] R.5 Rename the stale test name referencing the retired `content_type` field (pushdown.rs)
- [x] R.6 [expert] Fix E2E-caught integration bug: iceberg-rust 0.10 DOES surface the v3 Puffin DV file in `FileScanTask.deletes`, so `plan_files_from_table` produced a duplicate bogus POS_DEL (Parquet-typed) ref that opened the Puffin container as Parquet ("Corrupt footer"). Exclude DV Puffin container paths from the positional refs via new pure helper `positional_delete_refs`; the manifest walk stays authoritative for DVs (pushdown.rs)

## Phase 3: Verification
- [x] 3.1 Code review of all changed files (done — findings → Phase 2b, all fixed)
- [x] 3.2 Build (`make cross-musl-udf-build`) → exit 0 (release .so at v0.25.0)
- [x] 3.3 Test (`cargo test`, non-E2E) → 0 failures (457 lib + all integration; E2E compiles under feature)
- [x] 3.4 Lint (`cargo clippy --all-targets`) → 0 errors/warnings
- [x] 3.5 Format (`cargo fmt --check`) → no changes
- [x] 3.6 E2E (`make test-e2e`) → 83 passed, 0 failed (capability 8, count_distinct 6, deletion_vectors 9, join 6, positional_deletes 11, scan 43); R.6 fix confirmed, no regression
- [x] 3.7 Scenario coverage audit + verification report
