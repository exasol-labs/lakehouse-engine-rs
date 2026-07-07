# Plan: add-deletion-vector-application

## Summary

Apply Iceberg v3 **deletion vectors** (Roaring-bitmap `deletion-vector-v1` Puffin blobs, one per
data file) on read by decoding the blob ourselves and feeding the decoded positions into the exact
`RowSelection`/`ParquetAccessPlan` machinery the positional-delete path already uses, closing the
deletion-vector half of the issue #11 silent-correctness bug (issue #12) so Databricks-UniForm v3
tables return correct post-delete rows instead of silently pre-delete rows.

> **Precondition / blocker:** this plan is a followup to PR #72 (branch
> `feat/positional-delete-application`, closes #68) and depends on it being merged to `main` first.
> Every seam this plan extends — the read-time backstop, the manifest-level gate, the
> `DeleteFileRef` wire type, the shared per-data-file union point, and the `mor_dv_unsupported`
> fixture — lands in #72. Do not start implementation until #72 is on `main`. **(Satisfied: #72 is
> on `main` as of commit 7eccd75.)**
>
> **Reconciliation with the join-pushdown feature (#71, merged after this plan was authored):** `main`
> now also carries the broadcast inner-join pushdown feature, which added a shard-invariant
> `JoinSpec` block to `CommonScanSpec` (arg 0) whose dimension side is `JoinSpec.files:
> Vec<FileEntry>` and MAY carry its own positional-delete `DeleteFileRef`s. `JoinSpec.files` is
> therefore a SECOND consumer of the exact `FileEntry` / `DeleteFileRef` / `DeleteFileContentType` /
> `FileEntryWire` types this plan retires. The wire rewrite (Group A) MUST migrate the join
> dimension side to the same normalized file-set shape (see decision [9] / task 1.3) — retiring the
> tuple types without touching `JoinSpec` would not compile. The delete-classification code this
> plan relaxes (`classify_manifest_file`, `ensure_supported_delete_mechanisms`) was NOT touched by
> the join feature, so Groups B–E apply unchanged.

## Design

### Context

PR #72 applies Parquet positional deletes and explicitly fails loud on every other mechanism,
including v3 Puffin deletion vectors (DVs). DVs are not a niche case: Databricks UniForm exposes
Delta tables as Iceberg and steers managed Iceberg toward v3, where deletes are DVs, so a DV-blind
reader silently returns pre-delete (wrong) rows on exactly the mission's Databricks target. iceberg-
rust 0.10 reads the Puffin file container (footer parse + blob decompression via `PuffinReader`) but
does NOT decode the `deletion-vector-v1` payload, and its `plan_files`/`FileScanTask.deletes` does
not surface DV files at all — so both the decode and the DV-reference discovery must be done here.

- **Goals** — decode the `deletion-vector-v1` blob into per-data-file deleted positions; reuse the
  existing `RowSelection`/`ParquetAccessPlan` union point so DVs compose with pushdown identically
  to positional deletes; source DV references from the manifest-level walk; validate the DV against
  its declared cardinality, magic, and CRC and fail loud on any mismatch; support mixed shards where
  some data files use positional deletes and others use DVs.
- **Non-Goals** — equality deletes (still rejected, tracked under #11); ORC/Avro data or delete
  files (still rejected); any non-`deletion-vector-v1` Puffin blob type; bumping iceberg-rust off
  `v0.10.0-rc.2`; writing DVs (fixtures are Spark-produced); tracking an unmerged upstream
  iceberg-rust DV branch.

### Decision

Hand-roll a `deletion-vector-v1` decoder on top of iceberg-rust's `PuffinReader`, producing a
`RoaringTreemap` of positions that feeds the SAME `access_plan_for_data_file` union point the
positional-delete path uses. Replace the per-shard files wire (arg 1) with a normalized, interned
object shape: a `deleteFiles` pool that interns each physical delete file/container EXACTLY ONCE per
shard (`path`, `size`, `type` = `POS_DEL`/`EQ_DEL`/`DV`, `format` = `PARQUET`/`AVRO`/`ORC`/`PUFFIN`)
and a `dataFiles` list whose entries carry `df`-indexed `deletes` references (each an integer index
into the pool plus OPTIONAL `offset`/`length`, present only for a blob-addressed DV inside a Puffin
container). The association between a delete file and a data file is structural — it lives on the
data file's `deletes` list — so `referenced_data_file` is DROPPED from the wire entirely; the DV
decoder re-derives and cross-checks it at read time from the Puffin `BlobMetadata`, so no
correctness is lost. This is a clean break from the legacy `FileEntryWire` tuple serde (there is no
cross-version wire-compat requirement — the same `.so` produces and consumes the spec). Source DV
references from the existing manifest / `DataFile`-level walk (`ensure_supported_delete_mechanisms`)
— the only place the DV discriminator and DV coordinates survive — rather than from
`FileScanTask.deletes`.

#### Architecture

```
Adapter (plan time, resolve ONCE)
  manifest/DataFile walk ── classify: Parquet pos-delete OK, Puffin pos-delete = DV now OK,
       │                              equality/ORC/Avro still FAIL LOUD
       │  extract DV coordinates: content_offset, content_size_in_bytes
       │  (+ referenced_data_file — used to ASSOCIATE, NOT serialized)
       ▼
  per-shard wire (arg 1) = {
    deleteFiles: [ {path,size,type,format} ],                 ← each physical delete file interned ONCE
    dataFiles:   [ {path,size,deletes:[{df,offset?,length?}]} ]  ← deletes.df indexes deleteFiles;
  }                                                              offset/length present only for a DV blob
       ▼
Scan UDF (per shard)  access_plan_for_data_file(entry)
       │  for each deletes ref: resolve df → deleteFiles[df]
       ├─ deleteFiles[df].type == POS_DEL → union_delete_positions (existing)
       └─ deleteFiles[df].type == DV →
              PuffinReader::new(pooled file) → .blob(meta @ offset,length) → Blob::data()
              → decode deletion-vector-v1: BE length | magic D1 D3 39 64 | portable roaring | BE CRC-32
              → validate magic + CRC + cardinality + cross-check blob referenced-data-file
                (from Puffin BlobMetadata) → RoaringTreemap
                                    │
       both mechanisms union into ONE per-data-file RoaringTreemap
                                    ▼
       build_deletes_row_selection → build_access_plan (UNCHANGED) → ParquetAccessPlan
                                    ▼
       DataFusion ParquetSource opener intersects predicate/row-group/page pruning ON TOP
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Decode DV blob ourselves on `Blob::data()` bytes | new `src/scan/deletion_vectors.rs` | iceberg-rust reads the Puffin container but has no `deletion-vector-v1` decode; version is pinned |
| Reuse `RoaringTreemap` → `RowSelection` → `ParquetAccessPlan` verbatim | `access_plan_for_data_file` | A DV-derived selection is indistinguishable downstream from a positional-delete one; DVs compose with pushdown for free |
| Normalized interned per-shard wire: a `deleteFiles` pool + `df`-indexed `deletes` refs | `src/scan/spec.rs` | Interning each physical delete file/container once per shard is compact (path stored once even when 10k data files share one Puffin), consistent across mechanisms, and extensible via `type`+`format` |
| DV refs from the manifest walk build the interned pool + `df` refs, not `FileScanTask.deletes` | `ensure_supported_delete_mechanisms` → producer | `plan_files` drops the Puffin discriminator and DV coordinates; the manifest walk is the only place they survive. It sources the DV coordinates but does NOT serialize `referenced_data_file` — association is structural on the data file's `deletes` list |
| Per-data-file mechanism dispatch on the pooled delete file's `type` (resolved via `df`) | `access_plan_for_data_file` loop | v3 spec: at most one DV per data file, but a shard may mix DV-backed and pos-delete-backed files (Q2) |
| Fail loud on cardinality / magic / CRC mismatch | decoder + validation | Consistent with the fail-loud posture; a mismatch signals corruption or a parser bug — the exact silent-correctness failure this line closes (Q3) |
| `roaring` crate `RoaringBitmap::deserialize_from` (portable) + `crc32fast` | decoder | Both already in the lockfile; no new dependency |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Hand-roll the `deletion-vector-v1` decoder | Track/vendor apache/iceberg-rust #2681 DV read branch | Upstream is open/unmerged and could change shape; the project pins iceberg-rust by release tag; #12 names our own decoder as the fallback (Q1) |
| Normalized interned per-shard wire: a `deleteFiles` pool + `dataFiles` with `df`-indexed `deletes` (optional `offset`/`length`), dropping `referenced_data_file`; clean break from the `FileEntryWire` tuple serde | Grow the flat `DeleteFileRef` with optional DV coordinate fields keeping the untagged tuple serde; a separate `DeletionVectorRef` type; placing the pool in the shard-invariant common blob (arg 0) | The flat approach repeats each delete file's path per referencing data file (catastrophic when 10,000 data files share one packed Puffin DV container) and is inconsistent (partition pos-deletes duplicate a ref while DVs back-reference); interning each physical delete file once per shard is compact, uniform across mechanisms, and extensible via `type`+`format`. No cross-version wire-compat requirement (the same `.so` produces and consumes the spec), so the tuple serde is retired outright. Per-shard pool scope keeps each shard self-contained (see decision-log) |
| Validate decoded count against `cardinality`, fail loud on mismatch | Trust the bitmap; log-and-continue | Silent misapplication is the failure mode #11→#12 exists to close; a mismatch means corruption or parser bug (Q3) |
| Source DV refs from the manifest walk | Wait for iceberg-rust to surface DVs via `plan_files` | iceberg-rust has no DV scan-planning; the manifest walk already runs for the gate and is the only place DV fields are visible |
| DV support as new sibling features + narrowing deltas | Fold everything into the positional-delete features | Mirrors how #11 was narrowed into standalone positional-delete features; keeps each feature single-purpose while the shared union point is reused, not duplicated |
| Mixed-mechanism in scope with a dedicated fixture | DV-only and positional-only tables in isolation | Realistic v2→v3 migration keeps old data files under pos-deletes while new deletes are DVs; per-file resolution must be proven, not assumed (Q2) |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| datafusion-scan/scan-execution-deletion-vectors | NEW | `datafusion-scan/scan-execution-deletion-vectors/spec.md` |
| packaging/deletion-vector-fixtures | NEW | `packaging/deletion-vector-fixtures/spec.md` |
| packaging/e2e-harness-deletion-vectors | NEW | `packaging/e2e-harness-deletion-vectors/spec.md` |
| vs-adapter/pushdown-file-pruning | CHANGED | `vs-adapter/pushdown-file-pruning/spec.md` |
| datafusion-scan/scan-execution-spec-reconstitution | CHANGED | `datafusion-scan/scan-execution-spec-reconstitution/spec.md` |
| datafusion-scan/scan-execution-positional-deletes | CHANGED | `datafusion-scan/scan-execution-positional-deletes/spec.md` |
| packaging/e2e-harness-positional-deletes | CHANGED | `packaging/e2e-harness-positional-deletes/spec.md` |

## Dependencies

- **PR #72 merged to `main`** (hard precondition — see Summary).
- `roaring` 0.11 (workspace dep, already consumed by `positional_deletes.rs`) — `RoaringBitmap::deserialize_from` for the portable 32-bit bitmaps.
- `crc32fast` (already in `Cargo.lock`) — CRC-32 validation. Confirm during 2.1 that its polynomial (CRC-32/ISO-HDLC, zlib) matches the Iceberg/Delta DV CRC before relying on it; if it does not, add a small explicit CRC-32 rather than mis-validating.
- iceberg-rust `v0.10.0-rc.2` `PuffinReader`/`FileMetadata`/`BlobMetadata`/`Blob` (already vendored) — Puffin container read, footer parse, blob decompression. No version bump.

## Implementation Tasks

### Group A — Wire format (normalized interned per-shard structure)

- [ ] 1.1 Replace the per-shard files wire (`src/scan/spec.rs`) with the normalized object structure: a `deleteFiles` pool (`path`, `size`, `type` = `POS_DEL`/`EQ_DEL`/`DV`, `format` = `PARQUET`/`AVRO`/`ORC`/`PUFFIN`, serde `rename_all = "SCREAMING_SNAKE_CASE"`) that interns each physical delete file/container ONCE per shard, and `dataFiles` entries (`path`, `size`, optional `deletes`) whose `deletes` refs carry `df` (index into `deleteFiles`) plus OPTIONAL `offset`/`length` (present only for a blob-addressed DV). The per-shard arg (arg 1) is now a JSON OBJECT `{deleteFiles, dataFiles}`, not a bare array; retire the untagged `FileEntryWire` 2-tuple/3-tuple serde and the flat `DeleteFileRef` (path/size/content_type) shape entirely [expert]
- [ ] 1.2 `spec.rs` unit tests for the new shape: pool round-trip; interned dedup for a partition-granularity positional delete referenced by multiple data files (assert the delete file appears EXACTLY ONCE in `deleteFiles`); a DV ref with `offset`/`length`; a mixed shard (one POS_DEL-pooled and one DV-pooled data file); a single data file carrying BOTH a POS_DEL ref and a DV ref (union); and the no-deletes compact form (empty `deleteFiles`, `deletes` omitted on each data file)
- [ ] 1.3 Migrate `JoinSpec.files` (the shard-invariant broadcast dimension side in `CommonScanSpec`, arg 0) off `Vec<FileEntry>` onto the SAME normalized file-set shape introduced in 1.1, so the dimension side's own positional deletes (and, for free, any DV) reuse the interned pool + `df`-indexed refs identically to the fact side. Carry the `join` field through the `to_common` / `from_parts` split and merge and the `sample_spec()` test helper unchanged in meaning; the join pool is scoped to the join block (still shard-invariant in arg 0). Extend the `spec.rs` join round-trip test to cover a dimension file carrying a positional delete. This is what lets the tuple types (`FileEntry`/`DeleteFileRef`/`DeleteFileContentType`/`FileEntryWire`) be retired without breaking the join feature [expert]

### Group B — deletion-vector-v1 decoder (independent, pure computation)

- [ ] 2.1 Re-fetch `format/puffin-spec.md` "deletion-vector-v1" and cross-check against a real Spark-produced DV file's raw bytes; confirm exact endianness of the length prefix and CRC, the CRC polynomial vs `crc32fast`, and that the blob is never Puffin-compressed [expert]
- [ ] 2.2 Implement the `deletion-vector-v1` blob decoder in a new `src/scan/deletion_vectors.rs`: parse 4-byte BE combined length, verify `D1 D3 39 64` magic, deserialize the portable Roaring vector (8-byte LE bitmap count, per-bitmap 4-byte LE high key + `RoaringBitmap::deserialize_from`), reconstruct 64-bit positions (key<<32 | value) into a `RoaringTreemap`, verify the 4-byte BE CRC-32 over magic+vector, and validate decoded count == declared `cardinality` — failing loud with clean, credential-redacted errors on any mismatch [expert]
- [ ] 2.3 Decoder unit tests against known-good byte sequences: single-key, multi-key (positions > 2^32), empty bitmap, cardinality mismatch, corrupt magic, corrupt CRC [expert]

### Group C — Adapter DV-reference extraction (manifest walk becomes a producer)

- [ ] 3.1 Relax `classify_manifest_file` (`src/adapter/pushdown.rs`) so `PositionDeletes`+`Puffin` returns `Ok` (no longer `Err(DeletionVector)`); keep equality/ORC/Avro rejected; drop the now-unreachable `DeletionVector` arm of `UnsupportedDeleteMechanism` (dead-code removal)
- [ ] 3.2 Turn the manifest/`DataFile` walk into a DV-reference producer: for each alive DV `DataFile`, read `referenced_data_file()`, `content_offset()`, `content_size_in_bytes()`; intern the Puffin container into the shard's `deleteFiles` pool (ONCE, `type` `DV`, `format` `PUFFIN`) and add a `df`-indexed `deletes` reference (carrying the blob's `offset`/`length`) to the `dataFiles` entry for its referenced data file (keyed by the referenced data file path); do NOT serialize `referenced_data_file`. Union these DV refs with any positional-delete refs from `plan_files_from_table` into the same per-data-file `deletes` list [expert]
- [ ] 3.3 `pushdown.rs` unit tests: `classify_manifest_file` accepts Puffin position deletes and still rejects equality/ORC/Avro; DV-ref extraction populates the correct data file with the correct offset/length

### Group D — Scan-side DV application (depends on A, B, C)

- [ ] 4.1 Add the DV branch to the read-time backstop `ensure_positional_delete` (`src/scan/positional_deletes.rs`) so a pooled delete file of `type` `DV` is accepted (not rejected); keep equality/ORC/Avro/unknown rejected; update its rejection unit test
- [ ] 4.2 In `access_plan_for_data_file`, for each of the data file's `deletes` refs resolve the `df` index into the shard's `deleteFiles` pool and dispatch on the pooled entry's `type`: POS_DEL → existing `union_delete_positions`; DV → open the Puffin file via `PuffinReader` from the pooled entry, fetch the blob at the ref's `offset`/`length`, decode via Group B (cross-checking the blob's referenced-data-file from Puffin `BlobMetadata` against the data file it is applied to), and union its positions into the SAME per-data-file `RoaringTreemap` before the unchanged `build_access_plan` [expert]
- [ ] 4.3 Puffin file open + blob fetch plumbing: build the Puffin `InputFile` from the pooled DV `deleteFiles` entry's path + object store using the no-HEAD size, select the `BlobMetadata` at the ref's `offset`/`length`, and surface clean credential-redacted errors on open/read failure
- [ ] 4.4 Integration tests `tests/scan_deletion_vectors.rs` (mirror `scan_positional_deletes.rs`): DV removes flagged rows, fully-deleted file yields no rows, DV composes with projection/filter/LIMIT, and a mixed positional+DV shard resolves per file

### Group E — Fixtures + E2E (depends on D)

- [ ] 5.1 Repurpose `scripts/spark-fixtures/create_deletion_vector_fixture.sql` from a fail-loud-only fixture into a positive DV fixture (retain 10 rows / deleted `id IN (3,7)`, document post-delete ground truth); add a new mixed-mechanism fixture SQL whose data files split across positional-delete and DV mechanisms
- [ ] 5.2 Update `tests/common/pos_delete_fixtures.rs`: add post-delete row/id ground truth for `mor_dv_unsupported` (consider renaming the constant away from `_unsupported`) and add constants for the new mixed fixture, in lockstep with the SQL
- [ ] 5.3 New `tests/e2e_deletion_vectors_test.rs`: DV table returns post-delete rows; composes with projection/filter/LIMIT; composes with single-group and grouped aggregation; mixed table returns combined post-delete set; mixed result invariant across forced same-shard vs different-shard fan-out; fixture-produced and stack-unavailable-fails guards
- [ ] 5.4 Narrow `tests/e2e_positional_deletes_test.rs::e2e_unsupported_delete_fails_loud` so it targets a still-unsupported mechanism (equality or ORC/Avro), NOT the DV table which now succeeds

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1, 1.2, 1.3 |
| Group B | 2.1, 2.2, 2.3 |
| Group C | 3.1, 3.2, 3.3 |
| Group D | 4.1, 4.2, 4.3, 4.4 |
| Group E | 5.1, 5.2, 5.3, 5.4 |

Sequential dependencies:
- Within Group A, 1.3 depends on 1.1 (it reuses the normalized file-set type 1.1 introduces); 1.2 depends on 1.1.
- Groups A, B, C are independent and may run concurrently.
- Group D depends on A (wire fields), B (decoder), and C (DV refs reaching the scan).
- Group E depends on D (application must work before E2E asserts post-delete results).

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Enum + wire | `FileEntryWire` (`Legacy`/`WithDeletes` tuple enum) and the flat `DeleteFileRef` (path/size/content_type) (`src/scan/spec.rs`) | The per-shard arg becomes a normalized `{deleteFiles, dataFiles}` object; the untagged tuple serde and the flat delete-ref shape are replaced, and there is no cross-version wire-compat requirement to preserve them. NOTE: `FileEntry`/`DeleteFileRef` are ALSO consumed by `JoinSpec.files` (arg 0, added by the merged join feature #71) — task 1.3 migrates that consumer to the normalized shape as part of the retirement, so no dangling reference remains |
| Enum | `DeleteFileContentType` (`src/scan/spec.rs`, from #72) | Superseded by the `type` (`POS_DEL`/`EQ_DEL`/`DV`) + `format` (`PARQUET`/`AVRO`/`ORC`/`PUFFIN`) split on the pooled `deleteFiles` entry |
| Enum arm | `UnsupportedDeleteMechanism::DeletionVector` (`src/adapter/pushdown.rs`) | DVs are now supported; the arm and its `describe()`/classification usage become unreachable |
| Comment | `classify_manifest_file` "A position delete stored as a Puffin blob IS a v3 deletion vector" (pushdown.rs) | The arm it justifies now returns `Ok`; update to reflect DV acceptance |
| Test | `tests/e2e_positional_deletes_test.rs::e2e_unsupported_delete_fails_loud` (DV path) | The DV assertion is replaced; retarget to equality/ORC (task 5.4) |
| Doc/const | `DELETION_VECTOR_TABLE`'s "expected to reject any query" doc (`tests/common/pos_delete_fixtures.rs`) | Fixture becomes a positive fixture (task 5.2) |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| DV: A deletion vector removes flagged rows | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `dv_removes_flagged_rows` |
| DV: The decoder honors the deletion-vector-v1 binary layout | Unit | `crates/lakehouse-engine/src/scan/deletion_vectors.rs` | `decodes_portable_roaring_positions` |
| DV: A cardinality mismatch fails loud | Unit | `crates/lakehouse-engine/src/scan/deletion_vectors.rs` | `cardinality_mismatch_errors` |
| DV: A corrupt magic or checksum fails loud | Unit | `crates/lakehouse-engine/src/scan/deletion_vectors.rs` | `corrupt_magic_or_crc_errors` |
| DV: A referenced-data-file mismatch fails loud | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `dv_referenced_data_file_mismatch_errors` |
| DV: A fully deleted data file yields no rows | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `dv_fully_deleted_file_empty` |
| DV: Deletion vectors compose with projection, filter, LIMIT, and pruning | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `dv_composes_with_pushdown` |
| DV: Mixed positional-delete and deletion-vector files in one shard resolve per data file | Integration | `crates/lakehouse-engine/tests/scan_deletion_vectors.rs` | `mixed_mechanisms_resolve_per_file` |
| Reconstitution: The deleteFiles pool interns each delete file once and resolves df-indexed refs | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `interned_pool_dedups_and_resolves_df` |
| Reconstitution: Reconstitution carries per-file deletion-vector references (df + offset/length) | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `reconstitutes_dv_refs` |
| Reconstitution: A mixed positional-delete and deletion-vector shard round-trips | Unit | `crates/lakehouse-engine/src/scan/spec.rs` | `mixed_pos_and_dv_shard_round_trips` |
| Pruning: Deletion-vector files are preserved into the scan spec | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `dv_refs_preserved_into_scan_spec` |
| Pruning: An unsupported delete mechanism fails loud at plan time (DV excluded) | Unit | `crates/lakehouse-engine/src/adapter/pushdown.rs` | `classify_rejects_equality_orc_avro_accepts_dv` |
| Positional: An unapplicable delete file is rejected (backstop, DV excluded) | Unit | `crates/lakehouse-engine/src/scan/positional_deletes.rs` | `backstop_rejects_equality_not_dv` |
| Fixtures: Spark produces a deletion-vector fixture | Integration | `crates/lakehouse-engine/tests/e2e_deletion_vectors_test.rs` | `fixture_spark_deletion_vector_table` |
| Fixtures: Spark produces a mixed positional-delete and deletion-vector fixture | Integration | `crates/lakehouse-engine/tests/e2e_deletion_vectors_test.rs` | `fixture_spark_mixed_mechanism_table` |
| Fixtures: Ground truth stays in lockstep with the test constants | Unit | `crates/lakehouse-engine/tests/e2e_deletion_vectors_test.rs` | `fixture_ground_truth_lockstep` |
| E2E-DV: query over a deletion-vector table returns post-delete rows | Integration | `crates/lakehouse-engine/tests/e2e_deletion_vectors_test.rs` | `e2e_dv_returns_post_delete_rows` |
| E2E-DV: deletion vectors compose with projection, filter, and LIMIT | Integration | `crates/lakehouse-engine/tests/e2e_deletion_vectors_test.rs` | `e2e_dv_with_projection_filter_limit` |
| E2E-DV: deletion vectors compose with aggregation | Integration | `crates/lakehouse-engine/tests/e2e_deletion_vectors_test.rs` | `e2e_dv_with_single_and_grouped_agg` |
| E2E-DV: mixed table returns the combined post-delete set | Integration | `crates/lakehouse-engine/tests/e2e_deletion_vectors_test.rs` | `e2e_mixed_returns_combined_post_delete` |
| E2E-DV: mixed-mechanism result invariant across fan-out placement | Integration | `crates/lakehouse-engine/tests/e2e_deletion_vectors_test.rs` | `e2e_mixed_invariant_across_fanout` |
| E2E-pos: unsupported delete mechanism fails loud (retargeted, DV excluded) | Integration | `crates/lakehouse-engine/tests/e2e_positional_deletes_test.rs` | `e2e_unsupported_delete_fails_loud` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-execution-deletion-vectors + e2e-harness-deletion-vectors | `make test-e2e` then `SELECT id FROM <vs>.mor_dv_unsupported ORDER BY id` | Rows `1,2,4,5,6,8,9,10` (ids 3 and 7 deleted by the DV) |
| deletion-vector-fixtures (mixed) | `SELECT count(*) FROM <vs>.<mixed_table>` | Count equals seeded rows minus all rows deleted by both the positional-delete files and the deletion vectors |
| e2e-harness-positional-deletes (retargeted fail-loud) | Query the equality/ORC unsupported table through the VS | Plan-time error naming the unsupported mechanism; no rows; no secret in the message |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| E2E | `make test-e2e` | 0 failures (fails, never skips, without the stack) |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
