# Verification Report: add-deletion-vector-application

## Verdict: PASS ✅

Apple Iceberg v3 deletion vectors (`deletion-vector-v1` Puffin blobs) are applied on read through
the same `RowSelection`/`ParquetAccessPlan` union point as positional deletes. All build, lint,
format, unit, integration, and end-to-end gates are green against the live Exasol + MinIO + Iceberg
REST + Spark stack. Closes the deletion-vector half of #11 (issue #12).

Feature release: `lakehouse-engine` **0.24.0 → 0.25.0**.

## Checklist results

| Step | Command | Expected | Actual |
|------|---------|----------|--------|
| Build | `make cross-musl-udf-build` | Exit 0 | ✅ Exit 0 — release `.so` v0.25.0 (`rust:1.94-bookworm`, 1m14s) |
| Test | `cargo test` (non-E2E) | 0 failures | ✅ 457 lib + all integration (scan_deletion_vectors 5, scan_positional_deletes 10, others) — 0 failures |
| Lint | `cargo clippy --all-targets` | 0 warnings | ✅ Clean (workspace) |
| Format | `cargo fmt --check` | No changes | ✅ Clean |
| E2E | `make test-e2e` | 0 failures | ✅ **83 passed, 0 failed** |

### E2E breakdown (`--features exasol-e2e`, `--test-threads=1`, live stack)

| Suite | Result |
|-------|--------|
| e2e_capability_test | 8 passed |
| e2e_count_distinct_test | 6 passed |
| e2e_deletion_vectors_test | 9 passed |
| e2e_join_test | 6 passed |
| e2e_positional_deletes_test | 11 passed (retargeted fail-loud → ORC; no regression) |
| e2e_scan_test | 43 passed |

## Scenario coverage audit

Every scenario in the plan's Scenario Coverage table maps to a passing test:

| Scenario | Test | Status |
|----------|------|--------|
| DV removes flagged rows | `scan_deletion_vectors::dv_removes_flagged_rows` | ✅ |
| Decoder honors binary layout | `deletion_vectors::decodes_portable_roaring_positions` | ✅ |
| Cardinality mismatch fails loud | `deletion_vectors::cardinality_mismatch_errors` | ✅ |
| Corrupt magic/CRC fails loud | `deletion_vectors::corrupt_magic_or_crc_errors` | ✅ |
| Referenced-data-file mismatch fails loud | `scan_deletion_vectors::dv_referenced_data_file_mismatch_errors` | ✅ |
| Fully deleted file yields no rows | `scan_deletion_vectors::dv_fully_deleted_file_empty` | ✅ |
| DV composes with projection/filter/LIMIT/pruning | `scan_deletion_vectors::dv_composes_with_pushdown` | ✅ |
| Mixed positional+DV resolves per file | `scan_deletion_vectors::mixed_mechanisms_resolve_per_file` | ✅ |
| Pool interns once, resolves df refs | `spec::interned_pool_dedups_and_resolves_df` | ✅ |
| Reconstitution carries DV refs | `spec::reconstitutes_dv_refs` | ✅ |
| Mixed shard round-trips | `spec::mixed_pos_and_dv_shard_round_trips` | ✅ |
| DV files preserved into scan spec | `pushdown::dv_refs_preserved_into_scan_spec` | ✅ |
| Unsupported mechanism fails loud (DV excluded) | `pushdown::classify_rejects_equality_orc_avro_accepts_dv` | ✅ |
| Backstop rejects unapplicable (DV excluded) | `positional_deletes::backstop_rejects_equality_not_dv` | ✅ |
| Spark produces DV / mixed fixtures | `e2e_deletion_vectors_test::fixture_spark_*` | ✅ |
| Ground truth lockstep | `e2e_deletion_vectors_test::fixture_ground_truth_lockstep` | ✅ |
| E2E DV post-delete rows / projection-filter-limit / agg | `e2e_deletion_vectors_test::e2e_dv_*` | ✅ |
| E2E mixed combined / fan-out invariant | `e2e_deletion_vectors_test::e2e_mixed_*` | ✅ |
| E2E unsupported fails loud (retargeted) | `e2e_positional_deletes_test::e2e_unsupported_delete_fails_loud` (ORC) | ✅ |

## Manual testing

| Feature | Command | Expected | Actual |
|---------|---------|----------|--------|
| DV table | `SELECT id FROM <vs>.MOR_DV ORDER BY id` | ids 1,2,4,5,6,8,9,10 | ✅ (e2e_dv_returns_post_delete_rows) |
| Mixed table | combined post-delete set | 16 rows (20 − 4) | ✅ (e2e_mixed_returns_combined_post_delete) |
| Retargeted fail-loud | query ORC unsupported table | plan-time error, no secret | ✅ (e2e_unsupported_delete_fails_loud) |

## Reconciliation & review notes

- **Plan reconciled with the concurrently-merged join feature (#71).** `JoinSpec.files` migrated to
  the same normalized `{deleteFiles, dataFiles}` file-set shape as the fact side (decision [9], task 1.3).
- **Code review:** no correctness bugs; 5 quality/safety findings fixed (R.1 fail-loud on unmatched
  DV ref; R.2 per-shard Puffin container reuse + documented single unavoidable HEAD; R.3 messaging;
  R.4 DRY redact; R.5 test rename).
- **E2E-caught integration bug (R.6):** iceberg-rust 0.10 DOES surface the Puffin DV file in
  `FileScanTask.deletes` (the plan assumed otherwise), producing a duplicate mis-typed POS_DEL ref
  that opened the Puffin container as Parquet ("Corrupt footer"). Fixed by excluding manifest-collected
  DV container paths from the positional refs (`positional_delete_refs` helper); manifest walk stays
  authoritative for DVs. Correction recorded in decision-log [3]. This is the class of silent-
  correctness/robustness failure the E2E gate exists to catch.

## Known limitations (documented, not defects)

- One object-store HEAD per distinct Puffin container per shard: iceberg-rust's `InputFile` exposes
  no way to inject the pooled byte size, so `PuffinReader` stats the file for the footer. Collapsed
  to once-per-container-per-shard via the reader cache (R.2).
- Equality deletes and ORC/Avro delete files remain rejected (out of scope, tracked under #11).
