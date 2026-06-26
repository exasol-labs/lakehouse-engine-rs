# Verification Report: change-bounded-remote-scans

## Bottom Line

**PASS.** All implementation tasks complete, both blocker findings from code review fixed, and one real bug surfaced by live E2E (`emit_batch` rejecting `Utf8View`) fixed. Host unit suite, clippy, fmt, the Docker `.so` build, and the full local E2E suite are all green.

## Results

| Check | Command | Result |
|-------|---------|--------|
| Host unit tests | `cargo test` | **287 passed**, 0 failed |
| Lint | `cargo clippy --all-targets` | 0 warnings |
| Format | `cargo fmt --check` | clean |
| UDF `.so` build | `make cross-musl-udf-build` | exit 0 (rust:1.92-bookworm, glibc 2.36) |
| E2E (Exasol Docker + MinIO + Iceberg, SLC 0.18.0) | `make test-e2e` | **7/7** scan + **27/27** capability passed |

## Scenario Coverage

| Scenario | Test | Status |
|----------|------|--------|
| Arrow batches emitted incrementally as Arrow IPC, never double-materialized | `emit.rs::emits_batch_by_batch_without_materializing` (IPC emit + no-`Vec<Value>` invariant) | ✅ |
| Raw-row scan returns correct rows via the new emit path | `e2e_scan_test` raw-scan queries | ✅ |
| Clean memory-exhaustion error instead of VM crash (raw path) | `emit.rs::resources_exhausted_surfaces_as_memory_error_not_storage_error` | ✅ |
| Clean memory-exhaustion error on the partial-aggregate path | `mod.rs::resources_exhausted_on_partial_aggregate_path_surfaces_as_memory_error` | ✅ |
| Parquet decode bounded via configured batch size | `spec.rs::df_batch_size_round_trips_and_defaults`, `mod.rs::session_config_applies_batch_size_and_clamps_floor` | ✅ |
| `df_batch_size` flows create→adapterNote→pushdown→ScanSpec | `adapter/mod.rs::df_batch_size_uses_supplied_value` | ✅ |
| Remote bench wires NR_OF_CORES + PARALLELISM_FACTOR | `bench/run.sh selftest` (build_vs_extra_props remote case) | ✅ |
| View-type column (Utf8View) emits successfully via IPC | `emit.rs` view-type normalization unit test + 3 E2E string-expression queries | ✅ |

## Deviations from Plan

1. **SDK 0.18.0 dropped `connect-back` as a feature** (now always-on); `emit-arrow` is the only feature. Feature list corrected during task 1.
2. **SLC pin bumped 0.16.0 → 0.18.0** in both `e2e_*_test.rs` (not in original task list) — required because the SLC runtime must understand the `emit_record_batch_ipc` message the new emit path sends. Without it the E2E stack would reject IPC emits.
3. **`df_batch_size` adapterNote write-side** was missing after initial implementation (read-side only) — caught by code review as a blocker (feature was inert), fixed in R1.
4. **`classify_scan_error` was only on the raw-row path** initially — caught by code review as a blocker (the grouped-aggregate OOM path still misclassified), fixed in R2 across all 5 partial-aggregate error sites.
5. **`emit_batch` rejects Arrow `Utf8View`** — surfaced by live E2E (DataFusion 58 produces `Utf8View` for Parquet strings + string expressions; the old row-by-row path absorbed it via a stringify fallback). Fixed in R7 with a `normalize_view_types` pass (Utf8View→Utf8, BinaryView→Binary) before `emit_batch`, the bulletproof backstop against view types from any source.
6. **Version bump** 0.11.0 → 0.12.0 (feature addition; plan carried no `workspace/version` delta).
7. **Issues #15/#16 already existed** (from prior `next.md` work) and cover this work — no new issue created; the commit references `Closes #15 #16`.

## Outstanding (post-merge, observational — not blocking)

- **Live re-bench** on the AWS Glue cluster (`3.124.151.144`) of the two previously-crashing queries (grouped aggregate over full `lineitem`; ~60M-row join) to confirm they now complete or return a clean `ResourcesExhausted` rather than `VM crashed`, and to capture per-query wall-clock + `PROFILE`. This is the deliverable the bounding work unblocks; it runs against the live cluster after merge.
