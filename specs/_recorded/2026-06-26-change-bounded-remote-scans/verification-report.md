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

## Live Benchmark Results (2026-06-27 addendum)

Fulfils the post-merge re-bench below. Cluster `3.124.151.144` (3 nodes n11/n12/n13, DB 2025.1.11); AWS Glue catalog, namespace `tpch` ≈ SF10 (`lineitem` ≈ 60M rows / 10 data files / ~1.7 GB; `orders` ~861 MB). SLC **lc-rs 0.19.1** (released), `.so` built against `exasol-udf-sdk` 0.19.1 (matching fingerprint), `NR_OF_CORES=4`, `PARALLELISM_FACTOR=8`.

| Query | Shape | Wall-clock | Result |
|-------|-------|-----------|--------|
| Q1 | supplier ⋈ nation ⋈ region, GROUP BY (wiring) | **4.03 s** | 25 rows |
| Q2 | customer ⋈ orders ⋈ lineitem COUNT (full 60M join) | **9.44 s** | `ROWS_JOINED = 59,986,052` |
| Q3 | orders ⋈ lineitem + date filter + GROUP BY priority + SUM | **8.08 s** | 5 priority rows |
| Q4 | lineitem pricing summary (TPC-H Q1 shape; multi-file parallel scan) | **9.85 s** | correct |
| Pushdown | shard fan-out / LIMIT / filter+projection / filter+GROUP BY agg / IN-OR-comparison | — | all pushed ✅ |

**Crash-rate validation** (the previously intermittent failures): Q3 ×10 → **10/10 pass**; single-leg full-`lineitem` raw emit (`COUNT(*)` over `SELECT DISTINCT L_ORDERKEY, L_EXTENDEDPRICE`) ×10 → **10/10 pass**. **0/20 crashes**, down from ~67–90% before.

### Root-cause correction (important for the spec record)

The previously-crashing queries did **not** fail for the reason this plan targeted (per-instance memory / Parquet-decode working set). Live `lc-rs` 0.19.0 debug-surface telemetry + COS-side forensics proved peak UDF-VM RSS ≈ 120 MB — ~33× **below** the 4096 MB per-instance limit, flat, identical on pass vs crash; no kernel OOM, no core, no Rust panic. The `cleanup VM failed: VM crashed` (SQL state 22002, `err_zombie:TRUE/err_except:FALSE`) was an **SLC ZMQ wire-protocol bug**: the REQ socket's 1 s `RCVTIMEO`/`SNDTIMEO` expiry (`EAGAIN`) was treated as fatal with no retry, so under load a slow-but-alive `MT_EMIT`/`MT_FINISHED` ack broke the REQ/REP lockstep, one VM abnormally exited, and the engine SIGKILL'd the sibling VMs. Fixed in **lc-rs 0.19.1** (`exasol-labs/language-container-rs#37`, PR #38 — retry transient `EAGAIN`). The memory-bounding work in *this* plan (pool sizing, `batch_size`, `ResourcesExhausted` surfacing, `emit_batch`) remains correct and necessary, but was not the cause of the lineitem `VM crashed`.

### Measured vs not measured

- **Measured:** single 3-node SF10 run, per-query wall-clock + 0/20 crash rate above.
- **Not yet measured:** a multi-node **scaling sweep** (1→2→3 nodes) and a remote `PROFILE` overhead breakdown. Promote to a follow-up if the architect-persona deliverable needs the scaling curve.

## Outstanding (post-merge, observational — not blocking)

- ~~**Live re-bench** on the AWS Glue cluster of the previously-crashing queries + per-query wall-clock~~ — **RESOLVED**, see *Live Benchmark Results* above. Net outcome: green, 0/20 crashes; the crash root cause was an SLC bug (fixed in lc-rs 0.19.1), not the memory bounding. Remaining optional follow-up: multi-node scaling sweep + remote `PROFILE`.
