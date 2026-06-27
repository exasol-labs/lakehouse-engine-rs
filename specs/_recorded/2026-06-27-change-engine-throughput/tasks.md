# Tasks: change-engine-throughput

> Tasks 1–4 are ENGINE FEATURES (spec-covered → recordable). Tasks 5–9 are BENCHMARK / HARNESS /
> SWEEP / MEASUREMENT work (no spec). Status: `[ ]` pending · `[~]` started · `[x]` done.

## Phase 2: Engine features (Group A)

### Task 1 — Threading mode AUTO/FIXED (adapter)  [files: adapter/mod.rs, adapter/sharding.rs]
- [x] 1.1 Add `DATAFUSION_THREADING_MODE` VS/connection property (`AUTO`|`FIXED`, default `AUTO`, case-insensitive), parse + record in adapterNotes
- [x] 1.2 AUTO derivation: `df_threads_per_udf = max(1, floor(NR_OF_CORES / udf_instances_per_node))`, `df_target_partitions` in lockstep; udf_instances_per_node from per-node share of G; fall back to 1 when NR_OF_CORES=0 [expert]
- [x] 1.3 FIXED mode: supplied DATAFUSION_TARGET_PARTITIONS/THREADS_PER_UDF verbatim, each default max(NR_OF_CORES,1)
- [x] 1.4 Round-trip resolved integer fields into per-shard ScanSpec (UDF stays mode-agnostic)
- [x] 1.5 Unit tests: AUTO arithmetic (incl. oversubscription invariant), FIXED passthrough, default-AUTO, NR_OF_CORES=0 fallback

### Task 2 — Repartition-free raw-scan pipeline (scan)  [files: scan/mod.rs]
- [x] 2.1 Inspect raw-scan physical plan; assert ParquetExec→FilterExec→ProjectionExec→CoalesceBatchesExec, no Repartition/CoalescePartitions/global Sort/global agg when target_partitions==1 [expert]
- [x] 2.2 Elide any needless stage without changing result set [expert]
- [x] 2.3 Unit/integration test asserting physical-plan shape (string-match on displayable plan)

### Task 3 — Parquet row-group & page pruning (scan)  [files: scan/mod.rs]
- [x] 3.1 Enable Parquet predicate pushdown + row-group stats pruning + page-index pruning on session/Parquet scan options
- [x] 3.2 Test: pruning flags set; result rows identical pruning on vs off

### Task 4 — On-demand phase telemetry (scan)  [files: scan/diagnostics.rs (restore), scan/mod.rs, scan/emit.rs]
- [x] 4.1 Restore scan/diagnostics.rs checkpoint infra from archive/udf-diagnostics-checkpoints, deactivated by default
- [x] 4.2 Three monotonic-clock phase accumulators (startup, obj-store import, send-back/emit) wired at checkpoint sites [expert]
- [x] 4.3 Gate emission on debug level (ctx.debug_level()/LAKEHOUSE_UDF_DEBUG_LEVEL, default info); best-effort writes
- [x] 4.4 Emit one per-VM-tagged telemetry record at completion; phases sum to scan-body wall-clock within tolerance
- [x] 4.5 Unit tests: silent at default; three distinct phases when enabled; telemetry-write failure never fails scan

## Phase 2: Benchmarks / harness (Group B — micro-benchmarks)
### Task 5 — Synthetic micro-benchmarks (no spec)  [files: bench/, crate bench bin]
- [x] 5.1 Emit-only benchmark over BIGINT/DOUBLE/TIMESTAMP/DECIMAL/VARCHAR/production schemas through emit_batch [expert]
- [x] 5.2 Scan-only benchmark: Iceberg→DataFusion stream, NO emit (drain) [expert]
- [x] 5.3 Wire behind bench harness / cargo bench binary; record GB/sec, CPU, RSS

## Phase 2: Benchmarks (Group C — e2e + sweep, needs cluster) — DONE
### Task 6 — E2E benchmark & baseline (no spec)
- [x] 6.1 Baseline `make bench` remote, telemetry OFF; FIXED 4/4: Q1 3.75 / Q2 9.89 / Q3 8.20 / Q4 8.94s (~0.19 GB/s)
- [x] 6.2 Telemetry ON via jumphost connect-back listener: COUNT shard = startup ~110ms / import ~650ms / emit ~2ms → import-bound [expert]
- [x] 6.3 Startup ~110ms ≈ <2% of multi-second scans → NOT a bottleneck [expert]
- [x] 6.4 (folded into 6.3) startup small at G=12 fan-out; no sharding floor concern at current G

### Task 7 — Parameter sweep (no spec)
- [x] 7.1 Sweep driver: bench/sweep.sh + run.sh BENCH_DF_* knobs (config-driven, no recompile) [expert]
- [x] 7.2 Threading sweep on live cluster: 1/1, 2/2, 4/4, 8/8 — Q1-Q4 captured
- [x] 7.3 1 thread IS a bottleneck (+39% Q4); optimum ≈ NR_OF_CORES (4); 8/8 regresses on full scan [expert]
- [x] 7.4 Criteria verified (optimum=cores, startup<10%, 0 crash/OOM across sweep) → verification-report

## Phase 2: Conditional (Group D) — RESOLVED: NOT BUILT
### Task 8 — Conditional decode-emit overlap buffer (gated; no spec)
- [x] 8.1 GATE = DO NOT BUILD. Scan is import-bound (import ~650ms vs emit ~2ms); overlapping a near-zero emit with import yields ~0. Recorded in decision-log.
- [x] 8.2 Not built — correct per measure-first; buffer helps only an emit-bound workload, which the far-VPC scan is not.

## Phase 2: Goal benchmark (Group E) — DONE
### Task 9 — IMPORT FROM PARQUET goal benchmark (no spec)
- [x] 9.1 Resolved 20 lineitem Parquet files (same list the VS uses)
- [x] 9.2 Built IMPORT INTO ... FROM PARQUET ... SQL → bench/import_ceiling.sh
- [x] 9.3 IMPORT all-files COUNT 3×: 10.07 / 10.18 / 9.77s → median 10.07s (~0.17 GB/s full read)
- [x] 9.4 VS COUNT(*) same session 3×: 2.67 / 2.29 / 1.60s (metadata-pushdown); VS Q4 8.94s (fair aggregate)
- [x] 9.5 Ceiling recorded: native IMPORT full-read = same 0.17 GB/s S3 ceiling; VS competitive/faster via pushdown → bottleneck is S3, not UDF

## Phase 3: Verification
- [x] V.1 Build (`make cross-musl-udf-build` exit 0), host `cargo test` (260 lib + 8 integ, 0 fail), clippy 0, fmt clean
- [~] V.2 E2E `make test-e2e` (rebuilding .so after version bump, then docker e2e) — running
- [x] V.3 Code review (ponytail): 1 finding (dormant diagnostics infra, spec-sanctioned, flagged) → decision-log
- [x] V.4 Verification report written: how/what/why of each optimization + live-cluster evidence
