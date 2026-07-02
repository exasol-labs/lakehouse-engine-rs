[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

Optimizations, live-cluster numbers, and tuning headroom. Numbers from the last recorded
benchmark (`specs/_recorded/2026-06-27-change-engine-throughput/`) plus a larger-scale,
not-yet-recorded validation run (`bench/reports/bench-report-20260701-123648.txt`,
`bench/reports/import-ceiling-20260701-124836.txt`) — see
[Larger-scale validation](#larger-scale-validation-180m-row-lineitem-60-files) below.

> **Benchmark caveat — pre-0.20.1 node-count bug:** every benchmark below that predates the
> `exasol-udf-sdk`/`exasol-udf-macros` `0.20.1` bump (`add-scan-connection-concurrency`, closes
> #43) ran on live clusters where `ctx.node_count()` always returned `0` at
> `createVirtualSchema` time; `resolve_cluster_nodes` maps that to `1`. Any such run whose
> `adapterNotes` recorded `CLUSTER_NODES=1` on what was actually a multi-node cluster therefore
> silently computed shard count as `G = 1 × parallelism_factor` instead of
> `G = node_count × parallelism_factor` — collapsing cluster-wide sharding to single-node
> sharding. Single-node timings are unaffected; multi-node scaling claims recorded before the
> fix should be treated with suspicion. Full detail:
> `specs/_recorded/2026-07-02-add-scan-connection-concurrency/decision-log.md` (Design Decisions [1] and [5]).

## Optimizations delivered

- **Parquet predicate pushdown** (`datafusion.execution.parquet.pushdown_filters`) — filters
  push into the Parquet reader so non-matching rows are never decoded. (DataFusion defaults
  this off; the engine turns it on.) Distinct from Iceberg file pruning.
- **Row-group & page pruning** (`parquet.pruning`, `enable_page_index`) — skip whole row
  groups / pages whose statistics exclude the predicate.
- **Repartition-free raw-scan plan** — the single-partition raw-scan physical plan carries no
  `RepartitionExec`, `CoalescePartitionsExec`, or global sort/aggregate stage; filter and
  projection fuse into the data source.
- **Configurable per-instance threading** — `DATAFUSION_THREADING_MODE` (`AUTO`/`FIXED`),
  tunable without recompiling. See [Tuning](tuning.md).
- **Configurable object-store connection concurrency** — `S3_MAX_CONNECTIONS` sizes the
  per-instance S3 HTTP connection pool, oversubscribed relative to CPU threads on IO-bound
  scans. See [Tuning](tuning.md#s3_max_connections) and
  [Native `IMPORT` parity goal](#native-import-parity-goal) below.
- **Projection + partial-aggregate pushdown** — only projected columns are read; aggregates
  ship one partial row per group instead of raw rows.

## Native `IMPORT` parity goal

`S3_MAX_CONNECTIONS` targets closing the gap this doc measures below: native
`IMPORT FROM PARQUET` exposes an explicit `MaxConnections` knob for per-instance object-store
fetch concurrency, while the engine previously left that axis entirely to library defaults.
This knob is deliberately orthogonal to the two existing levers — `PARALLELISM_FACTOR` controls
*how many shards* run, `DATAFUSION_THREADING_MODE` controls *how many CPU threads* decode a
shard, and `S3_MAX_CONNECTIONS` controls *how many S3 fetches* a shard's threads can keep in
flight while they wait on the network. Raising it is the next lever toward approaching native
`IMPORT` throughput on IO-bound scans (the common case per the phase telemetry below —
`import ≫ emit`), layered on top of threading and sharding rather than replacing them. See
`specs/_recorded/2026-07-02-add-scan-connection-concurrency/decision-log.md` (Design Decisions [2]-[4]) for
the knob's design rationale, and Task 3.1 (benchmark sweep) for the validating measurement.

> **Sweep outcome (2026-07-02):** the validating sweep found `S3_MAX_CONNECTIONS` had **no
> measurable effect** on full-scan throughput on the tested 2-node cluster (< 2 % across 4→128,
> at either shard shape) — the object-store connection pool was not the limiter there; the S3
> read itself is network-distance bound (~0.17 GB/s). The knob is correctly wired and remains a
> supported lever for genuinely connection-starved deployments, but it did not close the gap
> here. Full results in [Connection-concurrency & shard-shape sweep](#connection-concurrency--shard-shape-sweep-2026-07-02-post-0201).

## Current benchmark results

**Cluster:** 3 nodes, Exasol DB 2025.1.11, AWS Glue catalog `tpch`, SLC lc-rs 0.19.1.
**Data:** `lineitem` ≈ 1.7 GB across 20 Parquet files on S3.
**Config:** `DATAFUSION_THREADING_MODE=FIXED`, threads = partitions = 4, telemetry off.

| Query | Time | Notes |
|---|---|---|
| Q1 (wiring) | 3.75 s | — |
| Q2 (3-way join) | 9.89 s | join handled by Exasol over per-table scans |
| Q3 (filter + GROUP BY) | 8.20 s | partial-agg pushdown |
| **Q4 (full `lineitem` scan)** | **8.94 s ≈ 0.19 GB/s** | reproduces prior baseline |
| `COUNT(*)` via metadata pushdown | 1.60–2.67 s | no row scan |

### Thread sweep (NR_OF_CORES = 4)

| threads / partitions | Q4 full scan | vs. 1/1 |
|---|---|---|
| 1 / 1 (what `AUTO` derives here) | 12.45 s | baseline |
| 2 / 2 | 10.52 s | +18 % |
| **4 / 4** | **8.94 s** | **+39 %** |
| 8 / 8 | 10.02 s | regresses |

⇒ **For read-bound scans, set `DATAFUSION_THREADING_MODE=FIXED`, threads = partitions =
`NR_OF_CORES`.** Single-thread leaves ~39 % unused; threads help by overlapping read latency,
not CPU.

### The engine is not the bottleneck — object-storage read is

Three independent confirmations:

1. **Native baseline** — Exasol's own `IMPORT FROM PARQUET` over the same files hits the same
   ~0.17 GB/s ceiling (≈ 10.07 s). The VS path is competitive or faster (pushdown). Same
   storage, same ceiling ⇒ the limit is the read, not the UDF layer.
2. **Phase telemetry** (single `COUNT` shard): startup ≈ 110 ms, import ≈ **650 ms**, emit
   ≈ 2 ms — overwhelmingly import-bound.
3. **Thread sweep** — added threads only help by overlapping read waits (above).

Memory stayed bounded across the sweep — no OOM, no VM crash.

> **Benchmark caveat:** this run's S3 was in a *different VPC* from the cluster, so the
> ~0.17 GB/s ceiling reflects network distance, not the engine. Co-locating storage would lift
> it — but that's a deployment property of the benchmark, not a knob the engine owns. Many
> deployments legitimately query distant storage, so the durable wins are the engine levers
> below.

Reproduce: `bench/run.sh`, `bench/sweep.sh` ([`bench/README.md`](../bench/README.md)).

## Larger-scale validation (180M-row lineitem, 60 files)

A 2026-07-01 run against the full TPC-H `lineitem` table — **60 Parquet files, 179,998,372
rows** on Glue (`eu-west-1`), SLC lc-rs 0.19.1, `NR_OF_CORES=8`, `PARALLELISM_FACTOR=8` — ~30×
the row count of the 20-file sample above. Not yet formally recorded as a spec benchmark; raw
output in `bench/reports/bench-report-20260701-123648.txt` and
`bench/reports/import-ceiling-20260701-124836.txt`.

| Query | Result | Time |
|---|---|---|
| Q1 (wiring check) | 25 rows | 2.12 s |
| Q2 (3-way join) | 179,998,372 rows joined | 22.17 s |
| Q3 (filter + GROUP BY) | 5 groups | 20.54 s |
| Q4 (lineitem pricing summary) | 4 groups | 28.51 s |

**Native `IMPORT` vs. VS, same 60 files (avg of 3 runs each):**

| Path | Avg time |
|---|---|
| Native `IMPORT` — `COUNT(*)` ceiling over all 60 files | ~28.8 s |
| VS — `SELECT COUNT(*)` (metadata pushdown) | ~1.3 s |
| Native `IMPORT INTO` (full load, 180M rows materialized) | **~80.4 s** |
| VS — `CREATE TABLE AS SELECT *` (full emit, 180M rows) | **~151 s** |

Metadata-only `COUNT(*)` still favors the VS path by ~20× (answered from Iceberg/row-group
stats, no row materialization). But **full materialization flips the earlier finding**: native
`IMPORT INTO` is ~1.9× faster than the VS full-emit CTAS at this scale, unlike the small-scale
run where the VS aggregate path was competitive with native IMPORT.

> **Confound resolved (re-gate, 2026-07-02, post-0.20.1):** the run above recorded
> `CLUSTER_NODES=1` despite `NR_OF_CORES=8` / `PARALLELISM_FACTOR=8` — the pre-0.20.1
> `ctx.node_count()==0` handshake bug (#43). After the 0.20.1 bump landed, the same cluster's
> `adapterNotes` now reports the **real `CLUSTER_NODES=2`**, so at the default
> `PARALLELISM_FACTOR=8` the shard count is `G = 2 × 8 = 16` (was `1 × 8 = 8`). Re-gate outcome:
>
> - **Scan/aggregate path improved.** Q4 (full-`lineitem` pricing summary) dropped from **28.5 s
>   → 20.5 s** (≈ −28 %) purely from the corrected shard count — no knob change.
> - **Full-emit gap persists and is *not* under-sharding.** The exact 60-file / 180M-row
>   materialization re-run could not be reproduced (the shared cluster's 10 GiB raw-size license
>   is exceeded by a single 180M-row `lineitem` table ≈ 24 GiB raw), so the emit path was
>   re-measured at reduced scale (≈ 30–33 M rows, same files, corrected `CLUSTER_NODES=2`):
>   native `IMPORT INTO` **2.07 M rows/s** vs. VS `CREATE TABLE AS SELECT *` **1.19 M rows/s** —
>   a **~1.74×** gap, essentially unchanged from the pre-fix **~1.88×**. Decisively, the VS
>   full-emit throughput (**1.19 M rows/s**) is *identical* to the pre-fix full-scale run
>   (180M / 151 s = 1.19 M rows/s): **doubling `G` (8 → 16) did not move full-emit throughput at
>   all**, so the emit gap is bottlenecked *downstream* of sharding, not by cluster parallelism.
>
> The confound is therefore resolved: the 1.9× emit gap was **not** primarily under-sharding.
> See the emit-path isolation under [Tuning levers & outlook](#tuning-levers--outlook) for why
> it is also **not** primarily the `Int64→Decimal128` coercion, and
> `specs/_recorded/2026-07-02-add-scan-connection-concurrency/decision-log.md` (Design Decision
> [5] + the 2026-07-02 validation addendum) for the full methodology and verdict.

## Tuning levers & outlook

The engine controls these regardless of where storage lives — apply them first:

- **Threading** — `DATAFUSION_THREADING_MODE=FIXED`, threads = partitions = `NR_OF_CORES` for
  read-bound scans (~+39 % vs. the `AUTO` single-thread default).
- **`PARALLELISM_FACTOR`** — more shards oversubscribe the cluster and overlap more reads;
  balances stragglers without raising peak memory.
- **`DATAFUSION_BATCH_SIZE`** — bounds the per-batch decode working set.
- **`MEMORY_POOL_FRACTION`** — pool size vs. the per-instance limit (default 0.6).

See [Tuning](tuning.md) for ranges and defaults.

### Connection-concurrency & shard-shape sweep (2026-07-02, post-0.20.1)

A sweep on the 2-node cluster (`CLUSTER_NODES=2` confirmed in `adapterNotes`), 60-file / 180M-row
`lineitem`, tested the hypothesis that *serial / under-concurrent fetching* — not the engine —
caps throughput, via three levers: (1) `PARALLELISM_FACTOR=1` (one shard per node), (2)
`DATAFUSION_THREADING_MODE=AUTO` (that single instance gets all the node's cores), and (3) a
swept `S3_MAX_CONNECTIONS`. **All three levers were refuted; the shipped default wins.**

| Config (all `CLUSTER_NODES=2`) | shards `G` | Q4 full scan | Q2 join | Q3 grp |
|---|---|---|---|---|
| **Default `PARALLELISM_FACTOR=8`, `S3_MAX_CONNECTIONS` AUTO (=4)** | **16** | **20.5 s** | 18.7 s | 16.0 s |
| `PARALLELISM_FACTOR=8`, `S3_MAX_CONNECTIONS` = 16 / 32 / 64 | 16 | 20.1 / 20.1 / 20.4 s | 17.8 s | 15.6–16.3 s |
| `PARALLELISM_FACTOR=1`, AUTO threads, `S3_MAX_CONNECTIONS` AUTO (=32) | 2 | 63.7 s | 30.2 s | 34.1 s |
| `PARALLELISM_FACTOR=1`, AUTO threads, `S3_MAX_CONNECTIONS` = 64 / 128 | 2 | 63.9 / 64.4 s | 29.8–30.1 s | 33.4–33.7 s |

- **Lever 1 (fewer, bigger shards) — refuted.** One shard per node (`G=2`) is **~3.1× slower** on
  the full-scan Q4 (63.7 s vs. 20.5 s) and ~2× slower on Q2/Q3. Inter-instance oversubscription
  (`G=16`, multiplexed onto each node's core pool) decisively beats a single big instance per node
  — DataFusion's intra-instance threading does not substitute for it.
- **Lever 2 (one AUTO instance/node) — refuted** (coupled with lever 1): AUTO correctly gave the
  single instance 8 threads / 8 partitions, yet the shape was still ~3× worse.
- **Lever 3 (`S3_MAX_CONNECTIONS`) — refuted.** Sweeping it changed Q4 by **< 2 %** at *either*
  shard shape (`PARALLELISM_FACTOR=8`: 20.1–20.4 s across 4→64; `PARALLELISM_FACTOR=1`:
  63.7–64.4 s across 32→128). The object-store HTTP connection pool is not the throughput limiter
  on this deployment. The knob remains a supported, correctly-wired lever (verified end-to-end via
  `adapterNotes`), just not the one that helps here.
- **The real win was the 0.20.1 `CLUSTER_NODES` fix**, not a new knob: correcting the node count
  from the buggy `1` to the real `2` doubled `G` at the default `PARALLELISM_FACTOR=8` (8 → 16),
  which is what cut Q4 from ~28.5 s to ~20.5 s.

**Native-`IMPORT` comparison (same 60 files, 5.40 GB / 180M rows, S3 in `eu-west-1`):** native
`IMPORT FROM PARQUET` `COUNT(*)` (full read) ≈ **30.6 s (0.176 GB/s)**; the VS scan/aggregate path
(Q4, with projection + predicate + Iceberg pruning) is **faster** at 20.5 s; VS metadata
`COUNT(*)` ≈ 1.6 s. The mission's ~1 GB/s target is **not reachable on this cluster** — the S3
read ceiling here is ~0.17 GB/s (storage network distance, a deployment property, consistent with
the earlier different-VPC caveat), not an engine limit. The durable engine wins are the sharding
correctness and pushdown, which already make the *scan* path competitive with (or faster than)
native; only the *full raw-emit* path trails native's bulk loader (~1.74×, see re-gate above).

Reproduce: `bench/sweep.sh` (shard/connection sweep) + `bench/import_ceiling.sh` (native ceiling).

Future engine work (deferred, evidence-gated):

- **I/O-aware `AUTO` threading** — `AUTO` stays the safe CPU/memory-bound default; a future
  variant could detect a read-bound scan and oversubscribe threads automatically (today that's
  the manual `FIXED` lever).
- **Decode-emit overlap buffer** — gate-failed here (emit ≈ 2 ms, nothing to overlap); revisit
  for an emit-bound workload (wide `SELECT *`).
- **Emit-path Arrow cast** — **evaluated 2026-07-02 and NOT pursued.** The `BIGINT`
  (Int64 → Decimal128) coercion measured 50–200× slower than zero-copy types *in a synthetic
  micro-bench*, but a column-isolation experiment on the real `lineitem` full-emit workload
  (33 M rows, four columns per class, corrected `CLUSTER_NODES=2`) does **not** reproduce that:
  the four Int64→Decimal columns emit at **4.63 M rows/s** vs. **5.89 M rows/s** for four
  zero-copy `Decimal128(15,2)` columns and **6.82 M rows/s** for four `Utf8` columns — the
  coercion is the slowest column class but only **~1.27×** slower, contributing **~5–6 %** of
  the full 16-column emit time. Eliminating it would move the native-vs-VS full-emit gap only
  from ~1.74× to ~1.65×. The gap is dominated by general per-row Arrow→`Value` conversion and the
  synchronous `MT_EMIT` request/reply round-trips, **not** the Int64 coercion. Per the project's
  evidence-gated convention (ADR-055), no emit-path coercion code was written — the micro-bench
  figure is not a real-workload bottleneck. Revisit only if a future profile isolates a workload
  where the coercion dominates.
