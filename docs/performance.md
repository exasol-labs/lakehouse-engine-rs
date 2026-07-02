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
> `specs/_plans/add-scan-connection-concurrency/decision-log.md` (Design Decisions [1] and [5]).

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
`specs/_plans/add-scan-connection-concurrency/decision-log.md` (Design Decisions [2]-[4]) for
the knob's design rationale, and Task 3.1 (benchmark sweep) for the validating measurement.

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

> **Open confound, not yet isolated:** this run's `adapterNotes` recorded `CLUSTER_NODES=1`
> despite explicit `NR_OF_CORES=8` / `PARALLELISM_FACTOR=8` — the pre-0.20.1
> `ctx.node_count()==0` handshake bug (tracked in #43, fixed by the SDK bump in
> `add-scan-connection-concurrency`). If this cluster has more than one node, shard count `G`
> was computed as `1 × 8` instead of `node_count × 8`, starving the full-scan/emit path of
> cluster parallelism — which could fully or partly explain the 151 s vs. 80 s gap rather than
> it being a genuine emit-path bottleneck. **Re-run this exact 60-file benchmark after the
> 0.20.1 bump lands** before concluding anything about the emit path itself. See
> `specs/_plans/add-scan-connection-concurrency/decision-log.md` (Design Decision [5]) and
> plan Task 3.2 for the tracked re-gate.

## Tuning levers & outlook

The engine controls these regardless of where storage lives — apply them first:

- **Threading** — `DATAFUSION_THREADING_MODE=FIXED`, threads = partitions = `NR_OF_CORES` for
  read-bound scans (~+39 % vs. the `AUTO` single-thread default).
- **`PARALLELISM_FACTOR`** — more shards oversubscribe the cluster and overlap more reads;
  balances stragglers without raising peak memory.
- **`DATAFUSION_BATCH_SIZE`** — bounds the per-batch decode working set.
- **`MEMORY_POOL_FRACTION`** — pool size vs. the per-instance limit (default 0.6).

See [Tuning](tuning.md) for ranges and defaults.

Future engine work (deferred, evidence-gated):

- **I/O-aware `AUTO` threading** — `AUTO` stays the safe CPU/memory-bound default; a future
  variant could detect a read-bound scan and oversubscribe threads automatically (today that's
  the manual `FIXED` lever).
- **Decode-emit overlap buffer** — gate-failed here (emit ≈ 2 ms, nothing to overlap); revisit
  for an emit-bound workload (wide `SELECT *`).
- **Emit-path Arrow cast** — `BIGINT` (Int64 → Decimal128) coercion is 50–200× slower than
  zero-copy types; worth optimizing only if a workload proves emit-bound. The 180M-row full-emit
  CTAS above (`SELECT *` = a wide, emit-heavy workload) is the first candidate — but re-run it
  post-0.20.1 first (see caveat above) to rule out under-sharding before attributing the gap to
  the emit path.
