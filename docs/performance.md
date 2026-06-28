[lakehouse-engine](../README.md) › [Docs](index.md) › Performance

---

# Performance

Optimizations, live-cluster numbers, and tuning headroom. Numbers from the last recorded
benchmark (`specs/_recorded/2026-06-27-change-engine-throughput/`).

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
- **Projection + partial-aggregate pushdown** — only projected columns are read; aggregates
  ship one partial row per group instead of raw rows.

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
  zero-copy types; worth optimizing only if a workload proves emit-bound.
