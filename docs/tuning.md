[lakehouse-engine](../README.md) › [Docs](index.md) › Tuning

---

# Parameters & Telemetry

Every knob, and how to see what a scan is doing. Set these properties on the `CREATE VIRTUAL SCHEMA` statement from [Install](install.md). Run the [Benchmark](benchmark.md) suite to find which values work best for your workload; for what the values mean architecturally, see [Architecture](architecture.md).

## Parameters

All are `CREATE VIRTUAL SCHEMA` properties unless noted, fixed at `CREATE VIRTUAL SCHEMA` time — changing one means recreating the VS.

| Property | Required | Default | Effect |
|---|---|---|---|
| `CATALOG_CONNECTION` | yes | — | Name of the Exasol CONNECTION holding the catalog URI + credentials JSON. See [Install](install.md). |
| `ICEBERG_NAMESPACE` | yes | — | Iceberg namespace; every table in it is exposed as a virtual table. |
| `ALLOW_HTTP` | no | `false` | `'true'` permits plain-HTTP catalog/S3 (e.g. local MinIO). |
| `NR_OF_CORES` | no | auto-detected (else 0) | Per-node core count; drives the parallelism factor and thread budget. Override only if auto-detection is wrong. |
| `PARALLELISM_FACTOR` | no | `max(NR_OF_CORES × 2, 8)` | Shard oversubscription multiplier. `G = node_count × factor`, capped 300. |
| `DATAFUSION_THREADING_MODE` | no | `AUTO` | `AUTO` derives a non-oversubscribing per-instance thread budget; `FIXED` uses the two properties below verbatim. |
| `DATAFUSION_THREADS_PER_UDF` | no | `max(NR_OF_CORES, 1)` | Tokio worker threads per UDF instance (`FIXED` mode only; `1` = single-threaded). |
| `DATAFUSION_TARGET_PARTITIONS` | no | `max(NR_OF_CORES, 1)` | DataFusion `target_partitions` per instance (`FIXED` mode only). |
| `DATAFUSION_BATCH_SIZE` | no | `8192` | Rows per Arrow `RecordBatch`; bounds the out-of-pool decode working set. |
| `MEMORY_POOL_FRACTION` | no | `0.6` | Fraction of the per-instance memory limit given to the DataFusion pool. Kept < the engine's 80 % stall threshold. |
| `INSTANCE_OVERHEAD_MB` | no | `200` | Per-instance overhead subtracted from the reported limit before the pool fraction applies. |
| `S3_MAX_CONNECTIONS` | no | `AUTO` | Object-store HTTP connection-pool budget per scan instance. `AUTO` derives from cores/threading; see below. |
| `JOIN_BROADCAST_MAX_BYTES` | no | `134217728` (128 MiB) | Byte-size threshold (from Iceberg manifest sizes, no Parquet read) below which a two-table inner equi-join's smaller side is broadcast into every shard; above it, falls back to an unaccelerated two-scan join. |
| `LAKEHOUSE_UDF_DEBUG_LEVEL` | no (env var) | `info` | `debug` emits per-scan phase telemetry; `info` is silent. See below. |

The `LAKEHOUSE_SCAN` scalar EMIT script, the `LAKEHOUSE_DISTRIBUTE_FILES` distributor, and the distinct-merge script MUST be created in the same schema as `LAKEHOUSE_ADAPTER` — the adapter qualifies its calls to them from its own running-script schema, not a configured property.

**Pool sizing:** `pool = MEMORY_POOL_FRACTION × (memory_limit − INSTANCE_OVERHEAD_MB)`. When the per-instance limit is reported as 0 (unknown), a conservative default budget is used instead.

**Quick recommendation:** for read-bound remote scans, set `DATAFUSION_THREADING_MODE='FIXED'`, `DATAFUSION_THREADS_PER_UDF='<NR_OF_CORES>'`, `DATAFUSION_TARGET_PARTITIONS='<NR_OF_CORES>'` — ~39 % faster than the `AUTO` default on a full scan.

### `S3_MAX_CONNECTIONS`

Sizes the object store's HTTP client connection pool for the scan instance — how many connections to S3 the client keeps warm (idle, reusable) per host. This knob has no hard cap on in-flight request concurrency; it only bounds how many established connections stay open for reuse rather than being torn down and re-negotiated, so it is a best-effort lever on connection reuse, not a guaranteed ceiling on concurrent fetches. It says nothing about how many shards run (`PARALLELISM_FACTOR`) or how many CPU threads decode them (`DATAFUSION_THREADING_MODE`) — those remain separate, orthogonal axes.

- **Explicit value** — a positive integer is used verbatim (FIXED-like), e.g. `S3_MAX_CONNECTIONS='64'`.
- **Absent, invalid, or `0`** — AUTO-derives the budget as `per_instance_threads × 4`, where `per_instance_threads` is the same thread budget `DATAFUSION_THREADING_MODE=AUTO` computes (cores available per instance, floored to `≥1`). The `×4` multiplier oversubscribes the IO axis relative to the CPU axis on purpose: S3 fetches are latency-bound, so a decode thread spends most of a byte-range GET waiting on a network round-trip — keeping several requests in flight per thread hides that latency and keeps the NIC busy (Little's law: fill-the-pipe concurrency ≈ bandwidth × latency). Idle pooled TCP connections are cheap relative to OS threads, so this asymmetry is deliberate.
- **`NR_OF_CORES` unknown (`0`)** — falls back to a built-in default of `16`.

This applies to the object store's HTTP client, not DataFusion — it does **not** touch DataFusion's `target_partitions`, which remains the threading knob's job.

**When to tune it:** in practice this knob moves throughput far less than the threading and parallelism knobs above — on one production cluster, raising it from `AUTO` to well beyond the derived default changed throughput by under 2%. It remains a legitimate knob to try on a deployment with a different network profile (genuinely connection-churn-bound rather than latency-bound), but don't expect it to be the lever that closes a gap against a native bulk-load path — reach for `PARALLELISM_FACTOR` and the threading mode first.

## Telemetry

The scan UDF can emit one phase-timing line per invocation. It is **silent by default** and
costs nothing at the production `info` level.

### Enable

```sql
ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<listener-host>:<port>';
```
and build/deploy the scan script with the `LAKEHOUSE_UDF_DEBUG_LEVEL` environment variable set to `debug` (wired into the script source as `%udf_debug_level`). The listener must be reachable **from the cluster nodes** (a jumphost/private IP — a NAT'd local client cannot receive the connect-back).

Capture it:
```sh
nc -l -p <port> > telemetry.log      # on the listener host
# run your query, then:
grep LHTELEM telemetry.log
```

### Record format

One line per scan, e.g.:
```
LHTELEM pid=12345 phase_startup_ms=110.2 phase_import_ms=650.8 phase_emit_ms=2.5 body_ms=763.5
```

| Field | Meaning |
|---|---|
| `pid` | Process id of the shard VM (unique per UDF instance). |
| `phase_startup_ms` | UDF entry → runtime + session + plan build, up to first batch fetch. |
| `phase_import_ms` | Cumulative time awaiting batches from the stream (S3 read + Parquet decode). |
| `phase_emit_ms` | Cumulative time in `emit_batch` / flush (column coercion + Arrow IPC). |
| `body_ms` | Total scan-body wall-clock; `startup + import + emit ≈ body`. |

### Interpret

- `import ≫ emit` → **read-bound** (S3 latency). More threads help; moving storage closer helps most. This is the common case for remote scans.
- `emit ≫ import` → **serialization-bound** (column coercion / IPC). Look at output types (`BIGINT` coercion is expensive) and the decode-emit overlap lever.
- `startup` material relative to `body` → startup is worth attacking; otherwise negligible.

Telemetry is best-effort: a failed write never fails the scan.
