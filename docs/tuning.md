[lakehouse-engine](../README.md) › [Docs](index.md) › Tuning

---

# Parameters & Telemetry

This page lists every property and shows how to see what a scan does. Set these properties on the `CREATE VIRTUAL SCHEMA` statement from [Install](install.md). Run the [Benchmark](benchmark.md) suite to find the best values for your workload. For the architectural meaning of the values, see [Architecture](architecture.md).

## Parameters

All properties belong to `CREATE VIRTUAL SCHEMA` unless the table says otherwise. Each value is fixed at `CREATE VIRTUAL SCHEMA` time. To change one value, you must recreate the VS.

| Property | Required | Default | Effect |
|---|---|---|---|
| `CATALOG_CONNECTION` | yes | — | Name of the Exasol CONNECTION that holds the catalog URI + credentials JSON. See [Install](install.md). |
| `CATALOG_KIND` | no | absent (Iceberg REST) | Which catalog backend to resolve against: leave it absent for an Iceberg REST catalog, or set `'UNITY_CATALOG'` for a native Unity Catalog. Any other value, including the literal `'ICEBERG_REST'`, is rejected — Iceberg REST is selected only by leaving the property unset. See [Catalogs](catalogs.md). |
| `NAMESPACE` | yes | — | Catalog namespace: dot-delimited Iceberg namespace segments under `ICEBERG_REST`, or `catalog.schema` under `UNITY_CATALOG`. Every table in the namespace becomes a virtual table. |
| `ALLOW_HTTP` | no | `false` | `'true'` permits plain-HTTP catalog/S3 (for example, local MinIO). |
| `NR_OF_CORES` | no | auto-detected (else 0) | Per-node core count. It drives the parallelism factor and the thread budget. Set it only if auto-detection gives a wrong value. |
| `PARALLELISM_FACTOR` | no | `max(NR_OF_CORES × 2, 8)` | Shard oversubscription multiplier. `G = node_count × factor`, capped 300. |
| `DATAFUSION_THREADING_MODE` | no | `AUTO` | `AUTO` derives a non-oversubscribing per-instance thread budget. `FIXED` uses the two properties below verbatim. |
| `DATAFUSION_THREADS_PER_UDF` | no | `max(NR_OF_CORES, 1)` | Tokio worker threads per UDF instance. `FIXED` mode only. `1` = single-threaded. |
| `DATAFUSION_TARGET_PARTITIONS` | no | `max(NR_OF_CORES, 1)` | DataFusion `target_partitions` per instance (`FIXED` mode only). |
| `DATAFUSION_BATCH_SIZE` | no | `8192` | Rows per Arrow `RecordBatch`. It bounds the out-of-pool decode working set. |
| `MEMORY_POOL_FRACTION` | no | `0.6` | Fraction of the per-instance memory limit given to the DataFusion pool. Kept < the engine's 80 % stall threshold. |
| `INSTANCE_OVERHEAD_MB` | no | `200` | Per-instance overhead subtracted from the reported limit before the pool fraction applies. |
| `S3_MAX_CONNECTIONS` | no | `AUTO` | Object-store HTTP connection-pool budget per scan instance. `AUTO` derives the value from the cores and the threading mode. See below. |
| `JOIN_BROADCAST_MAX_BYTES` | no | `134217728` (128 MiB) | Byte-size threshold from each side's total resolved file size (the Iceberg manifest's `file_size_in_bytes` or the Delta `add` action's `size`), with no Parquet read. Below this threshold, the engine broadcasts the smaller side of a two-table inner equi-join into every shard. Above it, the engine falls back to an unaccelerated two-scan join. |
| `LAKEHOUSE_UDF_DEBUG_LEVEL` | no (env var) | `info` | `debug` emits per-scan phase telemetry. `info` is silent. See below. |

Create these three scripts in the same schema as `LAKEHOUSE_ADAPTER`:

- the `LAKEHOUSE_SCAN` scalar EMIT script
- the `LAKEHOUSE_DISTRIBUTE_FILES` distributor
- the distinct-merge script

The adapter qualifies its calls to these scripts from its own running-script schema. It does not read a configured property for them.

**Pool sizing:** `pool = MEMORY_POOL_FRACTION × (memory_limit − INSTANCE_OVERHEAD_MB)`. When the per-instance limit is reported as 0 (unknown), a conservative default budget applies instead.

**Quick recommendation:** For read-bound remote scans, set `DATAFUSION_THREADING_MODE='FIXED'`, `DATAFUSION_THREADS_PER_UDF='<NR_OF_CORES>'`, and `DATAFUSION_TARGET_PARTITIONS='<NR_OF_CORES>'`. These values are ~39 % faster than the `AUTO` default on a full scan.

### `S3_MAX_CONNECTIONS`

This property sizes the HTTP client connection pool of the object store for the scan instance. It sets how many connections to S3 the client keeps warm (idle and reusable) per host.

The property has no hard cap on in-flight request concurrency. It only bounds how many established connections stay open for reuse. Without reuse, the client tears down a connection and negotiates a new one. The property therefore improves connection reuse on a best-effort basis. It is not a guaranteed ceiling on concurrent fetches.

The property says nothing about how many shards run (`PARALLELISM_FACTOR`). It also says nothing about how many CPU threads decode them (`DATAFUSION_THREADING_MODE`). Those two axes stay separate and orthogonal.

- **Explicit value** — a positive integer applies verbatim (FIXED-like), for example `S3_MAX_CONNECTIONS='64'`.
- **Absent, invalid, or `0`** — AUTO derives the budget as `per_instance_threads × 4`. The value `per_instance_threads` is the same thread budget that `DATAFUSION_THREADING_MODE=AUTO` computes (cores available per instance, floored to `≥1`). The `×4` multiplier oversubscribes the IO axis relative to the CPU axis on purpose. S3 fetches are latency-bound, so a decode thread waits on a network round-trip for most of a byte-range GET. Several requests in flight per thread hide that latency and keep the NIC busy (Little's law: fill-the-pipe concurrency ≈ bandwidth × latency). Idle pooled TCP connections are cheap relative to OS threads, so this asymmetry is deliberate.
- **`NR_OF_CORES` unknown (`0`)** — a built-in default of `16` applies.

This property applies to the HTTP client of the object store, not to DataFusion. It does **not** change DataFusion's `target_partitions`. That value stays the job of the threading properties.

**When to change it:** In practice this property moves throughput far less than the threading and parallelism properties above. On one production cluster, a change from `AUTO` to a value well beyond the derived default changed throughput by under 2%. The property is still legitimate to try on a deployment with a different network profile. Such a deployment is connection-churn-bound rather than latency-bound. Do not expect this property to close a gap against a native bulk-load path. Set `PARALLELISM_FACTOR` and the threading mode first.

## Telemetry

The scan UDF can emit one phase-timing line per invocation. This telemetry is **silent by default** and
costs nothing at the production `info` level.

### Enable

```sql
ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<listener-host>:<port>';
```
Then build and deploy the scan script with the `LAKEHOUSE_UDF_DEBUG_LEVEL` environment variable set to `debug`. The script source carries this value as `%udf_debug_level`. The listener must be reachable **from the cluster nodes**. Use a jumphost or a private IP. A NAT'd local client cannot receive the connect-back.

Capture it:
```sh
nc -l -p <port> > telemetry.log      # on the listener host
# run your query, then:
grep LHTELEM telemetry.log
```

### Record format

One line per scan, for example:
```
LHTELEM pid=12345 phase_startup_ms=110.2 phase_import_ms=650.8 phase_emit_ms=2.5 body_ms=763.5
```

| Field | Meaning |
|---|---|
| `pid` | Process id of the shard VM (unique per UDF instance). |
| `phase_startup_ms` | UDF entry → runtime + session + plan build, up to first batch fetch. |
| `phase_import_ms` | Cumulative wait time for batches from the stream (S3 read + Parquet decode). |
| `phase_emit_ms` | Cumulative time in `emit_batch` / flush (column coercion + Arrow IPC). |
| `body_ms` | Total scan-body wall-clock. `startup + import + emit ≈ body`. |

### Interpret

- `import ≫ emit` → **read-bound** (S3 latency). More threads help. Storage closer to the cluster helps most. This is the common case for remote scans.
- `emit ≫ import` → **serialization-bound** (column coercion / IPC). Examine the output types (`BIGINT` coercion is expensive) and the decode-emit overlap control.
- `startup` material relative to `body` → optimize the startup phase. Otherwise the startup phase is negligible.

Telemetry is best-effort: a failed write never fails the scan.
