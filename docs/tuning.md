[lakehouse-engine](../README.md) › [Docs](index.md) › Tuning

---

# Parameters & Telemetry

Every knob, and how to see what a scan is doing. For *which* values to set, see
[Performance](performance.md); for what the values mean architecturally, see
[Architecture](architecture.md).

## Parameters

All are `CREATE VIRTUAL SCHEMA` properties unless noted, resolved once at
`createVirtualSchema` time and round-tripped through `adapterNotes`. Defaults are from
`crates/lakehouse-engine/src/adapter/mod.rs`.

| Property | Required | Default | Effect |
|---|---|---|---|
| `CATALOG_CONNECTION` | yes | — | Name of the Exasol CONNECTION holding the catalog URI + credentials JSON. See [Install](install.md). |
| `ICEBERG_NAMESPACE` | yes | — | Iceberg namespace; every table in it is exposed as a virtual table. |
| `SCAN_SCHEMA` | yes | — | Schema holding the `LAKEHOUSE_SCAN` SET script. |
| `ALLOW_HTTP` | no | `false` | `'true'` permits plain-HTTP catalog/S3 (e.g. local MinIO). |
| `CONNECTION_NAME` | no | — | Connect-back CONNECTION used to capture `NPROC()` (node count). Absent ⇒ node count defaults to 1. |
| `NR_OF_CORES` | no | auto-detected (else 0) | Per-node core count; drives the parallelism factor and thread budget. Override only if auto-detection is wrong. |
| `PARALLELISM_FACTOR` | no | `max(NR_OF_CORES × 2, 8)` | Shard oversubscription multiplier. `G = node_count × factor`, capped 300. |
| `DATAFUSION_THREADING_MODE` | no | `AUTO` | `AUTO` derives a non-oversubscribing per-instance thread budget; `FIXED` uses the two properties below verbatim. |
| `DATAFUSION_THREADS_PER_UDF` | no | `max(NR_OF_CORES, 1)` | Tokio worker threads per UDF instance (`FIXED` mode only; `1` = single-threaded). |
| `DATAFUSION_TARGET_PARTITIONS` | no | `max(NR_OF_CORES, 1)` | DataFusion `target_partitions` per instance (`FIXED` mode only). |
| `DATAFUSION_BATCH_SIZE` | no | `8192` | Rows per Arrow `RecordBatch`; bounds the out-of-pool decode working set. |
| `MEMORY_POOL_FRACTION` | no | `0.6` | Fraction of the per-instance memory limit given to the DataFusion pool. Kept < the engine's 80 % stall threshold. |
| `INSTANCE_OVERHEAD_MB` | no | `200` | Per-instance overhead subtracted from the reported limit before the pool fraction applies. |
| `S3_MAX_CONNECTIONS` | no | `AUTO` | Object-store HTTP connection-pool budget per scan instance. `AUTO` derives from cores/threading; see below. |
| `LAKEHOUSE_UDF_DEBUG_LEVEL` | no (env var) | `info` | `debug` emits per-scan phase telemetry; `info` is silent. See below. |

**Pool sizing:** `pool = MEMORY_POOL_FRACTION × (memory_limit − INSTANCE_OVERHEAD_MB)`. When the
per-instance limit is reported as 0 (unknown), a conservative default budget is used instead.

**Quick recommendation:** for read-bound remote scans, set
`DATAFUSION_THREADING_MODE='FIXED'`, `DATAFUSION_THREADS_PER_UDF='<NR_OF_CORES>'`,
`DATAFUSION_TARGET_PARTITIONS='<NR_OF_CORES>'` — ~39 % faster than the `AUTO` default on a full
scan. See [Performance](performance.md#thread-sweep-nr_of_cores--4).

### `S3_MAX_CONNECTIONS`

Sizes the object store's HTTP client connection pool for the scan instance — how many
concurrent byte-range fetches a single UDF invocation can keep in flight against S3. This is
the IO/network-concurrency axis: it says nothing about how many shards run (`PARALLELISM_FACTOR`)
or how many CPU threads decode them (`DATAFUSION_THREADING_MODE`) — it governs how many requests
are in flight while those threads wait on the network. All three axes are orthogonal and
combine: shard count × per-instance threads × per-instance connections is the full concurrency
picture of a scan.

- **Explicit value** — a positive integer is used verbatim (FIXED-like), e.g.
  `S3_MAX_CONNECTIONS='64'`.
- **Absent, invalid, or `0`** — AUTO-derives the budget as
  `per_instance_threads × 4`, where `per_instance_threads` is the same AUTO thread budget
  `auto_threads_per_udf` computes for `DATAFUSION_THREADING_MODE=AUTO`
  (`nr_of_cores / udf_instances_per_node`, floored to `≥1`). The `×4` multiplier
  oversubscribes the IO axis relative to the CPU axis on purpose: S3 fetches are
  latency-bound, so a decode thread spends most of a byte-range GET waiting on a network
  round-trip — keeping several requests in flight per thread hides that latency and keeps the
  NIC busy (Little's law: fill-the-pipe concurrency ≈ bandwidth × latency). Idle pooled TCP
  connections are cheap relative to OS threads, so this asymmetry is deliberate.
- **`NR_OF_CORES` unknown (`0`)** — falls back to a built-in default of `16`, mirroring the
  `0`-cores fallback used elsewhere in the adapter.

Applied via the object store's HTTP client, not DataFusion: `AmazonS3Builder::with_client_options`
sets `ClientOptions::with_pool_max_idle_per_host(budget)` on the S3 client. It does **not**
touch DataFusion's `target_partitions` — that remains the threading knob's job.

**When to tune it:** if [telemetry](#interpret) shows a scan is import-bound (`import ≫ emit`)
and threads are already maxed per [Performance](performance.md#thread-sweep-nr_of_cores--4),
raising `S3_MAX_CONNECTIONS` lets more byte-range fetches overlap per instance — the next lever
to pull toward native `IMPORT FROM PARQUET` throughput on IO-bound scans. See
[Performance](performance.md#native-import-parity-goal) for the goal this knob targets.

## Telemetry

The scan UDF can emit one phase-timing line per invocation. It is **silent by default** and
costs nothing at the production `info` level.

### Enable

```sql
ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<listener-host>:<port>';
```
and build/deploy the scan script with `LAKEHOUSE_UDF_DEBUG_LEVEL=debug` (the bench harness
wires this via `%udf_debug_level`). The listener must be reachable **from the cluster nodes**
(a jumphost/private IP — a NAT'd local client cannot receive the connect-back).

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

- `import ≫ emit` → **read-bound** (S3 latency). More threads help; moving storage closer
  helps most. This is the common case for remote scans.
- `emit ≫ import` → **serialization-bound** (column coercion / IPC). Look at output types
  (`BIGINT` coercion is expensive) and the decode-emit overlap lever.
- `startup` material relative to `body` → startup is worth attacking; otherwise negligible.

Telemetry is best-effort: a failed write never fails the scan.
