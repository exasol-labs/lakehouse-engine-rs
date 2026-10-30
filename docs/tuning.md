[lakehouse-engine](../README.md) › [Docs](index.md) › Tuning

---

# Parameters & Telemetry

This page lists every property and shows how to see what a scan does. Set these properties on the `CREATE VIRTUAL SCHEMA` statement from [Install](install.md), and change them with `ALTER VIRTUAL SCHEMA ... SET`. Run the [Benchmark](benchmark.md) suite to find the best values for your workload. For the architectural meaning of the values, see [Architecture](architecture.md).

## Parameters

`CREATE VIRTUAL SCHEMA`, `ALTER VIRTUAL SCHEMA ... SET`, and `ALTER VIRTUAL SCHEMA ... REFRESH` fail on an unknown property name or an invalid value. One error names each problem and suggests the nearest known name for a close misspelling, for example `unknown property 'NAMESPCE' (did you mean 'NAMESPACE'?)`. Names are case-sensitive. `CREATE VIRTUAL SCHEMA` rejects an empty or `null` value. `ALTER VIRTUAL SCHEMA ... SET <name> = ''` or `SET <name> = null` removes the property, which then takes its default. To change a value, run `ALTER VIRTUAL SCHEMA ... SET`. A `REFRESH` re-reads the properties too.

| Property | Required | Default | Accepted values | Effect |
|---|---|---|---|---|
| `CATALOG_CONNECTION` | yes | none | Name of an Exasol CONNECTION | Holds the catalog URI and the credentials JSON. See [Install](install.md). |
| `CATALOG_KIND` | no | absent (Iceberg REST) | `UNITY_CATALOG`, `GLUE`, `DIRECT_STORAGE`, or absent | Which catalog backend to resolve against. Leave it absent for an Iceberg REST catalog. Any other value, including the literal `'ICEBERG_REST'`, is rejected. See [Catalogs](catalogs.md). |
| `NAMESPACE` | yes, except under `DIRECT_STORAGE` | none | Dot-delimited namespace, or `catalog.schema` under `UNITY_CATALOG`, or, under `DIRECT_STORAGE`, a path relative to the CONNECTION address with no URI scheme and no leading `/` | Catalog namespace. Every table in the namespace becomes a virtual table, unless `TABLE_INCLUDE` or `TABLE_EXCLUDE` hides it. See [Direct storage](catalogs.md#direct-storage-raw-parquet-no-catalog). |
| `ALLOW_HTTP` | no | `false` | `TRUE` or `FALSE`, any letter case | `TRUE` permits plain-HTTP catalog and S3 access (for example, local SeaweedFS). |
| `PARALLELISM_FACTOR` | no | `1` | Whole number, 1 to 255 | Shards per node. `G = node_count × factor`, capped at 300. Exasol scans a node's shards one after another, so a higher factor adds per-shard startup, not concurrency. |
| `DATAFUSION_THREADING_MODE` | no | `AUTO` | `AUTO` or `FIXED`, any letter case | `AUTO` uses the detected core count as threads and twice that as target partitions. `FIXED` uses the two properties below verbatim. |
| `DATAFUSION_THREADS_PER_UDF` | no | detected core count | Whole number, 1 to 65535 | Tokio worker threads per UDF instance. Read in `FIXED` mode only. `1` = single-threaded. |
| `DATAFUSION_TARGET_PARTITIONS` | no | 2 × detected core count | Whole number, 1 to 65535 | DataFusion `target_partitions` per instance. Read in `FIXED` mode only. Partitions beyond threads overlap object-store reads with decoding. |
| `DATAFUSION_BATCH_SIZE` | no | `8192` | Whole number, 1 to 4294967295 | Rows per Arrow `RecordBatch`. It bounds the out-of-pool decode working set. |
| `MEMORY_POOL_FRACTION` | no | `0.6` | Number greater than 0 and at most 1 | Fraction of the per-instance memory limit given to the DataFusion pool. Keep it below the engine's 80 % stall threshold. |
| `INSTANCE_OVERHEAD_MB` | no | `200` | Whole number, 0 to 65535 | Per-instance overhead subtracted from the reported limit before the pool fraction applies. |
| `S3_MAX_CONNECTIONS` | no | absent, which derives 4 × the resolved thread count | Whole number, 1 to 65535 | Object-store HTTP connection-pool budget per scan instance. `AUTO` and `0` fail. See below. |
| `JOIN_BROADCAST_MAX_BYTES` | no | `134217728` (128 MiB) | Whole number, 1 to 18446744073709551615 | Byte-size threshold from each side's total resolved file size, with no Parquet read. The size comes from the Iceberg manifest's `file_size_in_bytes`, the Delta `add` action's `size`, or, for a Parquet table (direct storage, Unity Catalog Parquet, and Glue Hive Parquet), the object size in the storage listing. Below this threshold, the engine broadcasts the smaller side of a two-table inner equi-join into every shard. Above it, the engine falls back to an unaccelerated two-scan join. |
| `MERGE_SCHEMA` | no | `TRUE` | `TRUE` or `FALSE`, any letter case | Direct storage only: folds every file's footer into the declared schema, or samples one. Other catalog kinds accept and ignore it. See [Direct storage](catalogs.md#direct-storage-raw-parquet-no-catalog). |
| `HIVE_PARTITIONING` | no | `TRUE` | `TRUE` or `FALSE`, any letter case | Direct storage only: declares `key=value` directory segments as partition columns. Other catalog kinds accept and ignore it. See [Direct storage](catalogs.md#direct-storage-raw-parquet-no-catalog). |
| `BINARY_AS_STRING` | no | `FALSE` | `TRUE` or `FALSE`, any letter case | `TRUE` reads every top-level variable-length binary column as UTF-8 text instead of lowercase hex, from the next query on. A value that is not valid UTF-8 fails the query with an error that names the column. Fixed-length binary, `uuid`, and nested binary members stay hex. See [Binary columns](catalogs.md#binary-columns). |
| `PERMISSION_CHECK` | no | absent (off) | `LAKEKEEPER`, any letter case | Turns on the Lakekeeper permission check. It needs the Iceberg REST catalog kind and `USER_MAPPING`. See [Lakekeeper permission check](security.md#lakekeeper-permission-check). |
| `USER_MAPPING` | with `PERMISSION_CHECK` | absent | A Jinja template with one variable, `user` | Maps the querying Exasol user to a Lakekeeper principal. Ignored while `PERMISSION_CHECK` is off. See [USER_MAPPING](security.md#user_mapping). |
| `TABLE_INCLUDE` | no | absent (every table) | Comma-separated patterns over the virtual table name. `*` matches any run, `?` matches one character, and an empty entry is rejected. | Exposes only the tables that match a pattern. See [Choosing which tables to expose](catalogs.md#choosing-which-tables-to-expose). |
| `TABLE_EXCLUDE` | no | absent (no table hidden) | Same as `TABLE_INCLUDE` | Hides every table that matches a pattern, including a table that `TABLE_INCLUDE` matches. See [Choosing which tables to expose](catalogs.md#choosing-which-tables-to-expose). |

Create these three scripts in the same schema as `LAKEHOUSE_ADAPTER`:

- the `LAKEHOUSE_SCAN` scalar EMIT script
- the `LAKEHOUSE_DISTRIBUTE_FILES` distributor
- the distinct-merge script

The adapter qualifies its calls to these scripts from its own running-script schema. It does not read a configured property for them.

**Pool sizing:** `pool = MEMORY_POOL_FRACTION × (memory_limit − INSTANCE_OVERHEAD_MB)`. When the per-instance limit is reported as 0 (unknown), a conservative default budget applies instead.

**Tables with deletes:** A scan of a table with positional deletes or deletion vectors holds its deleted row positions in this pool until the row selections are built, and the row selections until the scan finishes. That memory cannot spill. When a shard's delete state does not fit, the query fails with `the row-position delete state does not fit the memory pool`. Raise `PARALLELISM_FACTOR` so each shard holds fewer data files, or raise `MEMORY_POOL_FRACTION`. Reading and decoding the deletes runs on the scan's worker threads, so the thread budget set by `DATAFUSION_THREADING_MODE` also speeds up the delete preparation.

**Quick recommendation:** Keep the defaults. On scan-only queries, `DATAFUSION_TARGET_PARTITIONS` at 4 × cores (`FIXED` mode) can be faster, but it slows joins. Raise `PARALLELISM_FACTOR` to 2 only when file sizes are very uneven, so one node does not become the long pole.

### `S3_MAX_CONNECTIONS`

This property sizes the HTTP client connection pool of the object store for the scan instance. It sets how many connections to S3 the client keeps warm (idle and reusable) per host.

The property has no hard cap on in-flight request concurrency. It only bounds how many established connections stay open for reuse. Without reuse, the client tears down a connection and negotiates a new one. The property therefore improves connection reuse on a best-effort basis. It is not a guaranteed ceiling on concurrent data-file fetches.

One read path does have a hard cap. When a scan instance reads a table with deletes, it first fetches the positional-delete files, the deletion vectors, and the Parquet footers of the data files they apply to. The instance keeps at most this many of those reads in flight at once, across both sides of a join.

The property says nothing about how many shards run (`PARALLELISM_FACTOR`). It also says nothing about how many CPU threads decode them (`DATAFUSION_THREADING_MODE`). Those two axes stay separate and orthogonal.

- **Explicit value** — a whole number from 1 to 65535 applies verbatim (FIXED-like), for example `S3_MAX_CONNECTIONS='64'`. `AUTO`, `0`, and any other value fail the statement.
- **Absent** — AUTO derives the budget as `threads × 4`, where `threads` is the resolved `DATAFUSION_THREADS_PER_UDF` in either threading mode. The `×4` multiplier oversubscribes the IO axis relative to the CPU axis on purpose. S3 fetches are latency-bound, so a decode thread waits on a network round-trip for most of a byte-range GET. Several requests in flight per thread hide that latency and keep the NIC busy (Little's law: fill-the-pipe concurrency ≈ bandwidth × latency). Idle pooled TCP connections are cheap relative to OS threads, so this asymmetry is deliberate.
- **Core-count detection failure** — the detected core count floors to `1`, so the same AUTO formula yields `4`.

This property applies to the HTTP client of the object store, not to DataFusion. It does **not** change DataFusion's `target_partitions`. That value stays the job of the threading properties.

**When to change it:** In practice this property moves throughput far less than the threading and parallelism properties above. On one production cluster, a change from the derived budget to a value well beyond it changed throughput by under 2%. The property is still legitimate to try on a deployment with a different network profile. Such a deployment is connection-churn-bound rather than latency-bound. Do not expect this property to close a gap against a native bulk-load path. Set `PARALLELISM_FACTOR` and the threading mode first.

## Telemetry

The scan UDF can emit one phase-timing line per invocation. This telemetry is **silent by default** and
costs nothing at the production `info` level.

### Enable

```sql
ALTER SESSION SET SCRIPT_OUTPUT_ADDRESS = '<listener-host>:<port>';
```
Then deploy the `LAKEHOUSE_SCAN` script with the `%udf_debug_level debug` directive in its script source. That directive is the switch. The bench variable `LAKEHOUSE_UDF_DEBUG_LEVEL` only writes it into the script source. The listener must be reachable **from the cluster nodes**. Use a jumphost or a private IP. A NAT'd local client cannot receive the connect-back.

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
| `phase_emit_ms` | Cumulative time in `emit_batch` / flush (column coercion + Arrow C Data export). |
| `body_ms` | Total scan-body wall-clock. `startup + import + emit ≈ body`. |

### Interpret

- `import ≫ emit` → **read-bound** (S3 latency). More threads help. Storage closer to the cluster helps most. This is the common case for remote scans.
- `emit ≫ import` → **serialization-bound** (column coercion / IPC). Examine the output types (`BIGINT` coercion is expensive) and the decode-emit overlap control.
- `startup` material relative to `body` → optimize the startup phase. Otherwise the startup phase is negligible.

Telemetry is best-effort: a failed write never fails the scan.
