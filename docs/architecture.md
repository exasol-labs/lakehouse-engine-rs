[lakehouse-engine](../README.md) › [Docs](index.md) › Architecture

---

# Parallelism, Sharding & Pushdown

How one Exasol `SELECT` becomes parallel lakehouse scans and comes back as one result. See [Capabilities](capabilities.md) for the pushdown matrix. See [Tuning](tuning.md) for the tuning properties.

## Two levels of parallelism

The engine uses both levels at the same time:

- **Across nodes (Exasol)** — the VS splits the file list into work-unit shards across the cluster. Each node scans a disjoint file set.
- **Within a node (DataFusion)** — each shard runs in its own disposable DataFusion runtime inside a UDF instance. This runtime is multi-threaded.

Total ≈ *nodes × instances/node × threads/instance*.

## The flow

```
User Query
  → Virtual Schema (translate, pushdown analysis, parallelization plan, schema mapping)
  → resolve the table's file list ONCE per query (format reader)
  → partition files into G byte-balanced work-unit shards
  → GROUP BY shard_key fan-out  →  one DataFusion runtime per shard invocation
  → Parquet data files on object storage
  → partial results (raw rows, or node-local aggregate)
  → Exasol final processing / merge
  → Result
```

The VS layer resolves metadata **once per query**, never once per node. The format reader resolves the file list from an Iceberg snapshot, a Delta log, a catalog's Parquet locations, or a directory listing, depending on the virtual schema's `CATALOG_KIND` property (see [Catalogs](catalogs.md)). The scan reads Parquet data files on object storage. Each UDF invocation gets an explicit file list. This file list is a scan spec that carries the projection and the predicates. A node never discovers files itself. A node never scans the files of another node.

## Sharding rules

```
G = node_count × PARALLELISM_FACTOR
G = min(G, 300)              # cap at 300: ≤300 ⇒ round-robin (balanced), >300 ⇒ hash-partition (unbalanced)
G = clamp(G, 1, file_count)  # at least 1, never more shards than files
```

- **One scan instance per node.** The scan is a scalar UDF, so Exasol runs one scan VM per node and feeds it that node's shards one after another. CPU parallelism on a node comes from the DataFusion threads inside that instance, which `AUTO` sizes to the node's cores. A factor above 1 adds per-shard startup without adding concurrency, so `PARALLELISM_FACTOR` defaults to 1.
- **Byte-balanced shards.** The VS assigns files by greedy descending-size byte-balance, not by file count. Each shard therefore carries approximately equal bytes.
- **Memory.** Peak scan memory per node is one instance's DataFusion pool. The engine stalls new VMs at **80 %** of the per-instance limit, so the scan UDF sizes its pool to a fraction of that limit (default 0.6).

`PARALLELISM_FACTOR`, the threading mode, and the memory fraction are set at `CREATE VIRTUAL SCHEMA` time. Change one with `ALTER VIRTUAL SCHEMA ... SET`. See [Tuning](tuning.md) for the full property list.

### Example — 2 nodes × 2 cores, 10 files

```
10 files                  G = nodes × parallelism_factor = 2 × 1 = 2 shards
f1 … f10                  (cap 300, clamp to ≤ file_count)
   │  byte-balanced split (by size, not count)
   ▼
S0[f1,f3,f5,f7,f9]   S1[f2,f4,f6,f8,f10]
   │  GROUP BY shard_key → round-robin across nodes
   ▼
┌──── Node A · 2 cores ────┐   ┌──── Node B · 2 cores ────┐
│ scan VM: S0              │   │ scan VM: S1              │
│ 2 threads, 4 partitions  │   │ 2 threads, 4 partitions  │
└──────────────────────────┘   └──────────────────────────┘
   │  partial results (rows or partial aggregates)
   ▼
Exasol merges across shards → final result
```

## Pushdown vs. parent-level Exasol

**DataFusion does the per-shard work. Exasol coordinates across the shards and does the rest.**

| Done per shard (DataFusion, in the UDF) | Done at parent level (Exasol) |
|---|---|
| Column projection — scan only projected columns | Check returned columns vs. the EMITS list |
| Filter predicates — Iceberg file pruning + row-group/page skipping + full row-level filter | Re-check only the predicates that the VS cannot translate |
| `LIMIT` (no `ORDER BY`) — stop the scan early per shard | Re-apply `LIMIT` as a cross-shard backstop |
| Ordered top-N — bounded per-shard `ORDER BY ... LIMIT n` (a DataFusion `TopK`) when every sort key is a bare projected column, on a single table or a broadcast join, without aggregation | Merge the `shard_count × n` partial rows with a final `ORDER BY ... LIMIT n` |
| Partial aggregate — node-local `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` over a column or scalar expression (including two-column binary arithmetic, for example `SUM(a * b)`), per-group partials for `GROUP BY`, and a lone single-group `COUNT(DISTINCT col)` as each shard's distinct values | Merge partials: `SUM` of counts/sums, `MIN`/`MAX` of extrema, `SUM(sum)/SUM(count)` for `AVG`, re-group on the user keys. Exasol's own `COUNT(DISTINCT)` counts the per-shard distinct values |
| — | `HAVING` final pass, other `COUNT(DISTINCT)` shapes, `MEDIAN`, `LISTAGG`, joins that are not broadcast, `ORDER BY` that is not an eligible top-N shape (generated explicitly by the adapter itself — Exasol does not re-sort once `ORDER_BY_COLUMN` is advertised) |

Aggregates decompose into a partial step and a merge step. Each node therefore ships one partial row per group instead of raw rows. This minimizes the transfer. A lone single-group `COUNT(DISTINCT col)` over a bare column also decomposes. Each shard emits its distinct values, and Exasol's own `COUNT(DISTINCT)` counts them. Other `COUNT(DISTINCT)` shapes, `MEDIAN`, and `LISTAGG` stay non-decomposable.

Exasol runs these three functions on the returned rows. The engine does not push joins down. Exasol scans each table independently, then joins the result sets. See [Capabilities](capabilities.md) for the full list. The [benchmark query set](benchmark.md) exercises each of these pushdown paths end to end.
