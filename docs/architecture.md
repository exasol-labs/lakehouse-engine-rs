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
  → resolve Iceberg snapshot + file list ONCE per query
  → partition files into G byte-balanced work-unit shards
  → GROUP BY shard_key fan-out  →  one DataFusion runtime per shard invocation
  → Iceberg / Databricks Parquet on object storage
  → partial results (raw rows, or node-local aggregate)
  → Exasol final processing / merge
  → Result
```

The VS layer resolves metadata **once per query**, never once per node. The VS reads the Iceberg snapshot and file list from the catalog that the Virtual Schema CONNECTION names (see [Catalogs](catalogs.md)). Each UDF invocation gets an explicit file list. This file list is a scan spec that carries the projection and the predicates. A node never discovers files itself. A node never scans the files of another node.

## Sharding rules

```
G = CLUSTER_NODES × PARALLELISM_FACTOR
G = min(G, 300)              # cap at 300: ≤300 ⇒ round-robin (balanced), >300 ⇒ hash-partition (unbalanced)
G = clamp(G, 1, file_count)  # at least 1, never more shards than files
```

- **Oversubscription is deliberate** — a `G` larger than the core count of a node is intended. Extra shards queue on the instance pool of the node. They run when cores become free. This smooths stragglers. The VS assigns files by greedy descending-size byte-balance, not by file count. Each shard therefore carries approximately equal bytes.
- **Cores bound the peak memory, not `G`.** The parallel instances per node come from a fixed VM pool. Exasol sizes this pool to `NR_OF_CORES`. Oversubscription improves the balance, not the peak memory. The engine also stalls new VMs at **80 %** of the per-instance limit. The scan UDF therefore sizes its DataFusion pool to a fraction of that limit (default 0.6).

`PARALLELISM_FACTOR`, the threading mode, and the memory fraction are fixed at `CREATE VIRTUAL SCHEMA` time. A change to one of them requires a new VS. See [Tuning](tuning.md) for the full property list.

### Example — 2 nodes × 2 cores, 10 files

```
10 files                  G = nodes × parallelism_factor = 2 × 3 = 6 shards
f1 … f10                  (cap 300, clamp to ≤ file_count)
   │  byte-balanced split (by size, not count)
   ▼
S0[f1,f2] S1[f3,f4] S2[f5,f6] S3[f7,f8] S4[f9] S5[f10]
   │  GROUP BY shard_key → round-robin across nodes
   ▼
┌──── Node A · 2 cores ────┐   ┌──── Node B · 2 cores ────┐
│ core1: S0   core2: S2    │   │ core1: S1   core2: S3    │   4 shards run at once
│ queued: S4               │   │ queued: S5               │   2 wait → oversubscription
└──────────────────────────┘   └──────────────────────────┘
   │  partial results (rows or partial aggregates)
   ▼
Exasol merges across shards → final result
```

This example puts 6 shards on 4 cores. Each node runs 2 shards immediately and queues 1 shard. The queued shard fills any core that finishes early. No node stays idle while it waits for a slow shard.

## Pushdown vs. parent-level Exasol

**DataFusion does the per-shard work. Exasol coordinates across the shards and does the rest.**

| Done per shard (DataFusion, in the UDF) | Done at parent level (Exasol) |
|---|---|
| Column projection — scan only projected columns | Check returned columns vs. the EMITS list |
| Filter predicates — Iceberg file pruning + row-group/page skipping + full row-level filter | Re-check only the predicates that the VS cannot translate |
| `LIMIT` (no `ORDER BY`) — stop the scan early per shard | Re-apply `LIMIT` as a cross-shard backstop |
| Ordered top-N — bounded per-shard `ORDER BY ... LIMIT n` (a DataFusion `TopK`) when every sort key is a bare projected column, single table, no `GROUP BY` | Merge the `shard_count × n` partial rows with a final `ORDER BY ... LIMIT n` |
| Partial aggregate — node-local `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` over a column or scalar expression (including two-column binary arithmetic, for example `SUM(a * b)`), per-group partials for `GROUP BY`, and single-group `COUNT(DISTINCT)` as a per-shard local distinct set | Merge partials: `SUM` of counts/sums, `MIN`/`MAX` of extrema, `SUM(sum)/SUM(count)` for `AVG`, re-group on the user keys. A scalar UDF merges the `COUNT(DISTINCT)` sets |
| — | `HAVING` final pass, grouped `COUNT(DISTINCT)`/`MEDIAN`/`LISTAGG`, joins across virtual tables, `ORDER BY` that is not an eligible top-N shape (generated explicitly by the adapter itself — Exasol does not re-sort once `ORDER_BY_COLUMN` is advertised) |

Aggregates decompose into a partial step and a merge step. Each node therefore ships one partial row per group instead of raw rows. This minimizes the transfer. Single-group `COUNT(DISTINCT)` also decomposes. Each shard emits its local distinct set as JSON, and a dedicated scalar UDF merges these sets. Grouped `COUNT(DISTINCT)`, `MEDIAN`, and `LISTAGG` stay non-decomposable.

Exasol runs these three functions on the returned rows. The engine does not push joins down. Exasol scans each table independently, then joins the result sets. See [Capabilities](capabilities.md) for the full list. The [benchmark query set](benchmark.md) exercises each of these pushdown paths end to end.
