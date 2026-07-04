[lakehouse-engine](../README.md) › [Docs](index.md) › Architecture

---

# Parallelism, Sharding & Pushdown

How one Exasol `SELECT` becomes parallel lakehouse scans and comes back as one result. See
[Capabilities](capabilities.md) for the pushdown matrix, [Tuning](tuning.md) for the knobs.

## Two levels of parallelism

Both exploited at once:

- **Across nodes (Exasol)** — the file list is split into work-unit shards spread over the
  cluster; each node scans a disjoint file set.
- **Within a node (DataFusion)** — each shard runs in its own disposable DataFusion runtime
  inside a UDF instance, itself multi-threaded.

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

Metadata resolves **once per query in the VS layer**, never per node. Each UDF invocation
gets an explicit file list (a projection- + predicate-carrying scan spec); a node never
discovers files itself and never scans another node's files.

## Sharding rules

```
G = CLUSTER_NODES × PARALLELISM_FACTOR
G = min(G, 300)              # ≤300 ⇒ Exasol distributes round-robin (balanced)
G = clamp(G, 1, file_count)  # >300 ⇒ hash-partition (unbalanced) — so cap at 300
```

- **Oversubscribe on purpose** — `G` > a node's cores is intended. Extra shards queue on the
  node's instance pool and run as cores free up, smoothing stragglers. Files are assigned by
  greedy descending-size byte-balance (not file count), so shards carry ~equal bytes.
- **Peak memory is bounded by cores, not `G`.** Parallel instances per node = a fixed VM pool
  sized to `NR_OF_CORES`; oversubscription improves balancing, not peak memory. The engine
  also stalls new VMs at **80 %** of the per-instance limit, so the scan UDF sizes its
  DataFusion pool to a fraction (default 0.6) of that limit.

`PARALLELISM_FACTOR`, threading mode, and memory fraction resolve once at
`createVirtualSchema` and round-trip via `adapterNotes`; see [Tuning](tuning.md).

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

6 shards over 4 cores: each node runs 2 immediately and queues 1, which fills any core that
finishes early — no node sits idle waiting on a slow shard.

## Pushdown vs. parent-level Exasol

**DataFusion does per-shard work; Exasol coordinates across shards and handles the rest.**

| Done per shard (DataFusion, in the UDF) | Done at parent level (Exasol) |
|---|---|
| Column projection — scan only projected columns | Validate returned columns vs. the EMITS list |
| Filter predicates — Iceberg file pruning + row-group/page skipping + full row-level filter | Re-check only predicates the VS couldn't translate |
| `LIMIT` (no `ORDER BY`) — stop the scan early per shard | Re-apply `LIMIT` as a cross-shard backstop |
| Ordered top-N — bounded per-shard `ORDER BY ... LIMIT n` (a DataFusion `TopK`) when every sort key is a bare projected column, single table, no `GROUP BY` | Merge the `shard_count × n` partial rows with a final `ORDER BY ... LIMIT n` |
| Partial aggregate — node-local `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` over a column or scalar expression (including two-column binary arithmetic, e.g. `SUM(a * b)`), per-group partials for `GROUP BY`, and single-group `COUNT(DISTINCT)` as a per-shard local distinct set | Merge partials: `SUM` of counts/sums, `MIN`/`MAX` of extrema, `SUM(sum)/SUM(count)` for `AVG`, re-group on the user keys; a scalar UDF unions `COUNT(DISTINCT)` sets |
| — | `HAVING` final pass, grouped `COUNT(DISTINCT)`/`MEDIAN`/`LISTAGG`, joins across virtual tables, `ORDER BY` that isn't an eligible top-N shape (rendered explicitly by the adapter itself — Exasol does not re-sort once `ORDER_BY_COLUMN` is advertised) |

Aggregates decompose into partial/merge so each node ships one partial row per group, not raw
rows — minimizing transfer. Single-group `COUNT(DISTINCT)` decomposes too: each shard emits its
local distinct set (JSON-encoded), merged by a dedicated scalar UDF. Grouped `COUNT(DISTINCT)`,
`MEDIAN`, and `LISTAGG` remain non-decomposable and run in Exasol on returned rows. Joins aren't
pushed: Exasol scans each table independently, then
joins the result sets. Full list: [Capabilities](capabilities.md).
