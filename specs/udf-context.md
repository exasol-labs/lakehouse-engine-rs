# UDF Concurrency Model: Instances vs. Groups

> Source: conversation with the Exasol engine architect. This is the authoritative
> mental model for how `GROUP BY shard_key` fan-out actually executes on a node.
> Cross-reference the engine internals already cited in `CLAUDE.md`
> ("UDF parallelization & memory model").

## The two concepts

**Instance = a UDF process.** Per UDF call, a node starts up to `#cores` instances
(its fixed per-node VM pool, sized to `NR_OF_CORES`). A node starts as many
instances as it needs — or is allowed — up to that ceiling. This is the unit of
*actual* OS-level parallelism.

**Group = a `GROUP BY` partition of the input.** Groups are the unit of *work
assignment*, not of parallelism. The engine schedules groups onto instances.

**Instances and groups are independent.** You can have more groups than instances.
When you do, the groups are distributed across the available instances and each
instance works through its assigned groups one at a time. Fewer groups than
instances just leaves instances idle.

## Worked example

Table `t`:

```
a: 1, 1, 1, 2, 2, 2
b: 1, 2, 3, 5, 6, 7
```

Query:

```sql
SELECT set_udf(a, b) FROM t GROUP BY a
```

`GROUP BY a` produces two groups:

```
group 1:            group 2:
  a: 1, 1, 1          a: 2, 2, 2
  b: 1, 2, 3          b: 5, 6, 7
```

**One node, one instance:** the single instance processes group 1 to completion,
then processes group 2 — sequentially, on the same process. Two groups, one
instance → serial.

**Add instances (up to `#cores`):** the two groups distribute across instances and
run concurrently. The number of instances the node spins up is independent of the
number of groups; the groups land on whatever instances are available.

## Why this is the whole basis for the engine's sharding

This is exactly why the engine fans out on `GROUP BY shard_key` and **not** on
`GROUP BY IPROC()`:

* Each shard is one group carrying one file subset. G shards = G groups.
* Groups multiplex onto each node's instance pool. More groups than a node's cores
  is fine and *desirable* — extra groups queue onto instances as cores free up,
  which smooths out stragglers (uneven file sizes / scan times).
* `GROUP BY IPROC()` would collapse to exactly one group per node → one instance
  busy, the node's other cores idle.

### Consequence for peak memory

Because only `#cores` instances run **concurrently** on a node regardless of how
many groups are queued, peak concurrent DataFusion runtimes on a node is bounded
by the core pool — **not** by G. Oversubscribing G does **not** raise peak memory;
it only improves load balancing. (Memory is bounded by `#cores × per-instance
pool`, with the engine's 80% concurrency throttle on top.)

### Consequence: the real cost of oversubscription

The cost of a larger G is **not** memory — it is **per-invocation fixed overhead**:
each shard group is a separate UDF invocation that stands up its own DataFusion
session, registers the table, and parses its scan spec. Too many tiny shards means
session-startup cost dominates the actual scan. So G has a sweet spot: enough
groups to keep every core busy through stragglers, not so many that startup
overhead swamps the work.
