# UDF Concurrency Model: Instances vs. Groups

> Source: conversation with the Exasol engine architect. This is the authoritative
> mental model for how `GROUP BY shard_key` fan-out actually executes on a node.
> Cross-reference the engine internals already cited in `CLAUDE.md`
> ("UDF parallelization & memory model").

## The two concepts

**Instance = a UDF process.** Per UDF call, a node starts up to `#cores` instances
(its fixed per-node VM pool, sized to the node's core count). A node starts as many
instances as it needs, or is allowed, up to that ceiling. This is the unit of
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
then processes group 2, sequentially, on the same process. Two groups, one
instance → serial.

**Add instances (up to `#cores`):** the two groups distribute across instances and
run concurrently. The number of instances the node spins up is independent of the
number of groups; the groups land on whatever instances are available.

## The scalar scan gets one instance per node

The instance pool above applies to SET scripts. The scan runs as the scalar
`LAKEHOUSE_SCAN` over the output of the `LAKEHOUSE_DISTRIBUTE_FILES` SET
distributor, and Exasol runs exactly one scan VM per node for it. That VM scans
the node's shards one after another. A SET UDF over the same `GROUP BY shard_key`
runs on several VMs per node at once.

* Per-node CPU parallelism comes only from the DataFusion threads inside the one
  scan instance, so AUTO sizes threads to the node's cores
  (`scan-runtime/scan-execution-threading`).
* A parallelism factor above 1 adds no concurrency. Each extra shard adds
  per-invocation startup (session build, table registration, credential setup,
  first-byte latency) to the node's serial loop, so the factor defaults to 1.
* Peak scan memory per node is one instance's DataFusion pool.
* `GROUP BY shard_key` still spreads the shards across nodes round-robin for
  G ≤ 300.
* The scalar shape keeps raw-row output streaming instead of materializing it in
  temp DB. A SET-based scan would bring back the instance pool, at that cost.
